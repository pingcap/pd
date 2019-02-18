// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package simulator

import (
	"context"
	"math/rand"
	"sort"
	"sync"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/tools/pd-simulator/simulator/cases"
	"github.com/pingcap/pd/tools/pd-simulator/simulator/simutil"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// RaftEngine records all raft infomations.
type RaftEngine struct {
	sync.RWMutex
	regionsInfo     *core.RegionsInfo
	conn            *Connection
	regionChange    map[uint64][]uint64
	schedulerStats  *schedulerStatistics
	regionSplitSize int64
	regionSplitKeys int64
	storeConfig     *SimConfig
}

// NewRaftEngine creates the initialized raft with the configuration.
func NewRaftEngine(conf *cases.Case, conn *Connection, storeConfig *SimConfig) *RaftEngine {
	r := &RaftEngine{
		regionsInfo:     core.NewRegionsInfo(),
		conn:            conn,
		regionChange:    make(map[uint64][]uint64),
		schedulerStats:  newSchedulerStatistics(),
		regionSplitSize: conf.RegionSplitSize,
		regionSplitKeys: conf.RegionSplitKeys,
		storeConfig:     storeConfig,
	}

	splitKeys := generateKeys(len(conf.Regions) - 1)
	for i, region := range conf.Regions {
		meta := &metapb.Region{
			Id:          region.ID,
			Peers:       region.Peers,
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		}
		if i > 0 {
			meta.StartKey = []byte(splitKeys[i-1])
		}
		if i < len(conf.Regions)-1 {
			meta.EndKey = []byte(splitKeys[i])
		}
		regionInfo := core.NewRegionInfo(
			meta,
			region.Leader,
			core.SetApproximateSize(region.Size),
			core.SetApproximateKeys(region.Keys),
		)
		r.SetRegion(regionInfo)
		peers := region.Peers
		regionSize := uint64(region.Size)
		for _, peer := range peers {
			r.conn.Nodes[peer.StoreId].incUsedSize(regionSize)
		}
	}

	for _, node := range conn.Nodes {
		node.raftEngine = r
	}

	return r
}

func (r *RaftEngine) stepRegions() {
	regions := r.GetRegions()
	for _, region := range regions {
		r.stepLeader(region)
		r.stepSplit(region)
	}
}

func (r *RaftEngine) stepLeader(region *core.RegionInfo) {
	if region.GetLeader() != nil && r.conn.nodeHealth(region.GetLeader().GetStoreId()) {
		return
	}
	newLeader := r.electNewLeader(region)
	newRegion := region.Clone(core.WithLeader(newLeader))
	if newLeader == nil {
		r.SetRegion(newRegion)
		simutil.Logger.Info("region has no leader", zap.Uint64("region-id", region.GetID()))
		return
	}
	simutil.Logger.Info("region elects a new leader",
		zap.Uint64("region-id", region.GetID()),
		zap.Reflect("new-leader", newLeader),
		zap.Reflect("old-leader", region.GetLeader()))
	r.SetRegion(newRegion)
	r.recordRegionChange(newRegion)
}

func (r *RaftEngine) stepSplit(region *core.RegionInfo) {
	if region.GetLeader() == nil {
		return
	}
	if !r.NeedSplit(region.GetApproximateSize(), region.GetApproximateKeys()) {
		return
	}
	ids := make([]uint64, 1+len(region.GetPeers()))
	for i := range ids {
		var err error
		ids[i], err = r.allocID(region.GetLeader().GetStoreId())
		if err != nil {
			simutil.Logger.Error("alloc id failed", zap.Error(err))
			return
		}
	}

	splitKey := generateSplitKey(region.GetStartKey(), region.GetEndKey())
	left := region.Clone(
		core.WithNewRegionID(ids[len(ids)-1]),
		core.WithNewPeerIds(ids[0:len(ids)-1]...),
		core.WithIncVersion(),
		core.SetApproximateKeys(region.GetApproximateKeys()/2),
		core.SetApproximateSize(region.GetApproximateSize()/2),
		core.WithPendingPeers(nil),
		core.WithDownPeers(nil),
		core.WithEndKey(splitKey),
	)
	right := region.Clone(
		core.WithIncVersion(),
		core.SetApproximateKeys(region.GetApproximateKeys()/2),
		core.SetApproximateSize(region.GetApproximateSize()/2),
		core.WithStartKey(splitKey),
	)

	r.SetRegion(right)
	r.SetRegion(left)
	simutil.Logger.Debug("region split",
		zap.Uint64("region-id", region.GetID()),
		zap.Reflect("origin", region.GetMeta()),
		zap.Reflect("left", left.GetMeta()),
		zap.Reflect("right", right.GetMeta()))
	r.recordRegionChange(left)
	r.recordRegionChange(right)
}

// NeedSplit checks whether the region needs to split according its size
// and number of keys.
func (r *RaftEngine) NeedSplit(size, rows int64) bool {
	if r.regionSplitSize != 0 && size >= r.regionSplitSize {
		return true
	}
	if r.regionSplitKeys != 0 && rows >= r.regionSplitKeys {
		return true
	}
	return false
}

func (r *RaftEngine) recordRegionChange(region *core.RegionInfo) {
	n := region.GetLeader().GetStoreId()
	r.regionChange[n] = append(r.regionChange[n], region.GetID())
}

func (r *RaftEngine) updateRegionStore(region *core.RegionInfo, size int64) {
	newRegion := region.Clone(
		core.SetApproximateSize(region.GetApproximateSize()+size),
		core.SetWrittenBytes(uint64(size)),
	)
	storeIDs := region.GetStoreIds()
	for storeID := range storeIDs {
		r.conn.Nodes[storeID].incUsedSize(uint64(size))
	}
	r.SetRegion(newRegion)
}

func (r *RaftEngine) updateRegionReadBytes(readBytes map[uint64]int64) {
	for id, bytes := range readBytes {
		region := r.GetRegion(id)
		if region == nil {
			simutil.Logger.Error("region is not found", zap.Uint64("region-id", id))
			continue
		}
		newRegion := region.Clone(core.SetReadBytes(uint64(bytes)))
		r.SetRegion(newRegion)
	}
}

func (r *RaftEngine) electNewLeader(region *core.RegionInfo) *metapb.Peer {
	var (
		unhealth         int
		newLeaderStoreID uint64
	)
	ids := region.GetStoreIds()
	for id := range ids {
		if r.conn.nodeHealth(id) {
			newLeaderStoreID = id
		} else {
			unhealth++
		}
	}
	if unhealth > len(ids)/2 {
		return nil
	}
	for _, peer := range region.GetPeers() {
		if peer.GetStoreId() == newLeaderStoreID {
			return peer
		}
	}
	return nil
}

// GetRegion returns the RegionInfo with regionID
func (r *RaftEngine) GetRegion(regionID uint64) *core.RegionInfo {
	r.RLock()
	defer r.RUnlock()
	return r.regionsInfo.GetRegion(regionID)
}

// GetRegions gets all RegionInfo from regionMap
func (r *RaftEngine) GetRegions() []*core.RegionInfo {
	r.RLock()
	defer r.RUnlock()
	return r.regionsInfo.GetRegions()
}

// SetRegion sets the RegionInfo with regionID
func (r *RaftEngine) SetRegion(region *core.RegionInfo) []*metapb.Region {
	r.Lock()
	defer r.Unlock()
	return r.regionsInfo.SetRegion(region)
}

// SearchRegion searches the RegionInfo from regionTree
func (r *RaftEngine) SearchRegion(regionKey []byte) *core.RegionInfo {
	r.RLock()
	defer r.RUnlock()
	return r.regionsInfo.SearchRegion(regionKey)
}

// RandRegion gets a region by random
func (r *RaftEngine) RandRegion() *core.RegionInfo {
	r.RLock()
	defer r.RUnlock()
	return r.regionsInfo.RandRegion()
}

func (r *RaftEngine) allocID(storeID uint64) (uint64, error) {
	node, ok := r.conn.Nodes[storeID]
	if !ok {
		return 0, errors.Errorf("node %d not found", storeID)
	}
	id, err := node.client.AllocID(context.Background())
	return id, errors.WithStack(err)
}

const (
	// 26^10 ~= 1.4e+14, should be enough.
	keyChars = "abcdefghijklmnopqrstuvwxyz"
	keyLen   = 10
)

// generate ordered, unique strings.
func generateKeys(size int) []string {
	m := make(map[string]struct{}, size)
	for len(m) < size {
		k := make([]byte, keyLen)
		for i := range k {
			k[i] = keyChars[rand.Intn(len(keyChars))]
		}
		m[string(k)] = struct{}{}
	}

	v := make([]string, 0, size)
	for k := range m {
		v = append(v, k)
	}
	sort.Strings(v)
	return v
}

func generateSplitKey(start, end []byte) []byte {
	var key []byte
	// lessThanEnd is set as true when the key is already less than end key.
	lessThanEnd := len(end) == 0
	for i, s := range start {
		e := byte('z')
		if !lessThanEnd {
			e = end[i]
		}
		c := (s + e) / 2
		key = append(key, c)
		// case1: s = c < e. Continue with lessThanEnd=true.
		// case2: s < c < e. return key.
		// case3: s = c = e. Continue with lessThanEnd=false.
		lessThanEnd = c < e
		if c > s && c < e {
			return key
		}
	}
	key = append(key, ('a'+'z')/2)
	return key
}
