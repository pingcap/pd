// Copyright 2017 PingCAP, Inc.
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

package schedulers

import (
	"math"
	"math/rand"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("hot-region", func(opt schedule.Options, args []string) (schedule.Scheduler, error) {
		return newBalanceHotRegionsScheduler(opt), nil
	})
	// FIXME: remove this two schedule after the balance test move in schedulers package
	schedule.RegisterScheduler("hot-write-region", func(opt schedule.Options, args []string) (schedule.Scheduler, error) {
		return newBalanceHotWriteRegionsScheduler(opt), nil
	})
	schedule.RegisterScheduler("hot-read-region", func(opt schedule.Options, args []string) (schedule.Scheduler, error) {
		return newBalanceHotReadRegionsScheduler(opt), nil
	})
}

const (
	hotRegionLimitFactor      = 0.75
	storeHotRegionsDefaultLen = 100
	hotRegionScheduleFactor   = 0.9
)

// BalanceType : the perspective of balance
type BalanceType int

const (
	hotWriteRegionBalance BalanceType = iota
	hotReadRegionBalance
)

type storeStatistics struct {
	readStatAsLeader  core.StoreHotRegionsStat
	writeStatAsPeer   core.StoreHotRegionsStat
	writeStatAsLeader core.StoreHotRegionsStat
}

func newStoreStaticstics() *storeStatistics {
	return &storeStatistics{
		readStatAsLeader:  make(core.StoreHotRegionsStat),
		writeStatAsLeader: make(core.StoreHotRegionsStat),
		writeStatAsPeer:   make(core.StoreHotRegionsStat),
	}
}

type balanceHotRegionsScheduler struct {
	sync.RWMutex
	opt   schedule.Options
	limit uint64
	types []BalanceType

	// store id -> hot regions statistics as the role of leader
	stats *storeStatistics
	r     *rand.Rand
}

func newBalanceHotRegionsScheduler(opt schedule.Options) *balanceHotRegionsScheduler {
	return &balanceHotRegionsScheduler{
		opt:   opt,
		limit: 1,
		stats: newStoreStaticstics(),
		types: []BalanceType{hotWriteRegionBalance, hotReadRegionBalance},
		r:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func newBalanceHotReadRegionsScheduler(opt schedule.Options) *balanceHotRegionsScheduler {
	return &balanceHotRegionsScheduler{
		opt:   opt,
		limit: 1,
		stats: newStoreStaticstics(),
		types: []BalanceType{hotReadRegionBalance},
		r:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func newBalanceHotWriteRegionsScheduler(opt schedule.Options) *balanceHotRegionsScheduler {
	return &balanceHotRegionsScheduler{
		opt:   opt,
		limit: 1,
		stats: newStoreStaticstics(),
		types: []BalanceType{hotWriteRegionBalance},
		r:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (h *balanceHotRegionsScheduler) GetName() string {
	return "balance-hot-region-scheduler"
}

func (h *balanceHotRegionsScheduler) GetInterval() time.Duration {
	return schedule.MinSlowScheduleInterval
}

func (h *balanceHotRegionsScheduler) GetResourceKind() core.ResourceKind {
	return core.PriorityKind
}

func (h *balanceHotRegionsScheduler) GetResourceLimit() uint64 {
	return h.limit
}

func (h *balanceHotRegionsScheduler) Prepare(cluster schedule.Cluster) error { return nil }

func (h *balanceHotRegionsScheduler) Cleanup(cluster schedule.Cluster) {}

func (h *balanceHotRegionsScheduler) Schedule(cluster schedule.Cluster) *schedule.Operator {
	schedulerCounter.WithLabelValues(h.GetName(), "schedule").Inc()
	return h.dispatch(h.types[h.r.Int()%len(h.types)], cluster)
}

func (h *balanceHotRegionsScheduler) dispatch(typ BalanceType, cluster schedule.Cluster) *schedule.Operator {
	h.Lock()
	defer h.Unlock()
	switch typ {
	case hotReadRegionBalance:
		h.stats.readStatAsLeader = h.calcScore(cluster.RegionReadStats(), cluster, false)
		return h.balanceHotReadRegions(cluster)
	case hotWriteRegionBalance:
		h.stats.writeStatAsLeader = h.calcScore(cluster.RegionWriteStats(), cluster, false)
		h.stats.writeStatAsPeer = h.calcScore(cluster.RegionWriteStats(), cluster, true)
		return h.balanceHotWriteRegions(cluster)
	}
	return nil
}

func (h *balanceHotRegionsScheduler) balanceHotReadRegions(cluster schedule.Cluster) *schedule.Operator {
	// balance by leader
	srcRegion, newLeader := h.balanceByLeader(cluster, h.stats.readStatAsLeader)
	if srcRegion != nil {
		schedulerCounter.WithLabelValues(h.GetName(), "move_leader").Inc()
		step := schedule.TransferLeader{FromStore: srcRegion.Leader.GetStoreId(), ToStore: newLeader.GetStoreId()}
		return schedule.NewOperator("transferHotReadLeader", srcRegion.GetId(), core.PriorityKind, step)
	}

	// balance by peer
	srcRegion, srcPeer, destPeer := h.balanceByPeer(cluster, h.stats.readStatAsLeader)
	if srcRegion != nil {
		schedulerCounter.WithLabelValues(h.GetName(), "move_peer").Inc()
		return schedule.CreateMovePeerOperator("moveHotReadRegion", srcRegion, core.PriorityKind, srcPeer.GetStoreId(), destPeer.GetStoreId(), destPeer.GetId())
	}
	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

func (h *balanceHotRegionsScheduler) balanceHotWriteRegions(cluster schedule.Cluster) *schedule.Operator {
	// balance by peer
	srcRegion, srcPeer, destPeer := h.balanceByPeer(cluster, h.stats.writeStatAsPeer)
	if srcRegion != nil {
		schedulerCounter.WithLabelValues(h.GetName(), "move_peer").Inc()
		return schedule.CreateMovePeerOperator("moveHotWriteRegion", srcRegion, core.PriorityKind, srcPeer.GetStoreId(), destPeer.GetStoreId(), destPeer.GetId())
	}

	// balance by leader
	srcRegion, newLeader := h.balanceByLeader(cluster, h.stats.writeStatAsLeader)
	if srcRegion != nil {
		schedulerCounter.WithLabelValues(h.GetName(), "move_leader").Inc()
		step := schedule.TransferLeader{FromStore: srcRegion.Leader.GetStoreId(), ToStore: newLeader.GetStoreId()}
		return schedule.NewOperator("transferHotWriteLeader", srcRegion.GetId(), core.PriorityKind, step)
	}

	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

func (h *balanceHotRegionsScheduler) calcScore(items []*core.RegionStat, cluster schedule.Cluster, isCountReplica bool) core.StoreHotRegionsStat {
	stats := make(core.StoreHotRegionsStat)
	for _, r := range items {
		if r.HotDegree < h.opt.GetHotRegionLowThreshold() {
			continue
		}

		regionInfo := cluster.GetRegion(r.RegionID)

		var storeIDs []uint64
		if isCountReplica {
			for id := range regionInfo.GetStoreIds() {
				storeIDs = append(storeIDs, id)
			}
		} else {
			storeIDs = append(storeIDs, regionInfo.Leader.GetStoreId())
		}

		for _, storeID := range storeIDs {
			storeStat, ok := stats[storeID]
			if !ok {
				storeStat = &core.HotRegionsStat{
					RegionsStat: make(core.RegionsStat, 0, storeHotRegionsDefaultLen),
				}
				stats[storeID] = storeStat
			}

			s := core.RegionStat{
				RegionID:       r.RegionID,
				FlowBytes:      r.FlowBytes,
				HotDegree:      r.HotDegree,
				LastUpdateTime: r.LastUpdateTime,
				StoreID:        storeID,
				AntiCount:      r.AntiCount,
				Version:        r.Version,
			}
			storeStat.TotalFlowBytes += r.FlowBytes
			storeStat.RegionsCount++
			storeStat.RegionsStat = append(storeStat.RegionsStat, s)
		}
	}
	return stats
}

func (h *balanceHotRegionsScheduler) balanceByPeer(cluster schedule.Cluster, storesStat core.StoreHotRegionsStat) (*core.RegionInfo, *metapb.Peer, *metapb.Peer) {
	var (
		maxReadBytes           uint64
		srcStoreID             uint64
		maxHotStoreRegionCount int
	)

	// get the srcStoreId
	for storeID, statistics := range storesStat {
		count, readBytes := statistics.RegionsStat.Len(), statistics.TotalFlowBytes
		if count >= 2 && (count > maxHotStoreRegionCount || (count == maxHotStoreRegionCount && readBytes > maxReadBytes)) {
			maxHotStoreRegionCount = count
			maxReadBytes = readBytes
			srcStoreID = storeID
		}
	}
	if srcStoreID == 0 {
		return nil, nil, nil
	}

	stores := cluster.GetStores()
	var destStoreID uint64
	for _, i := range h.r.Perm(storesStat[srcStoreID].RegionsStat.Len()) {
		rs := storesStat[srcStoreID].RegionsStat[i]
		srcRegion := cluster.GetRegion(rs.RegionID)
		if len(srcRegion.DownPeers) != 0 || len(srcRegion.PendingPeers) != 0 {
			continue
		}

		filters := []schedule.Filter{
			schedule.NewExcludedFilter(srcRegion.GetStoreIds(), srcRegion.GetStoreIds()),
			schedule.NewDistinctScoreFilter(h.opt.GetLocationLabels(), stores, cluster.GetLeaderStore(srcRegion)),
			schedule.NewStateFilter(h.opt),
			schedule.NewStorageThresholdFilter(h.opt),
		}
		destStoreIDs := make([]uint64, 0, len(stores))
		for _, store := range stores {
			if schedule.FilterTarget(store, filters) {
				continue
			}
			destStoreIDs = append(destStoreIDs, store.GetId())
		}

		destStoreID = h.selectDestStoreByPeer(destStoreIDs, srcRegion, srcStoreID, storesStat)
		if destStoreID != 0 {
			srcRegion.ReadBytes = rs.FlowBytes
			h.adjustBalanceLimit(srcStoreID, storesStat)

			var srcPeer *metapb.Peer
			for _, peer := range srcRegion.GetPeers() {
				if peer.GetStoreId() == srcStoreID {
					srcPeer = peer
					break
				}
			}

			if srcPeer == nil {
				return nil, nil, nil
			}

			destPeer, err := cluster.AllocPeer(destStoreID)
			if err != nil {
				log.Errorf("failed to allocate peer: %v", err)
				return nil, nil, nil
			}

			return srcRegion, srcPeer, destPeer
		}
	}

	return nil, nil, nil
}

func (h *balanceHotRegionsScheduler) selectDestStoreByPeer(candidateStoreIDs []uint64, srcRegion *core.RegionInfo, srcStoreID uint64, storesStat core.StoreHotRegionsStat) uint64 {
	sr := storesStat[srcStoreID]
	srcReadBytes := sr.TotalFlowBytes
	srcHotRegionsCount := sr.RegionsStat.Len()

	var (
		destStoreID  uint64
		minReadBytes uint64 = math.MaxUint64
	)
	minRegionsCount := int(math.MaxInt32)
	for _, storeID := range candidateStoreIDs {
		if s, ok := storesStat[storeID]; ok {
			if srcHotRegionsCount-s.RegionsStat.Len() > 1 && minRegionsCount > s.RegionsStat.Len() {
				destStoreID = storeID
				minReadBytes = s.TotalFlowBytes
				minRegionsCount = s.RegionsStat.Len()
				continue
			}
			if minRegionsCount == s.RegionsStat.Len() && minReadBytes > s.TotalFlowBytes &&
				uint64(float64(srcReadBytes)*hotRegionScheduleFactor) > s.TotalFlowBytes+2*srcRegion.ReadBytes {
				minReadBytes = s.TotalFlowBytes
				destStoreID = storeID
			}
		} else {
			destStoreID = storeID
			break
		}
	}
	return destStoreID
}

func (h *balanceHotRegionsScheduler) balanceByLeader(cluster schedule.Cluster, storesStat core.StoreHotRegionsStat) (*core.RegionInfo, *metapb.Peer) {
	var (
		maxReadBytes           uint64
		srcStoreID             uint64
		maxHotStoreRegionCount int
	)

	// select srcStoreId by leader
	for storeID, statistics := range storesStat {
		if statistics.RegionsStat.Len() < 2 {
			continue
		}

		if maxHotStoreRegionCount < statistics.RegionsStat.Len() {
			maxHotStoreRegionCount = statistics.RegionsStat.Len()
			maxReadBytes = statistics.TotalFlowBytes
			srcStoreID = storeID
			continue
		}

		if maxHotStoreRegionCount == statistics.RegionsStat.Len() && maxReadBytes < statistics.TotalFlowBytes {
			maxReadBytes = statistics.TotalFlowBytes
			srcStoreID = storeID
		}
	}
	if srcStoreID == 0 {
		return nil, nil
	}

	// select destPeer
	for _, i := range h.r.Perm(storesStat[srcStoreID].RegionsStat.Len()) {
		rs := storesStat[srcStoreID].RegionsStat[i]
		srcRegion := cluster.GetRegion(rs.RegionID)
		if len(srcRegion.DownPeers) != 0 || len(srcRegion.PendingPeers) != 0 {
			continue
		}

		destPeer := h.selectDestStoreByLeader(srcRegion, storesStat)
		if destPeer != nil {
			h.adjustBalanceLimit(srcStoreID, storesStat)
			return srcRegion, destPeer
		}
	}
	return nil, nil
}

func (h *balanceHotRegionsScheduler) selectDestStoreByLeader(srcRegion *core.RegionInfo, storesStat core.StoreHotRegionsStat) *metapb.Peer {
	sr := storesStat[srcRegion.Leader.GetStoreId()]
	srcReadBytes := sr.TotalFlowBytes
	srcHotRegionsCount := sr.RegionsStat.Len()

	var (
		destPeer     *metapb.Peer
		minReadBytes uint64 = math.MaxUint64
	)
	minRegionsCount := int(math.MaxInt32)
	for storeID, peer := range srcRegion.GetFollowers() {
		if s, ok := storesStat[storeID]; ok {
			if srcHotRegionsCount-s.RegionsStat.Len() > 1 && minRegionsCount > s.RegionsStat.Len() {
				destPeer = peer
				minReadBytes = s.TotalFlowBytes
				minRegionsCount = s.RegionsStat.Len()
				continue
			}
			if minRegionsCount == s.RegionsStat.Len() && minReadBytes > s.TotalFlowBytes &&
				uint64(float64(srcReadBytes)*hotRegionScheduleFactor) > s.TotalFlowBytes+2*srcRegion.ReadBytes {
				minReadBytes = s.TotalFlowBytes
				destPeer = peer
			}
		} else {
			destPeer = peer
			break
		}
	}
	return destPeer
}

func (h *balanceHotRegionsScheduler) adjustBalanceLimit(storeID uint64, storesStat core.StoreHotRegionsStat) {
	srcStoreStatistics := storesStat[storeID]

	var hotRegionTotalCount float64
	for _, m := range storesStat {
		hotRegionTotalCount += float64(m.RegionsStat.Len())
	}

	avgRegionCount := hotRegionTotalCount / float64(len(storesStat))
	// Multiplied by hotRegionLimitFactor to avoid transfer back and forth
	limit := uint64((float64(srcStoreStatistics.RegionsStat.Len()) - avgRegionCount) * hotRegionLimitFactor)
	h.limit = maxUint64(1, limit)
}

func (h *balanceHotRegionsScheduler) GetHotReadStatus() *core.StoreHotRegionInfos {
	h.RLock()
	defer h.RUnlock()
	asLeader := make(core.StoreHotRegionsStat, len(h.stats.readStatAsLeader))
	for id, stat := range h.stats.readStatAsLeader {
		clone := *stat
		asLeader[id] = &clone
	}
	return &core.StoreHotRegionInfos{
		AsLeader: asLeader,
	}
}

func (h *balanceHotRegionsScheduler) GetHotWriteStatus() *core.StoreHotRegionInfos {
	h.RLock()
	defer h.RUnlock()
	asLeader := make(core.StoreHotRegionsStat, len(h.stats.writeStatAsLeader))
	asPeer := make(core.StoreHotRegionsStat, len(h.stats.writeStatAsPeer))
	for id, stat := range h.stats.writeStatAsLeader {
		clone := *stat
		asLeader[id] = &clone
	}
	for id, stat := range h.stats.writeStatAsPeer {
		clone := *stat
		asPeer[id] = &clone
	}
	return &core.StoreHotRegionInfos{
		AsLeader: asLeader,
		AsPeer:   asPeer,
	}
}
