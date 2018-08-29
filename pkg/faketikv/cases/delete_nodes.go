// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// //     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cases

import (
	"math/rand"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pkg/faketikv/simutil"
	"github.com/pingcap/pd/server/core"
)

func newDeleteNodes() *Conf {
	var conf Conf
	var id idAllocator

	for i := 1; i <= 8; i++ {
		conf.Stores = append(conf.Stores, &Store{
			ID:        id.nextID(),
			Status:    metapb.StoreState_Up,
			Capacity:  1 * TB,
			Available: 900 * GB,
			Version:   "2.1.0",
		})
	}

	for i := 0; i < 1000; i++ {
		peers := []*metapb.Peer{
			{Id: id.nextID(), StoreId: uint64(i)%8 + 1},
			{Id: id.nextID(), StoreId: uint64(i+1)%8 + 1},
			{Id: id.nextID(), StoreId: uint64(i+2)%8 + 1},
		}
		conf.Regions = append(conf.Regions, Region{
			ID:     id.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * MB,
			Keys:   960000,
		})
	}
	conf.MaxID = id.maxID

	var ids []uint64
	for _, store := range conf.Stores {
		ids = append(ids, store.ID)
	}

	numNodes := 8
	e := &DeleteNodesInner{}
	e.Step = func(tick int64) uint64 {
		if tick%100 == 0 && numNodes > 7 {
			idx := rand.Intn(numNodes)
			numNodes--
			nodeID := ids[idx]
			ids = append(ids[:idx], ids[idx+1:]...)
			return nodeID
		}
		return 0
	}
	conf.Events = []EventInner{e}

	conf.Checker = func(regions *core.RegionsInfo) bool {
		res := true
		leaderCounts := make([]int, 0, numNodes)
		regionCounts := make([]int, 0, numNodes)
		for _, i := range ids {
			leaderCount := regions.GetStoreLeaderCount(i)
			regionCount := regions.GetStoreRegionCount(i)
			leaderCounts = append(leaderCounts, leaderCount)
			regionCounts = append(regionCounts, regionCount)
			if leaderCount > 152 || leaderCount < 132 {
				res = false
			}
			if regionCount > 443 || regionCount < 413 {
				res = false
			}
		}

		simutil.Logger.Infof("leader counts: %v", leaderCounts)
		simutil.Logger.Infof("region counts: %v", regionCounts)
		return res
	}
	return &conf
}
