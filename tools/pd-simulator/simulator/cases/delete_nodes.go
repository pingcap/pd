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
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/tools/pd-simulator/simulator/simutil"
)

func newDeleteNodes() *Case {
	var simCase Case

	for i := 1; i <= 8; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:        IDAllocator.nextID(),
			Status:    metapb.StoreState_Up,
			Capacity:  1 * TB,
			Available: 900 * GB,
			Version:   "2.1.0",
		})
	}

	for i := 0; i < 1000; i++ {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(i)%8 + 1},
			{Id: IDAllocator.nextID(), StoreId: uint64(i+1)%8 + 1},
			{Id: IDAllocator.nextID(), StoreId: uint64(i+2)%8 + 1},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * MB,
			Keys:   960000,
		})
	}

	var ids []uint64
	for _, store := range simCase.Stores {
		ids = append(ids, store.ID)
	}

	numNodes := 8
	e := &DeleteNodesDescriptor{}
	e.Step = func(tick int64) uint64 {
		if numNodes > 7 && tick%100 == 0 {
			idx := rand.Intn(numNodes)
			numNodes--
			nodeID := ids[idx]
			ids = append(ids[:idx], ids[idx+1:]...)
			return nodeID
		}
		return 0
	}
	simCase.Events = []EventDescriptor{e}

	simCase.Checker = func(regions *core.RegionsInfo) bool {
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
	return &simCase
}
