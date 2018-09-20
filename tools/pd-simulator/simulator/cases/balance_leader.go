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

package cases

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/tools/pd-simulator/simulator/simutil"
)

func newBalanceLeader() *Case {
	var simCase Case
	var id idAllocator

	for i := 1; i <= 3; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:        id.nextID(),
			Status:    metapb.StoreState_Up,
			Capacity:  1 * TB,
			Available: 900 * GB,
			Version:   "2.1.0",
		})
	}

	for i := 0; i < 1000; i++ {
		peers := []*metapb.Peer{
			{Id: id.nextID(), StoreId: 1},
			{Id: id.nextID(), StoreId: 2},
			{Id: id.nextID(), StoreId: 3},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     id.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * MB,
			Keys:   960000,
		})
	}
	simCase.MaxID = id.maxID
	simCase.Checker = func(regions *core.RegionsInfo) bool {
		count1 := regions.GetStoreLeaderCount(1)
		count2 := regions.GetStoreLeaderCount(2)
		count3 := regions.GetStoreLeaderCount(3)
		simutil.Logger.Infof("leader counts: %v %v %v", count1, count2, count3)

		return count1 <= 350 &&
			count2 >= 300 &&
			count3 >= 300
	}
	return &simCase
}
