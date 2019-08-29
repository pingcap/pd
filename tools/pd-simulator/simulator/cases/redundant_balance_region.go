// Copyright 2019 PingCAP, Inc.
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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/tools/pd-simulator/simulator/dto"
	"github.com/pingcap/pd/tools/pd-simulator/simulator/simutil"
	"time"
)

func newRedundantBalanceRegion() *Case {
	var simCase Case

	storeNum := simutil.CaseConfigure.StoreNum
	regionNum := simutil.CaseConfigure.RegionNum * storeNum / 3
	if storeNum == 0 || regionNum == 0 {
		storeNum, regionNum = 6, 4000
	}

	for i := 0; i < storeNum; i++ {
		if i%2 == 1 {
			simCase.Stores = append(simCase.Stores, &Store{
				ID:        IDAllocator.nextID(),
				Status:    metapb.StoreState_Up,
				Capacity:  1 * TB,
				Available: 980 * GB,
				Version:   "2.1.0",
			})
		} else {
			simCase.Stores = append(simCase.Stores, &Store{
				ID:        IDAllocator.nextID(),
				Status:    metapb.StoreState_Up,
				Capacity:  1 * TB,
				Available: 1 * TB,
				Version:   "2.1.0",
			})
		}
	}

	for i := 0; i < regionNum; i++ {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(i%storeNum + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+1)%storeNum + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+2)%storeNum + 1)},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * MB,
			Keys:   960000,
		})
	}

	simCase.Checker = func(regions *core.RegionsInfo, stats []dto.StoreStats) bool {
		res := true
		curTime := time.Now().Unix()
		for i := 0; i < storeNum; i++ {
			sliceStats := stats[i]
			simCase.Stores[i].Available = sliceStats.GetAvailable()
			if curTime-simCase.Stores[i].LastUpdateTime > 60 {
				if simCase.Stores[i].LastAvailable != simCase.Stores[i].Available {
					res = false
				}
				if sliceStats.ToCompactionSize != 0 {
					res = false
				}
				simCase.Stores[i].LastUpdateTime = curTime
				simCase.Stores[i].LastAvailable = simCase.Stores[i].Available
			} else {
				res = false
			}
		}
		return res
	}
	return &simCase
}
