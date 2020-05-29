// Copyright 2016 PingCAP, Inc.
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

package core

import (
	"math"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// SplitRegions split a set of metapb.Region by the middle of regionKey
func SplitRegions(regions []*metapb.Region) []*metapb.Region {
	results := make([]*metapb.Region, 0, len(regions)*2)
	for _, region := range regions {
		start, end := byte(0), byte(math.MaxUint8)
		if len(region.StartKey) > 0 {
			start = region.StartKey[0]
		}
		if len(region.EndKey) > 0 {
			end = region.EndKey[0]
		}
		middle := []byte{start/2 + end/2}
		left := proto.Clone(region).(*metapb.Region)
		left.Id = region.Id + uint64(len(regions))
		left.EndKey = middle
		left.RegionEpoch.Version++
		right := proto.Clone(region).(*metapb.Region)
		right.Id = region.Id + uint64(len(regions)*2)
		right.StartKey = middle
		right.RegionEpoch.Version++
		results = append(results, left, right)
	}
	return results
}

// MergeRegions merge a set of metapb.Region by regionKey
func MergeRegions(regions []*metapb.Region) []*metapb.Region {
	results := make([]*metapb.Region, 0, len(regions)/2)
	for i := 0; i < len(regions); i += 2 {
		left := regions[i]
		right := regions[i]
		if i+1 < len(regions) {
			right = regions[i+1]
		}
		region := &metapb.Region{
			Id:       left.Id + uint64(len(regions)),
			StartKey: left.StartKey,
			EndKey:   right.EndKey,
		}
		if left.RegionEpoch.Version > right.RegionEpoch.Version {
			region.RegionEpoch = left.RegionEpoch
		} else {
			region.RegionEpoch = right.RegionEpoch
		}
		region.RegionEpoch.Version++
		results = append(results, region)
	}
	return results
}

// NewRegion create a metapb.Region
func NewRegion(start, end []byte) *metapb.Region {
	return &metapb.Region{
		StartKey:    start,
		EndKey:      end,
		RegionEpoch: &metapb.RegionEpoch{},
	}
}

// NewStoreInfoWithSizeCount is create a store with size and count.
func NewStoreInfoWithSizeCount(id uint64, regionCount, leaderCount int, regionSize, leaderSize int64) *StoreInfo {
	stats := &pdpb.StoreStats{}
	stats.Capacity = uint64(1024)
	stats.Available = uint64(1024)
	store := NewStoreInfo(
		&metapb.Store{
			Id: id,
		},
		SetStoreStats(stats),
		SetRegionCount(regionCount),
		SetRegionSize(regionSize),
		SetLeaderCount(leaderCount),
		SetLeaderSize(leaderSize),
	)
	return store
}
