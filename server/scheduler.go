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

package server

func scheduleLeader(cluster *clusterInfo, s Selector) (*regionInfo, *storeInfo, *storeInfo) {
	sourceStores := cluster.getStores()

	source := s.SelectSource(sourceStores)
	if source == nil {
		return nil, nil, nil
	}

	region := cluster.randLeaderRegion(source.GetId())
	if region == nil {
		return nil, nil, nil
	}

	followers := region.GetFollowers()
	targetStores := make([]*storeInfo, 0, len(followers))
	for id := range followers {
		if store := cluster.getStore(id); store != nil {
			targetStores = append(targetStores, store)
		}
	}

	target := s.SelectTarget(targetStores)
	if target == nil {
		return nil, nil, nil
	}

	return region, source, target
}
