// Copyright 2019 PingCAP, Inc.
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

package opt

import "github.com/pingcap/pd/server/core"

// IsRegionHealthy checks if a region is healthy for scheduling. It requires the
// region does not have any down or pending peers, and does not have any learner
// peers when placement rules is disabled.
func IsRegionHealthy(cluster Cluster, region *core.RegionInfo) bool {
	return IsRegionHealthyAllowPending(cluster, region) && len(region.GetPendingPeers()) == 0
}

// IsRegionHealthyAllowPending checks if a region is healthy for scheduling.
// Differs from IsRegionHealthy, it allows the region to have pending peers.
func IsRegionHealthyAllowPending(cluster Cluster, region *core.RegionInfo) bool {
	if !cluster.IsPlacementRulesEnabled() && len(region.GetLearners()) > 0 {
		return false
	}
	return len(region.GetDownPeers()) == 0
}

// HealthRegion returns a function that checks if a region is healthy for
// scheduling. It requires the region does not have any down or pending peers,
// and does not have any learner peers when placement rules is disabled.
func HealthRegion(cluster Cluster) func(*core.RegionInfo) bool {
	return func(region *core.RegionInfo) bool { return IsRegionHealthy(cluster, region) }
}

// HealthRegionAllowPending returns a function that checks if a region is
// healthy for scheduling. Differs from HealthRegion, it allows the region
// to have pending peers.
func HealthRegionAllowPending(cluster Cluster) func(*core.RegionInfo) bool {
	return func(region *core.RegionInfo) bool { return IsRegionHealthyAllowPending(cluster, region) }
}

// IsRegionReplicated checks if a region is fully replicated. When placement
// rules is enabled, its peers should fit corresponding rules. When placement
// rules is disabled, it should have enough replicas and no any learner peer.
func IsRegionReplicated(cluster Cluster, region *core.RegionInfo) bool {
	if cluster.IsPlacementRulesEnabled() {
		return cluster.FitRegion(region).IsSatisfied()
	}
	return len(region.GetLearners()) == 0 && len(region.GetPeers()) == cluster.GetMaxReplicas()
}

// ReplicatedRegion returns a function that checks if a region is fully replicated.
func ReplicatedRegion(cluster Cluster) func(*core.RegionInfo) bool {
	return func(region *core.RegionInfo) bool { return IsRegionReplicated(cluster, region) }
}
