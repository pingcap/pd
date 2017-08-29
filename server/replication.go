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

import (
	"math"
	"sync/atomic"
)

const replicaBaseScore = 100

// Replication provides some help to do replication.
type Replication struct {
	replicateCfg atomic.Value
}

func newReplication(cfg *ReplicationConfig) *Replication {
	r := &Replication{}
	r.store(cfg)
	return r
}

func (r *Replication) load() *ReplicationConfig {
	return r.replicateCfg.Load().(*ReplicationConfig)
}

func (r *Replication) store(cfg *ReplicationConfig) {
	r.replicateCfg.Store(cfg)
}

// GetMaxReplicas returns the number of replicas for each region.
func (r *Replication) GetMaxReplicas() int {
	return int(r.load().MaxReplicas)
}

// SetMaxReplicas set the replicas for each region.
func (r *Replication) SetMaxReplicas(replicas int) {
	c := r.load()
	v := c.clone()
	v.MaxReplicas = uint64(replicas)
	r.store(v)
}

// GetLocationLabels returns the location labels for each region
func (r *Replication) GetLocationLabels() []string {
	return r.load().LocationLabels
}

// GetDistinctScore returns the score that the other is distinct from the stores.
// A higher score means the other store is more different from the existed stores.
func (r *Replication) GetDistinctScore(stores []*StoreInfo, other *StoreInfo) float64 {
	score := float64(0)
	locationLabels := r.GetLocationLabels()

	for _, s := range stores {
		if s.GetId() == other.GetId() {
			continue
		}
		if index := s.compareLocation(other, locationLabels); index != -1 {
			score += math.Pow(replicaBaseScore, float64(len(locationLabels)-index-1))
		}
	}
	return score
}

// compareStoreScore compares which store is better for replication.
// Returns 0 if store A is as good as store B.
// Returns 1 if store A is better than store B.
// Returns -1 if store B is better than store A.
func compareStoreScore(storeA *StoreInfo, scoreA float64, storeB *StoreInfo, scoreB float64) int {
	// The store with higher score is better.
	if scoreA > scoreB {
		return 1
	}
	if scoreA < scoreB {
		return -1
	}
	// The store with lower region score is better.
	if storeA.RegionScore() < storeB.RegionScore() {
		return 1
	}
	if storeA.RegionScore() > storeB.RegionScore() {
		return -1
	}
	return 0
}
