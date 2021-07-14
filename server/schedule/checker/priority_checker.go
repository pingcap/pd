// Copyright 2021 TiKV Project Authors.
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

package checker

import (
	"time"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/opt"
)

const DefaultPriorityQueueSize = 1280 //// the default value of priority queue size

// PriorityChecker ensures high priority region should run first
type PriorityChecker struct {
	cluster opt.Cluster
	opts    *config.PersistOptions
	queue   *cache.PriorityQueue
}

// NewPriorityChecker creates a priority checker.
func NewPriorityChecker(cluster opt.Cluster) *PriorityChecker {
	opts := cluster.GetOpts()
	return &PriorityChecker{
		cluster: cluster,
		opts:    opts,
		queue:   cache.NewPriorityQueue(DefaultPriorityQueueSize),
	}
}

// GetType returns PriorityChecker's type
func (p *PriorityChecker) GetType() string {
	return "priority-checker"
}

// RegionPriorityEntry records region priority info
type RegionPriorityEntry struct {
	Retry    int
	Last     time.Time
	regionID uint64
}

// ID implement PriorityQueueItem interface
func (r RegionPriorityEntry) ID() uint64 {
	return r.regionID
}

// NewRegionEntry construct of region priority entry
func NewRegionEntry(regionID uint64) *RegionPriorityEntry {
	return &RegionPriorityEntry{regionID: regionID, Last: time.Now(), Retry: 1}
}

// Check check region's replicas, it will put into priority queue if the region lack of replicas.
func (p *PriorityChecker) Check(region *core.RegionInfo) {
	makeupCount := 0
	if p.opts.IsPlacementRulesEnabled() {
		makeupCount = p.checkRegionInPlacementRule(region)
	} else {
		makeupCount = p.checkRegionInReplica(region)
	}
	priority := 0 - makeupCount
	p.addPriorityQueue(priority, region.GetID())
}

// checkRegionInPlacementRule check region in placement rule mode
func (p *PriorityChecker) checkRegionInPlacementRule(region *core.RegionInfo) (makeupCount int) {
	fit := p.cluster.FitRegion(region)
	if len(fit.RuleFits) == 0 {
		return
	}
	for _, rf := range fit.RuleFits {
		makeupCount = makeupCount + rf.Rule.Count - len(rf.Peers)
	}
	return
}

// checkReplicas check region in replica mode
func (p *PriorityChecker) checkRegionInReplica(region *core.RegionInfo) (makeupCount int) {
	return p.opts.GetMaxReplicas() - len(region.GetPeers())
}

// addPriorityQueue add region into queue
// it will remove if region's priority equal 0
// it's retry will increase if region's priority equal last
func (p *PriorityChecker) addPriorityQueue(priority int, regionID uint64) {
	if priority < 0 {
		if entry := p.queue.Get(regionID); entry != nil && entry.Priority == priority {
			e := entry.Value.(*RegionPriorityEntry)
			e.Retry = e.Retry + 1
			e.Last = time.Now()
		}
		entry := NewRegionEntry(regionID)
		p.queue.Put(priority, entry)
	} else {
		p.queue.Remove(regionID)
	}
}

// GetPriorityRegions returns all regions in priority queue that needs rerun
func (p *PriorityChecker) GetPriorityRegions() (ids []uint64) {
	entries := p.queue.Elems()
	for _, e := range entries {
		re := e.Value.(*RegionPriorityEntry)
		if t := re.Last.Add(time.Duration(re.Retry*10) * p.opts.GetPatrolRegionInterval()); t.Before(time.Now()) {
			ids = append(ids, re.regionID)
		}
	}
	return
}

// RemovePriorityRegion removes priority region from priority queue
func (p *PriorityChecker) RemovePriorityRegion(regionID uint64) {
	p.queue.Remove(regionID)
}
