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
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

type evictLeaderScheduler struct {
	opt      schedule.Options
	name     string
	storeID  uint64
	selector schedule.Selector
}

// NewEvictLeaderScheduler creates an admin scheduler that transfers all leaders
// out of a store.
func NewEvictLeaderScheduler(opt schedule.Options, storeID uint64) schedule.Scheduler {
	filters := []schedule.Filter{
		schedule.NewStateFilter(opt),
		schedule.NewHealthFilter(opt),
	}

	return &evictLeaderScheduler{
		opt:      opt,
		name:     fmt.Sprintf("evict-leader-scheduler-%d", storeID),
		storeID:  storeID,
		selector: schedule.NewRandomSelector(filters),
	}
}

func (s *evictLeaderScheduler) GetName() string {
	return s.name
}

func (s *evictLeaderScheduler) GetResourceKind() core.ResourceKind {
	return core.LeaderKind
}

func (s *evictLeaderScheduler) GetResourceLimit() uint64 {
	return s.opt.GetLeaderScheduleLimit()
}

func (s *evictLeaderScheduler) Prepare(cluster schedule.Cluster) error {
	return errors.Trace(cluster.BlockStore(s.storeID))
}

func (s *evictLeaderScheduler) Cleanup(cluster schedule.Cluster) {
	cluster.UnblockStore(s.storeID)
}

func (s *evictLeaderScheduler) Schedule(cluster schedule.Cluster) schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region := cluster.RandLeaderRegion(s.storeID)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_leader").Inc()
		return nil
	}
	target := s.selector.SelectTarget(cluster.GetFollowerStores(region))
	if target == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_target_store").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	return schedule.CreateTransferLeaderOperator(region, region.GetStorePeer(target.GetId()))
}
