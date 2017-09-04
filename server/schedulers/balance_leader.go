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
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

type balanceLeaderScheduler struct {
	opt      schedule.Options
	limit    uint64
	selector schedule.Selector
}

// NewBalanceLeaderScheduler creates a scheduler that tends to keep leaders on
// each store balanced.
func NewBalanceLeaderScheduler(opt schedule.Options) schedule.Scheduler {
	filters := []schedule.Filter{
		schedule.NewBlockFilter(),
		schedule.NewStateFilter(opt),
		schedule.NewHealthFilter(opt),
	}
	return &balanceLeaderScheduler{
		opt:      opt,
		limit:    1,
		selector: schedule.NewBalanceSelector(core.LeaderKind, filters),
	}
}

func (l *balanceLeaderScheduler) GetName() string {
	return "balance-leader-scheduler"
}

func (l *balanceLeaderScheduler) GetResourceKind() core.ResourceKind {
	return core.LeaderKind
}

func (l *balanceLeaderScheduler) GetResourceLimit() uint64 {
	return minUint64(l.limit, l.opt.GetLeaderScheduleLimit())
}

func (l *balanceLeaderScheduler) Prepare(cluster schedule.Cluster) error { return nil }

func (l *balanceLeaderScheduler) Cleanup(cluster schedule.Cluster) {}

func (l *balanceLeaderScheduler) Schedule(cluster schedule.Cluster) schedule.Operator {
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()
	region, newLeader := scheduleTransferLeader(cluster, l.GetName(), l.selector)
	if region == nil {
		return nil
	}

	// Skip hot regions.
	if cluster.IsRegionHot(region.GetId()) {
		schedulerCounter.WithLabelValues(l.GetName(), "region_hot").Inc()
		return nil
	}

	source := cluster.GetStore(region.Leader.GetStoreId())
	target := cluster.GetStore(newLeader.GetStoreId())
	if !shouldBalance(source, target, l.GetResourceKind()) {
		schedulerCounter.WithLabelValues(l.GetName(), "skip").Inc()
		return nil
	}
	l.limit = adjustBalanceLimit(cluster, l.GetResourceKind())
	schedulerCounter.WithLabelValues(l.GetName(), "new_opeartor").Inc()
	return schedule.CreateTransferLeaderOperator(region, newLeader)
}
