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

func init() {
	schedule.RegisterScheduler("shuffle-leader", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		return newShuffleLeaderScheduler(opController), nil
	})
}

const shuffleLeaderName = "shuffle-leader-scheduler"

type shuffleLeaderScheduler struct {
	name string
	*baseScheduler
	selector *schedule.RandomSelector
}

// newShuffleLeaderScheduler creates an admin scheduler that shuffles leaders
// between stores.
func newShuffleLeaderScheduler(opController *schedule.OperatorController) schedule.Scheduler {
	filters := []schedule.Filter{
		schedule.StoreStateFilter{ActionScope: shuffleLeaderName, TransferLeader: true},
	}
	base := newBaseScheduler(opController)
	return &shuffleLeaderScheduler{
		name:          shuffleLeaderName,
		baseScheduler: base,
		selector:      schedule.NewRandomSelector(filters),
	}
}

func (s *shuffleLeaderScheduler) GetName() string {
	return s.name
}

func (s *shuffleLeaderScheduler) GetType() string {
	return "shuffle-leader"
}

func (s *shuffleLeaderScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.opController.OperatorCount(schedule.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *shuffleLeaderScheduler) Schedule(cluster schedule.Cluster) []*schedule.Operator {
	// We shuffle leaders between stores by:
	// 1. random select a valid store.
	// 2. transfer a leader to the store.
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	stores := cluster.GetStores()
	targetStore := s.selector.SelectTarget(cluster, stores)
	if targetStore == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_target_store").Inc()
		return nil
	}
	region := cluster.RandFollowerRegion(targetStore.GetID(), core.HealthRegion())
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_follower").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	op := schedule.CreateTransferLeaderOperator("shuffle-leader", region, region.GetLeader().GetId(), targetStore.GetID(), schedule.OpAdmin)
	op.SetPriorityLevel(core.HighPriority)
	return []*schedule.Operator{op}
}
