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
	"strconv"

	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/selector"
	"github.com/pkg/errors"
)

func init() {
	schedule.RegisterArgsToMapper("evict-leader", func(args []string) (schedule.ConfigMapper, error) {
		if len(args) != 1 {
			return nil, errors.New("should specify the store-id")
		}
		mapper := make(schedule.ConfigMapper)
		id, err := strconv.ParseFloat(args[0], 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		mapper["store-id"] = id
		return mapper, nil
	})

	schedule.RegisterScheduler("evict-leader", func(opController *schedule.OperatorController, storage *core.Storage, mapper schedule.ConfigMapper) (schedule.Scheduler, error) {
		if len(mapper) != 1 {
			return nil, errors.New("evict-leader needs 1 argument")
		}
		id := uint64(mapper["store-id"].(float64))
		name := fmt.Sprintf("evict-leader-scheduler-%d", id)
		conf := &evictLeaderSchedulerConf{
			Name:    name,
			StoreID: id,
		}
		return newEvictLeaderScheduler(opController, conf), nil
	})
}

type evictLeaderSchedulerConf struct {
	Name    string
	StoreID uint64 `json:"store-id"`
}

type evictLeaderScheduler struct {
	*baseScheduler
	conf     *evictLeaderSchedulerConf
	selector *selector.RandomSelector
}

// newEvictLeaderScheduler creates an admin scheduler that transfers all leaders
// out of a store.
func newEvictLeaderScheduler(opController *schedule.OperatorController, conf *evictLeaderSchedulerConf) schedule.Scheduler {
	filters := []filter.Filter{
		filter.StoreStateFilter{ActionScope: conf.Name, TransferLeader: true},
	}

	base := newBaseScheduler(opController)
	return &evictLeaderScheduler{
		baseScheduler: base,
		conf:          conf,
		selector:      selector.NewRandomSelector(filters),
	}
}

func (s *evictLeaderScheduler) GetName() string {
	return s.conf.Name
}

func (s *evictLeaderScheduler) GetType() string {
	return "evict-leader"
}

func (s *evictLeaderScheduler) GetConfig() interface{} {
	return s.conf
}

func (s *evictLeaderScheduler) Prepare(cluster schedule.Cluster) error {
	return cluster.BlockStore(s.conf.StoreID)
}

func (s *evictLeaderScheduler) Cleanup(cluster schedule.Cluster) {
	cluster.UnblockStore(s.conf.StoreID)
}

func (s *evictLeaderScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *evictLeaderScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region := cluster.RandLeaderRegion(s.conf.StoreID, core.HealthRegion())
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-leader").Inc()
		return nil
	}
	target := s.selector.SelectTarget(cluster, cluster.GetFollowerStores(region))
	if target == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-target-store").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new-operator").Inc()
	op := operator.CreateTransferLeaderOperator("evict-leader", region, region.GetLeader().GetStoreId(), target.GetID(), operator.OpLeader)
	op.SetPriorityLevel(core.HighPriority)
	return []*operator.Operator{op}
}
