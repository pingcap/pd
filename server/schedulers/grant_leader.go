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
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pkg/errors"
)

const (
	// GrantLeaderName is grant leader scheduler name.
	GrantLeaderName = "grant-leader-scheduler"
	// GrantLeaderType is grant leader scheduler type.
	GrantLeaderType = "grant-leader"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(GrantLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 1 {
				return errors.New("should specify the store-id")
			}

			conf, ok := v.(*grandLeaderConfig)
			if !ok {
				return ErrScheduleConfigNotExist
			}

			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errors.WithStack(err)
			}
			ranges, err := getKeyRanges(args[1:])
			if err != nil {
				return errors.WithStack(err)
			}
			conf.StoreID = id
			conf.Name = fmt.Sprintf("%s-%d", GrantLeaderName, id)
			conf.Ranges = ranges
			return nil
		}
	})

	schedule.RegisterScheduler(GrantLeaderType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &grandLeaderConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newGrantLeaderScheduler(opController, conf), nil
	})
}

type grandLeaderConfig struct {
	Name    string          `json:"name"`
	StoreID uint64          `json:"store-id"`
	Ranges  []core.KeyRange `json:"ranges"`
}

// grantLeaderScheduler transfers all leaders to peers in the store.
type grantLeaderScheduler struct {
	*baseScheduler
	conf *grandLeaderConfig
}

// newGrantLeaderScheduler creates an admin scheduler that transfers all leaders
// to a store.
func newGrantLeaderScheduler(opController *schedule.OperatorController, conf *grandLeaderConfig) schedule.Scheduler {
	base := newBaseScheduler(opController)
	return &grantLeaderScheduler{
		baseScheduler: base,
		conf:          conf,
	}
}

func (s *grantLeaderScheduler) GetName() string {
	return s.conf.Name
}

func (s *grantLeaderScheduler) GetType() string {
	return GrantLeaderType
}

func (s *grantLeaderScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *grantLeaderScheduler) Prepare(cluster opt.Cluster) error {
	return cluster.BlockStore(s.conf.StoreID)
}

func (s *grantLeaderScheduler) Cleanup(cluster opt.Cluster) {
	cluster.UnblockStore(s.conf.StoreID)
}

func (s *grantLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *grantLeaderScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region := cluster.RandFollowerRegion(s.conf.StoreID, s.conf.Ranges, opt.HealthRegion(cluster))
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-follower").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new-operator").Inc()
	op := operator.CreateTransferLeaderOperator(GrantLeaderType, region, region.GetLeader().GetStoreId(), s.conf.StoreID, operator.OpLeader)
	op.SetPriorityLevel(core.HighPriority)
	return []*operator.Operator{op}
}
