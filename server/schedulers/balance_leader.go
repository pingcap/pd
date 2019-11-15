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
	"sort"
	"strconv"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	// BalanceLeaderName is balance leader scheduler name.
	BalanceLeaderName = "balance-leader-scheduler"
	// BalanceLeaderType is balance leader scheduler type.
	BalanceLeaderType = "balance-leader"
	// balanceLeaderRetryLimit is the limit to retry schedule for selected source store and target store.
	balanceLeaderRetryLimit = 10
	balanceLeaderType       = "balance-leader"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(BalanceLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*balanceLeaderSchedulerConfig)
			if !ok {
				return ErrScheduleConfigNotExist
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return errors.WithStack(err)
			}
			conf.Ranges = ranges
			conf.Name = BalanceLeaderName
			return nil
		}
	})

	schedule.RegisterScheduler(BalanceLeaderType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceLeaderSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newBalanceLeaderScheduler(opController, conf), nil
	})
}

type balanceLeaderSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
}

type balanceLeaderScheduler struct {
	*baseScheduler
	conf         *balanceLeaderSchedulerConfig
	opController *schedule.OperatorController
	filters      []filter.Filter
	counter      *prometheus.CounterVec
}

// newBalanceLeaderScheduler creates a scheduler that tends to keep leaders on
// each store balanced.
func newBalanceLeaderScheduler(opController *schedule.OperatorController, conf *balanceLeaderSchedulerConfig, opts ...BalanceLeaderCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)

	s := &balanceLeaderScheduler{
		baseScheduler: base,
		conf:          conf,
		opController:  opController,
		counter:       balanceLeaderCounter,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.filters = []filter.Filter{filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true}}
	return s
}

// BalanceLeaderCreateOption is used to create a scheduler with an option.
type BalanceLeaderCreateOption func(s *balanceLeaderScheduler)

// WithBalanceLeaderCounter sets the counter for the scheduler.
func WithBalanceLeaderCounter(counter *prometheus.CounterVec) BalanceLeaderCreateOption {
	return func(s *balanceLeaderScheduler) {
		s.counter = counter
	}
}

// WithBalanceLeaderName sets the name for the scheduler.
func WithBalanceLeaderName(name string) BalanceLeaderCreateOption {
	return func(s *balanceLeaderScheduler) {
		s.conf.Name = name
	}
}

func (l *balanceLeaderScheduler) GetName() string {
	return l.conf.Name
}

func (l *balanceLeaderScheduler) GetType() string {
	return BalanceLeaderType
}

func (l *balanceLeaderScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(l.conf)
}

func (l *balanceLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return l.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (l *balanceLeaderScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()

	leaderScheduleStrategy := l.opController.GetLeaderScheduleStrategy()
	stores := cluster.GetStores()
	sources := filter.SelectSourceStores(stores, l.filters, cluster)
	targets := filter.SelectTargetStores(stores, l.filters, cluster)
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].LeaderScore(leaderScheduleStrategy, 0) > sources[j].LeaderScore(leaderScheduleStrategy, 0)
	})
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].LeaderScore(leaderScheduleStrategy, 0) < targets[j].LeaderScore(leaderScheduleStrategy, 0)
	})

	for i := 0; i < len(sources) || i < len(targets); i++ {
		if i < len(sources) {
			source := sources[i]
			sourceID := source.GetID()
			log.Debug("store leader score", zap.String("scheduler", l.GetName()), zap.Uint64("source-store", sourceID))
			sourceStoreLabel := strconv.FormatUint(sourceID, 10)
			sourceAddress := source.GetAddress()
			l.counter.WithLabelValues("high-score", sourceAddress, sourceStoreLabel).Inc()
			for j := 0; j < balanceLeaderRetryLimit; j++ {
				if op := l.transferLeaderOut(cluster, source); len(op) > 0 {
					l.counter.WithLabelValues("transfer-out", sourceAddress, sourceStoreLabel).Inc()
					return op
				}
			}
			log.Debug("no operator created for selected stores", zap.String("scheduler", l.GetName()), zap.Uint64("source", sourceID))
		}
		if i < len(targets) {
			target := targets[i]
			targetID := target.GetID()
			log.Debug("store leader score", zap.String("scheduler", l.GetName()), zap.Uint64("target-store", targetID))
			targetStoreLabel := strconv.FormatUint(targetID, 10)
			targetAddress := target.GetAddress()
			l.counter.WithLabelValues("low-score", targetAddress, targetStoreLabel).Inc()

			for j := 0; j < balanceLeaderRetryLimit; j++ {
				if op := l.transferLeaderIn(cluster, target); len(op) > 0 {
					l.counter.WithLabelValues("transfer-in", targetAddress, targetStoreLabel).Inc()
					return op
				}
			}
			log.Debug("no operator created for selected stores", zap.String("scheduler", l.GetName()), zap.Uint64("target", targetID))
		}
	}
	return nil
}

// transferLeaderOut transfers leader from the source store.
// It randomly selects a health region from the source store, then picks
// the best follower peer and transfers the leader.
func (l *balanceLeaderScheduler) transferLeaderOut(cluster opt.Cluster, source *core.StoreInfo) []*operator.Operator {
	sourceID := source.GetID()
	region := cluster.RandLeaderRegion(sourceID, l.conf.Ranges, opt.HealthRegion(cluster))
	if region == nil {
		log.Debug("store has no leader", zap.String("scheduler", l.GetName()), zap.Uint64("store-id", sourceID))
		schedulerCounter.WithLabelValues(l.GetName(), "no-leader-region").Inc()
		return nil
	}
	targets := cluster.GetFollowerStores(region)
	targets = filter.SelectTargetStores(targets, l.filters, cluster)
	leaderScheduleStrategy := l.opController.GetLeaderScheduleStrategy()
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].LeaderScore(leaderScheduleStrategy, 0) < targets[j].LeaderScore(leaderScheduleStrategy, 0)
	})
	for _, target := range targets {
		if op := l.createOperator(cluster, region, source, target); len(op) > 0 {
			return op
		}
	}
	log.Debug("region has no target store", zap.String("scheduler", l.GetName()), zap.Uint64("region-id", region.GetID()))
	schedulerCounter.WithLabelValues(l.GetName(), "no-target-store").Inc()
	return nil
}

// transferLeaderIn transfers leader to the target store.
// It randomly selects a health region from the target store, then picks
// the worst follower peer and transfers the leader.
func (l *balanceLeaderScheduler) transferLeaderIn(cluster opt.Cluster, target *core.StoreInfo) []*operator.Operator {
	targetID := target.GetID()
	region := cluster.RandFollowerRegion(targetID, l.conf.Ranges, opt.HealthRegion(cluster))
	if region == nil {
		log.Debug("store has no follower", zap.String("scheduler", l.GetName()), zap.Uint64("store-id", targetID))
		schedulerCounter.WithLabelValues(l.GetName(), "no-follower-region").Inc()
		return nil
	}
	leaderStoreID := region.GetLeader().GetStoreId()
	source := cluster.GetStore(leaderStoreID)
	if source == nil {
		log.Debug("region has no leader or leader store cannot be found",
			zap.String("scheduler", l.GetName()),
			zap.Uint64("region-id", region.GetID()),
			zap.Uint64("store-id", leaderStoreID),
		)
		schedulerCounter.WithLabelValues(l.GetName(), "no-leader").Inc()
		return nil
	}
	return l.createOperator(cluster, region, source, target)
}

// createOperator creates the operator according to the source and target store.
// If the region is hot or the difference between the two stores is tolerable, then
// no new operator need to be created, otherwise create an operator that transfers
// the leader from the source store to the target store for the region.
func (l *balanceLeaderScheduler) createOperator(cluster opt.Cluster, region *core.RegionInfo, source, target *core.StoreInfo) []*operator.Operator {
	if cluster.IsRegionHot(region) {
		log.Debug("region is hot region, ignore it", zap.String("scheduler", l.GetName()), zap.Uint64("region-id", region.GetID()))
		schedulerCounter.WithLabelValues(l.GetName(), "region-hot").Inc()
		return nil
	}

	sourceID := source.GetID()
	targetID := target.GetID()

	opInfluence := l.opController.GetOpInfluence(cluster)
	kind := core.NewScheduleKind(core.LeaderKind, cluster.GetLeaderScheduleStrategy())
	if !shouldBalance(cluster, source, target, region, kind, opInfluence, l.GetName()) {
		schedulerCounter.WithLabelValues(l.GetName(), "skip").Inc()
		return nil
	}

	schedulerCounter.WithLabelValues(l.GetName(), "new-operator").Inc()
	sourceLabel := strconv.FormatUint(sourceID, 10)
	targetLabel := strconv.FormatUint(targetID, 10)
	l.counter.WithLabelValues("move-leader", source.GetAddress()+"-out", sourceLabel).Inc()
	l.counter.WithLabelValues("move-leader", target.GetAddress()+"-in", targetLabel).Inc()
	balanceDirectionCounter.WithLabelValues(l.GetName(), sourceLabel, targetLabel).Inc()
	op := operator.CreateTransferLeaderOperator(BalanceLeaderType, region, region.GetLeader().GetStoreId(), targetID, operator.OpBalance)
	return []*operator.Operator{op}
}
