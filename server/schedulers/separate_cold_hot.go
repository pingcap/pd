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

package schedulers

import (
	"strconv"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/checker"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pingcap/pd/server/schedule/selector"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("separate-cold-hot", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("separate-cold-hot", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newSeparateColdHotScheduler(opController), nil
	})
}

const (
	separateColdHotName = "separate-cold-hot-scheduler"
	batchSzie           = 1
)

type separateColdHotScheduler struct {
	*baseScheduler
	name         string
	selector     *selector.BalanceSelector
	opController *schedule.OperatorController
	hitsCounter  *hitsStoreBuilder
	counter      *prometheus.CounterVec
}

// newSeparateColdHotScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newSeparateColdHotScheduler(opController *schedule.OperatorController, opts ...SeparateColdHotCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &separateColdHotScheduler{
		baseScheduler: base,
		opController:  opController,
		hitsCounter:   newHitsStoreBuilder(hitsStoreTTL, hitsStoreCountThreshold),
		counter:       balanceRegionCounter,
	}
	for _, opt := range opts {
		opt(s)
	}
	filters := []filter.Filter{
		filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true},
	}
	kind := core.NewScheduleKind(core.RegionKind, core.BySize)
	s.selector = selector.NewBalanceSelector(kind, filters)
	return s
}

// SeparateColdHotCreateOption is used to create a scheduler with an option.
type SeparateColdHotCreateOption func(s *separateColdHotScheduler)

// WithSeparateColdHotCounter sets the counter for the scheduler.
func WithSeparateColdHotCounter(counter *prometheus.CounterVec) SeparateColdHotCreateOption {
	return func(s *separateColdHotScheduler) {
		s.counter = counter
	}
}

// WithSeparateColdHotName sets the name for the scheduler.
func WithSeparateColdHotName(name string) SeparateColdHotCreateOption {
	return func(s *separateColdHotScheduler) {
		s.name = name
	}
}

func (s *separateColdHotScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return separateColdHotName
}

func (s *separateColdHotScheduler) GetType() string {
	return "separate-cold-hot"
}

func (s *separateColdHotScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *separateColdHotScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	stores := cluster.GetStores()
	performanceStoresIDs := make(map[uint64]struct{})
	storageStoresIDs := make(map[uint64]struct{})
	for _, store := range stores {
		storeType := store.GetMeta().GetStoreType()
		if storeType == metapb.StoreType_Performance {
			performanceStoresIDs[store.GetID()] = struct{}{}
		} else if storeType == metapb.StoreType_Storage {
			storageStoresIDs[store.GetID()] = struct{}{}
		} else {
			panic("unexpetecd store type")
		}
	}
	// 1st: move warm regions back into performance TiKVs
	if candidateRegions := cluster.ColdToWarmStats(batchSzie); len(candidateRegions) > 0 {
		if ops := s.migrateColdToWarm(cluster, candidateRegions, storageStoresIDs); len(ops) > 0 {
			return ops
		}
	}
	// 2nd: move cold regions into storage TiKVs
	if candidateRegions := cluster.WarmToColdStats(batchSzie); len(candidateRegions) > 0 {
		if ops := s.migrateWarmToCold(cluster, candidateRegions, performanceStoresIDs); len(ops) > 0 {
			return ops
		}
	}
	return nil
}

func (s *separateColdHotScheduler) migrateColdToWarm(cluster opt.Cluster, candidateRegions []*core.RegionInfo, storageStores map[uint64]struct{}) []*operator.Operator {
	for _, region := range candidateRegions {
		for _, peer := range region.GetPeers() {
			if _, ok := storageStores[peer.StoreId]; ok {
				if op := s.transferPeer(cluster, region, peer, storageStores); op != nil {
					schedulerCounter.WithLabelValues(s.GetName(), "new-operator").Inc()
					// TODO: currently pd only support schedule one operator
					return []*operator.Operator{op}
				}
			}
		}
	}
	return nil
}

func (s *separateColdHotScheduler) migrateWarmToCold(cluster opt.Cluster, candidateRegions []*core.RegionInfo, performanceStores map[uint64]struct{}) []*operator.Operator {
	for _, region := range candidateRegions {
		for _, peer := range region.GetPeers() {
			if _, ok := performanceStores[peer.StoreId]; ok {
				if op := s.transferPeer(cluster, region, peer, performanceStores); op != nil {
					schedulerCounter.WithLabelValues(s.GetName(), "new-operator").Inc()
					// TODO: currently pd only support schedule one operator
					return []*operator.Operator{op}
				}
			}
		}
	}
	return nil
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *separateColdHotScheduler) transferPeer(cluster opt.Cluster, region *core.RegionInfo, oldPeer *metapb.Peer, excludedStoresIDs map[uint64]struct{}) *operator.Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	stores := cluster.GetRegionStores(region)
	sourceStoreID := oldPeer.GetStoreId()
	source := cluster.GetStore(sourceStoreID)
	if source == nil {
		log.Error("failed to get the source store", zap.Uint64("store-id", sourceStoreID))
	}
	scoreGuard := filter.NewDistinctScoreFilter(s.GetName(), cluster.GetLocationLabels(), stores, source)
	hitsFilter := s.hitsCounter.buildTargetFilter(s.GetName(), cluster, source)
	coldHotFilter := filter.NewExcludedFilter(s.GetName(), excludedStoresIDs, excludedStoresIDs)
	checker := checker.NewReplicaChecker(cluster, nil, s.GetName())
	storeID, _ := checker.SelectBestReplacementStore(region, oldPeer, scoreGuard, hitsFilter, coldHotFilter)
	if storeID == 0 {
		schedulerCounter.WithLabelValues(s.GetName(), "no-replacement").Inc()
		s.hitsCounter.put(source, nil)
		return nil
	}

	target := cluster.GetStore(storeID)
	if target == nil {
		log.Error("failed to get the target store", zap.Uint64("store-id", storeID))
	}
	regionID := region.GetID()
	sourceID := source.GetID()
	targetID := target.GetID()
	log.Debug("", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

	opInfluence := s.opController.GetOpInfluence(cluster)
	kind := core.NewScheduleKind(core.RegionKind, core.BySize)
	if !shouldBalance(cluster, source, target, region, kind, opInfluence, s.GetName()) {
		schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
		s.hitsCounter.put(source, target)
		return nil
	}

	newPeer, err := cluster.AllocPeer(storeID)
	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-peer").Inc()
		return nil
	}
	op, err := operator.CreateMovePeerOperator("separate-cold-hot", cluster, region, operator.OpBalance, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "create-operator-fail").Inc()
		return nil
	}
	s.hitsCounter.remove(source, target)
	s.hitsCounter.remove(source, nil)
	sourceLabel := strconv.FormatUint(sourceID, 10)
	targetLabel := strconv.FormatUint(targetID, 10)
	s.counter.WithLabelValues("move-peer", source.GetAddress()+"-out", sourceLabel).Inc()
	s.counter.WithLabelValues("move-peer", target.GetAddress()+"-in", targetLabel).Inc()
	balanceDirectionCounter.WithLabelValues(s.GetName(), sourceLabel, targetLabel).Inc()
	return op
}
