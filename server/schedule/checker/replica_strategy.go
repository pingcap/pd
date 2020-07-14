// Copyright 2020 PingCAP, Inc.
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
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/filter"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"github.com/pingcap/pd/v4/server/schedule/selector"
	"go.uber.org/zap"
)

// ReplicaStrategy collects some utilities to manipulate region peers. It
// exists to allow replica_checker and rule_checker to reuse common logics.
type ReplicaStrategy struct {
	checkerName    string // replica-checker / rule-checker
	cluster        opt.Cluster
	locationLabels []string
	region         *core.RegionInfo
	extraFilters   []filter.Filter
}

// SelectStoreToAdd returns the store to add a replica to a region.
// `coLocationStores` are the stores used to compare location with target
// store.
// `extraFilters` is used to set up more filters based on the context that
// calling this method.
//
// For example, to select a target store to replace a region's peer, we can use
// the peer list without the peer to be removed as `coLocationStores`.
// Meanwhile, we need to provide more constraints to ensure that the isolation
// level cannot be reduced after replacement.
func (s *ReplicaStrategy) SelectStoreToAdd(coLocationStores []*core.StoreInfo, extraFilters ...filter.Filter) uint64 {
	// The selection process uses a two-stage fashion. The first stage
	// ignores the temporary state of the stores and selects the stores
	// with the highest score according to the location label. The second
	// stage considers all temporary states and capacity factors to select
	// the most suitable target.
	//
	// The reason for it is to prevent the non-optimal replica placement due
	// to the short-term state, resulting in redundant scheduling.

	filters := []filter.Filter{
		filter.NewExcludedFilter(s.checkerName, nil, s.region.GetStoreIds()),
		filter.NewStorageThresholdFilter(s.checkerName),
		filter.NewSpecialUseFilter(s.checkerName),
		filter.StoreStateFilter{ActionScope: s.checkerName, MoveRegion: true, AllowTemporaryStates: true},
	}
	if len(extraFilters) > 0 {
		filters = append(filters, extraFilters...)
	}
	if len(s.extraFilters) > 0 {
		filters = append(filters, s.extraFilters...)
	}

	isolationComparer := selector.IsolationComparer(s.locationLabels, coLocationStores)
	strictStateFilter := filter.StoreStateFilter{ActionScope: s.checkerName, MoveRegion: true}
	target := selector.NewCandidates(s.cluster.GetStores()).
		FilterTarget(s.cluster, filters...).
		Sort(isolationComparer).Reverse().Top(isolationComparer). // greater isolation score is better
		Sort(selector.RegionScoreComparer(s.cluster)).            // less region score is better
		FilterTarget(s.cluster, strictStateFilter).PickFirst()    // the filter does not ignore temp states
	if target == nil {
		return 0
	}
	return target.GetID()
}

// SelectStoreToReplace returns a store to replace oldStore. The location
// placement after scheduling should be not worse than original.
func (s *ReplicaStrategy) SelectStoreToReplace(coLocationStores []*core.StoreInfo, old uint64) uint64 {
	// trick to avoid creating a slice with `old` removed.
	s.swapStoreToFirst(coLocationStores, old)
	safeGuard := filter.NewLocationSafeguard(s.checkerName, s.locationLabels, coLocationStores, s.cluster.GetStore(old))
	return s.SelectStoreToAdd(coLocationStores[1:], safeGuard)
}

// SelectStoreToImprove returns a store to replace oldStore. The location
// placement after scheduling should be better than original.
func (s *ReplicaStrategy) SelectStoreToImprove(coLocationStores []*core.StoreInfo, old uint64) uint64 {
	// trick to avoid creating a slice with `old` removed.
	s.swapStoreToFirst(coLocationStores, old)
	improver := filter.NewLocationImprover(s.checkerName, s.locationLabels, coLocationStores, s.cluster.GetStore(old))
	return s.SelectStoreToAdd(coLocationStores[1:], improver)
}

func (s *ReplicaStrategy) swapStoreToFirst(stores []*core.StoreInfo, id uint64) {
	for i, s := range stores {
		if s.GetID() == id {
			stores[0], stores[i] = stores[i], stores[0]
			return
		}
	}
}

// SelectStoreToRemove returns the best option to remove from the region.
func (s *ReplicaStrategy) SelectStoreToRemove(coLocationStores []*core.StoreInfo) uint64 {
	isolationComparer := selector.IsolationComparer(s.locationLabels, coLocationStores)
	source := selector.NewCandidates(coLocationStores).
		FilterSource(s.cluster, filter.StoreStateFilter{ActionScope: replicaCheckerName, MoveRegion: true}).
		Sort(isolationComparer).Top(isolationComparer).
		Sort(selector.RegionScoreComparer(s.cluster)).Reverse().
		PickFirst()
	if source == nil {
		log.Debug("no removable store", zap.Uint64("region-id", s.region.GetID()))
		return 0
	}
	return source.GetID()
}
