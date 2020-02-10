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

package schedule

import (
	"fmt"

	"github.com/pingcap/pd/v3/server/cache"
	"github.com/pingcap/pd/v3/server/core"
	"github.com/pingcap/pd/v3/server/namespace"
)

//revive:disable:unused-parameter

// Filter is an interface to filter source and target store.
type Filter interface {
	// Scope is used to indicate where the filter will act on.
	Scope() string
	Type() string
	// Return true if the store should not be used as a source store.
	FilterSource(opt Options, store *core.StoreInfo) bool
	// Return true if the store should not be used as a target store.
	FilterTarget(opt Options, store *core.StoreInfo) bool
}

// FilterSource checks if store can pass all Filters as source store.
func FilterSource(opt Options, store *core.StoreInfo, filters []Filter) bool {
	storeAddress := store.GetAddress()
	storeID := fmt.Sprintf("%d", store.GetID())
	for _, filter := range filters {
		if filter.FilterSource(opt, store) {
			filterCounter.WithLabelValues("filter-source", storeAddress, storeID, filter.Scope(), filter.Type()).Inc()
			return true
		}
	}
	return false
}

// FilterTarget checks if store can pass all Filters as target store.
func FilterTarget(opt Options, store *core.StoreInfo, filters []Filter) bool {
	storeAddress := store.GetAddress()
	storeID := fmt.Sprintf("%d", store.GetID())
	for _, filter := range filters {
		if filter.FilterTarget(opt, store) {
			filterCounter.WithLabelValues("filter-target", storeAddress, storeID, filter.Scope(), filter.Type()).Inc()
			return true
		}
	}
	return false
}

type excludedFilter struct {
	scope   string
	sources map[uint64]struct{}
	targets map[uint64]struct{}
}

// NewExcludedFilter creates a Filter that filters all specified stores.
func NewExcludedFilter(scope string, sources, targets map[uint64]struct{}) Filter {
	return &excludedFilter{
		scope:   scope,
		sources: sources,
		targets: targets,
	}
}

func (f *excludedFilter) Scope() string {
	return f.scope
}

func (f *excludedFilter) Type() string {
	return "exclude-filter"
}

func (f *excludedFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	_, ok := f.sources[store.GetID()]
	return ok
}

func (f *excludedFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	_, ok := f.targets[store.GetID()]
	return ok
}

type overloadFilter struct{ scope string }

// NewOverloadFilter creates a Filter that filters all stores that are overloaded from balance.
func NewOverloadFilter(scope string) Filter {
	return &overloadFilter{scope: scope}
}

func (f *overloadFilter) Scope() string {
	return f.scope
}

func (f *overloadFilter) Type() string {
	return "overload-filter"
}

func (f *overloadFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return store.IsOverloaded()
}

func (f *overloadFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return store.IsOverloaded()
}

type stateFilter struct{ scope string }

// NewStateFilter creates a Filter that filters all stores that are not UP.
func NewStateFilter(scope string) Filter {
	return &stateFilter{scope: scope}
}

func (f *stateFilter) Scope() string {
	return f.scope
}

func (f *stateFilter) Type() string {
	return "state-filter"
}

func (f *stateFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return store.IsTombstone()
}

func (f *stateFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return !store.IsUp()
}

type healthFilter struct{ scope string }

// NewHealthFilter creates a Filter that filters all stores that are Busy or Down.
func NewHealthFilter(scope string) Filter {
	return &healthFilter{scope: scope}
}

func (f *healthFilter) Scope() string {
	return f.scope
}

func (f *healthFilter) Type() string {
	return "health-filter"
}

func (f *healthFilter) filter(opt Options, store *core.StoreInfo) bool {
	if store.GetIsBusy() {
		return true
	}
	return store.DownTime() > opt.GetMaxStoreDownTime()
}

func (f *healthFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

func (f *healthFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

type pendingPeerCountFilter struct{ scope string }

// NewPendingPeerCountFilter creates a Filter that filters all stores that are
// currently handling too many pending peers.
func NewPendingPeerCountFilter(scope string) Filter {
	return &pendingPeerCountFilter{scope: scope}
}

func (p *pendingPeerCountFilter) Scope() string {
	return p.scope
}

func (p *pendingPeerCountFilter) Type() string {
	return "pending-peer-filter"
}

func (p *pendingPeerCountFilter) filter(opt Options, store *core.StoreInfo) bool {
	if opt.GetMaxPendingPeerCount() == 0 {
		return false
	}
	return store.GetPendingPeerCount() > int(opt.GetMaxPendingPeerCount())
}

func (p *pendingPeerCountFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return p.filter(opt, store)
}

func (p *pendingPeerCountFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return p.filter(opt, store)
}

type snapshotCountFilter struct{ scope string }

// NewSnapshotCountFilter creates a Filter that filters all stores that are
// currently handling too many snapshots.
func NewSnapshotCountFilter(scope string) Filter {
	return &snapshotCountFilter{scope: scope}
}

func (f *snapshotCountFilter) Scope() string {
	return f.scope
}

func (f *snapshotCountFilter) Type() string {
	return "snapshot-filter"
}

func (f *snapshotCountFilter) filter(opt Options, store *core.StoreInfo) bool {
	return uint64(store.GetSendingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetReceivingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetApplyingSnapCount()) > opt.GetMaxSnapshotCount()
}

func (f *snapshotCountFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

func (f *snapshotCountFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

type cacheFilter struct {
	scope string
	cache *cache.TTLUint64
}

// NewCacheFilter creates a Filter that filters all stores that are in the cache.
func NewCacheFilter(scope string, cache *cache.TTLUint64) Filter {
	return &cacheFilter{scope: scope, cache: cache}
}

func (f *cacheFilter) Scope() string {
	return f.scope
}

func (f *cacheFilter) Type() string {
	return "cache-filter"
}

func (f *cacheFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return f.cache.Exists(store.GetID())
}

func (f *cacheFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return false
}

type storageThresholdFilter struct{ scope string }

// NewStorageThresholdFilter creates a Filter that filters all stores that are
// almost full.
func NewStorageThresholdFilter(scope string) Filter {
	return &storageThresholdFilter{scope: scope}
}

func (f *storageThresholdFilter) Scope() string {
	return f.scope
}

func (f *storageThresholdFilter) Type() string {
	return "storage-threshold-filter"
}

func (f *storageThresholdFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return false
}

func (f *storageThresholdFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return store.IsLowSpace(opt.GetLowSpaceRatio())
}

// distinctScoreFilter ensures that distinct score will not decrease.
type distinctScoreFilter struct {
	scope     string
	labels    []string
	stores    []*core.StoreInfo
	safeScore float64
}

// NewDistinctScoreFilter creates a filter that filters all stores that have
// lower distinct score than specified store.
func NewDistinctScoreFilter(scope string, labels []string, stores []*core.StoreInfo, source *core.StoreInfo) Filter {
	newStores := make([]*core.StoreInfo, 0, len(stores)-1)
	for _, s := range stores {
		if s.GetID() == source.GetID() {
			continue
		}
		newStores = append(newStores, s)
	}

	return &distinctScoreFilter{
		scope:     scope,
		labels:    labels,
		stores:    newStores,
		safeScore: DistinctScore(labels, newStores, source),
	}
}

func (f *distinctScoreFilter) Scope() string {
	return f.scope
}

func (f *distinctScoreFilter) Type() string {
	return "distinct-filter"
}

func (f *distinctScoreFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return false
}

func (f *distinctScoreFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return DistinctScore(f.labels, f.stores, store) < f.safeScore
}

type namespaceFilter struct {
	scope      string
	classifier namespace.Classifier
	namespace  string
}

// NewNamespaceFilter creates a Filter that filters all stores that are not
// belong to a namespace.
func NewNamespaceFilter(scope string, classifier namespace.Classifier, namespace string) Filter {
	return &namespaceFilter{
		scope:      scope,
		classifier: classifier,
		namespace:  namespace,
	}
}

func (f *namespaceFilter) Scope() string {
	return f.scope
}

func (f *namespaceFilter) Type() string {
	return "namespace-filter"
}

func (f *namespaceFilter) filter(store *core.StoreInfo) bool {
	return f.classifier.GetStoreNamespace(store) != f.namespace
}

func (f *namespaceFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return f.filter(store)
}

func (f *namespaceFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return f.filter(store)
}

// StoreStateFilter is used to determine whether a store can be selected as the
// source or target of the schedule based on the store's state.
type StoreStateFilter struct {
	ActionScope string
	// Set true if the schedule involves any transfer leader operation.
	TransferLeader bool
	// Set true if the schedule involves any move region operation.
	MoveRegion bool
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f StoreStateFilter) Scope() string {
	return f.ActionScope
}

// Type returns the type of the Filter.
func (f StoreStateFilter) Type() string {
	return "store-state-filter"
}

// FilterSource returns true when the store cannot be selected as the schedule
// source.
func (f StoreStateFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	if store.IsTombstone() ||
		store.DownTime() > opt.GetMaxStoreDownTime() {
		return true
	}
	if f.TransferLeader && (store.IsDisconnected() || store.IsBlocked()) {
		return true
	}

	if f.MoveRegion && f.filterMoveRegion(opt, store) {
		return true
	}
	return false
}

// FilterTarget returns true when the store cannot be selected as the schedule
// target.
func (f StoreStateFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	if store.IsTombstone() ||
		store.IsOffline() ||
		store.DownTime() > opt.GetMaxStoreDownTime() {
		return true
	}
	if f.TransferLeader &&
		(store.IsDisconnected() ||
			store.IsBlocked() ||
			store.GetIsBusy() ||
			opt.CheckLabelProperty(RejectLeader, store.GetLabels())) {
		return true
	}

	if f.MoveRegion {
		// only target consider the pending peers because pending more means the disk is slower.
		if opt.GetMaxPendingPeerCount() > 0 && store.GetPendingPeerCount() > int(opt.GetMaxPendingPeerCount()) {
			return true
		}

		if f.filterMoveRegion(opt, store) {
			return true
		}
	}
	return false
}

func (f StoreStateFilter) filterMoveRegion(opt Options, store *core.StoreInfo) bool {
	if store.GetIsBusy() {
		return true
	}

	if store.IsOverloaded() {
		return true
	}

	if uint64(store.GetSendingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetReceivingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetApplyingSnapCount()) > opt.GetMaxSnapshotCount() {
		return true
	}
	return false
}
