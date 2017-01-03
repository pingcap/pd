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

package server

// Filter is an interface to filter source and target store.
type Filter interface {
	// Return true if the store should not be used as a source store.
	FilterSource(store *storeInfo) bool
	// Return true if the store should not be used as a target store.
	FilterTarget(store *storeInfo) bool
}

func filterSource(store *storeInfo, filters []Filter) bool {
	for _, filter := range filters {
		if filter.FilterSource(store) {
			return true
		}
	}
	return false
}

func filterTarget(store *storeInfo, filters []Filter) bool {
	for _, filter := range filters {
		if filter.FilterTarget(store) {
			return true
		}
	}
	return false
}

type excludedFilter struct {
	sources map[uint64]struct{}
	targets map[uint64]struct{}
}

func newExcludedFilter(sources, targets map[uint64]struct{}) *excludedFilter {
	return &excludedFilter{
		sources: sources,
		targets: targets,
	}
}

func (f *excludedFilter) FilterSource(store *storeInfo) bool {
	_, ok := f.sources[store.GetId()]
	return ok
}

func (f *excludedFilter) FilterTarget(store *storeInfo) bool {
	_, ok := f.targets[store.GetId()]
	return ok
}

type cacheFilter struct {
	cache *idCache
}

func newCacheFilter(cache *idCache) *cacheFilter {
	return &cacheFilter{cache: cache}
}

func (f *cacheFilter) FilterSource(store *storeInfo) bool {
	return f.cache.get(store.GetId())
}

func (f *cacheFilter) FilterTarget(store *storeInfo) bool {
	return false
}

type stateFilter struct {
	opt *scheduleOption
}

func newStateFilter(opt *scheduleOption) *stateFilter {
	return &stateFilter{opt: opt}
}

func (f *stateFilter) filter(store *storeInfo) bool {
	if !store.isUp() {
		return true
	}
	if store.stats.GetIsBusy() {
		return true
	}
	return store.downTime() > f.opt.GetMaxStoreDownTime()
}

func (f *stateFilter) FilterSource(store *storeInfo) bool {
	return f.filter(store)
}

func (f *stateFilter) FilterTarget(store *storeInfo) bool {
	return f.filter(store)
}

type regionCountFilter struct {
	opt *scheduleOption
}

func newRegionCountFilter(opt *scheduleOption) *regionCountFilter {
	return &regionCountFilter{opt: opt}
}

func (f *regionCountFilter) FilterSource(store *storeInfo) bool {
	return uint64(store.stats.RegionCount) < f.opt.GetMinRegionCount()
}

func (f *regionCountFilter) FilterTarget(store *storeInfo) bool {
	return false
}

type leaderCountFilter struct {
	opt *scheduleOption
}

func newLeaderCountFilter(opt *scheduleOption) *leaderCountFilter {
	return &leaderCountFilter{opt: opt}
}

func (f *leaderCountFilter) FilterSource(store *storeInfo) bool {
	return uint64(store.stats.LeaderRegionCount) < f.opt.GetMinLeaderCount()
}

func (f *leaderCountFilter) FilterTarget(store *storeInfo) bool {
	return false
}

type snapshotCountFilter struct {
	opt *scheduleOption
}

func newSnapshotCountFilter(opt *scheduleOption) *snapshotCountFilter {
	return &snapshotCountFilter{opt: opt}
}

func (f *snapshotCountFilter) filter(store *storeInfo) bool {
	return uint64(store.stats.GetSendingSnapCount()) > f.opt.GetMaxSnapshotCount() ||
		uint64(store.stats.GetReceivingSnapCount()) > f.opt.GetMaxSnapshotCount() ||
		uint64(store.stats.GetApplyingSnapCount()) > f.opt.GetMaxSnapshotCount()
}

func (f *snapshotCountFilter) FilterSource(store *storeInfo) bool {
	return f.filter(store)
}

func (f *snapshotCountFilter) FilterTarget(store *storeInfo) bool {
	return f.filter(store)
}

// storageThresholdFilter ensures that we will not use an almost full store as a target.
type storageThresholdFilter struct{}

const storageRatioThreshold = 0.8

func newStorageThresholdFilter(opt *scheduleOption) *storageThresholdFilter {
	return &storageThresholdFilter{}
}

func (f *storageThresholdFilter) FilterSource(store *storeInfo) bool {
	return false
}

func (f *storageThresholdFilter) FilterTarget(store *storeInfo) bool {
	return store.storageRatio() > storageRatioThreshold
}

// replicationFilter ensures that the target store will not break the replication constraints.
type replicationFilter struct {
	rep        *Replication
	stores     []*storeInfo
	worstStore *storeInfo
	worstScore float64
}

func newReplicationFilter(rep *Replication, stores []*storeInfo, worstStore *storeInfo) *replicationFilter {
	for i, s := range stores {
		if s.GetId() == worstStore.GetId() {
			stores = append(stores[:i], stores[i+1:]...)
			break
		}
	}

	return &replicationFilter{
		rep:        rep,
		stores:     stores,
		worstStore: worstStore,
		worstScore: rep.GetReplicaScore(stores, worstStore),
	}
}

func (f *replicationFilter) FilterSource(store *storeInfo) bool {
	return false
}

func (f *replicationFilter) FilterTarget(store *storeInfo) bool {
	score := f.rep.GetReplicaScore(f.stores, store)
	return compareStoreScore(store, score, f.worstStore, f.worstScore) < 0
}
