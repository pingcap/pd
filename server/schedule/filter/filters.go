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

package filter

import (
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/slice"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	"github.com/pingcap/pd/v4/server/schedule/storelimit"
	"go.uber.org/zap"
)

// revive:disable:unused-parameter

// SelectSourceStores selects stores that be selected as source store from the list.
func SelectSourceStores(stores []*core.StoreInfo, filters []Filter, opt opt.Options) []*core.StoreInfo {
	return filterStoresBy(stores, func(s *core.StoreInfo) bool {
		return slice.AllOf(filters, func(i int) bool { return filters[i].Source(opt, s) })
	})
}

// SelectTargetStores selects stores that be selected as target store from the list.
func SelectTargetStores(stores []*core.StoreInfo, filters []Filter, opt opt.Options) []*core.StoreInfo {
	return filterStoresBy(stores, func(s *core.StoreInfo) bool {
		return slice.AllOf(filters, func(i int) bool { return filters[i].Target(opt, s) })
	})
}

func filterStoresBy(stores []*core.StoreInfo, keepPred func(*core.StoreInfo) bool) (selected []*core.StoreInfo) {
	for _, s := range stores {
		if keepPred(s) {
			selected = append(selected, s)
		}
	}
	return
}

// Filter is an interface to filter source and target store.
type Filter interface {
	// Scope is used to indicate where the filter will act on.
	Scope() string
	Type() string
	// Return true if the store can be used as a source store.
	Source(opt opt.Options, store *core.StoreInfo) bool
	// Return true if the store can be used as a target store.
	Target(opt opt.Options, store *core.StoreInfo) bool
}

// Source checks if store can pass all Filters as source store.
func Source(opt opt.Options, store *core.StoreInfo, filters []Filter) bool {
	storeAddress := store.GetAddress()
	storeID := fmt.Sprintf("%d", store.GetID())
	for _, filter := range filters {
		if !filter.Source(opt, store) {
			filterCounter.WithLabelValues("filter-source", storeAddress, storeID, filter.Scope(), filter.Type()).Inc()
			return false
		}
	}
	return true
}

// Target checks if store can pass all Filters as target store.
func Target(opt opt.Options, store *core.StoreInfo, filters []Filter) bool {
	storeAddress := store.GetAddress()
	storeID := fmt.Sprintf("%d", store.GetID())
	for _, filter := range filters {
		if !filter.Target(opt, store) {
			filterCounter.WithLabelValues("filter-target", storeAddress, storeID, filter.Scope(), filter.Type()).Inc()
			return false
		}
	}
	return true
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

func (f *excludedFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	_, ok := f.sources[store.GetID()]
	return !ok
}

func (f *excludedFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	_, ok := f.targets[store.GetID()]
	return !ok
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

func (f *storageThresholdFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return true
}

func (f *storageThresholdFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return !store.IsLowSpace(opt.GetLowSpaceRatio())
}

// distinctScoreFilter ensures that distinct score will not decrease.
type distinctScoreFilter struct {
	scope     string
	labels    []string
	stores    []*core.StoreInfo
	policy    string
	safeScore float64
}

const (
	// policies used by distinctScoreFilter.
	// 'safeguard' ensures replacement is NOT WORSE than before.
	// 'improve' ensures replacement is BETTER than before.
	locationSafeguard = "safeguard"
	locationImprove   = "improve"
)

// NewLocationSafeguard creates a filter that filters all stores that have
// lower distinct score than specified store.
func NewLocationSafeguard(scope string, labels []string, stores []*core.StoreInfo, source *core.StoreInfo) Filter {
	return newDistinctScoreFilter(scope, labels, stores, source, locationSafeguard)
}

// NewLocationImprover creates a filter that filters all stores that have
// lower or equal distinct score than specified store.
func NewLocationImprover(scope string, labels []string, stores []*core.StoreInfo, source *core.StoreInfo) Filter {
	return newDistinctScoreFilter(scope, labels, stores, source, locationImprove)
}

func newDistinctScoreFilter(scope string, labels []string, stores []*core.StoreInfo, source *core.StoreInfo, policy string) Filter {
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
		safeScore: core.DistinctScore(labels, newStores, source),
		policy:    policy,
	}
}

func (f *distinctScoreFilter) Scope() string {
	return f.scope
}

func (f *distinctScoreFilter) Type() string {
	return "distinct-filter"
}

func (f *distinctScoreFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return true
}

func (f *distinctScoreFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	score := core.DistinctScore(f.labels, f.stores, store)
	switch f.policy {
	case locationSafeguard:
		return score >= f.safeScore
	case locationImprove:
		return score > f.safeScore
	default:
		return false
	}
}

// StoreStateFilter is used to determine whether a store can be selected as the
// source or target of the schedule based on the store's state.
type StoreStateFilter struct {
	ActionScope string
	// Set true if the schedule involves any transfer leader operation.
	TransferLeader bool
	// Set true if the schedule involves any move region operation.
	MoveRegion bool
	// Set true if allows temporary states.
	AllowTemporaryStates bool
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f StoreStateFilter) Scope() string {
	return f.ActionScope
}

// Type returns the type of the Filter.
func (f StoreStateFilter) Type() string {
	return "store-state-filter"
}

// conditionFunc defines condition to determine a store should be selected.
// It should consider if the filter allows temporary states.
type conditionFunc func(opt.Options, *core.StoreInfo) bool

func (f StoreStateFilter) isTombstone(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsTombstone()
}

func (f StoreStateFilter) isDown(opt opt.Options, store *core.StoreInfo) bool {
	return store.DownTime() > opt.GetMaxStoreDownTime()
}

func (f StoreStateFilter) isOffline(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsOffline()
}

func (f StoreStateFilter) pauseLeaderTransfer(opt opt.Options, store *core.StoreInfo) bool {
	return !store.AllowLeaderTransfer()
}

func (f StoreStateFilter) isDisconnected(opt opt.Options, store *core.StoreInfo) bool {
	return !f.AllowTemporaryStates && store.IsDisconnected()
}

func (f StoreStateFilter) isBusy(opt opt.Options, store *core.StoreInfo) bool {
	return !f.AllowTemporaryStates && store.IsBusy()
}

func (f StoreStateFilter) exceedRemoveLimit(opt opt.Options, store *core.StoreInfo) bool {
	return !f.AllowTemporaryStates && !store.IsAvailable(storelimit.RemovePeer)
}

func (f StoreStateFilter) exceedAddLimit(opt opt.Options, store *core.StoreInfo) bool {
	return !f.AllowTemporaryStates && !store.IsAvailable(storelimit.AddPeer)
}

func (f StoreStateFilter) tooManySnapshots(opt opt.Options, store *core.StoreInfo) bool {
	return !f.AllowTemporaryStates && (uint64(store.GetSendingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetReceivingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetApplyingSnapCount()) > opt.GetMaxSnapshotCount())
}

func (f StoreStateFilter) tooManyPendingPeers(opt opt.Options, store *core.StoreInfo) bool {
	return !f.AllowTemporaryStates &&
		opt.GetMaxPendingPeerCount() > 0 &&
		store.GetPendingPeerCount() > int(opt.GetMaxPendingPeerCount())
}

func (f StoreStateFilter) hasRejectLeaderProperty(opts opt.Options, store *core.StoreInfo) bool {
	return opts.CheckLabelProperty(opt.RejectLeader, store.GetLabels())
}

// The condition table.
// Y: the condition is temporary (expected to become false soon).
// N: the condition is expected to be true for a long time.
// X means when the condition is true, the store CANNOT be selected.
//
// Condition    Down Offline Tomb Pause Disconn Busy RmLimit AddLimit Snap Pending Reject
// IsTemporary  N    N       N    N     Y       Y    Y       Y        Y    Y       N
//
// LeaderSource X            X    X     X
// RegionSource                                 X    X                X
// LeaderTarget X    X       X    X     X       X                                  X
// RegionTarget X    X       X          X       X            X        X    X

const (
	leaderSource = iota
	regionSource
	leaderTarget
	regionTarget
)

func (f StoreStateFilter) anyConditionMatch(typ int, opt opt.Options, store *core.StoreInfo) bool {
	var funcs []conditionFunc
	switch typ {
	case leaderSource:
		funcs = []conditionFunc{f.isTombstone, f.isDown, f.pauseLeaderTransfer, f.isDisconnected}
	case regionSource:
		funcs = []conditionFunc{f.isBusy, f.exceedRemoveLimit, f.tooManySnapshots}
	case leaderTarget:
		funcs = []conditionFunc{f.isTombstone, f.isOffline, f.isDown, f.pauseLeaderTransfer,
			f.isDisconnected, f.isBusy, f.hasRejectLeaderProperty}
	case regionTarget:
		funcs = []conditionFunc{f.isTombstone, f.isOffline, f.isDown, f.isDisconnected, f.isBusy,
			f.exceedAddLimit, f.tooManySnapshots, f.tooManyPendingPeers}
	}
	for _, cf := range funcs {
		if cf(opt, store) {
			return true
		}
	}
	return false
}

// Source returns true when the store can be selected as the schedule
// source.
func (f StoreStateFilter) Source(opts opt.Options, store *core.StoreInfo) bool {
	if f.TransferLeader && f.anyConditionMatch(leaderSource, opts, store) {
		return false
	}
	if f.MoveRegion && f.anyConditionMatch(regionSource, opts, store) {
		return false
	}
	return true
}

// Target returns true when the store can be selected as the schedule
// target.
func (f StoreStateFilter) Target(opts opt.Options, store *core.StoreInfo) bool {
	if f.TransferLeader && f.anyConditionMatch(leaderTarget, opts, store) {
		return false
	}
	if f.MoveRegion && f.anyConditionMatch(regionTarget, opts, store) {
		return false
	}
	return true
}

// labelConstraintFilter is a filter that selects stores satisfy the constraints.
type labelConstraintFilter struct {
	scope       string
	constraints []placement.LabelConstraint
}

// NewLabelConstaintFilter creates a filter that selects stores satisfy the constraints.
func NewLabelConstaintFilter(scope string, constraints []placement.LabelConstraint) Filter {
	return labelConstraintFilter{scope: scope, constraints: constraints}
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f labelConstraintFilter) Scope() string {
	return f.scope
}

// Type returns the name of the filter.
func (f labelConstraintFilter) Type() string {
	return "label-constraint-filter"
}

// Source filters stores when select them as schedule source.
func (f labelConstraintFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return placement.MatchLabelConstraints(store, f.constraints)
}

// Target filters stores when select them as schedule target.
func (f labelConstraintFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return placement.MatchLabelConstraints(store, f.constraints)
}

// RegionFitter is the interface that can fit a region against placement rules.
type RegionFitter interface {
	FitRegion(*core.RegionInfo) *placement.RegionFit
}

type ruleFitFilter struct {
	scope    string
	fitter   RegionFitter
	region   *core.RegionInfo
	oldFit   *placement.RegionFit
	oldStore uint64
}

// newRuleFitFilter creates a filter that ensures after replace a peer with new
// one, the isolation level will not decrease. Its function is the same as
// distinctScoreFilter but used when placement rules is enabled.
func newRuleFitFilter(scope string, fitter RegionFitter, region *core.RegionInfo, oldStoreID uint64) Filter {
	return &ruleFitFilter{
		scope:    scope,
		fitter:   fitter,
		region:   region,
		oldFit:   fitter.FitRegion(region),
		oldStore: oldStoreID,
	}
}

func (f *ruleFitFilter) Scope() string {
	return f.scope
}

func (f *ruleFitFilter) Type() string {
	return "rule-fit-filter"
}

func (f *ruleFitFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return true
}

func (f *ruleFitFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	region := f.region.Clone(core.WithReplacePeerStore(f.oldStore, store.GetID()))
	newFit := f.fitter.FitRegion(region)
	return placement.CompareRegionFit(f.oldFit, newFit) <= 0
}

type ruleLeaderFitFilter struct {
	scope            string
	fitter           RegionFitter
	region           *core.RegionInfo
	oldFit           *placement.RegionFit
	oldLeaderStoreID uint64
}

// newRuleLeaderFitFilter creates a filter that ensures after transfer leader with new store,
// the isolation level will not decrease.
func newRuleLeaderFitFilter(scope string, fitter RegionFitter, region *core.RegionInfo, oldLeaderStoreID uint64) Filter {
	return &ruleLeaderFitFilter{
		scope:            scope,
		fitter:           fitter,
		region:           region,
		oldFit:           fitter.FitRegion(region),
		oldLeaderStoreID: oldLeaderStoreID,
	}
}

func (f *ruleLeaderFitFilter) Scope() string {
	return f.scope
}

func (f *ruleLeaderFitFilter) Type() string {
	return "rule-fit-leader-filter"
}

func (f *ruleLeaderFitFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return true
}

func (f *ruleLeaderFitFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	targetPeer := f.region.GetStorePeer(store.GetID())
	if targetPeer == nil {
		log.Warn("ruleLeaderFitFilter couldn't find peer on target Store", zap.Uint64("target-store", store.GetID()))
		return false
	}
	region := f.region.Clone(core.WithLeader(targetPeer))
	newFit := f.fitter.FitRegion(region)
	return placement.CompareRegionFit(f.oldFit, newFit) <= 0
}

// NewPlacementSafeguard creates a filter that ensures after replace a peer with new
// peer, the placement restriction will not become worse.
func NewPlacementSafeguard(scope string, cluster opt.Cluster, region *core.RegionInfo, sourceStore *core.StoreInfo) Filter {
	if cluster.IsPlacementRulesEnabled() {
		return newRuleFitFilter(scope, cluster, region, sourceStore.GetID())
	}
	return NewLocationSafeguard(scope, cluster.GetLocationLabels(), cluster.GetRegionStores(region), sourceStore)
}

// NewPlacementLeaderSafeguard creates a filter that ensures after transfer a leader with
// existed peer, the placement restriction will not become worse.
// Note that it only worked when PlacementRules enabled otherwise it will always permit the sourceStore.
func NewPlacementLeaderSafeguard(scope string, cluster opt.Cluster, region *core.RegionInfo, sourceStore *core.StoreInfo) Filter {
	if cluster.IsPlacementRulesEnabled() {
		return newRuleLeaderFitFilter(scope, cluster, region, sourceStore.GetID())
	}
	return nil
}

type engineFilter struct {
	scope      string
	constraint placement.LabelConstraint
}

// NewEngineFilter creates a filter that only keeps allowedEngines.
func NewEngineFilter(scope string, allowedEngines ...string) Filter {
	return &engineFilter{
		scope:      scope,
		constraint: placement.LabelConstraint{Key: "engine", Op: "in", Values: allowedEngines},
	}
}

func (f *engineFilter) Scope() string {
	return f.scope
}

func (f *engineFilter) Type() string {
	return "engine-filter"
}

func (f *engineFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.constraint.MatchStore(store)
}

func (f *engineFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return f.constraint.MatchStore(store)
}

type ordinaryEngineFilter struct {
	scope      string
	constraint placement.LabelConstraint
}

// NewOrdinaryEngineFilter creates a filter that only keeps ordinary engine stores.
func NewOrdinaryEngineFilter(scope string) Filter {
	return &ordinaryEngineFilter{
		scope:      scope,
		constraint: placement.LabelConstraint{Key: "engine", Op: "notIn", Values: allSpeicalEngines},
	}
}

func (f *ordinaryEngineFilter) Scope() string {
	return f.scope
}

func (f *ordinaryEngineFilter) Type() string {
	return "ordinary-engine-filter"
}

func (f *ordinaryEngineFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.constraint.MatchStore(store)
}

func (f *ordinaryEngineFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return f.constraint.MatchStore(store)
}

type specialUseFilter struct {
	scope      string
	constraint placement.LabelConstraint
}

// NewSpecialUseFilter creates a filter that filters out normal stores.
// By default, all stores that are not marked with a special use will be filtered out.
// Specify the special use label if you want to include the special stores.
func NewSpecialUseFilter(scope string, allowUses ...string) Filter {
	var values []string
	for _, v := range allSpecialUses {
		if slice.NoneOf(allowUses, func(i int) bool { return allowUses[i] == v }) {
			values = append(values, v)
		}
	}
	return &specialUseFilter{
		scope:      scope,
		constraint: placement.LabelConstraint{Key: SpecialUseKey, Op: "in", Values: values},
	}
}

func (f *specialUseFilter) Scope() string {
	return f.scope
}

func (f *specialUseFilter) Type() string {
	return "special-use-filter"
}

func (f *specialUseFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	if store.IsLowSpace(opt.GetLowSpaceRatio()) {
		return true
	}
	return !f.constraint.MatchStore(store)
}

func (f *specialUseFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return !f.constraint.MatchStore(store)
}

const (
	// SpecialUseKey is the label used to indicate special use storage.
	SpecialUseKey = "specialUse"
	// SpecialUseHotRegion is the hot region value of special use label
	SpecialUseHotRegion = "hotRegion"
	// SpecialUseReserved is the reserved value of special use label
	SpecialUseReserved = "reserved"

	// EngineKey is the label key used to indicate engine.
	EngineKey = "engine"
	// EngineTiFlash is the tiflash value of the engine label.
	EngineTiFlash = "tiflash"
)

var allSpecialUses = []string{SpecialUseHotRegion, SpecialUseReserved}
var allSpeicalEngines = []string{EngineTiFlash}

type isolationFilter struct {
	scope          string
	locationLabels []string
	constraintSet  [][]string
}

// NewIsolationFilter creates a filter that filters out stores with isolationLevel
// For example, a region has 3 replicas in z1, z2 and z3 individually.
// With isolationLevel = zone, if the region on z1 is down, we need to filter out z2 and z3
// because these two zones already have one of the region's replicas on them.
// We need to choose a store on z1 or z4 to place the new replica to meet the isolationLevel explicitly and forcibly.
func NewIsolationFilter(scope, isolationLevel string, locationLabels []string, regionStores []*core.StoreInfo) Filter {
	isolationFilter := &isolationFilter{
		scope:          scope,
		locationLabels: locationLabels,
		constraintSet:  make([][]string, 0),
	}
	// Get which idx this isolationLevel at according to locationLabels
	var isolationLevelIdx int
	for level, label := range locationLabels {
		if label == isolationLevel {
			isolationLevelIdx = level
			break
		}
	}
	// Collect all constraints for given isolationLevel
	for _, regionStore := range regionStores {
		constraintList := make([]string, 0)
		for i := 0; i <= isolationLevelIdx; i++ {
			constraintList = append(constraintList, regionStore.GetLabelValue(locationLabels[i]))
		}
		isolationFilter.constraintSet = append(isolationFilter.constraintSet, constraintList)
	}
	return isolationFilter
}

func (f *isolationFilter) Scope() string {
	return f.scope
}

func (f *isolationFilter) Type() string {
	return "isolation-filter"
}

func (f *isolationFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return true
}

func (f *isolationFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	// No isolation constraint to fit
	if len(f.constraintSet) <= 0 {
		return true
	}
	for _, constrainList := range f.constraintSet {
		match := true
		for idx, constraint := range constrainList {
			// Check every constraint in constrainList
			match = store.GetLabelValue(f.locationLabels[idx]) == constraint && match
		}
		if len(constrainList) > 0 && match {
			return false
		}
	}
	return true
}
