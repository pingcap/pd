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

import (
	"math"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
)

const (
	storeCacheInterval    = 30 * time.Second
	bootstrapBalanceCount = 10
	bootstrapBalanceDiff  = 2
)

// minBalanceDiff returns the minimal diff to do balance. The formula is based
// on experience to let the diff increase alone with the count slowly.
func minBalanceDiff(count uint64) float64 {
	if count < bootstrapBalanceCount {
		return bootstrapBalanceDiff
	}
	return math.Sqrt(float64(count))
}

// shouldBalance returns true if we should balance the source and target store.
// The min balance diff provides a buffer to make the cluster stable, so that we
// don't need to schedule very frequently.
func shouldBalance(source, target *storeInfo, kind ResourceKind) bool {
	sourceCount := source.resourceCount(kind)
	sourceScore := source.resourceScore(kind)
	targetScore := target.resourceScore(kind)
	if targetScore >= sourceScore {
		return false
	}
	diffRatio := 1 - targetScore/sourceScore
	diffCount := diffRatio * float64(sourceCount)
	return diffCount >= minBalanceDiff(sourceCount)
}

func adjustBalanceLimit(cluster *clusterInfo, kind ResourceKind) uint64 {
	stores := cluster.getStores()
	counts := make([]float64, 0, len(stores))
	for _, s := range stores {
		if s.isUp() {
			counts = append(counts, float64(s.resourceCount(kind)))
		}
	}
	limit, _ := stats.StandardDeviation(stats.Float64Data(counts))
	return maxUint64(1, uint64(limit))
}

type balanceLeaderScheduler struct {
	opt      *scheduleOption
	limit    uint64
	selector Selector
}

func newBalanceLeaderScheduler(opt *scheduleOption) *balanceLeaderScheduler {
	var filters []Filter
	filters = append(filters, newBlockFilter())
	filters = append(filters, newStateFilter(opt))
	filters = append(filters, newHealthFilter(opt))

	return &balanceLeaderScheduler{
		opt:      opt,
		limit:    1,
		selector: newBalanceSelector(leaderKind, filters),
	}
}

func (l *balanceLeaderScheduler) GetName() string {
	return "balance-leader-scheduler"
}

func (l *balanceLeaderScheduler) GetResourceKind() ResourceKind {
	return leaderKind
}

func (l *balanceLeaderScheduler) GetResourceLimit() uint64 {
	return minUint64(l.limit, l.opt.GetLeaderScheduleLimit())
}

func (l *balanceLeaderScheduler) Prepare(cluster *clusterInfo) error { return nil }

func (l *balanceLeaderScheduler) Cleanup(cluster *clusterInfo) {}

func (l *balanceLeaderScheduler) Schedule(cluster *clusterInfo) Operator {
	region, newLeader := scheduleTransferLeader(cluster, l.selector)
	if region == nil {
		return nil
	}

	source := cluster.getStore(region.Leader.GetStoreId())
	target := cluster.getStore(newLeader.GetStoreId())
	if !shouldBalance(source, target, l.GetResourceKind()) {
		return nil
	}
	l.limit = adjustBalanceLimit(cluster, l.GetResourceKind())

	return newTransferLeader(region, newLeader)
}

type balanceRegionScheduler struct {
	opt      *scheduleOption
	rep      *Replication
	cache    *idCache
	limit    uint64
	selector Selector
}

func newBalanceRegionScheduler(opt *scheduleOption) *balanceRegionScheduler {
	cache := newIDCache(storeCacheInterval, 4*storeCacheInterval)

	var filters []Filter
	filters = append(filters, newCacheFilter(cache))
	filters = append(filters, newStateFilter(opt))
	filters = append(filters, newHealthFilter(opt))
	filters = append(filters, newSnapshotCountFilter(opt))
	filters = append(filters, newStorageThresholdFilter(opt))

	return &balanceRegionScheduler{
		opt:      opt,
		rep:      opt.GetReplication(),
		cache:    cache,
		limit:    1,
		selector: newBalanceSelector(regionKind, filters),
	}
}

func (s *balanceRegionScheduler) GetName() string {
	return "balance-region-scheduler"
}

func (s *balanceRegionScheduler) GetResourceKind() ResourceKind {
	return regionKind
}

func (s *balanceRegionScheduler) GetResourceLimit() uint64 {
	return minUint64(s.limit, s.opt.GetRegionScheduleLimit())
}

func (s *balanceRegionScheduler) Prepare(cluster *clusterInfo) error { return nil }

func (s *balanceRegionScheduler) Cleanup(cluster *clusterInfo) {}

func (s *balanceRegionScheduler) Schedule(cluster *clusterInfo) Operator {
	// Select a peer from the store with most regions.
	region, oldPeer := scheduleRemovePeer(cluster, s.selector)
	if region == nil {
		return nil
	}

	// We don't schedule region with abnormal number of replicas.
	if len(region.GetPeers()) != s.rep.GetMaxReplicas() {
		return nil
	}

	op := s.transferPeer(cluster, region, oldPeer)
	if op == nil {
		// We can't transfer peer from this store now, so we add it to the cache
		// and skip it for a while.
		s.cache.set(oldPeer.GetStoreId())
	}
	return op
}

func (s *balanceRegionScheduler) transferPeer(cluster *clusterInfo, region *RegionInfo, oldPeer *metapb.Peer) Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	stores := cluster.getRegionStores(region)
	source := cluster.getStore(oldPeer.GetStoreId())
	scoreGuard := newDistinctScoreFilter(s.rep, stores, source)

	checker := newReplicaChecker(s.opt, cluster)
	newPeer, _ := checker.selectBestPeer(region, scoreGuard)
	if newPeer == nil {
		return nil
	}

	target := cluster.getStore(newPeer.GetStoreId())
	if !shouldBalance(source, target, s.GetResourceKind()) {
		return nil
	}
	s.limit = adjustBalanceLimit(cluster, s.GetResourceKind())

	return newTransferPeer(region, oldPeer, newPeer)
}

// replicaChecker ensures region has the best replicas.
type replicaChecker struct {
	opt     *scheduleOption
	rep     *Replication
	cluster *clusterInfo
	filters []Filter
}

func newReplicaChecker(opt *scheduleOption, cluster *clusterInfo) *replicaChecker {
	var filters []Filter
	filters = append(filters, newHealthFilter(opt))
	filters = append(filters, newSnapshotCountFilter(opt))

	return &replicaChecker{
		opt:     opt,
		rep:     opt.GetReplication(),
		cluster: cluster,
		filters: filters,
	}
}

func (r *replicaChecker) Check(region *RegionInfo) Operator {
	if op := r.checkDownPeer(region); op != nil {
		return op
	}
	if op := r.checkOfflinePeer(region); op != nil {
		return op
	}

	if len(region.GetPeers()) < r.rep.GetMaxReplicas() {
		newPeer, _ := r.selectBestPeer(region, r.filters...)
		if newPeer == nil {
			return nil
		}
		return newAddPeer(region, newPeer)
	}

	if len(region.GetPeers()) > r.rep.GetMaxReplicas() {
		oldPeer, _ := r.selectWorstPeer(region)
		if oldPeer == nil {
			return nil
		}
		return newRemovePeer(region, oldPeer)
	}

	return r.checkBestReplacement(region)
}

// selectBestPeer returns the best peer in other stores.
func (r *replicaChecker) selectBestPeer(region *RegionInfo, filters ...Filter) (*metapb.Peer, float64) {
	// Add some must have filters.
	filters = append(filters, newStateFilter(r.opt))
	filters = append(filters, newStorageThresholdFilter(r.opt))
	filters = append(filters, newExcludedFilter(nil, region.GetStoreIds()))

	var (
		bestStore *storeInfo
		bestScore float64
	)

	// Select the store with best distinct score.
	// If the scores are the same, select the store with minimal region score.
	stores := r.cluster.getRegionStores(region)
	for _, store := range r.cluster.getStores() {
		if filterTarget(store, filters) {
			continue
		}
		score := r.rep.GetDistinctScore(stores, store)
		if bestStore == nil || compareStoreScore(store, score, bestStore, bestScore) > 0 {
			bestStore = store
			bestScore = score
		}
	}

	if bestStore == nil || filterTarget(bestStore, r.filters) {
		return nil, 0
	}

	newPeer, err := r.cluster.allocPeer(bestStore.GetId())
	if err != nil {
		log.Errorf("failed to allocate peer: %v", err)
		return nil, 0
	}
	return newPeer, bestScore
}

// selectWorstPeer returns the worst peer in the region.
func (r *replicaChecker) selectWorstPeer(region *RegionInfo, filters ...Filter) (*metapb.Peer, float64) {
	var (
		worstStore *storeInfo
		worstScore float64
	)

	// Select the store with lowest distinct score.
	// If the scores are the same, select the store with maximal region score.
	stores := r.cluster.getRegionStores(region)
	for _, store := range stores {
		if filterSource(store, filters) {
			continue
		}
		score := r.rep.GetDistinctScore(stores, store)
		if worstStore == nil || compareStoreScore(store, score, worstStore, worstScore) < 0 {
			worstStore = store
			worstScore = score
		}
	}

	if worstStore == nil || filterSource(worstStore, r.filters) {
		return nil, 0
	}
	return region.GetStorePeer(worstStore.GetId()), worstScore
}

// selectBestReplacement returns the best peer to replace the region peer.
func (r *replicaChecker) selectBestReplacement(region *RegionInfo, peer *metapb.Peer) (*metapb.Peer, float64) {
	// Get a new region without the peer we are going to replace.
	newRegion := region.clone()
	newRegion.RemoveStorePeer(peer.GetStoreId())
	return r.selectBestPeer(newRegion, newExcludedFilter(nil, region.GetStoreIds()))
}

func (r *replicaChecker) checkDownPeer(region *RegionInfo) Operator {
	for _, stats := range region.DownPeers {
		peer := stats.GetPeer()
		if peer == nil {
			continue
		}
		store := r.cluster.getStore(peer.GetStoreId())
		if store.downTime() < r.opt.GetMaxStoreDownTime() {
			continue
		}
		if stats.GetDownSeconds() < uint64(r.opt.GetMaxStoreDownTime().Seconds()) {
			continue
		}
		return newRemovePeer(region, peer)
	}
	return nil
}

func (r *replicaChecker) checkOfflinePeer(region *RegionInfo) Operator {
	for _, peer := range region.GetPeers() {
		store := r.cluster.getStore(peer.GetStoreId())
		if store.isUp() {
			continue
		}
		newPeer, _ := r.selectBestPeer(region)
		if newPeer == nil {
			return nil
		}
		return newTransferPeer(region, peer, newPeer)
	}

	return nil
}

func (r *replicaChecker) checkBestReplacement(region *RegionInfo) Operator {
	oldPeer, oldScore := r.selectWorstPeer(region)
	if oldPeer == nil {
		return nil
	}
	newPeer, newScore := r.selectBestReplacement(region, oldPeer)
	if newPeer == nil {
		return nil
	}
	// Make sure the new peer is better than the old peer.
	if newScore <= oldScore {
		return nil
	}
	return newTransferPeer(region, oldPeer, newPeer)
}
