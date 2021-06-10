// Copyright 2017 TiKV Project Authors.
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
	"fmt"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
)

const (
	replicaCheckerName = "replica-checker"
)

const (
	offlineStatus = "offline"
	downStatus    = "down"
)

// ReplicaChecker ensures region has the best replicas.
// Including the following:
// Replica number management.
// Unhealthy replica management, mainly used for disaster recovery of TiKV.
// Location management, mainly used for cross data center deployment.
type ReplicaChecker struct {
	cluster             opt.Cluster
	opts                *config.PersistOptions
	regionWaitingList   cache.Cache
	regionPriorityQueue *cache.PriorityQueue
}

// NewReplicaChecker creates a replica checker.
func NewReplicaChecker(cluster opt.Cluster, regionWaitingList cache.Cache, regionPriorityQueue *cache.PriorityQueue) *ReplicaChecker {
	return &ReplicaChecker{
		cluster:             cluster,
		opts:                cluster.GetOpts(),
		regionWaitingList:   regionWaitingList,
		regionPriorityQueue: regionPriorityQueue,
	}
}

// GetType return ReplicaChecker's type
func (r *ReplicaChecker) GetType() string {
	return "replica-checker"
}

// Check verifies a region's replicas, creating an operator.Operator if need.
func (r *ReplicaChecker) Check(region *core.RegionInfo) (op *operator.Operator) {
	checkerCounter.WithLabelValues("replica_checker", "check").Inc()
	abnormalCount := 0
	if downCount, storeID := r.checkDownPeer(region); downCount > 0 {
		op = r.fixPeer(region, storeID, downStatus)
		abnormalCount = abnormalCount + downCount
	}
	if offlineCount, storeID := r.checkOfflinePeer(region); offlineCount > 0 {
		if op == nil {
			op = r.fixPeer(region, storeID, offlineStatus)
		}
		abnormalCount = abnormalCount + offlineCount
	}
	if miss := r.checkMakeUpReplica(region); miss > 0 {
		if op == nil {
			op = r.fixMakeUpReplica(region)
		}
		abnormalCount = abnormalCount + miss
	}
	// It only add region follow only the normal region follow more than majority,
	// the priority is how many follows can it tolerate
	// region-1's replicate set 3, it lose one follow, so it's priority is 0(1-1),it can not loss any follows
	// region-2's replicate set 5, it lose one follow, so it's priority is 1(2-1),it can loss one follow at most
	// region-3's replicate set 3, it lose two follow, so it should not  put into the priority because it lost majority follows
	tolerate := r.opts.GetMaxReplicas() / 2
	if abnormalCount > tolerate {
		log.Warn("region lose majority follow peers, should manual recovery", zap.Uint64("region id", region.GetID()),
			zap.Int("miss peer", abnormalCount))
		return
	} else if abnormalCount <= tolerate && abnormalCount > 0 {
		r.regionPriorityQueue.Push(tolerate-abnormalCount, region.GetID())
	} else {
		r.regionPriorityQueue.RemoveValue(region.GetID())
	}

	if op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		return
	}
	if op = r.checkRemoveExtraReplica(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		return op
	}
	if op = r.checkLocationReplacement(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		return op
	}
	return nil
}

func (r *ReplicaChecker) checkDownPeer(region *core.RegionInfo) (downCount int, sID uint64) {
	if !r.opts.IsRemoveDownReplicaEnabled() {
		return
	}

	for _, stats := range region.GetDownPeers() {
		peer := stats.GetPeer()
		if peer == nil {
			continue
		}
		storeID := peer.GetStoreId()
		store := r.cluster.GetStore(storeID)
		if store == nil {
			log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", storeID))
			return
		}
		if store.DownTime() < r.opts.GetMaxStoreDownTime() {
			continue
		}
		if stats.GetDownSeconds() < uint64(r.opts.GetMaxStoreDownTime().Seconds()) {
			continue
		}
		if downCount <= 0 {
			sID = storeID
		}
		// avoid to add repeat in checkOfflinePeer
		if store.IsUp() {
			downCount = downCount + 1
		}
	}
	return
}

func (r *ReplicaChecker) checkOfflinePeer(region *core.RegionInfo) (offlineCount int, sID uint64) {
	if !r.opts.IsReplaceOfflineReplicaEnabled() {
		return
	}

	// just skip learner
	if len(region.GetLearners()) != 0 {
		return
	}

	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		store := r.cluster.GetStore(storeID)
		if store == nil {
			log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", storeID))
			continue
		}
		if store.IsUp() {
			continue
		}
		if offlineCount <= 0 {
			sID = storeID
		}
		offlineCount = offlineCount + 1
	}
	return
}
func (r *ReplicaChecker) checkMakeUpReplica(region *core.RegionInfo) int {
	if !r.opts.IsMakeUpReplicaEnabled() {
		return 0
	}
	if len(region.GetPeers()) < r.opts.GetMaxReplicas() {
		return r.opts.GetMaxReplicas() - len(region.GetPeers())
	}
	return 0
}

func (r *ReplicaChecker) fixMakeUpReplica(region *core.RegionInfo) *operator.Operator {
	log.Debug("region has fewer than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
	regionStores := r.cluster.GetRegionStores(region)
	target := r.strategy(region).SelectStoreToAdd(regionStores)
	if target == 0 {
		log.Debug("no store to add replica", zap.Uint64("region-id", region.GetID()))
		checkerCounter.WithLabelValues("replica_checker", "no-target-store").Inc()
		r.regionWaitingList.Put(region.GetID(), nil)
		return nil
	}
	newPeer := &metapb.Peer{StoreId: target}
	op, err := operator.CreateAddPeerOperator("make-up-replica", r.cluster, region, newPeer, operator.OpReplica)
	if err != nil {
		log.Debug("create make-up-replica operator fail", errs.ZapError(err))
		return nil
	}
	return op
}

func (r *ReplicaChecker) checkRemoveExtraReplica(region *core.RegionInfo) *operator.Operator {
	if !r.opts.IsRemoveExtraReplicaEnabled() {
		return nil
	}
	// when add learner peer, the number of peer will exceed max replicas for a while,
	// just comparing the the number of voters to avoid too many cancel add operator log.
	if len(region.GetVoters()) <= r.opts.GetMaxReplicas() {
		return nil
	}
	log.Debug("region has more than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
	regionStores := r.cluster.GetRegionStores(region)
	old := r.strategy(region).SelectStoreToRemove(regionStores)
	if old == 0 {
		checkerCounter.WithLabelValues("replica_checker", "no-worst-peer").Inc()
		r.regionWaitingList.Put(region.GetID(), nil)
		return nil
	}
	op, err := operator.CreateRemovePeerOperator("remove-extra-replica", r.cluster, operator.OpReplica, region, old)
	if err != nil {
		checkerCounter.WithLabelValues("replica_checker", "create-operator-fail").Inc()
		return nil
	}
	return op
}

func (r *ReplicaChecker) checkLocationReplacement(region *core.RegionInfo) *operator.Operator {
	if !r.opts.IsLocationReplacementEnabled() {
		return nil
	}

	strategy := r.strategy(region)
	regionStores := r.cluster.GetRegionStores(region)
	oldStore := strategy.SelectStoreToRemove(regionStores)
	if oldStore == 0 {
		checkerCounter.WithLabelValues("replica_checker", "all-right").Inc()
		return nil
	}
	newStore := strategy.SelectStoreToImprove(regionStores, oldStore)
	if newStore == 0 {
		log.Debug("no better peer", zap.Uint64("region-id", region.GetID()))
		checkerCounter.WithLabelValues("replica_checker", "not-better").Inc()
		return nil
	}

	newPeer := &metapb.Peer{StoreId: newStore}
	op, err := operator.CreateMovePeerOperator("move-to-better-location", r.cluster, region, operator.OpReplica, oldStore, newPeer)
	if err != nil {
		checkerCounter.WithLabelValues("replica_checker", "create-operator-fail").Inc()
		return nil
	}
	return op
}

func (r *ReplicaChecker) fixPeer(region *core.RegionInfo, storeID uint64, status string) *operator.Operator {
	// Check the number of replicas first.
	if len(region.GetVoters()) > r.opts.GetMaxReplicas() {
		removeExtra := fmt.Sprintf("remove-extra-%s-replica", status)
		op, err := operator.CreateRemovePeerOperator(removeExtra, r.cluster, operator.OpReplica, region, storeID)
		if err != nil {
			reason := fmt.Sprintf("%s-fail", removeExtra)
			checkerCounter.WithLabelValues("replica_checker", reason).Inc()
			return nil
		}
		return op
	}

	regionStores := r.cluster.GetRegionStores(region)
	target := r.strategy(region).SelectStoreToFix(regionStores, storeID)
	if target == 0 {
		reason := fmt.Sprintf("no-store-%s", status)
		checkerCounter.WithLabelValues("replica_checker", reason).Inc()
		r.regionWaitingList.Put(region.GetID(), nil)
		log.Debug("no best store to add replica", zap.Uint64("region-id", region.GetID()))
		return nil
	}
	newPeer := &metapb.Peer{StoreId: target}
	replace := fmt.Sprintf("replace-%s-replica", status)
	op, err := operator.CreateMovePeerOperator(replace, r.cluster, region, operator.OpReplica, storeID, newPeer)
	if err != nil {
		reason := fmt.Sprintf("%s-fail", replace)
		checkerCounter.WithLabelValues("replica_checker", reason).Inc()
		return nil
	}
	return op
}

func (r *ReplicaChecker) strategy(region *core.RegionInfo) *ReplicaStrategy {
	return &ReplicaStrategy{
		checkerName:    replicaCheckerName,
		cluster:        r.cluster,
		locationLabels: r.opts.GetLocationLabels(),
		isolationLevel: r.opts.GetIsolationLevel(),
		region:         region,
	}
}
