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

package checker

import (
	"fmt"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/filter"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/opt"
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
	cluster opt.Cluster
}

// NewReplicaChecker creates a replica checker.
func NewReplicaChecker(cluster opt.Cluster) *ReplicaChecker {
	return &ReplicaChecker{
		cluster: cluster,
	}
}

// Check verifies a region's replicas, creating an operator.Operator if need.
func (r *ReplicaChecker) Check(region *core.RegionInfo) *operator.Operator {
	checkerCounter.WithLabelValues("replica_checker", "check").Inc()
	if op := r.checkDownPeer(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		op.SetPriorityLevel(core.HighPriority)
		return op
	}
	if op := r.checkOfflinePeer(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		op.SetPriorityLevel(core.HighPriority)
		return op
	}
	if op := r.checkMakeUpReplica(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		return op
	}
	if op := r.checkRemoveExtraReplica(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		return op
	}
	if op := r.checkLocationReplacement(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		return op
	}
	return nil
}

func (r *ReplicaChecker) checkDownPeer(region *core.RegionInfo) *operator.Operator {
	if !r.cluster.IsRemoveDownReplicaEnabled() {
		return nil
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
			return nil
		}
		if store.DownTime() < r.cluster.GetMaxStoreDownTime() {
			continue
		}
		if stats.GetDownSeconds() < uint64(r.cluster.GetMaxStoreDownTime().Seconds()) {
			continue
		}

		return r.fixPeer(region, storeID, downStatus)
	}
	return nil
}

func (r *ReplicaChecker) checkOfflinePeer(region *core.RegionInfo) *operator.Operator {
	if !r.cluster.IsReplaceOfflineReplicaEnabled() {
		return nil
	}

	// just skip learner
	if len(region.GetLearners()) != 0 {
		return nil
	}

	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		store := r.cluster.GetStore(storeID)
		if store == nil {
			log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", storeID))
			return nil
		}
		if store.IsUp() {
			continue
		}

		return r.fixPeer(region, storeID, offlineStatus)
	}

	return nil
}

func (r *ReplicaChecker) checkMakeUpReplica(region *core.RegionInfo) *operator.Operator {
	if !r.cluster.IsMakeUpReplicaEnabled() {
		return nil
	}
	if len(region.GetPeers()) >= r.cluster.GetMaxReplicas() {
		return nil
	}
	log.Debug("region has fewer than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
	var (
		strategy     = r.strategy(region)
		regionStores = r.cluster.GetRegionStores(region)
		extraFilters = make([]filter.Filter, 0)
	)
	// IsolationLevel should only be available when PlacementRulesEnabled is false
	if !r.cluster.IsPlacementRulesEnabled() && len(strategy.locationLabels) > 0 && len(strategy.isolationLevel) > 0 {
		extraFilters = append(extraFilters, filter.NewIsolationFilter(replicaCheckerName, strategy.isolationLevel, strategy.locationLabels, regionStores))
	}
	target := strategy.SelectStoreToAdd(regionStores, extraFilters...)
	if target == 0 {
		log.Debug("no store to add replica", zap.Uint64("region-id", region.GetID()))
		checkerCounter.WithLabelValues("replica_checker", "no-target-store").Inc()
		return nil
	}
	newPeer := &metapb.Peer{StoreId: target}
	op, err := operator.CreateAddPeerOperator("make-up-replica", r.cluster, region, newPeer, operator.OpReplica)
	if err != nil {
		log.Debug("create make-up-replica operator fail", zap.Error(err))
		return nil
	}
	return op
}

func (r *ReplicaChecker) checkRemoveExtraReplica(region *core.RegionInfo) *operator.Operator {
	if !r.cluster.IsRemoveExtraReplicaEnabled() {
		return nil
	}
	// when add learner peer, the number of peer will exceed max replicas for a while,
	// just comparing the the number of voters to avoid too many cancel add operator log.
	if len(region.GetVoters()) <= r.cluster.GetMaxReplicas() {
		return nil
	}
	log.Debug("region has more than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
	regionStores := r.cluster.GetRegionStores(region)
	old := r.strategy(region).SelectStoreToRemove(regionStores)
	if old == 0 {
		checkerCounter.WithLabelValues("replica_checker", "no-worst-peer").Inc()
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
	if !r.cluster.IsLocationReplacementEnabled() {
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
	if len(region.GetPeers()) > r.cluster.GetMaxReplicas() {
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
	target := r.strategy(region).SelectStoreToReplace(regionStores, storeID)
	if target == 0 {
		reason := fmt.Sprintf("no-store-%s", status)
		checkerCounter.WithLabelValues("replica_checker", reason).Inc()
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
		locationLabels: r.cluster.GetLocationLabels(),
		isolationLevel: r.cluster.GetIsolationLevel(),
		region:         region,
	}
}
