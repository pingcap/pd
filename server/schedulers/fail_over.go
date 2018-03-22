// Copyright 2018 PingCAP, Inc.
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
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	log "github.com/sirupsen/logrus"
)

func init() {
	schedule.RegisterScheduler("fail-over", func(limiter *schedule.Limiter, args []string) (schedule.Scheduler, error) {
		return newFailOverScheduler(limiter), nil
	})
}

type failOverScheduler struct {
	*baseScheduler
}

func newFailOverScheduler(limiter *schedule.Limiter) schedule.Scheduler {
	base := newBaseScheduler(limiter)
	return &failOverScheduler{
		baseScheduler: base,
	}
}

func (f *failOverScheduler) GetName() string {
	return "fail-over-balance"
}

func (f *failOverScheduler) GetType() string {
	return "fail-over"
}

func (f *failOverScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return f.limiter.OperatorCount(schedule.OpReplica) < cluster.GetReplicaScheduleLimit()
}

func (f *failOverScheduler) Schedule(cluster schedule.Cluster, opInfluence schedule.OpInfluence) *schedule.Operator {
	stores := cluster.GetStores()
	for _, store := range stores {
		if store.DownTime() > cluster.GetMaxStoreDownTime() {
			op := f.handleDownStore(cluster, store)
			if op != nil {
				op.SetPriorityLevel(core.HighPriority)
				return op
			}
		}
		if store.IsOffline() {
			op := f.handleOfflineStore(cluster, store)
			if op != nil {
				op.SetPriorityLevel(core.HighPriority)
				return op
			}
		}
		if store.IsLowSpace() {
			op := f.handleLowSpaceStore(cluster, store)
			if op != nil {
				op.SetPriorityLevel(core.HighPriority)
				return op
			}
		}
	}
	return nil
}

func (f *failOverScheduler) handleDownStore(cluster schedule.Cluster, store *core.StoreInfo) *schedule.Operator {
	region := cluster.RandLeaderRegion(store.GetId(), false)
	if region != nil {
		log.Warnf("[region %d] Down Store [%v] have leader region, Mabye have network problem.", region.GetId(), store)
		return nil
	}
	region = cluster.RandFollowerRegion(store.GetId(), false)
	if region == nil {
		return nil
	}
	if region.HealthPeerCount() < cluster.GetMaxReplicas()/2+1 {
		log.Errorf("[region %d] region unhealth: %s", region.GetId(), region)
		return nil
	}

	peer := region.GetStorePeer(store.GetId())
	if peer == nil {
		return nil
	}
	downPeer := region.GetDownPeer(peer.GetId())
	if downPeer == nil {
		log.Warnf("[region %d] peer %v not down in down store", region.GetId(), peer)
	}
	return schedule.CreateRemovePeerOperator("removeDownReplica", cluster, schedule.OpReplica, region, peer.GetStoreId())
}

func (f *failOverScheduler) transferOutHotRegion(cluster schedule.Cluster, store *core.StoreInfo) *schedule.Operator {
	region := cluster.RandHotRegionFromStore(store.GetId(), schedule.ReadFlow)
	if region != nil {

	}
	return nil
}

func (f *failOverScheduler) handleOfflineStore(cluster schedule.Cluster, store *core.StoreInfo) *schedule.Operator {
	region := cluster.RandLeaderRegion(store.GetId(), false)
	if region == nil {
		region = cluster.RandFollowerRegion(store.GetId(), false)
	}
	if region == nil {
		log.Info("offline region nil")
		return nil
	}
	if region.HealthPeerCount() < cluster.GetMaxReplicas()/2+1 {
		log.Errorf("[region %d] region unhealth: %v", region)
		return nil
	}
	peer := region.GetStorePeer(store.GetId())
	if peer == nil {
		return nil
	}
	// Check the number of replicas first.
	if len(region.Peers) > cluster.GetMaxReplicas() && region.HealthPeerCount() > cluster.GetMaxReplicas()/2+1 {
		return schedule.CreateRemovePeerOperator("removeExtraOfflineReplica", cluster, schedule.OpReplica, region, peer.GetStoreId())
	}
	// Consider we have 3 peers (A, B, C), we set the store that contains C to
	// offline while C is pending. If we generate an operator that adds a replica
	// D then removes C, D will not be successfully added util C is normal again.
	// So it's better to remove C directly.
	if region.GetPendingPeer(peer.GetId()) != nil {
		return schedule.CreateRemovePeerOperator("removePendingOfflineReplica", cluster, schedule.OpReplica, region, peer.GetStoreId())
	}
	if region.GetDownPeer(peer.GetId()) != nil {
		return schedule.CreateRemovePeerOperator("removeDownOfflineReplica", cluster, schedule.OpReplica, region, peer.GetStoreId())
	}
	checker := schedule.NewReplicaChecker(cluster, nil)
	newPeer := checker.SelectBestReplacedPeerToAddReplica(region, peer)
	if newPeer == nil {
		log.Debugf("[region %d] no best peer to add replica", region.GetId())
		return nil
	}
	return schedule.CreateMovePeerOperator("makeUpOfflineReplica", cluster, region, schedule.OpReplica, peer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
}

func (f *failOverScheduler) handleLowSpaceStore(cluster schedule.Cluster, store *core.StoreInfo) *schedule.Operator {
	return nil
}
