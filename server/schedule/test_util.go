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

package schedule

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
)

// ApplyOperatorStep applies operator step. Only for test purpose.
func ApplyOperatorStep(region *core.RegionInfo, op *operator.Operator) *core.RegionInfo {
	_ = op.Start()
	if step := op.Check(region); step != nil {
		switch s := step.(type) {
		case operator.TransferLeader:
			region = region.Clone(core.WithLeader(region.GetStorePeer(s.ToStore)))
		case operator.AddPeer:
			if region.GetStorePeer(s.ToStore) != nil {
				panic("Add peer that exists")
			}
			peer := &metapb.Peer{
				Id:      s.PeerID,
				StoreId: s.ToStore,
			}
			region = region.Clone(core.WithAddPeer(peer))
		case operator.AddLightPeer:
			if region.GetStorePeer(s.ToStore) != nil {
				panic("Add peer that exists")
			}
			peer := &metapb.Peer{
				Id:      s.PeerID,
				StoreId: s.ToStore,
			}
			region = region.Clone(core.WithAddPeer(peer))
		case operator.RemovePeer:
			if region.GetStorePeer(s.FromStore) == nil {
				panic("Remove peer that doesn't exist")
			}
			if region.GetLeader().GetStoreId() == s.FromStore {
				panic("Cannot remove the leader peer")
			}
			region = region.Clone(core.WithRemoveStorePeer(s.FromStore))
		case operator.AddLearner:
			if region.GetStorePeer(s.ToStore) != nil {
				panic("Add learner that exists")
			}
			peer := &metapb.Peer{
				Id:      s.PeerID,
				StoreId: s.ToStore,
				Role:    metapb.PeerRole_Learner,
			}
			region = region.Clone(core.WithAddPeer(peer))
		case operator.AddLightLearner:
			if region.GetStorePeer(s.ToStore) != nil {
				panic("Add learner that exists")
			}
			peer := &metapb.Peer{
				Id:      s.PeerID,
				StoreId: s.ToStore,
				Role:    metapb.PeerRole_Learner,
			}
			region = region.Clone(core.WithAddPeer(peer))
		case operator.PromoteLearner:
			if region.GetStoreLearner(s.ToStore) == nil {
				panic("Promote peer that doesn't exist")
			}
			peer := &metapb.Peer{
				Id:      s.PeerID,
				StoreId: s.ToStore,
			}
			region = region.Clone(core.WithRemoveStorePeer(s.ToStore), core.WithAddPeer(peer))
		default:
			panic("Unknown operator step")
		}
	}
	return region
}

// ApplyOperator applies operator. Only for test purpose.
func ApplyOperator(mc *mockcluster.Cluster, op *operator.Operator) {
	origin := mc.GetRegion(op.RegionID())
	region := origin
	for !op.IsEnd() {
		region = ApplyOperatorStep(region, op)
	}
	mc.PutRegion(region)
	for id := range region.GetStoreIds() {
		mc.UpdateStoreStatus(id)
	}
	for id := range origin.GetStoreIds() {
		mc.UpdateStoreStatus(id)
	}
}
