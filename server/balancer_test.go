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
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

type testClusterInfo struct {
	*clusterInfo
}

func newTestClusterInfo(cluster *clusterInfo) *testClusterInfo {
	return &testClusterInfo{clusterInfo: cluster}
}

func (c *testClusterInfo) setStoreUp(storeID uint64) {
	store := c.getStore(storeID)
	store.State = metapb.StoreState_Up
	store.stats.LastHeartbeatTS = time.Now()
	c.putStore(store)
}

func (c *testClusterInfo) setStoreDown(storeID uint64) {
	store := c.getStore(storeID)
	store.State = metapb.StoreState_Up
	store.stats.LastHeartbeatTS = time.Time{}
	c.putStore(store)
}

func (c *testClusterInfo) setStoreOffline(storeID uint64) {
	store := c.getStore(storeID)
	store.State = metapb.StoreState_Offline
	c.putStore(store)
}

func (c *testClusterInfo) addLeaderStore(storeID uint64, leaderCount, regionCount int) {
	store := newStoreInfo(&metapb.Store{Id: storeID})
	store.stats.LastHeartbeatTS = time.Now()
	store.stats.TotalRegionCount = regionCount
	store.stats.LeaderRegionCount = leaderCount
	c.putStore(store)
}

func (c *testClusterInfo) addRegionStore(storeID uint64, regionCount int, storageRatio float64) {
	store := newStoreInfo(&metapb.Store{Id: storeID})
	store.stats.LastHeartbeatTS = time.Now()
	store.stats.RegionCount = uint32(regionCount)
	store.stats.Capacity = 100
	store.stats.Available = uint64((1 - storageRatio) * float64(store.stats.Capacity))
	c.putStore(store)
}

func (c *testClusterInfo) addLeaderRegion(regionID uint64, leaderID uint64, followerIds ...uint64) {
	region := &metapb.Region{Id: regionID}
	leader, _ := c.allocPeer(leaderID)
	region.Peers = []*metapb.Peer{leader}
	for _, id := range followerIds {
		peer, _ := c.allocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	c.putRegion(newRegionInfo(region, leader))
}

func (c *testClusterInfo) updateLeaderCount(storeID uint64, leaderCount, regionCount int) {
	store := c.getStore(storeID)
	store.stats.LeaderRegionCount = leaderCount
	c.putStore(store)
}

func (c *testClusterInfo) updateRegionCount(storeID uint64, regionCount int, storageRatio float64) {
	store := c.getStore(storeID)
	store.stats.RegionCount = uint32(regionCount)
	store.stats.Capacity = 100
	store.stats.Available = uint64((1 - storageRatio) * float64(store.stats.Capacity))
	c.putStore(store)
}

func (c *testClusterInfo) updateSnapshotCount(storeID uint64, snapshotCount int) {
	store := c.getStore(storeID)
	store.stats.ApplyingSnapCount = uint32(snapshotCount)
	c.putStore(store)
}

func newTestScheduleConfig() (*ScheduleConfig, *scheduleOption) {
	cfg := newScheduleConfig()
	cfg.adjust()
	opt := newScheduleOption(cfg)
	return cfg, opt
}

var _ = Suite(&testLeaderBalancerSuite{})

type testLeaderBalancerSuite struct{}

func (s *testLeaderBalancerSuite) Test(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	cfg, opt := newTestScheduleConfig()
	lb := newLeaderBalancer(opt)

	cfg.MinLeaderCount = 10
	cfg.MinBalanceDiffRatio = 0.1

	tc.addLeaderStore(1, 6, 30)
	tc.addLeaderStore(2, 7, 30)
	tc.addLeaderStore(3, 8, 30)
	tc.addLeaderStore(4, 9, 30)
	tc.addLeaderRegion(1, 4, 1, 2, 3)

	// Test leaderCountFilter.
	c.Assert(lb.Schedule(cluster), IsNil)
	tc.updateLeaderCount(4, 11, 30)
	checkTransferLeader(c, lb.Schedule(cluster), 4, 1)

	// Test stateFilter.
	tc.setStoreDown(1)
	checkTransferLeader(c, lb.Schedule(cluster), 4, 2)

	// Test MinBalanceDiffRatio.
	tc.updateLeaderCount(2, 10, 30)
	tc.updateLeaderCount(3, 10, 30)
	tc.updateLeaderCount(4, 11, 30)
	c.Assert(lb.Schedule(cluster), IsNil)
}

var _ = Suite(&testStorageBalancerSuite{})

type testStorageBalancerSuite struct{}

func (s *testStorageBalancerSuite) Test(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	cfg, opt := newTestScheduleConfig()
	sb := newStorageBalancer(opt)

	cfg.MinRegionCount = 10
	cfg.MinBalanceDiffRatio = 0.1

	tc.addRegionStore(1, 6, 0.1)
	tc.addRegionStore(2, 7, 0.2)
	tc.addRegionStore(3, 8, 0.3)
	tc.addRegionStore(4, 9, 0.4)
	tc.addLeaderRegion(1, 4)

	// Test regionCountFilter.
	c.Assert(sb.Schedule(cluster), IsNil)
	tc.updateRegionCount(4, 11, 0.4)
	checkTransferPeer(c, sb.Schedule(cluster), 4, 1)

	// Test stateFilter.
	tc.setStoreOffline(1)
	checkTransferPeer(c, sb.Schedule(cluster), 4, 2)

	// Test MinBalanceDiffRatio.
	tc.updateRegionCount(2, 6, 0.4)
	tc.updateRegionCount(3, 7, 0.4)
	tc.updateRegionCount(4, 8, 0.4)
	c.Assert(sb.Schedule(cluster), IsNil)
}

var _ = Suite(&testReplicaCheckerSuite{})

type testReplicaCheckerSuite struct{}

func (s *testReplicaCheckerSuite) Test(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	cfg, opt := newTestScheduleConfig()
	rc := newReplicaChecker(cluster, opt)

	cfg.MaxSnapshotCount = 2
	cluster.putMeta(&metapb.Cluster{Id: 1, MaxPeerCount: 3})

	tc.addRegionStore(1, 4, 0.4)
	tc.addRegionStore(2, 3, 0.3)
	tc.addRegionStore(3, 2, 0.1)
	tc.addRegionStore(4, 1, 0.2)
	tc.addLeaderRegion(1, 1, 2)

	// Region has 2 peers, we need 3.
	region := cluster.getRegion(1)
	checkAddPeer(c, rc.Check(region), 3)
	peer3, _ := cluster.allocPeer(3)
	region.Peers = append(region.Peers, peer3)
	c.Assert(rc.Check(region), IsNil)

	// Peer in store 2 is down, add peer in store 4.
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(2),
		DownSeconds: proto.Uint64(24 * 60 * 60),
	}
	region.DownPeers = append(region.DownPeers, downPeer)
	checkAddPeer(c, rc.Check(region), 4)
	region.DownPeers = nil
	c.Assert(rc.Check(region), IsNil)

	// Peer in store 1 is offline, add peer in store 4.
	tc.setStoreOffline(1)
	checkAddPeer(c, rc.Check(region), 4)

	// Test stateFilter.
	tc.setStoreDown(4)
	c.Assert(rc.Check(region), IsNil)

	// Test snapshotCountFilter.
	tc.setStoreUp(4)
	checkAddPeer(c, rc.Check(region), 4)
	tc.updateSnapshotCount(4, 3)
	c.Assert(rc.Check(region), IsNil)
	tc.updateSnapshotCount(4, 1)
	checkAddPeer(c, rc.Check(region), 4)

	// Remove redundant peer.
	peer4, _ := cluster.allocPeer(4)
	region.Peers = append(region.Peers, peer4)
	checkRemovePeer(c, rc.Check(region), 1)
}

func checkAddPeer(c *C, bop *balanceOperator, storeID uint64) {
	op := bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, storeID)
}

func checkRemovePeer(c *C, bop *balanceOperator, storeID uint64) {
	op := bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, storeID)
}

func checkTransferPeer(c *C, bop *balanceOperator, sourceID, targetID uint64) {
	op := bop.Ops[0].(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, targetID)
	op = bop.Ops[1].(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, sourceID)
}

func checkTransferLeader(c *C, bop *balanceOperator, sourceID, targetID uint64) {
	op := bop.Ops[0].(*transferLeaderOperator)
	c.Assert(op.OldLeader.GetStoreId(), Equals, sourceID)
	c.Assert(op.NewLeader.GetStoreId(), Equals, targetID)
}

var _ = Suite(&testBalancerSuite{})

type testBalancerSuite struct {
	testClusterBaseSuite

	cfg *ScheduleConfig
	opt *scheduleOption
}

func (s *testBalancerSuite) getRootPath() string {
	return "test_balancer"
}

func (s *testBalancerSuite) SetUpSuite(c *C) {
	s.cfg = newScheduleConfig()
	s.cfg.adjust()
	s.opt = newScheduleOption(s.cfg)
}

func (s *testBalancerSuite) newClusterInfo(c *C) *clusterInfo {
	clusterInfo := newClusterInfo(newMockIDAllocator())

	// Set cluster info.
	meta := &metapb.Cluster{
		Id:           0,
		MaxPeerCount: 3,
	}
	clusterInfo.putMeta(meta)

	var (
		id   uint64
		peer *metapb.Peer
		err  error
	)

	// Add 4 stores, store id will be 1,2,3,4.
	for i := 1; i < 5; i++ {
		id, err = clusterInfo.allocID()
		c.Assert(err, IsNil)

		addr := fmt.Sprintf("127.0.0.1:%d", i)
		store := s.newStore(c, id, addr)
		clusterInfo.putStore(newStoreInfo(store))
	}

	// Add 1 peer, id will be 5.
	id, err = clusterInfo.allocID()
	c.Assert(err, IsNil)
	peer = s.newPeer(c, 1, id)

	// Add 1 region, id will be 6.
	id, err = clusterInfo.allocID()
	c.Assert(err, IsNil)

	region := s.newRegion(c, id, []byte{}, []byte{}, []*metapb.Peer{peer}, nil)
	clusterInfo.putRegion(newRegionInfo(region, peer))

	stores := clusterInfo.getStores()
	c.Assert(stores, HasLen, 4)

	return clusterInfo
}

func (s *testBalancerSuite) updateStore(c *C, clusterInfo *clusterInfo, storeID uint64, capacity uint64, available uint64,
	sendingSnapCount uint32, receivingSnapCount uint32, applyingSnapCount uint32) {
	stats := &pdpb.StoreStats{
		StoreId:            storeID,
		Capacity:           capacity,
		Available:          available,
		SendingSnapCount:   sendingSnapCount,
		ReceivingSnapCount: receivingSnapCount,
		ApplyingSnapCount:  applyingSnapCount,
	}

	c.Assert(clusterInfo.handleStoreHeartbeat(stats), IsNil)
}

func (s *testBalancerSuite) updateStoreState(c *C, clusterInfo *clusterInfo, storeID uint64, state metapb.StoreState) {
	store := clusterInfo.getStore(storeID)
	store.State = state
	clusterInfo.putStore(store)
}

func (s *testBalancerSuite) addRegionPeer(c *C, clusterInfo *clusterInfo, storeID uint64, region *regionInfo) {
	r := newReplicaChecker(clusterInfo, s.opt)
	bop := r.Check(region)
	c.Assert(bop, NotNil)

	op, ok := bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
	c.Assert(ok, IsTrue)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)

	peer := op.ChangePeer.GetPeer()
	c.Assert(peer.GetStoreId(), Equals, storeID)

	addRegionPeer(c, region.Region, peer)

	clusterInfo.putRegion(region)
}
