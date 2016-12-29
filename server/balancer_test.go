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

func (c *testClusterInfo) setStoreBusy(storeID uint64) {
	store := c.getStore(storeID)
	store.stats.IsBusy = true
	store.stats.LastHeartbeatTS = time.Time{}
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

func (c *testClusterInfo) addLabelsStore(storeID uint64, regionCount int, storageRatio float64, labels map[string]string) {
	c.addRegionStore(storeID, regionCount, storageRatio)
	store := c.getStore(storeID)
	for k, v := range labels {
		store.Labels = append(store.Labels, &metapb.StoreLabel{Key: k, Value: v})
	}
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
	store.stats.TotalRegionCount = regionCount
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
	cfg := NewConfig()
	cfg.adjust()
	cfg.Schedule.MinLeaderCount = 1
	cfg.Schedule.MinRegionCount = 1
	cfg.Schedule.LeaderScheduleInterval.Duration = 10 * time.Millisecond
	cfg.Schedule.StorageScheduleInterval.Duration = 10 * time.Millisecond
	opt := newScheduleOption(cfg)
	return &cfg.Schedule, opt
}

var _ = Suite(&testLeaderBalancerSuite{})

type testLeaderBalancerSuite struct{}

func (s *testLeaderBalancerSuite) TestBalance(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	cfg, opt := newTestScheduleConfig()
	lb := newLeaderBalancer(opt)

	cfg.MinLeaderCount = 10
	cfg.MinBalanceDiffRatio = 0.1

	// Add stores 1,2,3,4
	tc.addLeaderStore(1, 6, 30)
	tc.addLeaderStore(2, 7, 30)
	tc.addLeaderStore(3, 8, 30)
	tc.addLeaderStore(4, 9, 30)
	// Add region 1 with leader in store 4 and followers in stores 1,2,3.
	tc.addLeaderRegion(1, 4, 1, 2, 3)

	// Test leaderCountFilter.
	// When leaderCount < 10, no schedule.
	c.Assert(lb.Schedule(cluster), IsNil)
	tc.updateLeaderCount(4, 12, 30)
	// When leaderCount > 10, transfer leader
	// from store 4 (with most leaders) to store 1 (with least leaders).
	checkTransferLeader(c, lb.Schedule(cluster), 4, 1)

	// Test stateFilter.
	// If store 1 is down, it will be filtered,
	// store 2 becomes the store with least leaders.
	tc.setStoreDown(1)
	checkTransferLeader(c, lb.Schedule(cluster), 4, 2)
	// If store 2 is busy, it will be filtered,
	// store 3 becomes the store with least leaders.
	tc.setStoreBusy(2)
	checkTransferLeader(c, lb.Schedule(cluster), 4, 3)

	// Test MinBalanceDiffRatio.
	// When diff leader ratio < MinBalanceDiffRatio, no schedule.
	tc.updateLeaderCount(2, 10, 30)
	tc.updateLeaderCount(3, 10, 30)
	tc.updateLeaderCount(4, 12, 30)
	c.Assert(lb.Schedule(cluster), IsNil)
}

var _ = Suite(&testStorageBalancerSuite{})

type testStorageBalancerSuite struct{}

func (s *testStorageBalancerSuite) TestBalance(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	cfg, opt := newTestScheduleConfig()
	sb := newStorageBalancer(opt)

	opt.SetMaxReplicas(1)
	cfg.MinRegionCount = 10
	cfg.MinBalanceDiffRatio = 0.1

	// Add stores 1,2,3,4.
	tc.addRegionStore(1, 6, 0.1)
	tc.addRegionStore(2, 7, 0.2)
	tc.addRegionStore(3, 8, 0.3)
	tc.addRegionStore(4, 9, 0.4)
	// Add region 1 with leader in store 4.
	tc.addLeaderRegion(1, 4)

	// Test regionCountFilter.
	// When regionCount < 10, no schedule.
	c.Assert(sb.Schedule(cluster), IsNil)
	tc.updateRegionCount(4, 11, 0.4)
	// When regionCount > 11, transfer peer
	// from store 4 (with most regions) to store 1 (with least regions).
	checkTransferPeer(c, sb.Schedule(cluster), 4, 1)

	// Test stateFilter.
	tc.setStoreOffline(1)
	// When store 1 is offline, it will be filtered,
	// store 2 becomes the store with least regions.
	checkTransferPeer(c, sb.Schedule(cluster), 4, 2)

	// Test MaxReplicas.
	opt.SetMaxReplicas(3)
	c.Assert(sb.Schedule(cluster), IsNil)
	opt.SetMaxReplicas(1)
	c.Assert(sb.Schedule(cluster), NotNil)

	// Test MinBalanceDiffRatio.
	// When diff storage ratio < MinBalanceDiffRatio, no schedule.
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

	// Add stores 1,2,3,4.
	tc.addRegionStore(1, 4, 0.4)
	tc.addRegionStore(2, 3, 0.3)
	tc.addRegionStore(3, 2, 0.1)
	tc.addRegionStore(4, 1, 0.2)
	// Add region 1 with leader in store 1 and follower in store 2.
	tc.addLeaderRegion(1, 1, 2)

	// Region has 2 peers, we need to add a new peer.
	region := cluster.getRegion(1)
	checkAddPeer(c, rc.Check(region), 3)

	// Test stateFilter.
	// If store 3 is down, we can add to store 4.
	tc.setStoreDown(3)
	checkAddPeer(c, rc.Check(region), 4)

	// Test snapshotCountFilter.
	// If snapshotCount > MaxSnapshotCount, we can not add peer.
	tc.updateSnapshotCount(4, 3)
	c.Assert(rc.Check(region), IsNil)
	// If snapshotCount < MaxSnapshotCount, we can add peer again.
	tc.updateSnapshotCount(4, 1)
	checkAddPeer(c, rc.Check(region), 4)

	// Add peer in store 4, and we have enough replicas.
	peer4, _ := cluster.allocPeer(4)
	region.Peers = append(region.Peers, peer4)
	c.Assert(rc.Check(region), IsNil)

	// Add peer in store 3, and we have redundant replicas.
	tc.setStoreUp(3)
	peer3, _ := cluster.allocPeer(3)
	region.Peers = append(region.Peers, peer3)
	checkRemovePeer(c, rc.Check(region), 1)
	region.Peers = region.Peers[1:]

	// Peer in store 2 is down, remove it.
	tc.setStoreDown(2)
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(2),
		DownSeconds: proto.Uint64(24 * 60 * 60),
	}
	region.DownPeers = append(region.DownPeers, downPeer)
	checkRemovePeer(c, rc.Check(region), 2)
	region.DownPeers = nil
	c.Assert(rc.Check(region), IsNil)

	// Peer in store 2 is offline, remove it.
	tc.setStoreOffline(3)
	checkRemovePeer(c, rc.Check(region), 3)
}

func checkAddPeer(c *C, bop Operator, storeID uint64) {
	op := bop.(*regionOperator).Ops[0].(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, storeID)
}

func checkRemovePeer(c *C, bop Operator, storeID uint64) {
	op := bop.(*regionOperator).Ops[0].(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, storeID)
}

func checkTransferPeer(c *C, bop Operator, sourceID, targetID uint64) {
	op := bop.(*regionOperator).Ops[0].(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, targetID)
	op = bop.(*regionOperator).Ops[1].(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, sourceID)
}

func checkTransferLeader(c *C, bop Operator, sourceID, targetID uint64) {
	op := bop.(*regionOperator).Ops[0].(*transferLeaderOperator)
	c.Assert(op.OldLeader.GetStoreId(), Equals, sourceID)
	c.Assert(op.NewLeader.GetStoreId(), Equals, targetID)
}
