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
	. "github.com/pingcap/check"
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
)

var _ = Suite(&testBalancerWorkerSuite{})

type testBalancerWorkerSuite struct {
	ts testBalancerSuite

	balancerWorker *balancerWorker
}

func (s *testBalancerWorkerSuite) getRootPath() string {
	return "test_balancer_worker"
}

func (s *testBalancerWorkerSuite) SetUpSuite(c *C) {
	s.ts.cfg = newBalanceConfig()
	s.ts.cfg.adjust()
}

func (s *testBalancerWorkerSuite) TestBalancerWorker(c *C) {
	clusterInfo := s.ts.newClusterInfo(c)
	c.Assert(clusterInfo, NotNil)

	region, leader := clusterInfo.regions.getRegion([]byte("a"))
	c.Assert(region.GetPeers(), HasLen, 1)
	c.Assert(leader, NotNil)

	s.balancerWorker = newBalancerWorker(clusterInfo, s.ts.cfg)

	// The store id will be 1,2,3,4.
	s.ts.updateStore(c, clusterInfo, 1, 100, 50, 0, 0)
	s.ts.updateStore(c, clusterInfo, 2, 100, 20, 0, 0)
	s.ts.updateStore(c, clusterInfo, 3, 100, 30, 0, 0)
	s.ts.updateStore(c, clusterInfo, 4, 100, 40, 0, 0)

	// Now we have no region to do balance.
	err := s.balancerWorker.doBalance()
	c.Assert(err, IsNil)

	// Add two peers.
	s.ts.addRegionPeer(c, clusterInfo, 4, region, leader)
	s.ts.addRegionPeer(c, clusterInfo, 3, region, leader)

	s.ts.cfg.MaxLeaderCount = 1

	// Now the region is (1,3,4), leader is 1, the balance operators should be
	// leader transfer: 1 -> 3/4
	err = s.balancerWorker.doBalance()
	c.Assert(err, IsNil)

	regionID := region.GetId()
	bop, ok := s.balancerWorker.balanceOperators[regionID]
	c.Assert(ok, IsTrue)
	c.Assert(bop.Ops, HasLen, 1)

	op := bop.Ops[0].(*transferLeaderOperator)
	c.Assert(op.cfg.MaxTransferWaitCount, Equals, defaultMaxTransferWaitCount)
	c.Assert(op.OldLeader.GetStoreId(), Equals, uint64(1))
	newLeaderStoreID := op.NewLeader.GetStoreId()
	c.Assert(newLeaderStoreID, Not(Equals), uint64(1))

	// Now we check the cfg.MaxTransferWaitCount for transferLeaderOperator.
	s.ts.cfg.MaxTransferWaitCount = 2

	ctx := newOpContext(nil, nil)
	ok, res, err := op.Do(ctx, region, leader)
	c.Assert(err, IsNil)
	c.Assert(ok, IsFalse)
	c.Assert(res.GetTransferLeader().GetPeer().GetStoreId(), Equals, newLeaderStoreID)
	c.Assert(op.Count, Equals, 1)

	ok, res, err = op.Do(ctx, region, leader)
	c.Assert(err, IsNil)
	c.Assert(ok, IsFalse)
	c.Assert(res, IsNil)
	c.Assert(op.Count, Equals, 2)

	ok, res, err = op.Do(ctx, region, leader)
	c.Assert(err, NotNil)
	c.Assert(ok, IsFalse)
	c.Assert(res, IsNil)
	c.Assert(op.Count, Equals, 2)

	s.ts.cfg.MaxTransferWaitCount = defaultMaxTransferWaitCount

	// Since we have already cached region balance operator, so recall doBalance will do nothing.
	s.ts.updateStore(c, clusterInfo, 1, 100, 50, 0, 0)
	s.ts.updateStore(c, clusterInfo, 2, 100, 90, 0, 0)
	s.ts.updateStore(c, clusterInfo, 3, 100, 30, 0, 0)
	s.ts.updateStore(c, clusterInfo, 4, 100, 40, 0, 0)

	err = s.balancerWorker.doBalance()
	c.Assert(err, IsNil)

	oldBop := bop
	bop, ok = s.balancerWorker.balanceOperators[regionID]
	c.Assert(ok, IsTrue)
	c.Assert(bop, DeepEquals, oldBop)

	// Try to remove region balance operator cache, but we also have balance expire cache, so
	// we also cannot get a new balancer.
	s.balancerWorker.removeBalanceOperator(regionID)
	c.Assert(s.balancerWorker.balanceOperators, HasLen, 0)
	c.Assert(s.balancerWorker.regionCache.count(), Equals, 1)

	err = s.balancerWorker.doBalance()
	c.Assert(err, IsNil)
	c.Assert(s.balancerWorker.balanceOperators, HasLen, 0)

	// Remove balance expire cache, this time we can get a new balancer now.
	s.balancerWorker.removeRegionCache(regionID)
	c.Assert(s.balancerWorker.balanceOperators, HasLen, 0)
	c.Assert(s.balancerWorker.regionCache.count(), Equals, 0)

	err = s.balancerWorker.doBalance()
	c.Assert(err, IsNil)

	// Now the region is (1,3,4), leader is 3/4, the balance operators should be
	// 1) add peer: 2
	// 2) remove peer: 3/4
	regionID = region.GetId()
	bop, ok = s.balancerWorker.balanceOperators[regionID]
	c.Assert(ok, IsTrue)
	c.Assert(bop.Ops, HasLen, 2)

	op1 := bop.Ops[0].(*changePeerOperator)
	c.Assert(op1.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op1.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

	op2 := bop.Ops[1].(*changePeerOperator)
	c.Assert(op2.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op2.ChangePeer.GetPeer().GetStoreId(), Not(Equals), uint64(2))

	c.Assert(s.balancerWorker.balanceOperators, HasLen, 1)
	c.Assert(s.balancerWorker.regionCache.count(), Equals, 1)
}
