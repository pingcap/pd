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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pkg/mock/mockcluster"
	"github.com/pingcap/pd/pkg/mock/mockoption"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

var _ = Suite(&testMergeCheckerSuite{})

type testMergeCheckerSuite struct {
	cluster *mockcluster.Cluster
	mc      *MergeChecker
	regions []*core.RegionInfo
}

func (s *testMergeCheckerSuite) SetUpTest(c *C) {
	cfg := mockoption.NewScheduleOptions()
	cfg.MaxMergeRegionSize = 2
	cfg.MaxMergeRegionKeys = 2
	cfg.EnableTwoWayMerge = true
	s.cluster = mockcluster.NewCluster(cfg)
	s.regions = []*core.RegionInfo{
		core.NewRegionInfo(
			&metapb.Region{
				Id:       1,
				StartKey: []byte(""),
				EndKey:   []byte("a"),
				Peers: []*metapb.Peer{
					{Id: 101, StoreId: 1},
					{Id: 102, StoreId: 2},
				},
			},
			&metapb.Peer{Id: 101, StoreId: 1},
			core.SetApproximateSize(1),
			core.SetApproximateKeys(1),
		),
		core.NewRegionInfo(
			&metapb.Region{
				Id:       2,
				StartKey: []byte("a"),
				EndKey:   []byte("t"),
				Peers: []*metapb.Peer{
					{Id: 103, StoreId: 1},
					{Id: 104, StoreId: 4},
					{Id: 105, StoreId: 5},
				},
			},
			&metapb.Peer{Id: 104, StoreId: 4},
			core.SetApproximateSize(200),
			core.SetApproximateKeys(200),
		),
		core.NewRegionInfo(
			&metapb.Region{
				Id:       3,
				StartKey: []byte("t"),
				EndKey:   []byte("x"),
				Peers: []*metapb.Peer{
					{Id: 106, StoreId: 2},
					{Id: 107, StoreId: 5},
					{Id: 108, StoreId: 6},
				},
			},
			&metapb.Peer{Id: 108, StoreId: 6},
			core.SetApproximateSize(1),
			core.SetApproximateKeys(1),
		),
		core.NewRegionInfo(
			&metapb.Region{
				Id:       4,
				StartKey: []byte("x"),
				EndKey:   []byte(""),
				Peers: []*metapb.Peer{
					{Id: 109, StoreId: 4},
				},
			},
			&metapb.Peer{Id: 109, StoreId: 4},
			core.SetApproximateSize(10),
			core.SetApproximateKeys(10),
		),
	}

	for _, region := range s.regions {
		s.cluster.PutRegion(region)
	}

	s.mc = NewMergeChecker(s.cluster, namespace.DefaultClassifier)
}

func (s *testMergeCheckerSuite) TestBasic(c *C) {
	s.cluster.ScheduleOptions.SplitMergeInterval = time.Hour

	// should with same peer count
	ops := s.mc.Check(s.regions[0])
	c.Assert(ops, IsNil)
	// The size should be small enough.
	ops = s.mc.Check(s.regions[1])
	c.Assert(ops, IsNil)
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	for _, op := range ops {
		op.createTime = op.createTime.Add(-LeaderOperatorWaitTime - time.Second)
		c.Assert(op.IsTimeout(), IsFalse)
		op.createTime = op.createTime.Add(-RegionOperatorWaitTime - time.Second)
		c.Assert(op.IsTimeout(), IsTrue)
	}

	// Disable two way merge
	s.cluster.ScheduleOptions.EnableTwoWayMerge = false
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, IsNil)
	s.cluster.ScheduleOptions.EnableTwoWayMerge = true

	// Skip recently split regions.
	s.mc.RecordRegionSplit(s.regions[2].GetID())
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, IsNil)
	ops = s.mc.Check(s.regions[3])
	c.Assert(ops, IsNil)
}

func (s *testMergeCheckerSuite) checkSteps(c *C, op *Operator, steps []OperatorStep) {
	c.Assert(op.Kind()&OpMerge, Not(Equals), 0)
	c.Assert(steps, NotNil)
	c.Assert(op.Len(), Equals, len(steps))
	for i := range steps {
		c.Assert(op.Step(i), DeepEquals, steps[i])
	}
}

func (s *testMergeCheckerSuite) TestMatchPeers(c *C) {
	// partial store overlap not including leader
	ops := s.mc.Check(s.regions[2])
	s.checkSteps(c, ops[0], []OperatorStep{
		TransferLeader{FromStore: 6, ToStore: 5},
		AddLearner{ToStore: 1, PeerID: 1},
		PromoteLearner{ToStore: 1, PeerID: 1},
		RemovePeer{FromStore: 2},
		AddLearner{ToStore: 4, PeerID: 2},
		PromoteLearner{ToStore: 4, PeerID: 2},
		RemovePeer{FromStore: 6},
		MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	s.checkSteps(c, ops[1], []OperatorStep{
		MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// partial store overlap including leader
	newRegion := s.regions[2].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 106, StoreId: 1},
			{Id: 107, StoreId: 5},
			{Id: 108, StoreId: 6},
		}),
		core.WithLeader(&metapb.Peer{Id: 106, StoreId: 1}),
	)
	s.regions[2] = newRegion
	s.cluster.PutRegion(s.regions[2])
	ops = s.mc.Check(s.regions[2])
	s.checkSteps(c, ops[0], []OperatorStep{
		AddLearner{ToStore: 4, PeerID: 3},
		PromoteLearner{ToStore: 4, PeerID: 3},
		RemovePeer{FromStore: 6},
		MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	s.checkSteps(c, ops[1], []OperatorStep{
		MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// all stores overlap
	s.regions[2] = s.regions[2].Clone(core.SetPeers([]*metapb.Peer{
		{Id: 106, StoreId: 1},
		{Id: 107, StoreId: 5},
		{Id: 108, StoreId: 4},
	}))
	s.cluster.PutRegion(s.regions[2])
	ops = s.mc.Check(s.regions[2])
	s.checkSteps(c, ops[0], []OperatorStep{
		MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	s.checkSteps(c, ops[1], []OperatorStep{
		MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// all stores not overlap
	s.regions[2] = s.regions[2].Clone(core.SetPeers([]*metapb.Peer{
		{Id: 109, StoreId: 2},
		{Id: 110, StoreId: 3},
		{Id: 111, StoreId: 6},
	}), core.WithLeader(&metapb.Peer{Id: 109, StoreId: 2}))
	s.cluster.PutRegion(s.regions[2])
	ops = s.mc.Check(s.regions[2])
	s.checkSteps(c, ops[0], []OperatorStep{
		AddLearner{ToStore: 1, PeerID: 4},
		PromoteLearner{ToStore: 1, PeerID: 4},
		RemovePeer{FromStore: 3},
		AddLearner{ToStore: 4, PeerID: 5},
		PromoteLearner{ToStore: 4, PeerID: 5},
		RemovePeer{FromStore: 6},
		AddLearner{ToStore: 5, PeerID: 6},
		PromoteLearner{ToStore: 5, PeerID: 6},
		TransferLeader{FromStore: 2, ToStore: 1},
		RemovePeer{FromStore: 2},
		MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	s.checkSteps(c, ops[1], []OperatorStep{
		MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})
}
