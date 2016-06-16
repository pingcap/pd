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
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raftpb"
)

var balancerIDs uint64

const (
	// TODO: we can make this as a config flag.
	// maxWaitCount is the heartbeat count when we check whether the operator is successful.
	maxWaitCount = 3
)

// Operator is the interface to do some operations.
type Operator interface {
	// Do does the operator, if finished then return true.
	Do(region *metapb.Region, leader *metapb.Peer) (bool, *pdpb.RegionHeartbeatResponse, error)
}

// balanceOperator is used to do region balance.
type balanceOperator struct {
	id       uint64
	index    int
	start    time.Time
	end      time.Time
	finished bool
	ops      []Operator
	region   *metapb.Region
}

func newBalanceOperator(region *metapb.Region, ops ...Operator) *balanceOperator {
	return &balanceOperator{
		id:     atomic.AddUint64(&balancerIDs, 1),
		ops:    ops,
		region: region,
	}
}

func (bo *balanceOperator) String() string {
	ret := fmt.Sprintf("[balanceOperator]id: %d, index: %d, start: %s, end: %s, finished: %v, region: %v, ops:",
		bo.id, bo.index, bo.start, bo.end, bo.finished, bo.region)

	for i := range bo.ops {
		ret += fmt.Sprintf(" [%d - %v] ", i, bo.ops[i])
	}

	return ret
}

// Check checks whether operator already finished or not.
func (bo *balanceOperator) check(region *metapb.Region, leader *metapb.Peer) (bool, error) {
	if bo.index >= len(bo.ops) {
		bo.finished = true
		return true, nil
	}

	err := checkStaleRegion(bo.region, region)
	if err != nil {
		return false, errors.Trace(err)
	}

	bo.region = cloneRegion(region)

	return false, nil
}

// Do implements Operator.Do interface.
func (bo *balanceOperator) Do(region *metapb.Region, leader *metapb.Peer) (bool, *pdpb.RegionHeartbeatResponse, error) {
	ok, err := bo.check(region, leader)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if ok {
		return true, nil, nil
	}

	finished, res, err := bo.ops[bo.index].Do(region, leader)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if !finished {
		return false, res, nil
	}

	bo.index++

	bo.finished = bo.index >= len(bo.ops)
	return bo.finished, res, nil
}

// getRegionID returns the region id which the operator for balance.
func (bo *balanceOperator) getRegionID() uint64 {
	return bo.region.GetId()
}

// onceOperator is the operator wrapping another operator
// and can be called only once. It will return finished every time.
type onceOperator struct {
	op       Operator
	finished bool
}

func newOnceOperator(op Operator) *onceOperator {
	return &onceOperator{
		op:       op,
		finished: false,
	}
}

func (op *onceOperator) String() string {
	return fmt.Sprintf("[onceOperator]op: %v, finished: %v", op.op, op.finished)
}

// Do implements Operator.Do interface.
func (op *onceOperator) Do(region *metapb.Region, leader *metapb.Peer) (bool, *pdpb.RegionHeartbeatResponse, error) {
	if op.finished {
		return true, nil, nil
	}

	op.finished = true
	_, resp, err := op.op.Do(region, leader)
	return true, resp, errors.Trace(err)
}

// changePeerOperator is used to do peer change.
type changePeerOperator struct {
	changePeer *pdpb.ChangePeer
}

func newAddPeerOperator(peer *metapb.Peer) *changePeerOperator {
	return &changePeerOperator{
		changePeer: &pdpb.ChangePeer{
			ChangeType: raftpb.ConfChangeType_AddNode.Enum(),
			Peer:       peer,
		},
	}
}

func newRemovePeerOperator(peer *metapb.Peer) *changePeerOperator {
	return &changePeerOperator{
		changePeer: &pdpb.ChangePeer{
			ChangeType: raftpb.ConfChangeType_RemoveNode.Enum(),
			Peer:       peer,
		},
	}
}

func (co *changePeerOperator) String() string {
	return fmt.Sprintf("[changePeerOperator]changePeer: %v", co.changePeer)
}

// check checks whether operator already finished or not.
func (co *changePeerOperator) check(region *metapb.Region, leader *metapb.Peer) (bool, error) {
	if region == nil {
		return false, errors.New("invalid region")
	}
	if leader == nil {
		return false, errors.New("invalid leader peer")
	}

	if co.changePeer.GetChangeType() == raftpb.ConfChangeType_AddNode {
		if containPeer(region, co.changePeer.GetPeer()) {
			return true, nil
		}
		log.Infof("balance [%s], try to add peer %s", region, co.changePeer.GetPeer())
	} else if co.changePeer.GetChangeType() == raftpb.ConfChangeType_RemoveNode {
		if !containPeer(region, co.changePeer.GetPeer()) {
			return true, nil
		}
		log.Infof("balance [%s], try to remove peer %s", region, co.changePeer.GetPeer())
	}

	return false, nil
}

// Do implements Operator.Do interface.
func (co *changePeerOperator) Do(region *metapb.Region, leader *metapb.Peer) (bool, *pdpb.RegionHeartbeatResponse, error) {
	ok, err := co.check(region, leader)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if ok {
		return true, nil, nil
	}

	res := &pdpb.RegionHeartbeatResponse{
		ChangePeer: co.changePeer,
	}
	return false, res, nil
}

// transferLeaderOperator is used to do leader transfer.
type transferLeaderOperator struct {
	count        int
	maxWaitCount int

	oldLeader *metapb.Peer
	newLeader *metapb.Peer
}

func newTransferLeaderOperator(oldLeader, newLeader *metapb.Peer, waitCount int) *transferLeaderOperator {
	return &transferLeaderOperator{
		oldLeader:    oldLeader,
		newLeader:    newLeader,
		count:        0,
		maxWaitCount: waitCount,
	}
}

func (tlo *transferLeaderOperator) String() string {
	return fmt.Sprintf("[transferLeaderOperator]count: %d, maxWaitCount: %d, oldLeader: %v, newLeader: %v",
		tlo.count, tlo.maxWaitCount, tlo.oldLeader, tlo.newLeader)
}

// check checks whether operator already finished or not.
func (tlo *transferLeaderOperator) check(region *metapb.Region, leader *metapb.Peer) (bool, error) {
	if leader == nil {
		return false, errors.New("invalid leader peer")
	}

	// If the leader has already been changed to new leader, we finish it.
	if leader.GetId() == tlo.newLeader.GetId() {
		return true, nil
	}

	// If the old leader has been changed but not be new leader, we also finish it.
	if leader.GetId() != tlo.oldLeader.GetId() {
		log.Warnf("old leader %v has changed to %v, but not %v", tlo.oldLeader, leader, tlo.newLeader)
		return true, nil
	}

	log.Infof("balance [%s], try to transfer leader from %s to %s", region, tlo.oldLeader, tlo.newLeader)
	return false, nil
}

// Do implements Operator.Do interface.
func (tlo *transferLeaderOperator) Do(region *metapb.Region, leader *metapb.Peer) (bool, *pdpb.RegionHeartbeatResponse, error) {
	ok, err := tlo.check(region, leader)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if ok {
		return true, nil, nil
	}

	// If tlo.count is greater than 0, then we should check whether it exceeds the tlo.maxWaitCount.
	if tlo.count > 0 {
		if tlo.count >= tlo.maxWaitCount {
			return false, nil, errors.Errorf("transfer leader operator called %d times but still be unsucceessful - %v", tlo.count, tlo)
		}

		tlo.count++
		return false, nil, nil
	}

	res := &pdpb.RegionHeartbeatResponse{
		TransferLeader: &pdpb.TransferLeader{
			Peer: tlo.newLeader,
		},
	}
	tlo.count++
	return false, res, nil
}
