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

	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

const maxOperatorWaitTime = 5 * time.Minute

// ResourceKind distinguishes different kinds of resources.
type ResourceKind int

const (
	// UnKnownKind indicates the unknown kind resource
	UnKnownKind ResourceKind = iota
	// AdminKind indicates that specify by admin
	AdminKind
	// LeaderKind indicates the leader kind resource
	LeaderKind
	// RegionKind indicates the region kind resource
	RegionKind
	// PriorityKind indicates the priority kind resource
	PriorityKind
	// OtherKind indicates the other kind resource
	OtherKind
)

var resourceKindToName = map[int]string{
	0: "unknown",
	1: "admin",
	2: "leader",
	3: "region",
	4: "priority",
	5: "other",
}

var resourceNameToValue = map[string]ResourceKind{
	"unknown":  UnKnownKind,
	"admin":    AdminKind,
	"leader":   LeaderKind,
	"region":   RegionKind,
	"priority": PriorityKind,
	"other":    OtherKind,
}

func (k ResourceKind) String() string {
	s, ok := resourceKindToName[int(k)]
	if ok {
		return s
	}
	return resourceKindToName[0]
}

// ParseResourceKind convert string to ResourceKind
func ParseResourceKind(name string) ResourceKind {
	k, ok := resourceNameToValue[name]
	if ok {
		return k
	}
	return UnKnownKind
}

// Operator is an interface to schedule region.
type Operator interface {
	GetRegionID() uint64
	GetResourceKind() ResourceKind
	Do(region *RegionInfo) (*pdpb.RegionHeartbeatResponse, bool)
}

type adminOperator struct {
	Region *RegionInfo `json:"region"`
	Start  time.Time   `json:"start"`
	Ops    []Operator  `json:"ops"`
}

func newAdminOperator(region *RegionInfo, ops ...Operator) *adminOperator {
	return &adminOperator{
		Region: region,
		Start:  time.Now(),
		Ops:    ops,
	}
}

func (op *adminOperator) String() string {
	return fmt.Sprintf("%+v", *op)
}

func (op *adminOperator) GetRegionID() uint64 {
	return op.Region.GetId()
}

func (op *adminOperator) GetResourceKind() ResourceKind {
	return AdminKind
}

func (op *adminOperator) Do(region *RegionInfo) (*pdpb.RegionHeartbeatResponse, bool) {
	// Update region.
	op.Region = region.clone()

	// Do all operators in order.
	for i := 0; i < len(op.Ops); i++ {
		if res, finished := op.Ops[i].Do(region); !finished {
			return res, false
		}
	}

	// Admin operator never ends, remove it from the API.
	return nil, false
}

type regionOperator struct {
	Region *RegionInfo  `json:"region"`
	Start  time.Time    `json:"start"`
	End    time.Time    `json:"end"`
	Index  int          `json:"index"`
	Ops    []Operator   `json:"ops"`
	Kind   ResourceKind `json:"kind"`
}

func newRegionOperator(region *RegionInfo, kind ResourceKind, ops ...Operator) *regionOperator {
	// Do some check here, just fatal because it must be bug.
	if len(ops) == 0 {
		log.Fatalf("[region %d] new region operator with no ops", region.GetId())
	}

	return &regionOperator{
		Region: region,
		Start:  time.Now(),
		Ops:    ops,
		Kind:   kind,
	}
}

func (op *regionOperator) String() string {
	return fmt.Sprintf("%+v", *op)
}

func (op *regionOperator) GetRegionID() uint64 {
	return op.Region.GetId()
}

func (op *regionOperator) GetResourceKind() ResourceKind {
	return op.Kind
}

func (op *regionOperator) Do(region *RegionInfo) (*pdpb.RegionHeartbeatResponse, bool) {
	if time.Since(op.Start) > maxOperatorWaitTime {
		log.Errorf("[region %d] Operator timeout:%s", region.GetId(), op)
		return nil, true
	}

	// Update region.
	op.Region = region.clone()

	// If an operator is not finished, do it.
	for ; op.Index < len(op.Ops); op.Index++ {
		if res, finished := op.Ops[op.Index].Do(region); !finished {
			return res, false
		}
	}

	op.End = time.Now()
	return nil, true
}

type changePeerOperator struct {
	Name       string           `json:"name"`
	RegionID   uint64           `json:"region_id"`
	ChangePeer *pdpb.ChangePeer `json:"change_peer"`
}

func newAddPeerOperator(regionID uint64, peer *metapb.Peer) *changePeerOperator {
	return &changePeerOperator{
		Name:     "add_peer",
		RegionID: regionID,
		ChangePeer: &pdpb.ChangePeer{
			// FIXME: replace with actual ConfChangeType once eraftpb uses proto3.
			ChangeType: pdpb.ConfChangeType_AddNode,
			Peer:       peer,
		},
	}
}

func newRemovePeerOperator(regionID uint64, peer *metapb.Peer) *changePeerOperator {
	return &changePeerOperator{
		Name:     "remove_peer",
		RegionID: regionID,
		ChangePeer: &pdpb.ChangePeer{
			// FIXME: replace with actual ConfChangeType once eraftpb uses proto3.
			ChangeType: pdpb.ConfChangeType_RemoveNode,
			Peer:       peer,
		},
	}
}

func (op *changePeerOperator) String() string {
	return fmt.Sprintf("%+v", *op)
}

func (op *changePeerOperator) GetRegionID() uint64 {
	return op.RegionID
}

func (op *changePeerOperator) GetResourceKind() ResourceKind {
	return RegionKind
}

func (op *changePeerOperator) Do(region *RegionInfo) (*pdpb.RegionHeartbeatResponse, bool) {
	// Check if operator is finished.
	peer := op.ChangePeer.GetPeer()
	switch op.ChangePeer.GetChangeType() {
	case pdpb.ConfChangeType_AddNode:
		if region.GetPendingPeer(peer.GetId()) != nil {
			// Peer is added but not finished.
			return nil, false
		}
		if region.GetPeer(peer.GetId()) != nil {
			// Peer is added and finished.
			return nil, true
		}
	case pdpb.ConfChangeType_RemoveNode:
		if region.GetPeer(peer.GetId()) == nil {
			// Peer is removed.
			return nil, true
		}
	}

	log.Infof("[region %d] Do operator %s {%v}", region.GetId(), op.Name, op.ChangePeer.GetPeer())

	res := &pdpb.RegionHeartbeatResponse{
		ChangePeer: op.ChangePeer,
	}
	return res, false
}

type transferLeaderOperator struct {
	Name      string       `json:"name"`
	RegionID  uint64       `json:"region_id"`
	OldLeader *metapb.Peer `json:"old_leader"`
	NewLeader *metapb.Peer `json:"new_leader"`
}

func newTransferLeaderOperator(regionID uint64, oldLeader, newLeader *metapb.Peer) *transferLeaderOperator {
	return &transferLeaderOperator{
		Name:      "transfer_leader",
		RegionID:  regionID,
		OldLeader: oldLeader,
		NewLeader: newLeader,
	}
}

func (op *transferLeaderOperator) String() string {
	return fmt.Sprintf("%+v", *op)
}

func (op *transferLeaderOperator) GetRegionID() uint64 {
	return op.RegionID
}

func (op *transferLeaderOperator) GetResourceKind() ResourceKind {
	return LeaderKind
}

func (op *transferLeaderOperator) Do(region *RegionInfo) (*pdpb.RegionHeartbeatResponse, bool) {
	// Check if operator is finished.
	if region.Leader.GetId() == op.NewLeader.GetId() {
		return nil, true
	}

	log.Infof("[region %d] Do operator %s,from peer:{%v} to peer:{%v}", region.GetId(), op.Name, op.OldLeader, op.NewLeader)
	res := &pdpb.RegionHeartbeatResponse{
		TransferLeader: &pdpb.TransferLeader{
			Peer: op.NewLeader,
		},
	}
	return res, false
}
