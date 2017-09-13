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

package schedule

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/pd/server/core"
)

// MaxOperatorWaitTime is the duration that if an operator lives longer that it,
// the operator is considered timeout.
const MaxOperatorWaitTime = 5 * time.Minute

// OperatorStep describes the basic scheduling steps that can not be subdivided.
type OperatorStep interface {
	fmt.Stringer
	IsFinish(region *core.RegionInfo) bool
}

// TransferLeader is an OperatorStep that transfers a region's leader.
type TransferLeader struct {
	FromStore, ToStore uint64
}

func (tl TransferLeader) String() string {
	return fmt.Sprintf("transfer leader from store %v to store %v", tl.FromStore, tl.ToStore)
}

// IsFinish checks if current step is finished.
func (tl TransferLeader) IsFinish(region *core.RegionInfo) bool {
	return region.Leader.GetStoreId() == tl.ToStore
}

// AddPeer is an OperatorStep that adds a region peer.
type AddPeer struct {
	ToStore, PeerID uint64
}

func (ap AddPeer) String() string {
	return fmt.Sprintf("add peer %v on store %v", ap.PeerID, ap.ToStore)
}

// IsFinish checks if current step is finished.
func (ap AddPeer) IsFinish(region *core.RegionInfo) bool {
	if p := region.GetStorePeer(ap.ToStore); p != nil {
		return region.GetPendingPeer(p.GetId()) == nil
	}
	return false
}

// RemovePeer is an OperatorStep that removes a region peer.
type RemovePeer struct {
	FromStore uint64
}

func (rp RemovePeer) String() string {
	return fmt.Sprintf("remove peer on store %v", rp.FromStore)
}

// IsFinish checks if current step is finished.
func (rp RemovePeer) IsFinish(region *core.RegionInfo) bool {
	return region.GetStorePeer(rp.FromStore) == nil
}

// Operator contains execution steps generated by scheduler.
type Operator struct {
	desc        string
	regionID    uint64
	kind        core.ResourceKind
	steps       []OperatorStep
	currentStep int32
	createTime  time.Time
}

// NewOperator creates a new operator.
func NewOperator(desc string, regionID uint64, kind core.ResourceKind, steps ...OperatorStep) *Operator {
	return &Operator{
		desc:       desc,
		regionID:   regionID,
		kind:       kind,
		steps:      steps,
		createTime: time.Now(),
	}
}

func (o *Operator) String() string {
	s := fmt.Sprintf("%s (kind:%s, region:%v, createAt:%s, currentStep:%v, steps:%+v) ", o.desc, o.kind, o.regionID, o.createTime, atomic.LoadInt32(&o.currentStep), o.steps)
	if o.IsTimeout() {
		s = s + "timeout"
	}
	if o.IsFinish() {
		s = s + "finished"
	}
	return s
}

// MarshalJSON serialize custom types to JSON
func (o *Operator) MarshalJSON() ([]byte, error) {
	return []byte(`"` + o.String() + `"`), nil
}

// Desc returns the operator's short description.
func (o *Operator) Desc() string {
	return o.desc
}

// RegionID returns the region that operator is targeted.
func (o *Operator) RegionID() uint64 {
	return o.regionID
}

// ResourceKind returns operator's resource kind.
func (o *Operator) ResourceKind() core.ResourceKind {
	return o.kind
}

// ElapsedTime returns duration since it was created.
func (o *Operator) ElapsedTime() time.Duration {
	return time.Since(o.createTime)
}

// Len returns the operator's steps count.
func (o *Operator) Len() int {
	return len(o.steps)
}

// Step returns the i-th step.
func (o *Operator) Step(i int) OperatorStep {
	if i >= 0 && i < len(o.steps) {
		return o.steps[i]
	}
	return nil
}

// Check checks if current step is finished, returns next step to take action.
// It's safe to be called by multiple goroutine concurrently.
func (o *Operator) Check(region *core.RegionInfo) OperatorStep {
	for step := atomic.LoadInt32(&o.currentStep); int(step) < len(o.steps); step++ {
		if o.steps[int(step)].IsFinish(region) {
			atomic.StoreInt32(&o.currentStep, step+1)
		} else {
			return o.steps[int(step)]
		}
	}
	return nil
}

// IsFinish checks if all steps are finished.
func (o *Operator) IsFinish() bool {
	return atomic.LoadInt32(&o.currentStep) >= int32(len(o.steps))
}

// IsTimeout checks the operator's create time and determines if it is timeout.
func (o *Operator) IsTimeout() bool {
	if o.IsFinish() {
		return false
	}
	return time.Since(o.createTime) > MaxOperatorWaitTime
}

// CreateRemovePeerOperator creates an Operator that removes a peer from region.
// It prevents removing leader by tranfer its leadership first.
func CreateRemovePeerOperator(desc string, region *core.RegionInfo, storeID uint64) *Operator {
	if region.Leader != nil && region.Leader.GetStoreId() == storeID {
		if follower := region.GetFollower(); follower != nil {
			steps := []OperatorStep{
				TransferLeader{FromStore: region.Leader.GetStoreId(), ToStore: follower.GetStoreId()},
				RemovePeer{FromStore: storeID},
			}
			return NewOperator(desc, region.GetId(), core.RegionKind, steps...)
		}
	}
	return NewOperator(desc, region.GetId(), core.RegionKind, RemovePeer{FromStore: storeID})
}

// CreateMovePeerOperator creates an Operator that replaces an old peer with a
// new peer. It prevents removing leader by transfer its leadership first.
func CreateMovePeerOperator(desc string, region *core.RegionInfo, kind core.ResourceKind, oldStore, newStore uint64, peerID uint64) *Operator {
	if region.Leader != nil && region.Leader.GetStoreId() == oldStore {
		newLeader := newStore
		if follower := region.GetFollower(); follower != nil {
			newLeader = follower.GetStoreId()
		}
		steps := []OperatorStep{
			AddPeer{ToStore: newStore, PeerID: peerID},
			TransferLeader{FromStore: region.Leader.GetStoreId(), ToStore: newLeader},
			RemovePeer{FromStore: oldStore},
		}
		return NewOperator(desc, region.GetId(), kind, steps...)
	}
	steps := []OperatorStep{
		AddPeer{ToStore: newStore, PeerID: peerID},
		RemovePeer{FromStore: oldStore},
	}
	return NewOperator(desc, region.GetId(), kind, steps...)
}
