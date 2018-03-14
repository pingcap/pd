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
	"reflect"
	"sync/atomic"
	"time"

	"github.com/pingcap/pd/server/core"
)

// MaxOperatorWaitTime is the duration that if an operator lives longer that it,
// the operator is considered timeout.
const MaxOperatorWaitTime = 10 * time.Minute

// OperatorStep describes the basic scheduling steps that can not be subdivided.
type OperatorStep interface {
	fmt.Stringer
	IsFinish(region *core.RegionInfo) bool
	Influence(opInfluence OpInfluence, region *core.RegionInfo)
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

// Influence calculates the store difference that current step make
func (tl TransferLeader) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	from := opInfluence.GetStoreInfluence(tl.FromStore)
	to := opInfluence.GetStoreInfluence(tl.ToStore)

	from.LeaderSize -= int(region.ApproximateSize)
	from.LeaderCount--
	to.LeaderSize += int(region.ApproximateSize)
	to.LeaderCount++
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

// Influence calculates the store difference that current step make
func (ap AddPeer) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	to := opInfluence.GetStoreInfluence(ap.ToStore)

	to.RegionSize += int(region.ApproximateSize)
	to.RegionCount++
}

// AddLearnerPeer is an OperatorStep that adds a region learner peer.
type AddLearnerPeer struct {
	ToStore, PeerID uint64
}

func (alp AddLearnerPeer) String() string {
	return fmt.Sprintf("add learner peer %v on store %v", alp.PeerID, alp.ToStore)
}

// IsFinish checks if current step is finished.
func (alp AddLearnerPeer) IsFinish(region *core.RegionInfo) bool {
	if p := region.GetStoreLearner(alp.ToStore); p != nil {
		return region.GetPendingLearner(p.GetId()) == nil
	}
	return false
}

// Influence calculates the store difference that current step make
func (alp AddLearnerPeer) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	to := opInfluence.GetStoreInfluence(alp.ToStore)

	to.RegionSize += int(region.ApproximateSize)
	to.RegionCount++
}

// PromoteLearnerPeer is an OperatorStep that promotes a region learner peer to normal voter.
type PromoteLearnerPeer struct {
	ToStore, PeerID uint64
}

func (plp PromoteLearnerPeer) String() string {
	return fmt.Sprintf("promote learner peer %v on store %v to voter", plp.PeerID, plp.ToStore)
}

// IsFinish checks if current step is finished.
func (plp PromoteLearnerPeer) IsFinish(region *core.RegionInfo) bool {
	if p := region.GetStorePeer(plp.ToStore); p != nil {
		return p.GetId() == plp.PeerID
	}
	return false
}

// Influence calculates the store difference that current step make
func (plp PromoteLearnerPeer) Influence(opInfluence OpInfluence, region *core.RegionInfo) {}

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

// Influence calculates the store difference that current step make
func (rp RemovePeer) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	from := opInfluence.GetStoreInfluence(rp.FromStore)

	from.RegionSize -= int(region.ApproximateSize)
	from.RegionCount--
}

// Operator contains execution steps generated by scheduler.
type Operator struct {
	desc        string
	regionID    uint64
	kind        OperatorKind
	steps       []OperatorStep
	currentStep int32
	createTime  time.Time
	stepTime    int64
	level       core.PriorityLevel
}

// NewOperator creates a new operator.
func NewOperator(desc string, regionID uint64, kind OperatorKind, steps ...OperatorStep) *Operator {
	return &Operator{
		desc:       desc,
		regionID:   regionID,
		kind:       kind,
		steps:      steps,
		createTime: time.Now(),
		stepTime:   time.Now().UnixNano(),
		level:      core.NormalPriority,
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

// Kind returns operator's kind.
func (o *Operator) Kind() OperatorKind {
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
			operatorStepDuration.WithLabelValues(reflect.TypeOf(o.steps[int(step)]).Name()).
				Observe(time.Since(time.Unix(0, atomic.LoadInt64(&o.stepTime))).Seconds())
			atomic.StoreInt32(&o.currentStep, step+1)
			atomic.StoreInt64(&o.stepTime, time.Now().UnixNano())
		} else {
			return o.steps[int(step)]
		}
	}
	return nil
}

// SetPriorityLevel set the priority level for operator
func (o *Operator) SetPriorityLevel(level core.PriorityLevel) {
	o.level = level
}

// GetPriorityLevel get the priority level
func (o *Operator) GetPriorityLevel() core.PriorityLevel {
	return o.level
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

// Influence calculates the store difference which unfinished operator steps make
func (o *Operator) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	for step := atomic.LoadInt32(&o.currentStep); int(step) < len(o.steps); step++ {
		if !o.steps[int(step)].IsFinish(region) {
			o.steps[int(step)].Influence(opInfluence, region)
		}
	}
}

// OperatorHistory is used to log and visualize completed operators.
type OperatorHistory struct {
	FinishTime time.Time
	From, To   uint64
	Kind       core.ResourceKind
}

// History transfers the operator's steps to operator histories.
func (o *Operator) History() []OperatorHistory {
	now := time.Now()
	var histories []OperatorHistory
	var addPeerStores, removePeerStores []uint64
	for _, step := range o.steps {
		switch s := step.(type) {
		case TransferLeader:
			histories = append(histories, OperatorHistory{
				FinishTime: now,
				From:       s.FromStore,
				To:         s.ToStore,
				Kind:       core.LeaderKind,
			})
		case AddPeer:
			addPeerStores = append(addPeerStores, s.ToStore)
		case RemovePeer:
			removePeerStores = append(removePeerStores, s.FromStore)
		}
	}
	for i := range addPeerStores {
		if i < len(removePeerStores) {
			histories = append(histories, OperatorHistory{
				FinishTime: now,
				From:       removePeerStores[i],
				To:         addPeerStores[i],
				Kind:       core.RegionKind,
			})
		}
	}
	return histories
}

// CreateRemovePeerOperator creates an Operator that removes a peer from region.
func CreateRemovePeerOperator(desc string, cluster Cluster, kind OperatorKind, region *core.RegionInfo, storeID uint64) *Operator {
	removeKind, steps := removePeerSteps(cluster, region, storeID)
	return NewOperator(desc, region.GetId(), removeKind|kind, steps...)
}

// CreateMovePeerOperator creates an Operator that replaces an old peer with a new peer.
func CreateMovePeerOperator(desc string, cluster Cluster, region *core.RegionInfo, kind OperatorKind, oldStore, newStore uint64, peerID uint64) *Operator {
	removeKind, steps := removePeerSteps(cluster, region, oldStore)
	var st []OperatorStep
	if cluster.IsEnableRaftLearner() {
		st = []OperatorStep{
			AddLearnerPeer{ToStore: newStore, PeerID: peerID},
			PromoteLearnerPeer{ToStore: newStore, PeerID: peerID},
		}
	} else {
		st = []OperatorStep{
			AddPeer{ToStore: newStore, PeerID: peerID},
		}
	}
	steps = append(st, steps...)
	return NewOperator(desc, region.GetId(), removeKind|kind|OpRegion, steps...)
}

// removePeerSteps returns the steps to safely remove a peer. It prevents removing leader by transfer its leadership first.
func removePeerSteps(cluster Cluster, region *core.RegionInfo, storeID uint64) (kind OperatorKind, steps []OperatorStep) {
	if region.Leader != nil && region.Leader.GetStoreId() == storeID {
		for id := range region.GetFollowers() {
			follower := cluster.GetStore(id)
			if follower != nil && !cluster.CheckLabelProperty(RejectLeader, follower.Labels) {
				steps = append(steps, TransferLeader{FromStore: storeID, ToStore: id})
				kind = OpLeader
				break
			}
		}
	}
	steps = append(steps, RemovePeer{FromStore: storeID})
	kind |= OpRegion
	return
}
