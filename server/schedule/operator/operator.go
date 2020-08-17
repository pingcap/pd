// Copyright 2016 TiKV Project Authors.
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

package operator

import (
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/schedule/placement"
)

const (
	// OperatorExpireTime is the duration that when an operator is not started
	// after it, the operator will be considered expired.
	OperatorExpireTime = 3 * time.Second
	// LeaderOperatorWaitTime is the duration that when a leader operator runs
	// longer than it, the operator will be considered timeout.
	LeaderOperatorWaitTime = 10 * time.Second
	// RegionOperatorWaitTime is the duration that when a region operator runs
	// longer than it, the operator will be considered timeout.
	RegionOperatorWaitTime = 10 * time.Minute
)

// Cluster provides an overview of a cluster's regions distribution.
type Cluster interface {
	opt.Options
	GetStore(id uint64) *core.StoreInfo
	AllocID() (uint64, error)
	FitRegion(region *core.RegionInfo) *placement.RegionFit
}

// Operator contains execution steps generated by scheduler.
type Operator struct {
	desc        string
	brief       string
	regionID    uint64
	regionEpoch *metapb.RegionEpoch
	kind        OpKind
	steps       []OpStep
	currentStep int32
	status      OpStatusTracker
	stepTime    int64
	level       core.PriorityLevel
	Counters    []prometheus.Counter
}

// NewOperator creates a new operator.
func NewOperator(desc, brief string, regionID uint64, regionEpoch *metapb.RegionEpoch, kind OpKind, steps ...OpStep) *Operator {
	level := core.NormalPriority
	if kind&OpAdmin != 0 {
		level = core.HighPriority
	}
	return &Operator{
		desc:        desc,
		brief:       brief,
		regionID:    regionID,
		regionEpoch: regionEpoch,
		kind:        kind,
		steps:       steps,
		status:      NewOpStatusTracker(),
		level:       level,
	}
}

func (o *Operator) String() string {
	stepStrs := make([]string, len(o.steps))
	for i := range o.steps {
		stepStrs[i] = o.steps[i].String()
	}
	s := fmt.Sprintf("%s {%s} (kind:%s, region:%v(%v,%v), createAt:%s, startAt:%s, currentStep:%v, steps:[%s])", o.desc, o.brief, o.kind, o.regionID, o.regionEpoch.GetVersion(), o.regionEpoch.GetConfVer(), o.GetCreateTime(), o.GetStartTime(), atomic.LoadInt32(&o.currentStep), strings.Join(stepStrs, ", "))
	if o.CheckSuccess() {
		s = s + " finished"
	}
	if o.CheckTimeout() {
		s = s + " timeout"
	}
	return s
}

// MarshalJSON serializes custom types to JSON.
func (o *Operator) MarshalJSON() ([]byte, error) {
	return []byte(`"` + o.String() + `"`), nil
}

// Desc returns the operator's short description.
func (o *Operator) Desc() string {
	return o.desc
}

// SetDesc sets the description for the operator.
func (o *Operator) SetDesc(desc string) {
	o.desc = desc
}

// AttachKind attaches an operator kind for the operator.
func (o *Operator) AttachKind(kind OpKind) {
	o.kind |= kind
}

// RegionID returns the region that operator is targeted.
func (o *Operator) RegionID() uint64 {
	return o.regionID
}

// RegionEpoch returns the region's epoch that is attached to the operator.
func (o *Operator) RegionEpoch() *metapb.RegionEpoch {
	return o.regionEpoch
}

// Kind returns operator's kind.
func (o *Operator) Kind() OpKind {
	return o.kind
}

// Status returns operator status.
func (o *Operator) Status() OpStatus {
	return o.status.Status()
}

// GetReachTimeOf returns the time when operator reaches the given status.
func (o *Operator) GetReachTimeOf(st OpStatus) time.Time {
	return o.status.ReachTimeOf(st)
}

// GetCreateTime gets the create time of operator.
func (o *Operator) GetCreateTime() time.Time {
	return o.status.ReachTimeOf(CREATED)
}

// ElapsedTime returns duration since it was created.
func (o *Operator) ElapsedTime() time.Duration {
	return time.Since(o.GetCreateTime())
}

// Start sets the operator to STARTED status, returns whether succeeded.
func (o *Operator) Start() bool {
	if o.status.To(STARTED) {
		atomic.StoreInt64(&o.stepTime, time.Now().UnixNano())
		return true
	}
	return false
}

// HasStarted returns whether operator has started.
func (o *Operator) HasStarted() bool {
	return !o.GetStartTime().IsZero()
}

// GetStartTime gets the start time of operator.
func (o *Operator) GetStartTime() time.Time {
	return o.status.ReachTimeOf(STARTED)
}

// RunningTime returns duration since it started.
func (o *Operator) RunningTime() time.Duration {
	if o.HasStarted() {
		return time.Since(o.GetStartTime())
	}
	return 0
}

// IsEnd checks if the operator is at and end status.
func (o *Operator) IsEnd() bool {
	return o.status.IsEnd()
}

// CheckSuccess checks if all steps are finished, and update the status.
func (o *Operator) CheckSuccess() bool {
	if atomic.LoadInt32(&o.currentStep) >= int32(len(o.steps)) {
		return o.status.To(SUCCESS) || o.Status() == SUCCESS
	}
	return false
}

// Cancel marks the operator canceled.
func (o *Operator) Cancel() bool {
	return o.status.To(CANCELED)
}

// Replace marks the operator replaced.
func (o *Operator) Replace() bool {
	return o.status.To(REPLACED)
}

// CheckExpired checks if the operator is expired, and update the status.
func (o *Operator) CheckExpired() bool {
	return o.status.CheckExpired(OperatorExpireTime)
}

// CheckTimeout checks if the operator is timeout, and update the status.
func (o *Operator) CheckTimeout() bool {
	if o.CheckSuccess() {
		return false
	}
	if o.kind&OpRegion != 0 {
		return o.status.CheckTimeout(RegionOperatorWaitTime)
	}
	return o.status.CheckTimeout(LeaderOperatorWaitTime)
}

// Len returns the operator's steps count.
func (o *Operator) Len() int {
	return len(o.steps)
}

// Step returns the i-th step.
func (o *Operator) Step(i int) OpStep {
	if i >= 0 && i < len(o.steps) {
		return o.steps[i]
	}
	return nil
}

// Check checks if current step is finished, returns next step to take action.
// If operator is at an end status, check returns nil.
// It's safe to be called by multiple goroutine concurrently.
// FIXME: metrics is not thread-safe
func (o *Operator) Check(region *core.RegionInfo) OpStep {
	if o.IsEnd() {
		return nil
	}
	// CheckTimeout will call CheckSuccess first
	defer func() { _ = o.CheckTimeout() }()
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

// ConfVerChanged returns the number of confver has consumed by steps
func (o *Operator) ConfVerChanged(region *core.RegionInfo) int {
	total := 0
	current := atomic.LoadInt32(&o.currentStep)
	if current == int32(len(o.steps)) {
		current--
	}
	// including current step, it may has taken effects in this heartbeat
	for _, step := range o.steps[0 : current+1] {
		if step.ConfVerChanged(region) {
			total++
		}
	}
	return total
}

// SetPriorityLevel sets the priority level for operator.
func (o *Operator) SetPriorityLevel(level core.PriorityLevel) {
	o.level = level
}

// GetPriorityLevel gets the priority level.
func (o *Operator) GetPriorityLevel() core.PriorityLevel {
	return o.level
}

// UnfinishedInfluence calculates the store difference which unfinished operator steps make.
func (o *Operator) UnfinishedInfluence(opInfluence OpInfluence, region *core.RegionInfo) {
	for step := atomic.LoadInt32(&o.currentStep); int(step) < len(o.steps); step++ {
		if !o.steps[int(step)].IsFinish(region) {
			o.steps[int(step)].Influence(opInfluence, region)
		}
	}
}

// TotalInfluence calculates the store difference which whole operator steps make.
func (o *Operator) TotalInfluence(opInfluence OpInfluence, region *core.RegionInfo) {
	for step := 0; step < len(o.steps); step++ {
		o.steps[step].Influence(opInfluence, region)
	}
}

// OpHistory is used to log and visualize completed operators.
type OpHistory struct {
	FinishTime time.Time
	From, To   uint64
	Kind       core.ResourceKind
}

// History transfers the operator's steps to operator histories.
func (o *Operator) History() []OpHistory {
	now := time.Now()
	var histories []OpHistory
	var addPeerStores, removePeerStores []uint64
	for _, step := range o.steps {
		switch s := step.(type) {
		case TransferLeader:
			histories = append(histories, OpHistory{
				FinishTime: now,
				From:       s.FromStore,
				To:         s.ToStore,
				Kind:       core.LeaderKind,
			})
		case AddPeer:
			addPeerStores = append(addPeerStores, s.ToStore)
		case AddLightPeer:
			addPeerStores = append(addPeerStores, s.ToStore)
		case AddLearner:
			addPeerStores = append(addPeerStores, s.ToStore)
		case AddLightLearner:
			addPeerStores = append(addPeerStores, s.ToStore)
		case RemovePeer:
			removePeerStores = append(removePeerStores, s.FromStore)
		}
	}
	for i := range addPeerStores {
		if i < len(removePeerStores) {
			histories = append(histories, OpHistory{
				FinishTime: now,
				From:       removePeerStores[i],
				To:         addPeerStores[i],
				Kind:       core.RegionKind,
			})
		}
	}
	return histories
}
