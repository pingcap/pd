// Copyright 2020 TiKV Project Authors.
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

package tso

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/election"
	"go.uber.org/zap"
)

// Allocator is a Timestamp Orcale allocator.
type Allocator interface {
	// Initialize is used to initialize a TSO allocator.
	// It will synchronize TSO with etcd and initialize the
	// memory for later allocation work.
	Initialize() error
	// UpdateTSO is used to update the TSO in memory and the time window in etcd.
	UpdateTSO() error
	// SetTSO sets the physical part with given tso. It's mainly used for BR restore
	// and can not forcibly set the TSO smaller than now.
	SetTSO(tso uint64) error
	// GenerateTSO is used to generate a given number of TSOs.
	// Make sure you have initialized the TSO allocator before calling.
	GenerateTSO(count uint32) (pdpb.Timestamp, error)
	// Reset is uesed to reset the TSO allocator.
	Reset()
}

// GlobalTSOAllocator is the global single point TSO allocator.
type GlobalTSOAllocator struct {
	// leadership is used to check the current PD server's leadership
	// to determine whether a tso request could be processed and
	// it's stored as *election.Leadership
	leadership      atomic.Value
	timestampOracle *timestampOracle
}

// NewGlobalTSOAllocator creates a new global TSO allocator.
func NewGlobalTSOAllocator(leadership *election.Leadership, rootPath string, saveInterval time.Duration, maxResetTSGap func() time.Duration) Allocator {
	gta := &GlobalTSOAllocator{
		timestampOracle: &timestampOracle{
			client:        leadership.GetClient(),
			rootPath:      rootPath,
			saveInterval:  saveInterval,
			maxResetTSGap: maxResetTSGap,
		},
	}
	gta.setLeadership(leadership)
	return gta
}

func (gta *GlobalTSOAllocator) getLeadership() *election.Leadership {
	leadership := gta.leadership.Load()
	if leadership == nil {
		return nil
	}
	return leadership.(*election.Leadership)
}

func (gta *GlobalTSOAllocator) setLeadership(leadership *election.Leadership) {
	gta.leadership.Store(leadership)
}

// Initialize will initialize the created global TSO allocator.
func (gta *GlobalTSOAllocator) Initialize() error {
	return gta.timestampOracle.SyncTimestamp(gta.getLeadership())
}

// UpdateTSO is used to update the TSO in memory and the time window in etcd.
func (gta *GlobalTSOAllocator) UpdateTSO() error {
	return gta.timestampOracle.UpdateTimestamp(gta.getLeadership())
}

// SetTSO sets the physical part with given tso.
func (gta *GlobalTSOAllocator) SetTSO(tso uint64) error {
	return gta.timestampOracle.ResetUserTimestamp(gta.getLeadership(), tso)
}

// GenerateTSO is used to generate a given number of TSOs.
// Make sure you have initialized the TSO allocator before calling.
func (gta *GlobalTSOAllocator) GenerateTSO(count uint32) (pdpb.Timestamp, error) {
	var resp pdpb.Timestamp

	if count == 0 {
		return resp, errors.New("tso count should be positive")
	}

	maxRetryCount := 10
	failpoint.Inject("skipRetryGetTS", func() {
		maxRetryCount = 1
	})

	for i := 0; i < maxRetryCount; i++ {
		current := (*atomicObject)(atomic.LoadPointer(&gta.timestampOracle.TSO))
		if current == nil || current.physical == typeutil.ZeroTime {
			// If it's leader, maybe SyncTimestamp hasn't completed yet
			if gta.getLeadership().Check() {
				log.Info("sync hasn't completed yet, wait for a while")
				time.Sleep(200 * time.Millisecond)
				continue
			}
			log.Error("invalid timestamp", zap.Any("timestamp", current), zap.Error(errs.ErrInvalidTimestamp.FastGenByArgs()))
			return pdpb.Timestamp{}, errors.New("can not get timestamp, may be not leader")
		}

		resp.Physical = current.physical.UnixNano() / int64(time.Millisecond)
		resp.Logical = atomic.AddInt64(&current.logical, int64(count))
		if resp.Logical >= maxLogical {
			log.Error("logical part outside of max logical interval, please check ntp time",
				zap.Reflect("response", resp),
				zap.Int("retry-count", i), zap.Error(errs.ErrLogicOverflow.FastGenByArgs()))
			tsoCounter.WithLabelValues("logical_overflow").Inc()
			time.Sleep(UpdateTimestampStep)
			continue
		}
		// In case lease expired after the first check.
		if !gta.getLeadership().Check() {
			return pdpb.Timestamp{}, errors.New("alloc timestamp failed, lease expired")
		}
		return resp, nil
	}
	return resp, errors.New("can not get timestamp")
}

// Reset is uesed to reset the TSO allocator.
func (gta *GlobalTSOAllocator) Reset() {
	gta.timestampOracle.ResetTimestamp()
}
