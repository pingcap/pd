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
	"path"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	log "github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	// update timestamp every updateTimestampStep.
	updateTimestampStep  = 50 * time.Millisecond
	updateTimestampGuard = time.Millisecond
	maxLogical           = int64(1 << 18)
)

var (
	zeroTime = time.Time{}
)

type atomicObject struct {
	physical time.Time
	logical  int64
}

func (s *Server) getTimestampPath() string {
	return path.Join(s.rootPath, "timestamp")
}

func (s *Server) loadTimestamp() (time.Time, error) {
	data, err := getValue(s.client, s.getTimestampPath())
	if err != nil {
		return zeroTime, err
	}
	if len(data) == 0 {
		return zeroTime, nil
	}
	return parseTimestamp(data)
}

// save timestamp, if lastTs is 0, we think the timestamp doesn't exist, so create it,
// otherwise, update it.
func (s *Server) saveTimestamp(ts time.Time) error {
	data := uint64ToBytes(uint64(ts.UnixNano()))
	key := s.getTimestampPath()

	resp, err := s.leaderTxn().Then(clientv3.OpPut(key, string(data))).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !resp.Succeeded {
		return errors.New("save timestamp failed, maybe we lost leader")
	}

	s.lastSavedTime = ts

	return nil
}

func (s *Server) syncTimestamp(lease *LeaderLease) error {
	tsoCounter.WithLabelValues("sync").Inc()

	last, err := s.loadTimestamp()
	if err != nil {
		return err
	}

	next := time.Now()
	failpoint.Inject("fallBackSync", func() {
		next = next.Add(time.Hour)
	})

	// If the current system time minus the saved etcd timestamp is less than `updateTimestampGuard`,
	// the timestamp allocation will start from the saved etcd timestamp temporarily.
	if subTimeByWallClock(next, last) < updateTimestampGuard {
		log.Error("system time may be incorrect", zap.Time("last", last), zap.Time("next", next))
		next = last.Add(updateTimestampGuard)
	}

	save := next.Add(s.cfg.TsoSaveInterval.Duration)
	if err = s.saveTimestamp(save); err != nil {
		return err
	}

	tsoCounter.WithLabelValues("sync_ok").Inc()
	log.Info("sync and save timestamp", zap.Time("last", last), zap.Time("save", save), zap.Time("next", next))

	current := &atomicObject{
		physical: next,
	}
	s.lease = lease
	s.ts.Store(current)

	return nil
}

// This function will do two things:
// 1. When the logical time is going to be used up, the current physical time needs to increase.
// 2. If the time window is not enough, which means the saved etcd time minus the next physical time
//    is less than or equal to `updateTimestampGuard`, it will need to be updated and save the
//    next physical time plus `TsoSaveInterval` into etcd.
//
// Here is some constraints that this function must satisfy:
// 1. The physical time is monotonically increasing.
// 2. The saved time is monotonically increasing.
// 3. The physical time is always less than the saved timestamp.
func (s *Server) updateTimestamp() error {
	prev := s.ts.Load().(*atomicObject)
	now := time.Now()

	failpoint.Inject("fallBackUpdate", func() {
		now = now.Add(time.Hour)
	})

	tsoCounter.WithLabelValues("save").Inc()

	jetLag := subTimeByWallClock(now, prev.physical)
	if jetLag > 3*updateTimestampStep {
		log.Warn("clock offset", zap.Duration("jet-lag", jetLag), zap.Time("prev-physical", prev.physical), zap.Time("now", now))
		tsoCounter.WithLabelValues("slow_save").Inc()
	}

	if jetLag < 0 {
		tsoCounter.WithLabelValues("system_time_slow").Inc()
	}

	var next time.Time
	prevLogical := atomic.LoadInt64(&prev.logical)
	// If the system time is greater, it will be synchronized with the system time.
	if jetLag > updateTimestampGuard {
		next = now
	} else if prevLogical > maxLogical/2 {
		// The reason choosing maxLogical/2 here is that it's big enough for common cases.
		// Because there is enough timestamp can be allocated before next update.
		log.Warn("the logical time may be not enough", zap.Int64("prev-logical", prevLogical))
		next = prev.physical.Add(time.Millisecond)
	} else {
		// It will still use the previous physical time to alloc the timestamp.
		tsoCounter.WithLabelValues("skip_save").Inc()
		return nil
	}

	// It is not safe to increase the physical time to `next`.
	// The time window needs to be updated and saved to etcd.
	if subTimeByWallClock(s.lastSavedTime, next) <= updateTimestampGuard {
		save := next.Add(s.cfg.TsoSaveInterval.Duration)
		if err := s.saveTimestamp(save); err != nil {
			return err
		}
	}

	current := &atomicObject{
		physical: next,
		logical:  0,
	}

	s.ts.Store(current)
	metadataGauge.WithLabelValues("tso").Set(float64(next.Unix()))

	return nil
}

const maxRetryCount = 100

func (s *Server) getRespTS(count uint32) (pdpb.Timestamp, error) {
	var resp pdpb.Timestamp

	if count == 0 {
		return resp, errors.New("tso count should be positive")
	}

	for i := 0; i < maxRetryCount; i++ {
		current, ok := s.ts.Load().(*atomicObject)
		if !ok || current.physical == zeroTime {
			log.Error("we haven't synced timestamp ok, wait and retry", zap.Int("retry-count", i))
			time.Sleep(200 * time.Millisecond)
			continue
		}

		resp.Physical = current.physical.UnixNano() / int64(time.Millisecond)
		resp.Logical = atomic.AddInt64(&current.logical, int64(count))
		if resp.Logical >= maxLogical {
			log.Error("logical part outside of max logical interval, please check ntp time",
				zap.Reflect("response", resp),
				zap.Int("retry-count", i))
			tsoCounter.WithLabelValues("logical_overflow").Inc()
			time.Sleep(updateTimestampStep)
			continue
		}
		if s.lease == nil || s.lease.IsExpired() {
			return pdpb.Timestamp{}, errors.New("alloc timestamp failed, lease expired")
		}
		return resp, nil
	}
	return resp, errors.New("can not get timestamp")
}
