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

package statistics

import (
	"time"

	"github.com/phf/go-queue/queue"
)

type deltaWithInterval struct {
	delta    float64
	interval time.Duration
}

// AvgOverTime maintains change rate in the last interval.
type AvgOverTime struct {
	que      *queue.Queue
	deltaSum float64
	durSum   time.Duration
	interval time.Duration
}

// NewAvgOverTime returns an AvgOverTime with given interval.
func NewAvgOverTime(interval time.Duration) *AvgOverTime {
	return &AvgOverTime{
		que:      queue.New(),
		deltaSum: 0,
		durSum:   0,
		interval: interval,
	}
}

// Get returns change rate in the last interval.
func (aot *AvgOverTime) Get() float64 {
	if aot.durSum.Seconds() < 1 {
		return 0
	}
	return aot.deltaSum / aot.durSum.Seconds()
}

// Add adds recent change to AvgOverTime.
func (aot *AvgOverTime) Add(delta float64, interval time.Duration) {
	aot.que.PushBack(deltaWithInterval{delta, interval})
	aot.deltaSum += delta
	aot.durSum += interval
	if aot.durSum <= aot.interval {
		return
	}
	for aot.que.Len() > 0 {
		front := aot.que.Front().(deltaWithInterval)
		if aot.durSum-front.interval >= aot.interval {
			aot.que.PopFront()
			aot.deltaSum -= front.delta
			aot.durSum -= front.interval
		} else {
			break
		}
	}
}

// Set sets AvgOverTime to the given average.
func (aot *AvgOverTime) Set(avg float64) {
	for aot.que.Len() > 0 {
		aot.que.PopFront()
	}
	aot.deltaSum = avg * aot.interval.Seconds()
	aot.durSum = aot.interval
	aot.que.PushBack(deltaWithInterval{delta: aot.deltaSum, interval: aot.durSum})
}
