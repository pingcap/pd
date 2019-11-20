// Copyright 2018 PingCAP, Inc.
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
	"math"
	"math/rand"
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMovingAvg{})

type testMovingAvg struct{}

func addRandData(ma MovingAvg, n int, mx float64) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < n; i++ {
		ma.Add(rand.Float64() * mx)
	}
}

// checkReset checks the Reset works properly.
// emptyValue is the moving average of empty data set.
func (t *testMovingAvg) checkReset(c *C, ma MovingAvg, emptyValue float64) {
	addRandData(ma, 100, 1000)
	ma.Reset()
	c.Assert(ma.Get(), Equals, emptyValue)
}

// checkAddGet checks Add works properly.
func (t *testMovingAvg) checkAdd(c *C, ma MovingAvg, data []float64, expected []float64) {
	c.Assert(len(data), Equals, len(expected))
	for i, x := range data {
		ma.Add(x)
		c.Assert(math.Abs(ma.Get()-expected[i]), LessEqual, 1e-7)
	}
}

// checkSet checks Set = Reset + Add
func (t *testMovingAvg) checkSet(c *C, ma MovingAvg, data []float64, expected []float64) {
	c.Assert(len(data), Equals, len(expected))

	// Reset + Add
	addRandData(ma, 100, 1000)
	ma.Reset()
	t.checkAdd(c, ma, data, expected)

	// Set
	addRandData(ma, 100, 1000)
	ma.Set(data[0])
	c.Assert(ma.Get(), Equals, expected[0])
	t.checkAdd(c, ma, data[1:], expected[1:])
}

type testCase struct {
	ma       MovingAvg
	expected []float64
}

func (t *testMovingAvg) TestMovingAvg(c *C) {
	var empty float64 = 0
	data := []float64{120, 130, 140, 150, 145, 136, 121, 132, 145, 156, 148, 157, 175}
	testCases := []testCase{{
		ma:       NewEMA(0.9),
		expected: []float64{120, 129, 138.9, 148.89, 145.389, 136.9389, 122.59389, 131.059389, 143.6059389, 154.76059389, 148.676059389, 156.1676059389, 173.11676059389},
	}, {
		ma:       NewWMA(5),
		expected: []float64{120.0, 125.0, 130.0, 135.0, 136.0, 137.9333333, 135.4666667, 137.2, 138.6, 141.6, 139.1333333, 145.9333333, 156.53333333},
	}, {
		ma:       NewHMA(5),
		expected: []float64{120.0, 120.5555555, 126.6666667, 141.6666667, 154.6666666, 155.8, 139.5555555, 121.7333333, 119.4444444, 141.2888888, 159.6666666, 163.7111111, 160.5333333},
	}, {
		ma:       NewMedianFilter(5),
		expected: []float64{120, 125, 130, 135, 140, 140, 140, 136, 136, 136, 145, 148, 156},
	},
	}
	for _, test := range testCases {
		c.Assert(test.ma.Get(), Equals, empty)
		t.checkReset(c, test.ma, empty)
		t.checkAdd(c, test.ma, data, test.expected)
		t.checkSet(c, test.ma, data, test.expected)
	}
}
func (t *testMovingAvg) TestTickMovingAvg(c *C) {
	tickMovingAverages := NewTickMovingAverages()
	c.Assert(tickMovingAverages.Get(1), Equals, 0.0)
	tickMovingAverages.Add(1, 3)
	c.Assert(tickMovingAverages.Get(1), Equals, 3.0)
	tickMovingAverages.Add(1, 10)
	c.Assert(tickMovingAverages.Get(1), Equals, 3.0)
	tickMovingAverages.SetInterval(time.Millisecond)
	time.Sleep(time.Millisecond)
	tickMovingAverages.Add(1, 10)
	c.Assert(tickMovingAverages.Get(1), Equals, 4.75)
}
