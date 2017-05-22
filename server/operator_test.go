// Copyright 2017 PingCAP, Inc.
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
	"encoding/json"

	. "github.com/pingcap/check"
)

var _ = Suite(&testResouceKindSuite{})

type testResouceKindSuite struct{}

func (s *testResouceKindSuite) TestString(c *C) {
	tbl := []struct {
		value ResourceKind
		name  string
	}{
		{UnKnownKind, "unknown"},
		{AdminKind, "admin"},
		{LeaderKind, "leader"},
		{RegionKind, "region"},
		{PriorityKind, "priority"},
		{OtherKind, "other"},
		{ResourceKind(404), "unknown"},
	}
	for _, t := range tbl {
		c.Assert(t.value.String(), Equals, t.name)
	}
}

func (s *testResouceKindSuite) TestParseResouceKind(c *C) {
	tbl := []struct {
		name  string
		value ResourceKind
	}{
		{"unknown", UnKnownKind},
		{"admin", AdminKind},
		{"leader", LeaderKind},
		{"region", RegionKind},
		{"priority", PriorityKind},
		{"other", OtherKind},
		{"test", UnKnownKind},
	}
	for _, t := range tbl {
		c.Assert(ParseResourceKind(t.name), Equals, t.value)
	}
}

var _ = Suite(&testOperatorSuite{})

type testOperatorSuite struct{}

func (o *testOperatorSuite) TestOperatorStateString(c *C) {
	tbl := []struct {
		value OperatorState
		name  string
	}{
		{OperatorUnKnownState, "unknown"},
		{OperatorDoing, "doing"},
		{OperatorFinished, "finished"},
		{OperatorTimeOut, "time_out"},
		{OperatorState(404), "unknown"},
	}
	for _, t := range tbl {
		c.Assert(t.value.String(), Equals, t.name)
	}
}

func (o *testOperatorSuite) TestOperatorStateMarshal(c *C) {
	states := []OperatorState{OperatorUnKnownState, OperatorDoing, OperatorFinished, OperatorTimeOut}
	for _, s := range states {
		data, err := json.Marshal(s)
		c.Assert(err, IsNil)
		var newState OperatorState
		err = json.Unmarshal(data, &newState)
		c.Assert(err, IsNil)
		c.Assert(newState, Equals, s)
	}
}
