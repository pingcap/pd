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

package api

import (
	"encoding/json"
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	_ "github.com/pingcap/pd/server/schedulers"
)

var _ = Suite(&testScheduleSuite{})

type testScheduleSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testScheduleSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/schedulers", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
	mustPutStore(c, s.svr, 1, metapb.StoreState_Up, nil)
}

func (s *testScheduleSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testScheduleSuite) TestAPI(c *C) {
	type arg struct {
		opt   string
		value interface{}
	}
	cases := []struct {
		name          string
		createdName   string
		args          []arg
		extraTestFunc func(name string, c *C)
	}{
		{name: "balance-leader-scheduler"},
		{name: "balance-hot-region-scheduler"},
		{name: "balance-region-scheduler"},
		{name: "shuffle-leader-scheduler"},
		{name: "shuffle-region-scheduler"},
		{
			name:        "grant-leader-scheduler",
			createdName: "grant-leader-scheduler-1",
			args:        []arg{{"store_id", 1}},
		},
		{
			name:        "evict-leader-scheduler",
			createdName: "evict-leader-scheduler-1",
			args:        []arg{{"store_id", 1}},
		},
		{
			name:        "scatter-range",
			createdName: "scatter-range-test",
			args:        []arg{{"start_key", ""}, {"end_key", ""}, {"range_name", "test"}},
			// Test the scheduler config handler.
			extraTestFunc: func(name string, c *C) {
				resp := make(map[string]interface{})
				listURL := fmt.Sprintf("%s%s%s/%s/list", s.svr.GetAddr(), apiPrefix, server.ScheduleConfigHandlerPath, name)
				c.Assert(readJSON(listURL, &resp), IsNil)
				c.Assert(resp["start-key"], Equals, "")
				c.Assert(resp["end-key"], Equals, "")
				c.Assert(resp["range-name"], Equals, "test")
				resp["start-key"] = "a_00"
				resp["end-key"] = "a_99"
				updateURL := fmt.Sprintf("%s%s%s/%s/config", s.svr.GetAddr(), apiPrefix, server.ScheduleConfigHandlerPath, name)
				body, err := json.Marshal(resp)
				c.Assert(err, IsNil)
				c.Assert(postJSON(updateURL, body), IsNil)
				resp = make(map[string]interface{})
				c.Assert(readJSON(listURL, &resp), IsNil)
				c.Assert(resp["start-key"], Equals, "a_00")
				c.Assert(resp["end-key"], Equals, "a_99")
				c.Assert(resp["range-name"], Equals, "test")
			},
		},
	}
	for _, ca := range cases {
		input := make(map[string]interface{})
		input["name"] = ca.name
		for _, a := range ca.args {
			input[a.opt] = a.value
		}
		body, err := json.Marshal(input)
		c.Assert(err, IsNil)
		s.testAddAndRemoveScheduler(ca.name, ca.createdName, body, ca.extraTestFunc, c)
		s.testPauseAndResumeScheduler(ca.name, ca.createdName, body, ca.extraTestFunc, c)
	}

	// test pause and resume all schedulers.

	// add schedulers.
	cases = cases[:3]
	for _, ca := range cases {
		input := make(map[string]interface{})
		input["name"] = ca.name
		for _, a := range ca.args {
			input[a.opt] = a.value
		}
		body, err := json.Marshal(input)
		c.Assert(err, IsNil)
		s.addScheduler(ca.name, ca.createdName, body, ca.extraTestFunc, c)
	}

	// test pause all schedulers.
	input := make(map[string]interface{})
	input["delay"] = "5"
	pauseArgs, err := json.Marshal(input)
	c.Assert(err, IsNil)
	err = postJSON(s.urlPrefix+"/all", pauseArgs)
	c.Assert(err, IsNil)
	time.Sleep(time.Second)
	handler := s.svr.GetHandler()
	for _, ca := range cases {
		createdName := ca.createdName
		if createdName == "" {
			createdName = ca.name
		}
		isPaused, err := handler.IsSchedulerPause(createdName)
		c.Assert(err, IsNil)
		c.Assert(isPaused, Equals, true)
	}
	time.Sleep(time.Second * 4)
	for _, ca := range cases {
		createdName := ca.createdName
		if createdName == "" {
			createdName = ca.name
		}
		isPaused, err := handler.IsSchedulerPause(createdName)
		c.Assert(err, IsNil)
		c.Assert(isPaused, Equals, false)
	}

	// test resume all schedulers.
	err = postJSON(s.urlPrefix+"/all", pauseArgs)
	c.Assert(err, IsNil)
	input["delay"] = "0"
	pauseArgs, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = postJSON(s.urlPrefix+"/all", pauseArgs)
	c.Assert(err, IsNil)
	for _, ca := range cases {
		createdName := ca.createdName
		if createdName == "" {
			createdName = ca.name
		}
		isPaused, err := handler.IsSchedulerPause(createdName)
		c.Assert(err, IsNil)
		c.Assert(isPaused, Equals, false)
	}

	// delete schedulers.
	for _, ca := range cases {
		createdName := ca.createdName
		if createdName == "" {
			createdName = ca.name
		}
		s.deleteScheduler(createdName, c)
	}

}

func (s *testScheduleSuite) addScheduler(name, createdName string, body []byte, extraTest func(string, *C), c *C) {
	if createdName == "" {
		createdName = name
	}
	err := postJSON(s.urlPrefix, body)
	c.Assert(err, IsNil)

	if extraTest != nil {
		extraTest(createdName, c)
	}
}

func (s *testScheduleSuite) deleteScheduler(createdName string, c *C) {
	deleteURL := fmt.Sprintf("%s/%s", s.urlPrefix, createdName)
	err := doDelete(deleteURL)
	c.Assert(err, IsNil)
}

func (s *testScheduleSuite) testPauseAndResumeScheduler(name, createdName string, body []byte, extraTest func(string, *C), c *C) {
	if createdName == "" {
		createdName = name
	}
	err := postJSON(s.urlPrefix, body)
	c.Assert(err, IsNil)
	handler := s.svr.GetHandler()
	sches, err := handler.GetSchedulers()
	c.Assert(err, IsNil)
	c.Assert(sches[0], Equals, createdName)

	// test pause.
	input := make(map[string]interface{})
	input["delay"] = "5"
	pauseArgs, err := json.Marshal(input)
	c.Assert(err, IsNil)
	err = postJSON(s.urlPrefix+"/"+createdName, pauseArgs)
	c.Assert(err, IsNil)
	time.Sleep(time.Second)
	isPaused, err := handler.IsSchedulerPause(createdName)
	c.Assert(err, IsNil)
	c.Assert(isPaused, Equals, true)
	time.Sleep(time.Second * 4)
	isPaused, err = handler.IsSchedulerPause(createdName)
	c.Assert(err, IsNil)
	c.Assert(isPaused, Equals, false)

	// test resume.
	input = make(map[string]interface{})
	input["delay"] = "5"
	pauseArgs, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = postJSON(s.urlPrefix+"/"+createdName, pauseArgs)
	c.Assert(err, IsNil)
	input["delay"] = "0"
	pauseArgs, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = postJSON(s.urlPrefix+"/"+createdName, pauseArgs)
	c.Assert(err, IsNil)
	isPaused, err = handler.IsSchedulerPause(createdName)
	c.Assert(err, IsNil)
	c.Assert(isPaused, Equals, false)

	if extraTest != nil {
		extraTest(createdName, c)
	}

	s.deleteScheduler(createdName, c)
}

func (s *testScheduleSuite) testAddAndRemoveScheduler(name, createdName string, body []byte, extraTest func(string, *C), c *C) {
	if createdName == "" {
		createdName = name
	}
	err := postJSON(s.urlPrefix, body)
	c.Assert(err, IsNil)
	handler := s.svr.GetHandler()
	sches, err := handler.GetSchedulers()
	c.Assert(err, IsNil)
	c.Assert(sches[0], Equals, createdName)

	if extraTest != nil {
		extraTest(createdName, c)
	}

	deleteURL := fmt.Sprintf("%s/%s", s.urlPrefix, createdName)
	err = doDelete(deleteURL)
	c.Assert(err, IsNil)
}
