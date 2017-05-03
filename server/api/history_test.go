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
	"net/http"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testHistorySuite{})

type testHistorySuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
	cli       *http.Client
}

type adminOperator struct {
	Region *server.RegionInfo `json:"region"`
	Start  time.Time          `json:"start"`
	Ops    []server.Operator  `json:"ops"`
}

func (s *testHistorySuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	httpAddr := mustUnixAddrToHTTPAddr(c, addr)
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", httpAddr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
	s.cli = newUnixSocketClient()

	r := newTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	mustRegionHeartBeat(c, s.svr, r)

	r = newTestRegionInfo(3, 1, []byte("b"), []byte("f"))
	mustRegionHeartBeat(c, s.svr, r)
}

func (s *testHistorySuite) TearDownSuite(c *C) {
	s.cleanup()
}

func addTransferLeaderOperator(cli *http.Client, urlPrefix string, regionID uint64, storeID uint64) error {
	req := map[string]interface{}{
		"name":        "transfer-leader",
		"region_id":   regionID,
		"to_store_id": storeID,
	}
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/operators", urlPrefix)
	return postJSON(cli, url, data)
}

func (s *testHistorySuite) TestHistroyOperators(c *C) {
	err := addTransferLeaderOperator(s.cli, s.urlPrefix, 2, 1)
	c.Assert(err, IsNil)
	err = addTransferLeaderOperator(s.cli, s.urlPrefix, 3, 1)
	c.Assert(err, IsNil)

	// gets all history
	url := fmt.Sprintf("%s/history", s.urlPrefix)
	resp, err := s.cli.Get(url)
	c.Assert(err, IsNil)
	res := []interface{}{}
	err = readJSON(resp.Body, &res)
	c.Assert(err, IsNil)
	c.Assert(len(res), Equals, 2)

	// gets history by kind and limit
	tbl := []struct {
		kind   int
		limit  int
		result int
	}{
		{0, 0, 0},
		{0, 1, 1},
		{0, 2, 2},
		{0, 3, 2},
	}

	for _, t := range tbl {
		url = fmt.Sprintf("%s/history/%d/%d", s.urlPrefix, t.kind, t.limit)
		resp, err = s.cli.Get(url)
		c.Assert(err, IsNil)
		res = []interface{}{}
		err = readJSON(resp.Body, &res)
		c.Assert(err, IsNil)
		c.Assert(len(res), Equals, t.result)
	}
}
