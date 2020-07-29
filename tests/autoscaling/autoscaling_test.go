// Copyright 2020 PingCAP, Inc.
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

package autoscalingtest

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/pkg/testutil"
	"github.com/pingcap/pd/v4/tests"
	"go.uber.org/goleak"
)

func Test(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

var _ = Suite(&apiTestSuite{})

type apiTestSuite struct{}

func (s *apiTestSuite) TestAPI(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)

	var jsonStr = []byte(`
{
    "rules":[
        {
            "component":"tikv",
            "cpu_rule":{
                "max_threshold":0.8,
                "min_threshold":0.2,
                "max_count":3,
                "resource_type":[
                    "resource_a",
                    "resource_b"
                ]
            },
            "storage_rule":{
                "min_threshold":0.2,
                "max_count":3,
                "resource_type":[
                    "resource_a"
                ]
            },
            "scale_out_interval_seconds":30,
            "scale_in_interval_seconds":30
        },
        {
            "component":"tidb",
            "cpu_rule":{
                "max_threshold":0.8,
                "min_threshold":0.2,
                "max_count":2,
                "resource_type":[
                    "resource_a"
                ]
            },
            "scale_out_interval_seconds":30,
            "scale_in_interval_seconds":30
        }
    ],
    "resource_expectations":[
        {
            "component":"tikv",
            "cpu_expectation":4,
            "count":8
        },
        {
            "component":"tikv",
            "cpu_expectation":8,
            "count":2
        },
        {
            "component":"tidb",
            "cpu_expectation":2,
            "count":2
        }
    ],
    "resources":[
        {
            "resource_type":"resource_a",
            "cpu":1,
            "memory":8,
            "storage":1000
        },
        {
            "resource_type":"resource_b",
            "cpu":2,
            "memory":4,
            "storage":2000
        }
    ]
}`)
	resp, err := http.Post(leaderServer.GetAddr()+"/autoscaling", "application/json", bytes.NewBuffer(jsonStr))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, 200)
}
