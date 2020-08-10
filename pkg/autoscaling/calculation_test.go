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

package autoscaling

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/pkg/mock/mockcluster"
	"github.com/pingcap/pd/v4/pkg/mock/mockoption"
	"github.com/pingcap/pd/v4/server/core"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&calculationTestSuite{})

type calculationTestSuite struct{}

func (s *calculationTestSuite) TestGetScaledTiKVGroups(c *C) {
	case1 := mockcluster.NewCluster(mockoption.NewScheduleOptions())
	case1.AddLabelsStore(1, 1, map[string]string{})
	case1.AddLabelsStore(2, 1, map[string]string{
		"foo": "bar",
	})
	case1.AddLabelsStore(3, 1, map[string]string{
		"id": "3",
	})

	testcases := []struct {
		name             string
		informer         core.StoreSetInformer
		healthyInstances []instance
		expectedPlan     []*Plan
	}{
		{
			name:     "no scaled tikv group",
			informer: case1,
			healthyInstances: []instance{
				{
					id:      1,
					address: "1",
				},
				{
					id:      2,
					address: "2",
				},
				{
					id:      3,
					address: "3",
				},
			},
			expectedPlan: nil,
		},
	}

	for _, testcase := range testcases {
		c.Logf(testcase.name)
		plans := getScaledTiKVGroups(testcase.informer, testcase.healthyInstances)
		if testcase.expectedPlan == nil {
			c.Assert(plans, IsNil)
		} else {
			c.Assert(plans, DeepEquals, testcase.expectedPlan)
		}
	}
}
