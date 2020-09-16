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

package checker

import (
	"github.com/pingcap/log"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
)

// JointStateChecker ensures region is in joint state will leave.
type JointStateChecker struct {
	cluster opt.Cluster
}

// NewJointStateChecker creates a joint state checker.
func NewJointStateChecker(cluster opt.Cluster) *JointStateChecker {
	return &JointStateChecker{
		cluster: cluster,
	}
}

// Check verifies a region's role, creating an Operator if need.
func (c *JointStateChecker) Check(region *core.RegionInfo) *operator.Operator {
	if !core.IsInJointState(region.GetPeers()...) {
		return nil
	}
	op, err := operator.CreateLeaveJointStateOperator("leave-joint-state", c.cluster, region)
	if err != nil {
		log.Debug("fail to create leave joint state operator", zap.Error(err))
		return nil
	}
	op.SetPriorityLevel(core.HighPriority)
	return op
}
