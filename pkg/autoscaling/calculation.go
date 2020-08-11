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
	"math"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/server/cluster"
)

// MetricsTimeDuration is used to get the metrics of a certain time period.
// TODO: adjust the value or make it configurable.
var MetricsTimeDuration = 5 * time.Second

func calculate(rc *cluster.RaftCluster, strategy *Strategy) []*Plan {
	var plans []*Plan
	var afterQuota uint64
	if tikvPlans, tikvAfterQuota := getPlans(rc, strategy, TiKV); tikvPlans != nil {
		plans = append(plans, tikvPlans...)
		afterQuota += tikvAfterQuota
	}
	if tidbPlans, tidbAfterQuota := getPlans(rc, strategy, TiDB); tidbPlans != nil {
		plans = append(plans, tidbPlans...)
		afterQuota += tidbAfterQuota
	}
	if exceedMaxCPUQuota(strategy, afterQuota) {
		return nil
	}
	return plans
}

func getPlans(rc *cluster.RaftCluster, strategy *Strategy, component ComponentType) ([]*Plan, uint64) {
	var instances []string
	if component == TiKV {
		instances = filterTiKVInstances(rc)
	} else {
		instances = getTiDBInstances()
	}

	if len(instances) == 0 {
		return nil, 0
	}

	totalCPUUseTime := getTotalCPUUseTime(component, instances, MetricsTimeDuration)
	currentQuota := getTotalCPUQuota(component, instances)
	totalCPUTime := float64(currentQuota) * MetricsTimeDuration.Seconds()
	usage := totalCPUUseTime / totalCPUTime
	maxThreshold, minThreshold := getCPUThresholdByComponent(strategy, component)

	// TODO: add metrics to show why it triggers scale in/out.
	if usage > maxThreshold {
		scaleOutQuota := (totalCPUUseTime - totalCPUTime*maxThreshold) / MetricsTimeDuration.Seconds()
		return calculateScaleOutPlan(rc, strategy, component, scaleOutQuota, currentQuota)
	}
	if usage < minThreshold {
		scaleInQuota := (totalCPUTime*minThreshold - totalCPUUseTime) / MetricsTimeDuration.Seconds()
		return calculateScaleInPlan(rc, strategy, component, scaleInQuota), 0
	}
	return nil, 0
}

func filterTiKVInstances(rc *cluster.RaftCluster) []string {
	var instances []string
	stores := rc.GetStores()
	for _, store := range stores {
		if store.GetState() == metapb.StoreState_Up {
			instances = append(instances, store.GetAddress())
		}
	}
	return instances
}

// TODO: get TiDB instances
func getTiDBInstances() []string {
	return []string{}
}

// TODO: get total CPU use time through Prometheus.
func getTotalCPUUseTime(component ComponentType, instances []string, duration time.Duration) float64 {
	return 1.0
}

// TODO: get total CPU quota through Prometheus.
func getTotalCPUQuota(component ComponentType, instances []string) uint64 {
	return 1
}

func getCPUThresholdByComponent(strategy *Strategy, component ComponentType) (maxThreshold float64, minThreshold float64) {
	for _, rule := range strategy.Rules {
		if rule.Component == component.String() {
			return rule.CPURule.MaxThreshold, rule.CPURule.MinThreshold
		}
	}
	return 0, 0
}

func getResourcesByComponent(strategy *Strategy, component ComponentType) []*Resource {
	var resTyp []string
	var resources []*Resource
	for _, rule := range strategy.Rules {
		if rule.Component == component.String() {
			resTyp = rule.CPURule.ResourceTypes
		}
	}
	for _, res := range strategy.Resources {
		for _, typ := range resTyp {
			if res.ResourceType == typ {
				resources = append(resources, res)
			}
		}
	}
	return resources
}

func calculateScaleOutPlan(rc *cluster.RaftCluster, strategy *Strategy, component ComponentType, scaleOutQuota float64, currentQuota uint64) ([]*Plan, uint64) {
	groups := getScaledGroupsByComponent(component)
	group := findBestGroupToScaleOut(rc, strategy, scaleOutQuota, groups, component)

	resCPU := float64(getCPUByResourceType(strategy, group.ResourceType))
	// TODO: support customized step
	scaleOutCount := uint64(math.Ceil(scaleOutQuota / resCPU))
	increasedQuota := getCPUByResourceType(strategy, group.ResourceType) * scaleOutCount
	afterQuota := currentQuota + increasedQuota

	// A new group created
	if len(groups) == 0 {
		return []*Plan{&group}, afterQuota
	}

	// update the existed group
	for i, g := range groups {
		if g.ResourceType == group.ResourceType {
			group.Count += scaleOutCount
			groups[i] = &group
		}
	}
	return groups, afterQuota
}

func calculateScaleInPlan(rc *cluster.RaftCluster, strategy *Strategy, component ComponentType, scaleInQuota float64) []*Plan {
	groups := getScaledGroupsByComponent(component)
	if len(groups) == 0 {
		return nil
	}
	group := findBestGroupToScaleIn(rc, strategy, scaleInQuota, groups)
	resCPU := float64(getCPUByResourceType(strategy, group.ResourceType))
	// TODO: support customized step
	scaleInCount := uint64(math.Ceil(scaleInQuota / resCPU))
	for i, g := range groups {
		if g.ResourceType == group.ResourceType {
			if group.Count > scaleInCount {
				group.Count -= scaleInCount
				groups[i] = &group
			} else {
				groups = append(groups[:i], groups[i+1:]...)
			}
		}
	}
	return groups
}

func exceedMaxCPUQuota(strategy *Strategy, totalCPUQuota uint64) bool {
	return totalCPUQuota > strategy.MaxCPUQuota
}

func getCPUByResourceType(strategy *Strategy, resourceType string) uint64 {
	for _, res := range strategy.Resources {
		if res.ResourceType == resourceType {
			return res.CPU
		}
	}
	return 0
}

// TODO: get the scaled groups
func getScaledGroupsByComponent(component ComponentType) []*Plan {
	return []*Plan{}
}

// TODO: implement heterogeneous logic
func findBestGroupToScaleIn(rc *cluster.RaftCluster, strategy *Strategy, scaleInQuota float64, groups []*Plan) Plan {
	return *groups[0]
}

// TODO: implement heterogeneous logic
func findBestGroupToScaleOut(rc *cluster.RaftCluster, strategy *Strategy, scaleOutQuota float64, groups []*Plan, component ComponentType) Plan {
	if len(groups) != 0 {
		return *groups[0]
	}

	resources := getResourcesByComponent(strategy, component)
	group := Plan{
		Component:    component.String(),
		Count:        0,
		ResourceType: resources[0].ResourceType,
	}
	return group
}
