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
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/prometheus/client_golang/prometheus"
)

func cmdTypes() map[string]string {
	types := make(map[string]string)
	for name := range pdpb.CommandType_value {
		types[name] = convertName(name)
	}

	return types
}

var (
	cmds = cmdTypes()

	cmdCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "cmd",
			Name:      "cmds_total",
			Help:      "Counter of cmds.",
		}, []string{"type"})

	cmdFailedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "cmd",
			Name:      "cmds_failed_total",
			Help:      "Counter of failed cmds.",
		}, []string{"type"})

	cmdDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "cmd",
			Name:      "handle_cmds_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled success cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"type"})

	cmdFailedDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "cmd",
			Name:      "handle_failed_cmds_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of failed handled cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"type"})

	balancerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "balancer",
			Name:      "balancers_count",
			Help:      "Counter of balancers.",
		}, []string{"result"})

	txnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "txn",
			Name:      "txns_count",
			Help:      "Counter of txns.",
		}, []string{"result"})

	txnDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "txn",
			Name:      "handle_txns_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled txns.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"result"})

	randRegionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "cache",
			Name:      "random_region_seconds",
			Help:      "Bucketed histogram of processing time (s) of random regions.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"role"})
)

func init() {
	prometheus.MustRegister(cmdCounter)
	prometheus.MustRegister(cmdFailedCounter)
	prometheus.MustRegister(cmdDuration)
	prometheus.MustRegister(cmdFailedDuration)
	prometheus.MustRegister(balancerCounter)
	prometheus.MustRegister(txnCounter)
	prometheus.MustRegister(txnDuration)
	prometheus.MustRegister(randRegionDuration)
}
