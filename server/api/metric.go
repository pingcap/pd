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

package api

import (
	"fmt"
	"github.com/pingcap/pd/server"
	"net/http"
	"net/url"
)

const prometheusQueryAPI = "/api/v1/query"

type queryMetric struct {
	s *server.Server
}

func newQueryMetric(s *server.Server) *queryMetric {
	return &queryMetric{s: s}
}

func (h *queryMetric) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	metricAddr := h.s.GetConfig().PDServerCfg.MetricStorageAddress
	if metricAddr == "" {
		http.Error(w, "metric storage address doesn't set", http.StatusInternalServerError)
		return
	}
	u, err := url.Parse(metricAddr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	switch u.Scheme {
	case "http", "https":
		r.URL.Path = prometheusQueryAPI
		newCustomReverseProxies([]url.URL{*u}).ServeHTTP(w, r)
	default:
		// TODO: Support read data by self after support store metric data in PD/TiKV.
		http.Error(w, fmt.Sprintf("schema of metric storage address is no support, address: %v", metricAddr), http.StatusInternalServerError)
	}
}
