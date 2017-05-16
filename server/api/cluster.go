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

package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/juju/errors"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

var errUnknownStatusOption = errors.New("unKnown status option")

type clusterHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newClusterHandler(svr *server.Server, rd *render.Render) *clusterHandler {
	return &clusterHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *clusterHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetCluster())
}

func (h *clusterHandler) GetRaftClusterBootstrapTime(w http.ResponseWriter, r *http.Request) {
	option := mux.Vars(r)["bootstrap_time"]
	switch option {
	case option:
		data, err := h.svr.GetRaftClusterBootstrapTime()
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusOK, data)
	default:
		h.rd.JSON(w, http.StatusInternalServerError, errUnknownStatusOption.Error())
	}
}
