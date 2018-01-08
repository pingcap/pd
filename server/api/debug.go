// Copyright 2018 PingCAP, Inc.
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
	"io/ioutil"
	"net/http"

	"github.com/pingcap/pd/pkg/logutil"
	"github.com/pingcap/pd/server"
	log "github.com/sirupsen/logrus"
	"github.com/unrolled/render"
)

type debugHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newDebugHandler(svr *server.Server, rd *render.Render) *debugHandler {
	return &debugHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *debugHandler) Handle(w http.ResponseWriter, r *http.Request) {
	var debug bool
	data, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	err = json.Unmarshal(data, &debug)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	level := "info"
	if debug {
		level = "debug"
	}
	h.svr.SetLogLevel(level)
	log.SetLevel(logutil.StringToLogLevel(level))

	h.rd.JSON(w, http.StatusOK, nil)
}
