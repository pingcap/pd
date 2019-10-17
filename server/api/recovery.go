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
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type recoveryHandler struct {
	h  *server.Handler
	rd *render.Render
}

func newRecoveryHandler(handler *server.Handler, rd *render.Render) *recoveryHandler {
	return &recoveryHandler{
		h:  handler,
		rd: rd,
	}
}

func (t *recoveryHandler) ResetTS(w http.ResponseWriter, r *http.Request) {
	tsString := mux.Vars(r)["ts"]
	ts, err := strconv.ParseInt(tsString, 10, 64)
	if err != nil {
		t.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	err = t.h.ResetTS(ts)
	if err != nil {
		t.rd.JSON(w, http.StatusInternalServerError, err.Error())
	}
}
