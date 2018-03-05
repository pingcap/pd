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
	"fmt"
	"net/http"

	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type healthHandler struct {
	svr *server.Server
	rd  *render.Render
}

type health struct {
	Name       string   `json:"name"`
	MemberID   uint64   `json:"member_id"`
	ClientUrls []string `json:"client_urls"`
	Health     bool     `json:"health"`
}

func newHealthHandler(svr *server.Server, rd *render.Render) *healthHandler {
	return &healthHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *healthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	client := h.svr.GetClient()
	members, err := server.GetMembers(client)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	healths := []health{}
	for _, member := range members {
		h := health{
			Name:       member.Name,
			MemberID:   member.MemberId,
			ClientUrls: member.ClientUrls,
			Health:     true,
		}
		for _, cURL := range member.ClientUrls {
			resp, err := doGet(fmt.Sprintf("%s%s%s", cURL, apiPrefix, pingAPI))
			if err != nil {
				h.Health = false
			}
			resp.Body.Close()
		}
		healths = append(healths, h)
	}
	h.rd.JSON(w, http.StatusOK, healths)
}
