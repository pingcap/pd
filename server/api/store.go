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
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type storeInfo struct {
	Store  *metapb.Store       `json:"store"`
	Status *server.StoreStatus `json:"status"`
}

type storesInfo struct {
	Count  int          `json:"count"`
	Stores []*storeInfo `json:"stores"`
}

type storeHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newStoreHandler(svr *server.Server, rd *render.Render) *storeHandler {
	return &storeHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *storeHandler) Get(w http.ResponseWriter, r *http.Request) {
	cluster, err := h.svr.GetRaftCluster()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}
	if cluster == nil {
		h.rd.JSON(w, http.StatusOK, nil)
		return
	}

	vars := mux.Vars(r)
	storeIDStr := vars["id"]
	storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}

	store, status, err := cluster.GetStore(storeID)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}

	storeInfo := &storeInfo{
		Store:  store,
		Status: status,
	}
	storeInfo.Status.Scores = cluster.GetScores(storeInfo.Store, storeInfo.Status)

	h.rd.JSON(w, http.StatusOK, storeInfo)
}

func (h *storeHandler) Delete(w http.ResponseWriter, r *http.Request) {
	cluster, err := h.svr.GetRaftCluster()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}
	if cluster == nil {
		h.rd.JSON(w, http.StatusOK, nil)
		return
	}

	vars := mux.Vars(r)
	storeIDStr := vars["id"]
	storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}

	if err := cluster.RemoveStore(storeID); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}

type storesHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newStoresHandler(svr *server.Server, rd *render.Render) *storesHandler {
	return &storesHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *storesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cluster, err := h.svr.GetRaftCluster()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}
	if cluster == nil {
		h.rd.JSON(w, http.StatusOK, nil)
		return
	}

	stores := cluster.GetStores()
	storesInfo := &storesInfo{
		Count:  len(stores),
		Stores: make([]*storeInfo, 0, len(stores)),
	}

	for _, s := range stores {
		store, status, err := cluster.GetStore(s.GetId())
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err)
			return
		}

		storeInfo := &storeInfo{
			Store:  store,
			Status: status,
		}
		storeInfo.Status.Scores = cluster.GetScores(storeInfo.Store, storeInfo.Status)
		storesInfo.Stores = append(storesInfo.Stores, storeInfo)
	}

	h.rd.JSON(w, http.StatusOK, storesInfo)
}
