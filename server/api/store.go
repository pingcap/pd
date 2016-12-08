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
	"net/url"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pkg/timeutil"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type storeStatus struct {
	*server.StoreStatus
	Uptime timeutil.Duration `json:"uptime"`
}

type storeInfo struct {
	Store  *metapb.Store `json:"store"`
	Status *storeStatus  `json:"status"`
	Scores []int         `json:"scores"`
}

func newStoreInfo(store *metapb.Store, status *server.StoreStatus, scores []int) *storeInfo {
	return &storeInfo{
		Store: store,
		Status: &storeStatus{
			StoreStatus: status,
			Uptime:      timeutil.NewDuration(status.GetUptime()),
		},
		Scores: scores,
	}
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
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, errNotBootstrapped.Error())
		return
	}

	vars := mux.Vars(r)
	storeIDStr := vars["id"]
	storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	store, status, err := cluster.GetStore(storeID)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	storeInfo := newStoreInfo(store, status, cluster.GetScores(store, status))
	h.rd.JSON(w, http.StatusOK, storeInfo)
}

func (h *storeHandler) Delete(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, errNotBootstrapped.Error())
		return
	}

	vars := mux.Vars(r)
	storeIDStr := vars["id"]
	storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	_, force := r.URL.Query()["force"]
	if force {
		err = cluster.BuryStore(storeID, force)
	} else {
		err = cluster.RemoveStore(storeID)
	}

	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
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
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, errNotBootstrapped.Error())
		return
	}

	stores := cluster.GetStores()
	storesInfo := &storesInfo{
		Stores: make([]*storeInfo, 0, len(stores)),
	}

	urlFilter := newStoreStateFilter(r.URL)
	stores = urlFilter.filter(cluster.GetStores())
	for _, s := range stores {
		store, status, err := cluster.GetStore(s.GetId())
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}

		storeInfo := newStoreInfo(store, status, cluster.GetScores(store, status))
		storesInfo.Stores = append(storesInfo.Stores, storeInfo)
	}
	storesInfo.Count = len(storesInfo.Stores)

	h.rd.JSON(w, http.StatusOK, storesInfo)
}

type storeStateFilter struct {
	acceptAll   bool
	acceptState metapb.StoreState
}

func newStoreStateFilter(u *url.URL) *storeStateFilter {
	acceptAll := true
	acceptState := metapb.StoreState_Up
	if v, ok := u.Query()["state"]; ok {
		if len(v) == 1 {
			state, err := strconv.Atoi(v[0])
			if err != nil {
				log.Errorf("store api: %s", err)
			} else {
				switch metapb.StoreState(state) {
				case metapb.StoreState_Up, metapb.StoreState_Offline, metapb.StoreState_Tombstone:
					acceptAll = false
					acceptState = metapb.StoreState(state)
				default:
				}
			}
		}
	}

	return &storeStateFilter{
		acceptAll:   acceptAll,
		acceptState: acceptState,
	}
}

func (filter *storeStateFilter) filter(stores []*metapb.Store) []*metapb.Store {
	ret := make([]*metapb.Store, 0, len(stores))
	for _, s := range stores {
		if !filter.acceptAll && s.GetState() != filter.acceptState {
			continue
		}
		ret = append(ret, s)
	}
	return ret
}
