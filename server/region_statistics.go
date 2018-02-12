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

package server

import (
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

type regionStatisticType uint32

const (
	missPeer regionStatisticType = 1 << iota
	extraPeer
	downPeer
	pendingPeer
	incorrectNamespace
)

type regionStatistics struct {
	opt        *scheduleOption
	classifier namespace.Classifier
	stats      map[regionStatisticType]map[uint64]*core.RegionInfo
	index      map[uint64]regionStatisticType
}

func newRegionStatistics(opt *scheduleOption, classifier namespace.Classifier) *regionStatistics {
	r := &regionStatistics{
		opt:        opt,
		classifier: classifier,
		stats:      make(map[regionStatisticType]map[uint64]*core.RegionInfo),
		index:      make(map[uint64]regionStatisticType),
	}
	r.stats[missPeer] = make(map[uint64]*core.RegionInfo)
	r.stats[extraPeer] = make(map[uint64]*core.RegionInfo)
	r.stats[downPeer] = make(map[uint64]*core.RegionInfo)
	r.stats[pendingPeer] = make(map[uint64]*core.RegionInfo)
	r.stats[incorrectNamespace] = make(map[uint64]*core.RegionInfo)
	return r
}

func (r *regionStatistics) getRegionStatsByType(typ regionStatisticType) []*core.RegionInfo {
	res := make([]*core.RegionInfo, 0, len(r.stats[typ]))
	for _, r := range r.stats[typ] {
		res = append(res, r.Clone())
	}
	return res
}

func (r *regionStatistics) deleteEntry(deleteIndex regionStatisticType, regionID uint64) {
	for typ := regionStatisticType(1); typ <= deleteIndex; typ <<= 1 {
		if deleteIndex&typ != 0 {
			delete(r.stats[typ], regionID)
		}
	}
}

func (r *regionStatistics) Observe(region *core.RegionInfo, stores []*core.StoreInfo) {
	// Region state.
	regionID := region.GetId()
	namespace := r.classifier.GetRegionNamespace(region)
	var (
		peerTypeIndex regionStatisticType
		deleteIndex   regionStatisticType
	)
	if len(region.Peers) < r.opt.GetMaxReplicas(namespace) {
		r.stats[missPeer][regionID] = region
		peerTypeIndex |= missPeer
	} else if len(region.Peers) > r.opt.GetMaxReplicas(namespace) {
		r.stats[extraPeer][regionID] = region
		peerTypeIndex |= extraPeer
	}
	if len(region.DownPeers) > 0 {
		r.stats[downPeer][regionID] = region
		peerTypeIndex |= downPeer
	}
	if len(region.PendingPeers) > 0 {
		r.stats[pendingPeer][regionID] = region
		peerTypeIndex |= pendingPeer
	}
	for _, store := range stores {
		ns := r.classifier.GetStoreNamespace(store)
		if ns == namespace {
			continue
		}
		r.stats[incorrectNamespace][regionID] = region
		peerTypeIndex |= incorrectNamespace
		break
	}

	if oldIndex, ok := r.index[regionID]; ok {
		deleteIndex = oldIndex &^ peerTypeIndex
	}
	r.deleteEntry(deleteIndex, regionID)
	r.index[regionID] = peerTypeIndex
}

func (r *regionStatistics) Collect() {
	regionStatusGauge.WithLabelValues("miss_peer_region_count").Set(float64(len(r.stats[missPeer])))
	regionStatusGauge.WithLabelValues("extra_peer_region_count").Set(float64(len(r.stats[extraPeer])))
	regionStatusGauge.WithLabelValues("down_peer_region_count").Set(float64(len(r.stats[downPeer])))
	regionStatusGauge.WithLabelValues("pending_peer_region_count").Set(float64(len(r.stats[pendingPeer])))
	regionStatusGauge.WithLabelValues("incorrect_namespace_region_count").Set(float64(len(r.stats[incorrectNamespace])))
}
