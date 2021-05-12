// Copyright 2018 TiKV Project Authors.
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

package statistics

import (
	"context"
	"github.com/tikv/pd/server/core"
)

// Denoising is an option to calculate flow base on the real heartbeats. Should
// only turned off by the simulator and the test.
var Denoising = true

const queueCap = 1000

// HotCache is a cache hold hot regions.
type HotCache struct {
	writeFlowQueue chan *core.PeerInfo
	readFlowQueue  chan *core.PeerInfo
	writeFlow      *hotPeerCache
	readFlow       *hotPeerCache
}

// NewHotCache creates a new hot spot cache.
func NewHotCache(ctx context.Context) *HotCache {
	w := &HotCache{
		writeFlowQueue: make(chan *core.PeerInfo, queueCap),
		readFlowQueue:  make(chan *core.PeerInfo, queueCap),
		writeFlow:      NewHotStoresStats(WriteFlow),
		readFlow:       NewHotStoresStats(ReadFlow),
	}
	go w.updateItems(ctx)
	return w
}

// ExpiredItems returns the items which are already expired.:
func (w *HotCache) ExpiredItems(region *core.RegionInfo) (expiredItems []*HotPeerStat) {
	expiredItems = append(expiredItems, w.writeFlow.CollectExpiredItems(region)...)
	expiredItems = append(expiredItems, w.readFlow.CollectExpiredItems(region)...)
	return
}

// CheckWriteSync checks the write status, returns update items.
// This is used for mockcluster.
func (w *HotCache) CheckWriteSync(region *core.RegionInfo) []*HotPeerStat {
	return w.writeFlow.CheckRegionFlow(region)
}

// CheckReadSync checks the read status, returns update items.
// This is used for mockcluster.
func (w *HotCache) CheckReadSync(region *core.RegionInfo) []*HotPeerStat {
	return w.readFlow.CheckRegionFlow(region)
}

// CheckWriteAsync puts the peerInfo into queue, and check it asynchronously
func (w *HotCache) CheckWriteAsync(peer *core.PeerInfo) {
	w.writeFlowQueue <- peer
}

// CheckReadAsync puts the peerInfo into queue, and check it asynchronously
func (w *HotCache) CheckReadAsync(peer *core.PeerInfo) {
	w.readFlowQueue <- peer
}

// Update updates the cache.
func (w *HotCache) Update(item *HotPeerStat) {
	switch item.Kind {
	case WriteFlow:
		w.writeFlow.Update(item)
	case ReadFlow:
		w.readFlow.Update(item)
	}

	if item.IsNeedDelete() {
		w.incMetrics("remove_item", item.StoreID, item.Kind)
	} else if item.IsNew() {
		w.incMetrics("add_item", item.StoreID, item.Kind)
	} else {
		w.incMetrics("update_item", item.StoreID, item.Kind)
	}
}

// RegionStats returns hot items according to kind
func (w *HotCache) RegionStats(kind FlowKind, minHotDegree int) map[uint64][]*HotPeerStat {
	switch kind {
	case WriteFlow:
		return w.writeFlow.RegionStats(minHotDegree)
	case ReadFlow:
		return w.readFlow.RegionStats(minHotDegree)
	}
	return nil
}

// HotRegionsFromStore picks hot region in specify store.
func (w *HotCache) HotRegionsFromStore(storeID uint64, kind FlowKind, minHotDegree int) []*HotPeerStat {
	if stats, ok := w.RegionStats(kind, minHotDegree)[storeID]; ok && len(stats) > 0 {
		return stats
	}
	return []*HotPeerStat{}
}

// IsRegionHot checks if the region is hot.
func (w *HotCache) IsRegionHot(region *core.RegionInfo, minHotDegree int) bool {
	return w.writeFlow.IsRegionHot(region, minHotDegree) ||
		w.readFlow.IsRegionHot(region, minHotDegree)
}

// CollectMetrics collects the hot cache metrics.
func (w *HotCache) CollectMetrics() {
	w.writeFlow.CollectMetrics("write")
	w.readFlow.CollectMetrics("read")
}

// ResetMetrics resets the hot cache metrics.
func (w *HotCache) ResetMetrics() {
	hotCacheStatusGauge.Reset()
}

func (w *HotCache) incMetrics(name string, storeID uint64, kind FlowKind) {
	store := storeTag(storeID)
	switch kind {
	case WriteFlow:
		hotCacheStatusGauge.WithLabelValues(name, store, "write").Inc()
	case ReadFlow:
		hotCacheStatusGauge.WithLabelValues(name, store, "read").Inc()
	}
}

// GetFilledPeriod returns filled period.
func (w *HotCache) GetFilledPeriod(kind FlowKind) int {
	switch kind {
	case WriteFlow:
		return w.writeFlow.getDefaultTimeMedian().GetFilledPeriod()
	case ReadFlow:
		return w.readFlow.getDefaultTimeMedian().GetFilledPeriod()
	}
	return 0
}

func (w *HotCache) updateItems(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case peer, ok := <-w.writeFlowQueue:
			if ok && peer != nil {
				item := w.writeFlow.CheckPeerFlow(peer, peer.GetBelongedRegion(), peer.GetIntervals())
				if item != nil {
					w.Update(item)
				}
			}
		case peer, ok := <-w.readFlowQueue:
			if ok && peer != nil {
				item := w.readFlow.CheckPeerFlow(peer, peer.GetBelongedRegion(), peer.GetIntervals())
				if item != nil {
					w.Update(item)
				}
			}
		}
	}
}
