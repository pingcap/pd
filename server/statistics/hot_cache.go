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

package statistics

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
)

// Denoising is an option to calculate flow base on the real heartbeats. Should
// only turned off by the simulator and the test.
var Denoising = true

const (
	// RegionHeartBeatReportInterval is the heartbeat report interval of a region.
	RegionHeartBeatReportInterval = 60 // seconds
	// StoreHeartBeatReportInterval is the heartbeat report interval of a store.
	StoreHeartBeatReportInterval = 10 // seconds

	statCacheMaxLen = 1000
	//storeStatCacheMaxLen = 200
	storeStatCacheMaxLen = 50

	hotWriteRegionMinFlowRate = 16 * 1024
	hotReadRegionMinFlowRate  = 128 * 1024

	activeWriteRegionMinFlowRate = 100 // B/s
	activeReadRegionMinFlowRate  = 100 // B/s

	minHotRegionReportInterval = 3
	hotRegionAntiCount         = 1
)

// FlowKind is a identify Flow types.
type FlowKind uint32

// Flags for flow.
const (
	WriteFlow FlowKind = iota
	ReadFlow
)

func (k FlowKind) String() string {
	switch k {
	case WriteFlow:
		return "write"
	case ReadFlow:
		return "read"
	}
	return "unimplemented"
}

// HotStoresStats saves the hotspot peer's statistics.
type HotStoresStats struct {
	hotStoreStats  map[uint64]*TopN               // storeID -> hot regions
	storesOfRegion map[uint64]map[uint64]struct{} // regionID -> storeIDs
}

// NewHotStoresStats creates a HotStoresStats
func NewHotStoresStats() *HotStoresStats {
	return &HotStoresStats{
		hotStoreStats:  make(map[uint64]*TopN),
		storesOfRegion: make(map[uint64]map[uint64]struct{}),
	}
}

// CheckRegionFlow checks the flow information of region.
func (f *HotStoresStats) CheckRegionFlow(region *core.RegionInfo, kind FlowKind) []HotSpotPeerStatGenerator {
	var (
		generators   []HotSpotPeerStatGenerator
		getBytesFlow func() uint64
		getKeysFlow  func() uint64
		bytesPerSec  uint64
		keysPerSec   uint64

		isExpiredInStore func(region *core.RegionInfo, storeID uint64) bool
	)

	storeIDs := make(map[uint64]struct{})
	// gets the storeIDs, including old region and new region
	ids, ok := f.storesOfRegion[region.GetID()]
	if ok {
		for storeID := range ids {
			storeIDs[storeID] = struct{}{}
		}
	}

	for _, peer := range region.GetPeers() {
		// ReadFlow no need consider the followers.
		if kind == ReadFlow && peer.GetStoreId() != region.GetLeader().GetStoreId() {
			continue
		}
		if _, ok := storeIDs[peer.GetStoreId()]; !ok {
			storeIDs[peer.GetStoreId()] = struct{}{}
		}
	}

	switch kind {
	case WriteFlow:
		getBytesFlow = region.GetBytesWritten
		getKeysFlow = region.GetKeysWritten
		isExpiredInStore = func(region *core.RegionInfo, storeID uint64) bool {
			return region.GetStorePeer(storeID) == nil
		}
	case ReadFlow:
		getBytesFlow = region.GetBytesRead
		getKeysFlow = region.GetKeysRead
		isExpiredInStore = func(region *core.RegionInfo, storeID uint64) bool {
			return region.GetLeader().GetStoreId() != storeID
		}
	}

	bytesPerSecInit := uint64(float64(getBytesFlow()) / float64(RegionHeartBeatReportInterval))
	keysPerSecInit := uint64(float64(getKeysFlow()) / float64(RegionHeartBeatReportInterval))
	for storeID := range storeIDs {
		bytesPerSec = bytesPerSecInit
		keysPerSec = keysPerSecInit
		var oldRegionStat *HotSpotPeerStat

		hotStoreStats, ok := f.hotStoreStats[storeID]
		if ok {
			if v := hotStoreStats.GetTopN(region.GetID()); v != nil {
				oldRegionStat = v.(*HotSpotPeerStat)
				// This is used for the simulator.
				if Denoising {
					interval := time.Since(oldRegionStat.LastUpdateTime).Seconds()
					if interval < minHotRegionReportInterval && !isExpiredInStore(region, storeID) {
						continue
					}
					bytesPerSec = uint64(float64(getBytesFlow()) / interval)
					keysPerSec = uint64(float64(getKeysFlow()) / interval)
				}
			}
		}

		generator := &hotSpotPeerStatGenerator{
			Region:    region,
			StoreID:   storeID,
			FlowBytes: bytesPerSec,
			FlowKeys:  keysPerSec,
			Kind:      kind,

			lastHotSpotPeerStats: oldRegionStat,
		}

		if isExpiredInStore(region, storeID) {
			generator.Expired = true
		}
		generators = append(generators, generator)
	}
	return generators
}

// Update updates the items in statistics.
func (f *HotStoresStats) Update(item *HotSpotPeerStat) {
	if item.IsNeedDelete() {
		if hotStoreStat, ok := f.hotStoreStats[item.StoreID]; ok {
			hotStoreStat.Remove(item.RegionID)
		}
		if index, ok := f.storesOfRegion[item.RegionID]; ok {
			delete(index, item.StoreID)
		}
	} else {
		hotStoreStat, ok := f.hotStoreStats[item.StoreID]
		if !ok {
			hotStoreStat = NewTopN(storeStatCacheMaxLen)
			f.hotStoreStats[item.StoreID] = hotStoreStat
		}
		hotStoreStat.Put(item)
		index, ok := f.storesOfRegion[item.RegionID]
		if !ok {
			index = make(map[uint64]struct{})
		}
		index[item.StoreID] = struct{}{}
		f.storesOfRegion[item.RegionID] = index
	}
}

func (f *HotStoresStats) isRegionHotWithAnyPeers(region *core.RegionInfo, hotThreshold int) bool {
	for _, peer := range region.GetPeers() {
		if f.isRegionHotWithPeer(region, peer, hotThreshold) {
			return true
		}
	}
	return false

}

func (f *HotStoresStats) isRegionHotWithPeer(region *core.RegionInfo, peer *metapb.Peer, hotThreshold int) bool {
	if peer == nil {
		return false
	}
	storeID := peer.GetStoreId()
	stats, ok := f.hotStoreStats[storeID]
	if !ok {
		return false
	}
	if stat := stats.GetTopN(region.GetID()); stat != nil {
		return stat.(*HotSpotPeerStat).HotDegree >= hotThreshold
	}
	return false
}

// HotSpotPeerStatGenerator used to produce new hotspot statistics.
type HotSpotPeerStatGenerator interface {
	GenHotSpotPeerStats(stats *StoresStats) *HotSpotPeerStat
}

// hotSpotPeerStatBuilder used to produce new hotspot statistics.
type hotSpotPeerStatGenerator struct {
	Region    *core.RegionInfo
	StoreID   uint64
	FlowKeys  uint64
	FlowBytes uint64
	Expired   bool
	Kind      FlowKind

	lastHotSpotPeerStats *HotSpotPeerStat
}

const rollingWindowsSize = 5

// GenHotSpotPeerStats implements HotSpotPeerStatsGenerator.
func (statGen *hotSpotPeerStatGenerator) GenHotSpotPeerStats(stats *StoresStats) *HotSpotPeerStat {
	var hotRegionThreshold uint64
	switch statGen.Kind {
	case WriteFlow:
		hotRegionThreshold = activeWriteRegionMinFlowRate
	case ReadFlow:
		hotRegionThreshold = activeReadRegionMinFlowRate
	}
	flowBytes := statGen.FlowBytes
	oldItem := statGen.lastHotSpotPeerStats
	region := statGen.Region
	newItem := &HotSpotPeerStat{
		RegionID:       region.GetID(),
		FlowBytes:      statGen.FlowBytes,
		FlowKeys:       statGen.FlowKeys,
		LastUpdateTime: time.Now(),
		StoreID:        statGen.StoreID,
		Version:        region.GetMeta().GetRegionEpoch().GetVersion(),
		AntiCount:      hotRegionAntiCount,
		Kind:           statGen.Kind,
		needDelete:     statGen.Expired,
	}

	if region.GetLeader().GetStoreId() == statGen.StoreID {
		newItem.isLeader = true
	}

	if newItem.IsNeedDelete() {
		return newItem
	}

	if oldItem != nil {
		newItem.HotDegree = oldItem.HotDegree + 1
		newItem.Stats = oldItem.Stats
	}

	if flowBytes >= hotRegionThreshold {
		if oldItem == nil {
			newItem.Stats = NewRollingStats(rollingWindowsSize)
		}
		newItem.isNew = true
		newItem.Stats.Add(float64(flowBytes))
		return newItem
	}

	// smaller than hotRegionThreshold
	if oldItem == nil {
		return nil
	}
	if oldItem.AntiCount <= 0 {
		newItem.needDelete = true
		return newItem
	}
	// eliminate some noise
	newItem.HotDegree = oldItem.HotDegree - 1
	newItem.AntiCount = oldItem.AntiCount - 1
	newItem.Stats.Add(float64(flowBytes))
	return newItem
}

// HotSpotCache is a cache hold hot regions.
type HotSpotCache struct {
	writeFlow *HotStoresStats
	readFlow  *HotStoresStats
}

// NewHotSpotCache creates a new hot spot cache.
func NewHotSpotCache() *HotSpotCache {
	return &HotSpotCache{
		writeFlow: NewHotStoresStats(),
		readFlow:  NewHotStoresStats(),
	}
}

// CheckWrite checks the write status, returns update items.
func (w *HotSpotCache) CheckWrite(region *core.RegionInfo, stats *StoresStats) []*HotSpotPeerStat {
	var updateItems []*HotSpotPeerStat
	hotStatGenerators := w.writeFlow.CheckRegionFlow(region, WriteFlow)
	for _, hotGen := range hotStatGenerators {
		item := hotGen.GenHotSpotPeerStats(stats)
		if item != nil {
			updateItems = append(updateItems, item)
		}
	}
	return updateItems
}

// CheckRead checks the read status, returns update items.
func (w *HotSpotCache) CheckRead(region *core.RegionInfo, stats *StoresStats) []*HotSpotPeerStat {
	var updateItems []*HotSpotPeerStat
	hotStatGenerators := w.readFlow.CheckRegionFlow(region, ReadFlow)
	for _, hotGen := range hotStatGenerators {
		item := hotGen.GenHotSpotPeerStats(stats)
		if item != nil {
			updateItems = append(updateItems, item)
		}
	}
	return updateItems
}

func (w *HotSpotCache) incMetrics(name string, storeID uint64, kind FlowKind) {
	storeTag := fmt.Sprintf("store-%d", storeID)
	switch kind {
	case WriteFlow:
		hotCacheStatusGauge.WithLabelValues(name, storeTag, "write").Inc()
	case ReadFlow:
		hotCacheStatusGauge.WithLabelValues(name, storeTag, "read").Inc()
	}
}

// Update updates the cache.
func (w *HotSpotCache) Update(item *HotSpotPeerStat) {
	var stats *HotStoresStats
	switch item.Kind {
	case WriteFlow:
		stats = w.writeFlow
	case ReadFlow:
		stats = w.readFlow
	}
	stats.Update(item)
	if item.IsNeedDelete() {
		w.incMetrics("remove_item", item.StoreID, item.Kind)
	} else if item.IsNew() {
		w.incMetrics("add_item", item.StoreID, item.Kind)
	} else {
		w.incMetrics("update_item", item.StoreID, item.Kind)
	}
}

// RegionStats returns hot items according to kind
func (w *HotSpotCache) RegionStats(kind FlowKind) map[uint64][]*HotSpotPeerStat {
	var flowMap map[uint64]*TopN
	switch kind {
	case WriteFlow:
		flowMap = w.writeFlow.hotStoreStats
	case ReadFlow:
		flowMap = w.readFlow.hotStoreStats
	}
	res := make(map[uint64][]*HotSpotPeerStat)
	for storeID, tn := range flowMap {
		topn := tn.GetAllTopN()
		stat, ok := res[storeID]
		if !ok {
			stat = make([]*HotSpotPeerStat, len(topn))
			res[storeID] = stat
		}
		for i := range topn {
			stat[i] = topn[i].(*HotSpotPeerStat)
		}
	}
	return res
}

// RandHotRegionFromStore random picks a hot region in specify store.
func (w *HotSpotCache) RandHotRegionFromStore(storeID uint64, kind FlowKind, hotThreshold int) *HotSpotPeerStat {
	stats, ok := w.RegionStats(kind)[storeID]
	if !ok {
		return nil
	}
	//for _, i := range rand.Perm(len(stats)) {
	var first *HotSpotPeerStat = nil
	for i := 0; i < len(stats); i++ {
		if stats[i].HotDegree >= hotThreshold {
			if first == nil {
				first = stats[i]
			}
			if rand.Intn(10) <= 5 {
				return stats[i]
			}
		}
	}
	return first
}

// CollectMetrics collect the hot cache metrics
func (w *HotSpotCache) CollectMetrics(stats *StoresStats) {
	for storeID, flowStats := range w.writeFlow.hotStoreStats {
		storeTag := fmt.Sprintf("store-%d", storeID)
		hotCacheStatusGauge.WithLabelValues("total_length", storeTag, "write").Set(float64(flowStats.Len()))
		var topnMin, restMax uint64
		if stat := flowStats.GetTopNMin(); stat != nil {
			topnMin = stat.(*HotSpotPeerStat).FlowBytes
		}
		if stat := flowStats.GetRestMax(); stat != nil {
			restMax = stat.(*HotSpotPeerStat).FlowBytes
		}
		hotCacheStatusGauge.WithLabelValues("topn_min", storeTag, "write").Set(float64(topnMin))
		hotCacheStatusGauge.WithLabelValues("rest_max", storeTag, "write").Set(float64(restMax))
	}

	for storeID, flowStats := range w.readFlow.hotStoreStats {
		storeTag := fmt.Sprintf("store-%d", storeID)
		hotCacheStatusGauge.WithLabelValues("total_length", storeTag, "read").Set(float64(flowStats.Len()))
		var topnMin, restMax uint64
		if stat := flowStats.GetTopNMin(); stat != nil {
			topnMin = stat.(*HotSpotPeerStat).FlowBytes
		}
		if stat := flowStats.GetRestMax(); stat != nil {
			restMax = stat.(*HotSpotPeerStat).FlowBytes
		}
		hotCacheStatusGauge.WithLabelValues("topn_min", storeTag, "read").Set(float64(topnMin))
		hotCacheStatusGauge.WithLabelValues("rest_max", storeTag, "read").Set(float64(restMax))
	}
}

// IsRegionHot checks if the region is hot.
func (w *HotSpotCache) IsRegionHot(region *core.RegionInfo, hotThreshold int) bool {
	stats := w.writeFlow
	if stats.isRegionHotWithAnyPeers(region, hotThreshold) {
		return true
	}
	stats = w.readFlow
	return stats.isRegionHotWithPeer(region, region.GetLeader(), hotThreshold)
}

// Utils
func calculateWriteHotThreshold(stats *StoresStats) uint64 {
	// hotRegionThreshold is used to pick hot region
	// suppose the number of the hot Regions is statCacheMaxLen
	// and we use total written Bytes past storeHeartBeatReportInterval seconds to divide the number of hot Regions
	// divide 2 because the store reports data about two times than the region record write to rocksdb
	divisor := float64(statCacheMaxLen) * 2
	hotRegionThreshold := uint64(stats.TotalBytesWriteRate() / divisor)

	if hotRegionThreshold < hotWriteRegionMinFlowRate {
		hotRegionThreshold = hotWriteRegionMinFlowRate
	}
	return hotRegionThreshold
}

func calculateWriteHotThresholdWithStore(stats *StoresStats, storeID uint64) uint64 {
	writeBytes, _ := stats.GetStoreBytesRate(storeID)
	divisor := float64(storeStatCacheMaxLen) * 2
	hotRegionThreshold := uint64(float64(writeBytes) / divisor)

	if hotRegionThreshold < hotWriteRegionMinFlowRate {
		hotRegionThreshold = hotWriteRegionMinFlowRate
	}
	return hotRegionThreshold
}

func calculateReadHotThresholdWithStore(stats *StoresStats, storeID uint64) uint64 {
	_, readBytes := stats.GetStoreBytesRate(storeID)
	divisor := float64(storeStatCacheMaxLen) * 2
	hotRegionThreshold := uint64(float64(readBytes) / divisor)

	if hotRegionThreshold < hotReadRegionMinFlowRate {
		hotRegionThreshold = hotReadRegionMinFlowRate
	}
	return hotRegionThreshold
}

func calculateReadHotThreshold(stats *StoresStats) uint64 {
	// hotRegionThreshold is used to pick hot region
	// suppose the number of the hot Regions is statCacheMaxLen
	// and we use total Read Bytes past storeHeartBeatReportInterval seconds to divide the number of hot Regions
	divisor := float64(statCacheMaxLen)
	hotRegionThreshold := uint64(stats.TotalBytesReadRate() / divisor)

	if hotRegionThreshold < hotReadRegionMinFlowRate {
		hotRegionThreshold = hotReadRegionMinFlowRate
	}
	return hotRegionThreshold
}

// RegionStatInformer provides access to a shared informer of statistics.
type RegionStatInformer interface {
	IsRegionHot(region *core.RegionInfo) bool
	RegionWriteStats() map[uint64][]*HotSpotPeerStat
	RegionReadStats() map[uint64][]*HotSpotPeerStat
	RandHotRegionFromStore(store uint64, kind FlowKind) *core.RegionInfo
}
