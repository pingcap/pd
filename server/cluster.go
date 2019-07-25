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

package server

import (
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errcode"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/logutil"
	"github.com/pingcap/pd/pkg/typeutil"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/id"
	"github.com/pingcap/pd/server/namespace"
	syncer "github.com/pingcap/pd/server/region_syncer"
	"github.com/pingcap/pd/server/statistics"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	backgroundJobInterval      = time.Minute
	defaultChangedRegionsLimit = 10000
)

// RaftCluster is used for cluster config management.
// Raft cluster key format:
// cluster 1 -> /1/raft, value is metapb.Cluster
// cluster 2 -> /2/raft
// For cluster 1
// store 1 -> /1/raft/s/1, value is metapb.Store
// region 1 -> /1/raft/r/1, value is metapb.Region
type RaftCluster struct {
	sync.RWMutex

	s *Server

	running bool

	clusterID   uint64
	clusterRoot string

	// cached cluster info
	core    *core.BasicCluster
	meta    *metapb.Cluster
	opt     *scheduleOption
	storage *core.Storage
	id      id.Allocator

	prepareChecker *prepareChecker
	changedRegions chan *core.RegionInfo

	labelLevelStats *statistics.LabelLevelStatistics
	regionStats     *statistics.RegionStatistics
	storesStats     *statistics.StoresStats
	hotSpotCache    *statistics.HotSpotCache

	coordinator *coordinator

	wg           sync.WaitGroup
	quit         chan struct{}
	regionSyncer *syncer.RegionSyncer
}

// ClusterStatus saves some state information
type ClusterStatus struct {
	RaftBootstrapTime time.Time `json:"raft_bootstrap_time,omitempty"`
	IsInitialized     bool      `json:"is_initialized"`
}

func newRaftCluster(s *Server, clusterID uint64) *RaftCluster {
	return &RaftCluster{
		s:            s,
		running:      false,
		clusterID:    clusterID,
		clusterRoot:  s.getClusterRootPath(),
		regionSyncer: syncer.NewRegionSyncer(s),
	}
}

func (c *RaftCluster) loadClusterStatus() (*ClusterStatus, error) {
	bootstrapTime, err := c.loadBootstrapTime()
	if err != nil {
		return nil, err
	}
	var isInitialized bool
	if bootstrapTime != typeutil.ZeroTime {
		isInitialized = c.isInitialized()
	}
	return &ClusterStatus{
		RaftBootstrapTime: bootstrapTime,
		IsInitialized:     isInitialized,
	}, nil
}

func (c *RaftCluster) isInitialized() bool {
	if c.core.Regions.GetRegionCount() > 1 {
		return true
	}
	region := c.core.Regions.SearchRegion(nil)
	return region != nil &&
		len(region.GetVoters()) >= int(c.s.GetReplicationConfig().MaxReplicas) &&
		len(region.GetPendingPeers()) == 0
}

// loadBootstrapTime loads the saved bootstrap time from etcd. It returns zero
// value of time.Time when there is error or the cluster is not bootstrapped
// yet.
func (c *RaftCluster) loadBootstrapTime() (time.Time, error) {
	var t time.Time
	data, err := c.s.storage.Load(c.s.storage.ClusterStatePath("raft_bootstrap_time"))
	if err != nil {
		return t, err
	}
	if data == "" {
		return t, nil
	}
	return typeutil.ParseTimestamp([]byte(data))
}

func (c *RaftCluster) initCluster(id id.Allocator, opt *scheduleOption, storage *core.Storage) {
	c.core = core.NewBasicCluster()
	c.opt = opt
	c.storage = storage
	c.id = id
	c.labelLevelStats = statistics.NewLabelLevelStatistics()
	c.storesStats = statistics.NewStoresStats()
	c.prepareChecker = newPrepareChecker()
	c.changedRegions = make(chan *core.RegionInfo, defaultChangedRegionsLimit)
	c.hotSpotCache = statistics.NewHotSpotCache()
}

func (c *RaftCluster) start() error {
	c.Lock()
	defer c.Unlock()

	if c.running {
		log.Warn("raft cluster has already been started")
		return nil
	}

	c.initCluster(c.s.idAllocator, c.s.scheduleOpt, c.s.storage)
	cluster, err := c.loadClusterInfo()
	if err != nil {
		return err
	}
	if cluster == nil {
		return nil
	}

	err = c.s.classifier.ReloadNamespaces()
	if err != nil {
		return err
	}

	c.coordinator = newCoordinator(cluster, c.s.hbStreams, c.s.classifier)
	c.regionStats = statistics.NewRegionStatistics(c.s.scheduleOpt, c.s.classifier)
	c.quit = make(chan struct{})

	c.wg.Add(3)
	go c.runCoordinator()
	failpoint.Inject("highFrequencyClusterJobs", func() {
		backgroundJobInterval = 100 * time.Microsecond
	})
	go c.runBackgroundJobs(backgroundJobInterval)
	go c.syncRegions()
	c.running = true

	return nil
}

// Return nil if cluster is not bootstrapped.
func (c *RaftCluster) loadClusterInfo() (*RaftCluster, error) {
	c.meta = &metapb.Cluster{}
	ok, err := c.storage.LoadMeta(c.meta)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	start := time.Now()
	if err := c.storage.LoadStores(c.core.Stores); err != nil {
		return nil, err
	}
	log.Info("load stores",
		zap.Int("count", c.core.Stores.GetStoreCount()),
		zap.Duration("cost", time.Since(start)),
	)

	start = time.Now()
	if err := c.storage.LoadRegions(c.core.Regions); err != nil {
		return nil, err
	}
	log.Info("load regions",
		zap.Int("count", c.core.Regions.GetRegionCount()),
		zap.Duration("cost", time.Since(start)),
	)
	for _, store := range c.core.Stores.GetStores() {
		c.storesStats.CreateRollingStoreStats(store.GetID())
	}
	return c, nil
}

func (c *RaftCluster) runBackgroundJobs(interval time.Duration) {
	defer logutil.LogPanic()
	defer c.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.quit:
			log.Info("background jobs has been stopped")
			return
		case <-ticker.C:
			c.checkOperators()
			c.checkStores()
			c.collectMetrics()
			c.coordinator.opController.PruneHistory()
		}
	}
}

func (c *RaftCluster) runCoordinator() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	defer func() {
		c.coordinator.wg.Wait()
		log.Info("coordinator has been stopped")
	}()
	c.coordinator.run()
	<-c.coordinator.ctx.Done()
	log.Info("coordinator is stopping")
}

func (c *RaftCluster) syncRegions() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	c.regionSyncer.RunServer(c.changedRegionNotifier(), c.quit)
}

func (c *RaftCluster) stop() {
	c.Lock()

	if !c.running {
		c.Unlock()
		return
	}

	c.running = false

	close(c.quit)
	c.coordinator.stop()
	c.Unlock()
	c.wg.Wait()
}

func (c *RaftCluster) isRunning() bool {
	c.RLock()
	defer c.RUnlock()

	return c.running
}

// handleStoreHeartbeat updates the store status.
func (c *RaftCluster) handleStoreHeartbeat(stats *pdpb.StoreStats) error {
	c.Lock()
	defer c.Unlock()

	storeID := stats.GetStoreId()
	store := c.core.Stores.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}
	newStore := store.Clone(core.SetStoreStats(stats), core.SetLastHeartbeatTS(time.Now()))
	c.core.Stores.SetStore(newStore)
	c.storesStats.Observe(newStore.GetID(), newStore.GetStoreStats())
	c.storesStats.UpdateTotalBytesRate(c.core.Stores)
	return nil
}

// processRegionHeartbeat updates the region information.
func (c *RaftCluster) processRegionHeartbeat(region *core.RegionInfo) error {
	c.RLock()
	origin := c.core.Regions.GetRegion(region.GetID())
	if origin == nil {
		for _, item := range c.core.Regions.GetOverlaps(region) {
			if region.GetRegionEpoch().GetVersion() < item.GetRegionEpoch().GetVersion() {
				c.RUnlock()
				return ErrRegionIsStale(region.GetMeta(), item)
			}
		}
	}
	isWriteUpdate, writeItem := c.CheckWriteStatus(region)
	isReadUpdate, readItem := c.CheckReadStatus(region)
	c.RUnlock()

	// Save to storage if meta is updated.
	// Save to cache if meta or leader is updated, or contains any down/pending peer.
	// Mark isNew if the region in cache does not have leader.
	var saveKV, saveCache, isNew bool
	if origin == nil {
		log.Debug("insert new region",
			zap.Uint64("region-id", region.GetID()),
			zap.Stringer("meta-region", core.RegionToHexMeta(region.GetMeta())),
		)
		saveKV, saveCache, isNew = true, true, true
	} else {
		r := region.GetRegionEpoch()
		o := origin.GetRegionEpoch()
		// Region meta is stale, return an error.
		if r.GetVersion() < o.GetVersion() || r.GetConfVer() < o.GetConfVer() {
			return ErrRegionIsStale(region.GetMeta(), origin.GetMeta())
		}
		if r.GetVersion() > o.GetVersion() {
			log.Info("region Version changed",
				zap.Uint64("region-id", region.GetID()),
				zap.String("detail", core.DiffRegionKeyInfo(origin, region)),
				zap.Uint64("old-version", o.GetVersion()),
				zap.Uint64("new-version", r.GetVersion()),
			)
			saveKV, saveCache = true, true
		}
		if r.GetConfVer() > o.GetConfVer() {
			log.Info("region ConfVer changed",
				zap.Uint64("region-id", region.GetID()),
				zap.String("detail", core.DiffRegionPeersInfo(origin, region)),
				zap.Uint64("old-confver", o.GetConfVer()),
				zap.Uint64("new-confver", r.GetConfVer()),
			)
			saveKV, saveCache = true, true
		}
		if region.GetLeader().GetId() != origin.GetLeader().GetId() {
			if origin.GetLeader().GetId() == 0 {
				isNew = true
			} else {
				log.Info("leader changed",
					zap.Uint64("region-id", region.GetID()),
					zap.Uint64("from", origin.GetLeader().GetStoreId()),
					zap.Uint64("to", region.GetLeader().GetStoreId()),
				)
			}
			saveCache = true
		}
		if len(region.GetDownPeers()) > 0 || len(region.GetPendingPeers()) > 0 {
			saveCache = true
		}
		if len(origin.GetDownPeers()) > 0 || len(origin.GetPendingPeers()) > 0 {
			saveCache = true
		}
		if len(region.GetPeers()) != len(origin.GetPeers()) {
			saveKV, saveCache = true, true
		}
		if region.GetApproximateSize() != origin.GetApproximateSize() {
			saveCache = true
		}
		if region.GetApproximateKeys() != origin.GetApproximateKeys() {
			saveCache = true
		}
	}

	if saveKV && c.storage != nil {
		if err := c.storage.SaveRegion(region.GetMeta()); err != nil {
			// Not successfully saved to storage is not fatal, it only leads to longer warm-up
			// after restart. Here we only log the error then go on updating cache.
			log.Error("fail to save region to storage",
				zap.Uint64("region-id", region.GetID()),
				zap.Stringer("region-meta", core.RegionToHexMeta(region.GetMeta())),
				zap.Error(err))
		}
		select {
		case c.changedRegions <- region:
		default:
		}
	}
	if !isWriteUpdate && !isReadUpdate && !saveCache && !isNew {
		return nil
	}

	c.Lock()
	defer c.Unlock()
	if isNew {
		c.prepareChecker.collect(region)
	}

	if saveCache {
		overlaps := c.core.Regions.SetRegion(region)
		if c.storage != nil {
			for _, item := range overlaps {
				if err := c.storage.DeleteRegion(item); err != nil {
					log.Error("fail to delete region from storage",
						zap.Uint64("region-id", item.GetId()),
						zap.Stringer("region-meta", core.RegionToHexMeta(item)),
						zap.Error(err))
				}
			}
		}
		for _, item := range overlaps {
			if c.regionStats != nil {
				c.regionStats.ClearDefunctRegion(item.GetId())
			}
			c.labelLevelStats.ClearDefunctRegion(item.GetId())
		}

		// Update related stores.
		if origin != nil {
			for _, p := range origin.GetPeers() {
				c.updateStoreStatusLocked(p.GetStoreId())
			}
		}
		for _, p := range region.GetPeers() {
			c.updateStoreStatusLocked(p.GetStoreId())
		}
	}

	if c.regionStats != nil {
		c.regionStats.Observe(region, c.takeRegionStoresLocked(region))
	}

	key := region.GetID()
	if isWriteUpdate {
		c.hotSpotCache.Update(key, writeItem, statistics.WriteFlow)
	}
	if isReadUpdate {
		c.hotSpotCache.Update(key, readItem, statistics.ReadFlow)
	}
	return nil
}

func (c *RaftCluster) updateStoreStatusLocked(id uint64) {
	leaderCount := c.core.Regions.GetStoreLeaderCount(id)
	regionCount := c.core.Regions.GetStoreRegionCount(id)
	pendingPeerCount := c.core.Regions.GetStorePendingPeerCount(id)
	leaderRegionSize := c.core.Regions.GetStoreLeaderRegionSize(id)
	regionSize := c.core.Regions.GetStoreRegionSize(id)
	c.core.Stores.UpdateStoreStatusLocked(id, leaderCount, regionCount, pendingPeerCount, leaderRegionSize, regionSize)
}

func makeStoreKey(clusterRootPath string, storeID uint64) string {
	return path.Join(clusterRootPath, "s", fmt.Sprintf("%020d", storeID))
}

func makeRegionKey(clusterRootPath string, regionID uint64) string {
	return path.Join(clusterRootPath, "r", fmt.Sprintf("%020d", regionID))
}

func makeRaftClusterStatusPrefix(clusterRootPath string) string {
	return path.Join(clusterRootPath, "status")
}

func makeBootstrapTimeKey(clusterRootPath string) string {
	return path.Join(makeRaftClusterStatusPrefix(clusterRootPath), "raft_bootstrap_time")
}

func checkBootstrapRequest(clusterID uint64, req *pdpb.BootstrapRequest) error {
	// TODO: do more check for request fields validation.

	storeMeta := req.GetStore()
	if storeMeta == nil {
		return errors.Errorf("missing store meta for bootstrap %d", clusterID)
	} else if storeMeta.GetId() == 0 {
		return errors.New("invalid zero store id")
	}

	regionMeta := req.GetRegion()
	if regionMeta == nil {
		return errors.Errorf("missing region meta for bootstrap %d", clusterID)
	} else if len(regionMeta.GetStartKey()) > 0 || len(regionMeta.GetEndKey()) > 0 {
		// first region start/end key must be empty
		return errors.Errorf("invalid first region key range, must all be empty for bootstrap %d", clusterID)
	} else if regionMeta.GetId() == 0 {
		return errors.New("invalid zero region id")
	}

	peers := regionMeta.GetPeers()
	if len(peers) != 1 {
		return errors.Errorf("invalid first region peer count %d, must be 1 for bootstrap %d", len(peers), clusterID)
	}

	peer := peers[0]
	if peer.GetStoreId() != storeMeta.GetId() {
		return errors.Errorf("invalid peer store id %d != %d for bootstrap %d", peer.GetStoreId(), storeMeta.GetId(), clusterID)
	}
	if peer.GetId() == 0 {
		return errors.New("invalid zero peer id")
	}

	return nil
}

func (c *RaftCluster) getClusterID() uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.meta.GetId()
}

func (c *RaftCluster) putMetaLocked(meta *metapb.Cluster) error {
	if c.storage != nil {
		if err := c.storage.SaveMeta(meta); err != nil {
			return err
		}
	}
	c.meta = meta
	return nil
}

// GetRegionByKey gets region and leader peer by region key from cluster.
func (c *RaftCluster) GetRegionByKey(regionKey []byte) (*metapb.Region, *metapb.Peer) {
	c.RLock()
	defer c.RUnlock()
	region := c.core.Regions.SearchRegion(regionKey)
	if region == nil {
		return nil, nil
	}
	return region.GetMeta(), region.GetLeader()
}

// GetPrevRegionByKey gets previous region and leader peer by the region key from cluster.
func (c *RaftCluster) GetPrevRegionByKey(regionKey []byte) (*metapb.Region, *metapb.Peer) {
	c.RLock()
	defer c.RUnlock()
	region := c.core.Regions.SearchPrevRegion(regionKey)
	if region == nil {
		return nil, nil
	}
	return region.GetMeta(), region.GetLeader()
}

// GetRegionInfoByKey gets regionInfo by region key from cluster.
func (c *RaftCluster) GetRegionInfoByKey(regionKey []byte) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.Regions.SearchRegion(regionKey)
}

// ScanRegionsByKey scans region with start key, until number greater than limit.
func (c *RaftCluster) ScanRegionsByKey(startKey []byte, limit int) []*core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.Regions.ScanRange(startKey, limit)
}

// ScanRegions scans region with start key, until number greater than limit.
func (c *RaftCluster) ScanRegions(startKey []byte, limit int) []*core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.Regions.ScanRange(startKey, limit)
}

// GetRegionByID gets region and leader peer by regionID from cluster.
func (c *RaftCluster) GetRegionByID(regionID uint64) (*metapb.Region, *metapb.Peer) {
	c.RLock()
	defer c.RUnlock()
	region := c.core.GetRegion(regionID)
	if region == nil {
		return nil, nil
	}
	return region.GetMeta(), region.GetLeader()
}

// GetRegion searches for a region by ID.
func (c *RaftCluster) GetRegion(regionID uint64) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.GetRegion(regionID)
}

// GetMetaRegions gets regions from cluster.
func (c *RaftCluster) GetMetaRegions() []*metapb.Region {
	c.RLock()
	defer c.RUnlock()
	return c.core.Regions.GetMetaRegions()
}

// GetRegions returns all regions' information in detail.
func (c *RaftCluster) GetRegions() []*core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.Regions.GetRegions()
}

// GetStoreRegions returns all regions' information with a given storeID.
func (c *RaftCluster) GetStoreRegions(storeID uint64) []*core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.Regions.GetStoreRegions(storeID)
}

// RandLeaderRegion returns a random region that has leader on the store.
func (c *RaftCluster) RandLeaderRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.RandLeaderRegion(storeID, opts...)
}

// RandFollowerRegion returns a random region that has a follower on the store.
func (c *RaftCluster) RandFollowerRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.RandFollowerRegion(storeID, opts...)
}

// RandPendingRegion returns a random region that has a pending peer on the store.
func (c *RaftCluster) RandPendingRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.RandPendingRegion(storeID, opts...)
}

// RandHotRegionFromStore randomly picks a hot region in specified store.
func (c *RaftCluster) RandHotRegionFromStore(store uint64, kind statistics.FlowKind) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	r := c.hotSpotCache.RandHotRegionFromStore(store, kind, c.GetHotRegionCacheHitsThreshold())
	if r == nil {
		return nil
	}
	return c.core.GetRegion(r.RegionID)
}

// GetLeaderStore returns all stores that contains the region's leader peer.
func (c *RaftCluster) GetLeaderStore(region *core.RegionInfo) *core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.Stores.GetStore(region.GetLeader().GetStoreId())
}

// GetFollowerStores returns all stores that contains the region's follower peer.
func (c *RaftCluster) GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	var stores []*core.StoreInfo
	for id := range region.GetFollowers() {
		if store := c.core.Stores.GetStore(id); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

// GetRegionStores returns all stores that contains the region's peer.
func (c *RaftCluster) GetRegionStores(region *core.RegionInfo) []*core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	stores := make([]*core.StoreInfo, 0, len(region.GetPeers()))
	for _, p := range region.GetPeers() {
		if store := c.core.Stores.GetStore(p.StoreId); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

func (c *RaftCluster) getStoreCount() int {
	c.RLock()
	defer c.RUnlock()
	return c.core.Stores.GetStoreCount()
}

// GetStoreRegionCount returns the number of regions for a given store.
func (c *RaftCluster) GetStoreRegionCount(storeID uint64) int {
	c.RLock()
	defer c.RUnlock()
	return c.core.Regions.GetStoreRegionCount(storeID)
}

// GetAverageRegionSize returns the average region approximate size.
func (c *RaftCluster) GetAverageRegionSize() int64 {
	c.RLock()
	defer c.RUnlock()
	return c.core.GetAverageRegionSize()
}

// GetRegionStats returns region statistics from cluster.
func (c *RaftCluster) GetRegionStats(startKey, endKey []byte) *statistics.RegionStats {
	c.RLock()
	defer c.RUnlock()
	return statistics.GetRegionStats(c.core.Regions, startKey, endKey)
}

// GetStoresStats returns stores' statistics from cluster.
func (c *RaftCluster) GetStoresStats() *statistics.StoresStats {
	c.RLock()
	defer c.RUnlock()
	return c.storesStats
}

// DropCacheRegion removes a region from the cache.
func (c *RaftCluster) DropCacheRegion(id uint64) {
	c.RLock()
	defer c.RUnlock()
	if region := c.core.GetRegion(id); region != nil {
		c.core.Regions.RemoveRegion(region)
	}
}

// GetMetaStores gets stores from cluster.
func (c *RaftCluster) GetMetaStores() []*metapb.Store {
	c.RLock()
	defer c.RUnlock()
	return c.core.Stores.GetMetaStores()
}

// GetStores returns all stores in the cluster.
func (c *RaftCluster) GetStores() []*core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.GetStores()
}

// TryGetStore gets store from cluster.
func (c *RaftCluster) TryGetStore(storeID uint64) (*core.StoreInfo, error) {
	if storeID == 0 {
		return nil, errors.New("invalid zero store id")
	}
	store := c.GetStore(storeID)
	if store == nil {
		return nil, errors.Errorf("invalid store ID %d, not found", storeID)
	}
	return store, nil
}

// GetStore gets store from cluster.
func (c *RaftCluster) GetStore(storeID uint64) *core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.GetStore(storeID)
}

// IsRegionHot checks if a region is in hot state.
func (c *RaftCluster) IsRegionHot(id uint64) bool {
	c.RLock()
	defer c.RUnlock()
	return c.hotSpotCache.IsRegionHot(id, c.GetHotRegionCacheHitsThreshold())
}

// GetAdjacentRegions returns regions' information that are adjacent with the specific region ID.
func (c *RaftCluster) GetAdjacentRegions(region *core.RegionInfo) (*core.RegionInfo, *core.RegionInfo) {
	c.RLock()
	defer c.RUnlock()
	return c.core.GetAdjacentRegions(region)
}

// UpdateStoreLabels updates a store's location labels.
func (c *RaftCluster) UpdateStoreLabels(storeID uint64, labels []*metapb.StoreLabel) error {
	c.RLock()
	store := c.core.GetStore(storeID)
	c.RUnlock()
	if store == nil {
		return errors.Errorf("invalid store ID %d, not found", storeID)
	}
	newStore := proto.Clone(store.GetMeta()).(*metapb.Store)
	newStore.Labels = labels
	// putStore will perform label merge.
	err := c.putStore(newStore)
	return err
}

func (c *RaftCluster) putStore(store *metapb.Store) error {
	c.Lock()
	defer c.Unlock()

	if store.GetId() == 0 {
		return errors.Errorf("invalid put store %v", store)
	}

	v, err := ParseVersion(store.GetVersion())
	if err != nil {
		return errors.Errorf("invalid put store %v, error: %s", store, err)
	}
	clusterVersion := c.opt.LoadClusterVersion()
	if !IsCompatible(clusterVersion, *v) {
		return errors.Errorf("version should compatible with version  %s, got %s", clusterVersion, v)
	}

	// Store address can not be the same as other stores.
	for _, s := range c.core.GetStores() {
		// It's OK to start a new store on the same address if the old store has been removed.
		if s.IsTombstone() {
			continue
		}
		if s.GetID() != store.GetId() && s.GetAddress() == store.GetAddress() {
			return errors.Errorf("duplicated store address: %v, already registered by %v", store, s.GetMeta())
		}
	}

	s := c.core.GetStore(store.GetId())
	if s == nil {
		// Add a new store.
		s = core.NewStoreInfo(store)
	} else {
		// Update an existed store.
		labels := s.MergeLabels(store.GetLabels())

		s = s.Clone(
			core.SetStoreAddress(store.Address),
			core.SetStoreVersion(store.Version),
			core.SetStoreLabels(labels),
		)
	}
	// Check location labels.
	keysSet := make(map[string]struct{})
	for _, k := range c.GetLocationLabels() {
		keysSet[k] = struct{}{}
		if v := s.GetLabelValue(k); len(v) == 0 {
			log.Warn("label configuration is incorrect",
				zap.Stringer("store", s.GetMeta()),
				zap.String("label-key", k))
			if c.GetStrictlyMatchLabel() {
				return errors.Errorf("label configuration is incorrect, need to specify the key: %s ", k)
			}
		}
	}
	for _, label := range s.GetLabels() {
		key := label.GetKey()
		if _, ok := keysSet[key]; !ok {
			log.Warn("not found the key match with the store label",
				zap.Stringer("store", s.GetMeta()),
				zap.String("label-key", key))
			if c.GetStrictlyMatchLabel() {
				return errors.Errorf("key matching the label was not found in the PD, store label key: %s ", key)
			}
		}
	}
	return c.putStoreLocked(s)
}

// RemoveStore marks a store as offline in cluster.
// State transition: Up -> Offline.
func (c *RaftCluster) RemoveStore(storeID uint64) error {
	op := errcode.Op("store.remove")
	c.Lock()
	defer c.Unlock()

	store := c.core.GetStore(storeID)
	if store == nil {
		return op.AddTo(core.NewStoreNotFoundErr(storeID))
	}

	// Remove an offline store should be OK, nothing to do.
	if store.IsOffline() {
		return nil
	}

	if store.IsTombstone() {
		return op.AddTo(core.StoreTombstonedErr{StoreID: storeID})
	}

	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Offline))
	log.Warn("store has been offline",
		zap.Uint64("store-id", newStore.GetID()),
		zap.String("store-address", newStore.GetAddress()))
	return c.putStoreLocked(newStore)
}

// BuryStore marks a store as tombstone in cluster.
// State transition:
// Case 1: Up -> Tombstone (if force is true);
// Case 2: Offline -> Tombstone.
func (c *RaftCluster) BuryStore(storeID uint64, force bool) error { // revive:disable-line:flag-parameter
	c.Lock()
	defer c.Unlock()

	store := c.core.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}

	// Bury a tombstone store should be OK, nothing to do.
	if store.IsTombstone() {
		return nil
	}

	if store.IsUp() {
		if !force {
			return errors.New("store is still up, please remove store gracefully")
		}
		log.Warn("forcedly bury store", zap.Stringer("store", store.GetMeta()))
	}

	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Tombstone))
	log.Warn("store has been Tombstone",
		zap.Uint64("store-id", newStore.GetID()),
		zap.String("store-address", newStore.GetAddress()))
	return c.putStoreLocked(newStore)
}

// BlockStore stops balancer from selecting the store.
func (c *RaftCluster) BlockStore(storeID uint64) error {
	c.Lock()
	defer c.Unlock()
	return c.core.BlockStore(storeID)
}

// UnblockStore allows balancer to select the store.
func (c *RaftCluster) UnblockStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()
	c.core.UnblockStore(storeID)
}

// AttachOverloadStatus attaches the overload status to a store.
func (c *RaftCluster) AttachOverloadStatus(storeID uint64, f func() bool) {
	c.Lock()
	defer c.Unlock()
	c.core.AttachOverloadStatus(storeID, f)
}

// SetStoreState sets up a store's state.
func (c *RaftCluster) SetStoreState(storeID uint64, state metapb.StoreState) error {
	c.Lock()
	defer c.Unlock()

	store := c.core.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}

	newStore := store.Clone(core.SetStoreState(state))
	log.Warn("store update state",
		zap.Uint64("store-id", storeID),
		zap.Stringer("new-state", state))
	return c.putStoreLocked(newStore)
}

// SetStoreWeight sets up a store's leader/region balance weight.
func (c *RaftCluster) SetStoreWeight(storeID uint64, leaderWeight, regionWeight float64) error {
	c.Lock()
	defer c.Unlock()

	store := c.core.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}

	if err := c.s.storage.SaveStoreWeight(storeID, leaderWeight, regionWeight); err != nil {
		return err
	}

	newStore := store.Clone(
		core.SetLeaderWeight(leaderWeight),
		core.SetRegionWeight(regionWeight),
	)

	return c.putStoreLocked(newStore)
}

func (c *RaftCluster) putStoreLocked(store *core.StoreInfo) error {
	if c.storage != nil {
		if err := c.storage.SaveStore(store.GetMeta()); err != nil {
			return err
		}
	}
	c.core.PutStore(store)
	c.storesStats.CreateRollingStoreStats(store.GetID())
	return nil
}

func (c *RaftCluster) checkStores() {
	var offlineStores []*metapb.Store
	var upStoreCount int

	c.RLock()
	stores := c.core.GetStores()
	c.RUnlock()
	for _, store := range stores {
		// the store has already been tombstone
		if store.IsTombstone() {
			continue
		}

		if store.IsUp() {
			if !store.IsLowSpace(c.GetLowSpaceRatio()) {
				upStoreCount++
			}
			continue
		}

		offlineStore := store.GetMeta()
		// If the store is empty, it can be buried.
		c.RLock()
		regionCount := c.core.Regions.GetStoreRegionCount(offlineStore.GetId())
		c.RUnlock()
		if regionCount == 0 {
			if err := c.BuryStore(offlineStore.GetId(), false); err != nil {
				log.Error("bury store failed",
					zap.Stringer("store", offlineStore),
					zap.Error(err))
			}
		} else {
			offlineStores = append(offlineStores, offlineStore)
		}
	}

	if len(offlineStores) == 0 {
		return
	}

	if upStoreCount < c.GetMaxReplicas() {
		for _, offlineStore := range offlineStores {
			log.Warn("store may not turn into Tombstone, there are no extra up node has enough space to accommodate the extra replica", zap.Stringer("store", offlineStore))
		}
	}
}

// RemoveTombStoneRecords removes the tombStone Records.
func (c *RaftCluster) RemoveTombStoneRecords() error {
	c.Lock()
	defer c.Unlock()

	for _, store := range c.core.GetStores() {
		if store.IsTombstone() {
			// the store has already been tombstone
			err := c.deleteStoreLocked(store)
			if err != nil {
				log.Error("delete store failed",
					zap.Stringer("store", store.GetMeta()),
					zap.Error(err))
				return err
			}
			log.Info("delete store successed",
				zap.Stringer("store", store.GetMeta()))
		}
	}
	return nil
}

func (c *RaftCluster) deleteStoreLocked(store *core.StoreInfo) error {
	if c.storage != nil {
		if err := c.storage.DeleteStore(store.GetMeta()); err != nil {
			return err
		}
	}
	c.core.DeleteStore(store)
	c.storesStats.RemoveRollingStoreStats(store.GetID())
	return nil
}

func (c *RaftCluster) checkOperators() {
	opController := c.coordinator.opController
	for _, op := range opController.GetOperators() {
		// after region is merged, it will not heartbeat anymore
		// the operator of merged region will not timeout actively
		c.RLock()
		region := c.core.GetRegion(op.RegionID())
		c.RUnlock()
		if region == nil {
			log.Debug("remove operator cause region is merged",
				zap.Uint64("region-id", op.RegionID()),
				zap.Stringer("operator", op))
			opController.RemoveOperator(op)
			continue
		}

		if op.IsTimeout() {
			log.Info("operator timeout",
				zap.Uint64("region-id", op.RegionID()),
				zap.Stringer("operator", op))
			opController.RemoveTimeoutOperator(op)
		}
	}
}

func (c *RaftCluster) collectMetrics() {
	statsMap := statistics.NewStoreStatisticsMap(c.opt, c.GetNamespaceClassifier())
	c.RLock()
	stores := c.core.GetStores()
	c.RUnlock()
	for _, s := range stores {
		statsMap.Observe(s, c.storesStats)
	}
	statsMap.Collect()

	c.coordinator.collectSchedulerMetrics()
	c.coordinator.collectHotSpotMetrics()
	c.collectClusterMetrics()
	c.collectHealthStatus()
}

func (c *RaftCluster) collectClusterMetrics() {
	c.RLock()
	defer c.RUnlock()
	if c.regionStats == nil {
		return
	}
	c.regionStats.Collect()
	c.labelLevelStats.Collect()
	// collect hot cache metrics
	c.hotSpotCache.CollectMetrics(c.storesStats)
}

func (c *RaftCluster) collectHealthStatus() {
	client := c.s.GetClient()
	members, err := GetMembers(client)
	if err != nil {
		log.Error("get members error", zap.Error(err))
	}
	unhealth := c.s.CheckHealth(members)
	for _, member := range members {
		if _, ok := unhealth[member.GetMemberId()]; ok {
			healthStatusGauge.WithLabelValues(member.GetName()).Set(0)
			continue
		}
		healthStatusGauge.WithLabelValues(member.GetName()).Set(1)
	}
}

// GetRegionStatsByType gets the status of the region by types.
func (c *RaftCluster) GetRegionStatsByType(typ statistics.RegionStatisticType) []*core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	if c.regionStats == nil {
		return nil
	}
	return c.regionStats.GetRegionStatsByType(typ)
}

func (c *RaftCluster) updateRegionsLabelLevelStats(regions []*core.RegionInfo) {
	c.Lock()
	defer c.Unlock()
	for _, region := range regions {
		c.labelLevelStats.Observe(region, c.takeRegionStoresLocked(region), c.GetLocationLabels())
	}
}

func (c *RaftCluster) takeRegionStoresLocked(region *core.RegionInfo) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(region.GetPeers()))
	for _, p := range region.GetPeers() {
		if store := c.core.Stores.TakeStore(p.StoreId); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

func (c *RaftCluster) allocID() (uint64, error) {
	return c.id.Alloc()
}

// AllocPeer allocs a new peer on a store.
func (c *RaftCluster) AllocPeer(storeID uint64) (*metapb.Peer, error) {
	peerID, err := c.allocID()
	if err != nil {
		log.Error("failed to alloc peer", zap.Error(err))
		return nil, err
	}
	peer := &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
	return peer, nil
}

// OnStoreVersionChange changes the version of the cluster when needed.
func (c *RaftCluster) OnStoreVersionChange() {
	c.Lock()
	defer c.Unlock()
	var (
		minVersion     *semver.Version
		clusterVersion semver.Version
	)

	clusterVersion = c.opt.loadClusterVersion()
	stores := c.core.GetStores()
	for _, s := range stores {
		if s.IsTombstone() {
			continue
		}
		v := MustParseVersion(s.GetVersion())

		if minVersion == nil || v.LessThan(*minVersion) {
			minVersion = v
		}
	}
	// If the cluster version of PD is less than the minimum version of all stores,
	// it will update the cluster version.
	if clusterVersion.LessThan(*minVersion) {
		c.opt.SetClusterVersion(*minVersion)
		err := c.opt.persist(c.storage)
		if err != nil {
			log.Error("persist cluster version meet error", zap.Error(err))
		}
		log.Info("cluster version changed",
			zap.Stringer("old-cluster-version", clusterVersion),
			zap.Stringer("new-cluster-version", minVersion))
		CheckPDVersion(c.opt)
	}
}

func (c *RaftCluster) changedRegionNotifier() <-chan *core.RegionInfo {
	return c.changedRegions
}

// IsFeatureSupported checks if the feature is supported by current cluster.
func (c *RaftCluster) IsFeatureSupported(f Feature) bool {
	clusterVersion := c.opt.loadClusterVersion()
	minSupportVersion := MinSupportedVersion(f)
	return !clusterVersion.LessThan(minSupportVersion)
}

// GetConfig gets config from cluster.
func (c *RaftCluster) GetConfig() *metapb.Cluster {
	c.RLock()
	defer c.RUnlock()
	return proto.Clone(c.meta).(*metapb.Cluster)
}

func (c *RaftCluster) putConfig(meta *metapb.Cluster) error {
	c.Lock()
	defer c.Unlock()
	if meta.GetId() != c.clusterID {
		return errors.Errorf("invalid cluster %v, mismatch cluster id %d", meta, c.clusterID)
	}
	return c.putMetaLocked(proto.Clone(meta).(*metapb.Cluster))
}

// GetNamespaceClassifier returns current namespace classifier.
func (c *RaftCluster) GetNamespaceClassifier() namespace.Classifier {
	return c.s.classifier
}

// GetOpt returns the scheduling options.
func (c *RaftCluster) GetOpt() namespace.ScheduleOptions {
	return c.opt
}

// GetLeaderScheduleLimit returns the limit for leader schedule.
func (c *RaftCluster) GetLeaderScheduleLimit() uint64 {
	return c.opt.GetLeaderScheduleLimit(namespace.DefaultNamespace)
}

// GetRegionScheduleLimit returns the limit for region schedule.
func (c *RaftCluster) GetRegionScheduleLimit() uint64 {
	return c.opt.GetRegionScheduleLimit(namespace.DefaultNamespace)
}

// GetReplicaScheduleLimit returns the limit for replica schedule.
func (c *RaftCluster) GetReplicaScheduleLimit() uint64 {
	return c.opt.GetReplicaScheduleLimit(namespace.DefaultNamespace)
}

// GetMergeScheduleLimit returns the limit for merge schedule.
func (c *RaftCluster) GetMergeScheduleLimit() uint64 {
	return c.opt.GetMergeScheduleLimit(namespace.DefaultNamespace)
}

// GetHotRegionScheduleLimit returns the limit for hot region schedule.
func (c *RaftCluster) GetHotRegionScheduleLimit() uint64 {
	return c.opt.GetHotRegionScheduleLimit(namespace.DefaultNamespace)
}

// GetStoreBalanceRate returns the balance rate of a store.
func (c *RaftCluster) GetStoreBalanceRate() float64 {
	return c.opt.GetStoreBalanceRate()
}

// GetTolerantSizeRatio gets the tolerant size ratio.
func (c *RaftCluster) GetTolerantSizeRatio() float64 {
	return c.opt.GetTolerantSizeRatio()
}

// GetLowSpaceRatio returns the low space ratio.
func (c *RaftCluster) GetLowSpaceRatio() float64 {
	return c.opt.GetLowSpaceRatio()
}

// GetHighSpaceRatio returns the high space ratio.
func (c *RaftCluster) GetHighSpaceRatio() float64 {
	return c.opt.GetHighSpaceRatio()
}

// GetSchedulerMaxWaitingOperator returns the number of the max waiting operators.
func (c *RaftCluster) GetSchedulerMaxWaitingOperator() uint64 {
	return c.opt.GetSchedulerMaxWaitingOperator()
}

// GetMaxSnapshotCount returns the number of the max snapshot which is allowed to send.
func (c *RaftCluster) GetMaxSnapshotCount() uint64 {
	return c.opt.GetMaxSnapshotCount()
}

// GetMaxPendingPeerCount returns the number of the max pending peers.
func (c *RaftCluster) GetMaxPendingPeerCount() uint64 {
	return c.opt.GetMaxPendingPeerCount()
}

// GetMaxMergeRegionSize returns the max region size.
func (c *RaftCluster) GetMaxMergeRegionSize() uint64 {
	return c.opt.GetMaxMergeRegionSize()
}

// GetMaxMergeRegionKeys returns the max number of keys.
func (c *RaftCluster) GetMaxMergeRegionKeys() uint64 {
	return c.opt.GetMaxMergeRegionKeys()
}

// GetSplitMergeInterval returns the interval between finishing split and starting to merge.
func (c *RaftCluster) GetSplitMergeInterval() time.Duration {
	return c.opt.GetSplitMergeInterval()
}

// GetEnableOneWayMerge returns if the one way merge is enabled.
func (c *RaftCluster) GetEnableOneWayMerge() bool {
	return c.opt.GetEnableOneWayMerge()
}

// GetPatrolRegionInterval returns the interval of patroling region.
func (c *RaftCluster) GetPatrolRegionInterval() time.Duration {
	return c.opt.GetPatrolRegionInterval()
}

// GetMaxStoreDownTime returns the max down time of a store.
func (c *RaftCluster) GetMaxStoreDownTime() time.Duration {
	return c.opt.GetMaxStoreDownTime()
}

// GetMaxReplicas returns the number of replicas.
func (c *RaftCluster) GetMaxReplicas() int {
	return c.opt.GetMaxReplicas(namespace.DefaultNamespace)
}

// GetLocationLabels returns the location labels for each region
func (c *RaftCluster) GetLocationLabels() []string {
	return c.opt.GetLocationLabels()
}

// GetStrictlyMatchLabel returns if the strictly label check is enabled.
func (c *RaftCluster) GetStrictlyMatchLabel() bool {
	return c.opt.rep.GetStrictlyMatchLabel()
}

// GetHotRegionCacheHitsThreshold gets the threshold of hitting hot region cache.
func (c *RaftCluster) GetHotRegionCacheHitsThreshold() int {
	return c.opt.GetHotRegionCacheHitsThreshold()
}

// IsRaftLearnerEnabled returns if raft learner is enabled.
func (c *RaftCluster) IsRaftLearnerEnabled() bool {
	if !c.IsFeatureSupported(RaftLearner) {
		return false
	}
	return c.opt.IsRaftLearnerEnabled()
}

// IsRemoveDownReplicaEnabled returns if remove down replica is enabled.
func (c *RaftCluster) IsRemoveDownReplicaEnabled() bool {
	return c.opt.IsRemoveDownReplicaEnabled()
}

// IsReplaceOfflineReplicaEnabled returns if replace offline replica is enabled.
func (c *RaftCluster) IsReplaceOfflineReplicaEnabled() bool {
	return c.opt.IsReplaceOfflineReplicaEnabled()
}

// IsMakeUpReplicaEnabled returns if make up replica is enabled.
func (c *RaftCluster) IsMakeUpReplicaEnabled() bool {
	return c.opt.IsMakeUpReplicaEnabled()
}

// IsRemoveExtraReplicaEnabled returns if remove extra replica is enabled.
func (c *RaftCluster) IsRemoveExtraReplicaEnabled() bool {
	return c.opt.IsRemoveExtraReplicaEnabled()
}

// IsLocationReplacementEnabled returns if location replace is enabled.
func (c *RaftCluster) IsLocationReplacementEnabled() bool {
	return c.opt.IsLocationReplacementEnabled()
}

// IsNamespaceRelocationEnabled returns if namespace relocation is enabled.
func (c *RaftCluster) IsNamespaceRelocationEnabled() bool {
	return c.opt.IsNamespaceRelocationEnabled()
}

// CheckLabelProperty is used to check label property.
func (c *RaftCluster) CheckLabelProperty(typ string, labels []*metapb.StoreLabel) bool {
	return c.opt.CheckLabelProperty(typ, labels)
}

// isPrepared if the cluster information is collected
func (c *RaftCluster) isPrepared() bool {
	c.RLock()
	defer c.RUnlock()
	return c.prepareChecker.check(c)
}

func (c *RaftCluster) getStoresBytesWriteStat() map[uint64]uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.storesStats.GetStoresBytesWriteStat()
}

func (c *RaftCluster) getStoresBytesReadStat() map[uint64]uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.storesStats.GetStoresBytesReadStat()
}

func (c *RaftCluster) getStoresKeysWriteStat() map[uint64]uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.storesStats.GetStoresKeysWriteStat()
}

func (c *RaftCluster) getStoresKeysReadStat() map[uint64]uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.storesStats.GetStoresKeysReadStat()
}

// RegionReadStats returns hot region's read stats.
func (c *RaftCluster) RegionReadStats() []*statistics.RegionStat {
	// RegionStats is a thread-safe method
	return c.hotSpotCache.RegionStats(statistics.ReadFlow)
}

// RegionWriteStats returns hot region's write stats.
func (c *RaftCluster) RegionWriteStats() []*statistics.RegionStat {
	// RegionStats is a thread-safe method
	return c.hotSpotCache.RegionStats(statistics.WriteFlow)
}

// CheckWriteStatus checks the write status, returns whether need update statistics and item.
func (c *RaftCluster) CheckWriteStatus(region *core.RegionInfo) (bool, *statistics.RegionStat) {
	return c.hotSpotCache.CheckWrite(region, c.storesStats)
}

// CheckReadStatus checks the read status, returns whether need update statistics and item.
func (c *RaftCluster) CheckReadStatus(region *core.RegionInfo) (bool, *statistics.RegionStat) {
	return c.hotSpotCache.CheckRead(region, c.storesStats)
}

func (c *RaftCluster) putRegion(region *core.RegionInfo) error {
	c.Lock()
	defer c.Unlock()
	if c.storage != nil {
		if err := c.storage.SaveRegion(region.GetMeta()); err != nil {
			return err
		}
	}
	c.core.PutRegion(region)
	return nil
}

type prepareChecker struct {
	reactiveRegions map[uint64]int
	start           time.Time
	sum             int
	isPrepared      bool
}

func newPrepareChecker() *prepareChecker {
	return &prepareChecker{
		start:           time.Now(),
		reactiveRegions: make(map[uint64]int),
	}
}

// Before starting up the scheduler, we need to take the proportion of the regions on each store into consideration.
func (checker *prepareChecker) check(c *RaftCluster) bool {
	if checker.isPrepared || time.Since(checker.start) > collectTimeout {
		return true
	}
	// The number of active regions should be more than total region of all stores * collectFactor
	if float64(c.core.Regions.Length())*collectFactor > float64(checker.sum) {
		return false
	}
	for _, store := range c.core.GetStores() {
		if !store.IsUp() {
			continue
		}
		storeID := store.GetID()
		// For each store, the number of active regions should be more than total region of the store * collectFactor
		if float64(c.core.Regions.GetStoreRegionCount(storeID))*collectFactor > float64(checker.reactiveRegions[storeID]) {
			return false
		}
	}
	checker.isPrepared = true
	return true
}

func (checker *prepareChecker) collect(region *core.RegionInfo) {
	for _, p := range region.GetPeers() {
		checker.reactiveRegions[p.GetStoreId()]++
	}
	checker.sum++
}
