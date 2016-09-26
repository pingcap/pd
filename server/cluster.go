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
	"math"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var (
	errClusterNotBootstrapped = errors.New("cluster is not bootstrapped")
	errRegionNotFound         = func(regionID uint64) error {
		return errors.Errorf("region %v not found", regionID)
	}
)

const (
	maxBatchRegionCount = 10000
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
	cachedCluster *clusterInfo

	// balancer worker
	balancerWorker *balancerWorker

	wg   sync.WaitGroup
	quit chan struct{}
}

func newRaftCluster(s *Server, clusterID uint64) *RaftCluster {
	return &RaftCluster{
		s:           s,
		running:     false,
		clusterID:   clusterID,
		clusterRoot: s.getClusterRootPath(),
	}
}

func (c *RaftCluster) start(meta metapb.Cluster) error {
	c.Lock()
	defer c.Unlock()

	if c.running {
		log.Warn("raft cluster has already been started")
		return nil
	}

	c.cachedCluster = newClusterInfo(c.clusterRoot)
	c.cachedCluster.idAlloc = c.s.idAlloc

	c.cachedCluster.setMeta(&meta)

	// Cache all stores when start the cluster. We don't have
	// many stores, so it is OK to cache them all.
	// And we should use these cache for later ChangePeer too.
	if err := c.cacheAllStores(); err != nil {
		return errors.Trace(err)
	}

	if err := c.cacheAllRegions(); err != nil {
		return errors.Trace(err)
	}

	c.balancerWorker = newBalancerWorker(c.cachedCluster, &c.s.cfg.BalanceCfg)
	c.balancerWorker.run()

	c.wg.Add(1)
	c.quit = make(chan struct{})
	go c.runBackgroundJobs(c.s.cfg.BalanceCfg.BalanceInterval)

	c.running = true

	return nil
}

func (c *RaftCluster) stop() {
	c.Lock()
	defer c.Unlock()

	if !c.running {
		return
	}

	c.running = false

	close(c.quit)
	c.wg.Wait()

	c.balancerWorker.stop()
}

func (c *RaftCluster) isRunning() bool {
	c.RLock()
	defer c.RUnlock()

	return c.running
}

// GetConfig gets config information.
func (s *Server) GetConfig() *Config {
	return s.cfg.clone()
}

// SetBalanceConfig sets the balance config information.
func (s *Server) SetBalanceConfig(cfg BalanceConfig) {
	s.cfg.setBalanceConfig(cfg)
}

func (s *Server) getClusterRootPath() string {
	return path.Join(s.rootPath, "raft")
}

// GetRaftCluster gets raft cluster.
// If cluster has not been bootstrapped, return nil.
func (s *Server) GetRaftCluster() *RaftCluster {
	if s.cluster.isRunning() {
		return s.cluster
	}

	return nil
}

func (s *Server) createRaftCluster() error {
	if s.cluster.isRunning() {
		return nil
	}

	value, err := getValue(s.client, s.getClusterRootPath())
	if err != nil {
		return errors.Trace(err)
	}
	if value == nil {
		return nil
	}

	clusterMeta := metapb.Cluster{}
	if err = clusterMeta.Unmarshal(value); err != nil {
		return errors.Trace(err)
	}

	if err = s.cluster.start(clusterMeta); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func makeStoreKey(clusterRootPath string, storeID uint64) string {
	return strings.Join([]string{clusterRootPath, "s", fmt.Sprintf("%020d", storeID)}, "/")
}

func makeRegionKey(clusterRootPath string, regionID uint64) string {
	return strings.Join([]string{clusterRootPath, "r", fmt.Sprintf("%020d", regionID)}, "/")
}

func makeStoreKeyPrefix(clusterRootPath string) string {
	return strings.Join([]string{clusterRootPath, "s", ""}, "/")
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

func (s *Server) bootstrapCluster(req *pdpb.BootstrapRequest) (*pdpb.Response, error) {
	clusterID := s.cfg.ClusterID

	log.Infof("try to bootstrap raft cluster %d with %v", clusterID, req)

	if err := checkBootstrapRequest(clusterID, req); err != nil {
		return nil, errors.Trace(err)
	}

	clusterMeta := metapb.Cluster{
		Id:           clusterID,
		MaxPeerCount: uint32(s.cfg.MaxPeerCount),
	}

	// Set cluster meta
	clusterValue, err := clusterMeta.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	clusterRootPath := s.getClusterRootPath()

	var ops []clientv3.Op
	ops = append(ops, clientv3.OpPut(clusterRootPath, string(clusterValue)))

	// Set store meta
	storeMeta := req.GetStore()
	storePath := makeStoreKey(clusterRootPath, storeMeta.GetId())
	storeValue, err := storeMeta.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	ops = append(ops, clientv3.OpPut(storePath, string(storeValue)))

	regionValue, err := req.GetRegion().Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Set region meta with region id.
	regionPath := makeRegionKey(clusterRootPath, req.GetRegion().GetId())
	ops = append(ops, clientv3.OpPut(regionPath, string(regionValue)))

	// TODO: we must figure out a better way to handle bootstrap failed, maybe intervene manually.
	bootstrapCmp := clientv3.Compare(clientv3.CreateRevision(clusterRootPath), "=", 0)
	resp, err := s.txn().If(bootstrapCmp).Then(ops...).Commit()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !resp.Succeeded {
		log.Warnf("cluster %d already bootstrapped", clusterID)
		return newBootstrappedError(), nil
	}

	log.Infof("bootstrap cluster %d ok", clusterID)

	if err = s.cluster.start(clusterMeta); err != nil {
		return nil, errors.Trace(err)
	}

	return &pdpb.Response{
		Bootstrap: &pdpb.BootstrapResponse{},
	}, nil
}

func (c *RaftCluster) cacheAllStores() error {
	start := time.Now()

	key := makeStoreKeyPrefix(c.clusterRoot)
	resp, err := kvGet(c.s.client, key, clientv3.WithPrefix())
	if err != nil {
		return errors.Trace(err)
	}

	for _, kv := range resp.Kvs {
		store := &metapb.Store{}
		if err = store.Unmarshal(kv.Value); err != nil {
			return errors.Trace(err)
		}

		c.cachedCluster.addStore(store)
	}
	log.Infof("cache all %d stores cost %s", len(resp.Kvs), time.Now().Sub(start))
	return nil
}

func (c *RaftCluster) cacheAllRegions() error {
	start := time.Now()

	nextID := uint64(0)
	endRegionKey := makeRegionKey(c.clusterRoot, math.MaxUint64)

	c.cachedCluster.regions.Lock()
	defer c.cachedCluster.regions.Unlock()

	for {
		key := makeRegionKey(c.clusterRoot, nextID)
		resp, err := kvGet(c.s.client, key, clientv3.WithRange(endRegionKey))
		if err != nil {
			return errors.Trace(err)
		}

		if len(resp.Kvs) == 0 {
			// No more data
			break
		}

		for _, kv := range resp.Kvs {
			region := &metapb.Region{}
			if err = region.Unmarshal(kv.Value); err != nil {
				return errors.Trace(err)
			}

			nextID = region.GetId() + 1
			c.cachedCluster.regions.addRegion(region)
		}
	}

	log.Infof("cache all %d regions cost %s", len(c.cachedCluster.regions.regions), time.Now().Sub(start))
	return nil
}

func (c *RaftCluster) getRegion(regionKey []byte) (*metapb.Region, *metapb.Peer) {
	return c.cachedCluster.regions.getRegion(regionKey)
}

// GetRegionByID gets region and leader peer by regionID from cluster.
func (c *RaftCluster) GetRegionByID(regionID uint64) (*metapb.Region, *metapb.Peer) {
	return c.cachedCluster.regions.getRegionByID(regionID)
}

// GetRegions gets regions from cluster.
func (c *RaftCluster) GetRegions() []*metapb.Region {
	return c.cachedCluster.regions.getRegions()
}

// GetStores gets stores from cluster.
func (c *RaftCluster) GetStores() []*metapb.Store {
	return c.cachedCluster.getMetaStores()
}

// GetStore gets store from cluster.
func (c *RaftCluster) GetStore(storeID uint64) (*metapb.Store, *StoreStatus, error) {
	if storeID == 0 {
		return nil, nil, errors.New("invalid zero store id")
	}

	store := c.cachedCluster.getStore(storeID)
	if store == nil {
		return nil, nil, errors.Errorf("invalid store ID %d, not found", storeID)
	}

	return store.store, store.stats, nil
}

func (c *RaftCluster) putStore(store *metapb.Store) error {
	c.Lock()
	defer c.Unlock()

	if store.GetId() == 0 {
		return errors.Errorf("invalid put store %v", store)
	}

	// There are 3 cases here:
	// Case 1: store id exists with the same address - do nothing;
	// Case 2: store id exists with different address - update address;
	if s := c.cachedCluster.getStore(store.GetId()); s != nil {
		if s.store.GetAddress() == store.GetAddress() {
			return nil
		}
		s.store.Address = store.Address
		return c.saveStore(s.store)
	}

	// Case 3: store id does not exist, check duplicated address.
	for _, s := range c.cachedCluster.getStores() {
		// It's OK to start a new store on the same address if the old store has been removed.
		if s.store.GetState() == metapb.StoreState_Tombstone {
			continue
		}
		if s.store.GetAddress() == store.GetAddress() {
			return errors.Errorf("duplicated store address: %v, already registered by %v", store, s.store)
		}
	}
	return c.saveStore(store)
}

func (c *RaftCluster) saveStore(store *metapb.Store) error {
	storeValue, err := store.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	storePath := makeStoreKey(c.clusterRoot, store.GetId())

	resp, err := c.s.leaderTxn().Then(clientv3.OpPut(storePath, string(storeValue))).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Errorf("save store %v fail", store)
	}

	c.cachedCluster.addStore(store)
	return nil
}

// RemoveStore marks a store as offline in cluster.
// State transition: Up -> Offline.
func (c *RaftCluster) RemoveStore(storeID uint64) error {
	c.Lock()
	defer c.Unlock()

	store, _, err := c.GetStore(storeID)
	if err != nil {
		return errors.Trace(err)
	}

	// Remove an offline store should be OK, nothing to do.
	if store.State == metapb.StoreState_Offline {
		return nil
	}

	if store.State == metapb.StoreState_Tombstone {
		return errors.New("store has been removed")
	}

	store.State = metapb.StoreState_Offline
	return c.saveStore(store)
}

// BuryStore marks a store as tombstone in cluster.
// State transition:
// Case 1: Up -> Tombstone (if force is true);
// Case 2: Offline -> Tombstone.
func (c *RaftCluster) BuryStore(storeID uint64, force bool) error {
	c.Lock()
	defer c.Unlock()

	store, _, err := c.GetStore(storeID)
	if err != nil {
		return errors.Trace(err)
	}

	// Bury a tombstone store should be OK, nothing to do.
	if store.State == metapb.StoreState_Tombstone {
		return nil
	}

	if store.State == metapb.StoreState_Up {
		if !force {
			return errors.New("store is still up, please remove store gracefully")
		}
		log.Warnf("forcedly bury store %v", store)
	}

	store.State = metapb.StoreState_Tombstone
	return c.saveStore(store)
}

func (c *RaftCluster) checkStores() {
	cluster := c.cachedCluster
	for _, store := range cluster.getMetaStores() {
		if store.GetState() != metapb.StoreState_Offline {
			continue
		}
		if cluster.regions.getStoreRegionCount(store.GetId()) == 0 {
			err := c.BuryStore(store.GetId(), false)
			if err != nil {
				log.Errorf("bury store %v failed: %v", store, err)
			} else {
				log.Infof("buried store %v", store)
			}
		}
	}
}

func (c *RaftCluster) collectMetrics() {
	cluster := c.cachedCluster
	metrics := make(map[string]float64)
	regionTotalCount := 0
	minUsedRatio, maxUsedRatio := float64(1.0), float64(0.0)
	minLeaderRatio, maxLeaderRatio := float64(1.0), float64(0.0)

	for _, s := range cluster.getStores() {
		// Store state.
		switch s.store.GetState() {
		case metapb.StoreState_Up:
			metrics["store_up_count"]++
		case metapb.StoreState_Offline:
			metrics["store_offline_count"]++
		case metapb.StoreState_Tombstone:
			metrics["store_tombstone_count"]++
		}
		if s.downSeconds() >= c.balancerWorker.cfg.MaxStoreDownDuration.Seconds() {
			metrics["store_down_count"]++
		}
		if s.store.GetState() == metapb.StoreState_Tombstone {
			continue
		}

		// Storage.
		stats := s.stats.Stats
		metrics["storage_size"] += float64(stats.GetCapacity() - stats.GetAvailable())
		metrics["storage_capacity"] += float64(stats.GetCapacity())
		if regionTotalCount < s.stats.TotalRegionCount {
			regionTotalCount = s.stats.TotalRegionCount
		}

		// Balance.
		if minUsedRatio > s.usedRatio() {
			minUsedRatio = s.usedRatio()
		}
		if maxUsedRatio < s.usedRatio() {
			maxUsedRatio = s.usedRatio()
		}
		if minLeaderRatio > s.leaderRatio() {
			minLeaderRatio = s.leaderRatio()
		}
		if maxLeaderRatio < s.leaderRatio() {
			maxLeaderRatio = s.leaderRatio()
		}
	}

	metrics["region_total_count"] = float64(regionTotalCount)
	metrics["store_max_diff_used_ratio"] = maxUsedRatio - minUsedRatio
	metrics["store_max_diff_leader_ratio"] = maxLeaderRatio - minLeaderRatio

	for label, value := range metrics {
		clusterStatusGauge.WithLabelValues(label).Set(value)
	}
}

func (c *RaftCluster) runBackgroundJobs(interval uint64) {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.quit:
			return
		case <-ticker.C:
			c.checkStores()
			c.collectMetrics()
		}
	}
}

// GetConfig gets config from cluster.
func (c *RaftCluster) GetConfig() *metapb.Cluster {
	return c.cachedCluster.getMeta()
}

func (c *RaftCluster) putConfig(meta *metapb.Cluster) error {
	if meta.GetId() != c.clusterID {
		return errors.Errorf("invalid cluster %v, mismatch cluster id %d", meta, c.clusterID)
	}

	metaValue, err := meta.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	resp, err := c.s.leaderTxn().Then(clientv3.OpPut(c.clusterRoot, string(metaValue))).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Errorf("put cluster meta %v error", meta)
	}

	c.cachedCluster.setMeta(meta)

	return nil
}

// NewAddPeerOperator creates an operator to add a peer to the region.
// If storeID is 0, it will be chosen according to the balance rules.
func (c *RaftCluster) NewAddPeerOperator(regionID uint64, storeID uint64) (Operator, error) {
	region, _ := c.GetRegionByID(regionID)
	if region == nil {
		return nil, errRegionNotFound(regionID)
	}

	var (
		peer *metapb.Peer
		err  error
	)

	cluster := c.cachedCluster
	if storeID == 0 {
		cb := newCapacityBalancer(&c.s.cfg.BalanceCfg)
		excluded := getExcludedStores(region)
		peer, err = cb.selectAddPeer(cluster, cluster.getStores(), excluded)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		_, _, err = c.GetStore(storeID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		peerID, err := cluster.idAlloc.Alloc()
		if err != nil {
			return nil, errors.Trace(err)
		}
		peer = &metapb.Peer{
			Id:      peerID,
			StoreId: storeID,
		}
	}

	return newAddPeerOperator(regionID, peer), nil
}

// NewRemovePeerOperator creates an operator to remove a peer from the region.
func (c *RaftCluster) NewRemovePeerOperator(regionID uint64, peerID uint64) (Operator, error) {
	region, _ := c.GetRegionByID(regionID)
	if region == nil {
		return nil, errRegionNotFound(regionID)
	}

	for _, peer := range region.GetPeers() {
		if peer.GetId() == peerID {
			return newRemovePeerOperator(regionID, peer), nil
		}
	}
	return nil, errors.Errorf("region %v peer %v not found", regionID, peerID)
}

// SetAdminOperator sets the balance operator of the region.
func (c *RaftCluster) SetAdminOperator(regionID uint64, ops []Operator) error {
	region, _ := c.GetRegionByID(regionID)
	if region == nil {
		return errRegionNotFound(regionID)
	}
	bop := newBalanceOperator(region, adminOP, ops...)
	c.balancerWorker.addBalanceOperator(regionID, bop)
	return nil
}

// GetBalanceOperators gets the balance operators from cluster.
func (c *RaftCluster) GetBalanceOperators() map[uint64]Operator {
	return c.balancerWorker.getBalanceOperators()
}

// GetHistoryOperators gets the history operators from cluster.
func (c *RaftCluster) GetHistoryOperators() []Operator {
	return c.balancerWorker.getHistoryOperators()
}

// GetScores gets store scores from balancer.
func (c *RaftCluster) GetScores(store *metapb.Store, status *StoreStatus) []int {
	storeInfo := &storeInfo{
		store: store,
		stats: status,
	}

	return c.balancerWorker.storeScores(storeInfo)
}

// FetchEvents fetches the operator events.
func (c *RaftCluster) FetchEvents(key uint64, all bool) []LogEvent {
	return c.balancerWorker.fetchEvents(key, all)
}
