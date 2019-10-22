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
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/mock/mockid"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/server/config"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/kv"
	"github.com/pkg/errors"
)

const (
	initEpochVersion uint64 = 1
	initEpochConfVer uint64 = 1
)

var _ = Suite(&testClusterSuite{})

type baseCluster struct {
	svr          *Server
	grpcPDClient pdpb.PDClient
}

type testClusterSuite struct {
	baseCluster
}

type testErrorKV struct {
	kv.Base
}

func (kv *testErrorKV) Save(key, value string) error {
	return errors.New("save failed")
}

func (s *baseCluster) allocID(c *C) uint64 {
	id, err := s.svr.idAllocator.Alloc()
	c.Assert(err, IsNil)
	return id
}

func (s *baseCluster) newPeer(c *C, storeID uint64, peerID uint64) *metapb.Peer {
	c.Assert(storeID, Greater, uint64(0))

	if peerID == 0 {
		peerID = s.allocID(c)
	}

	return &metapb.Peer{
		StoreId: storeID,
		Id:      peerID,
	}
}

func (s *baseCluster) newStore(c *C, storeID uint64, addr string, version string) *metapb.Store {
	if storeID == 0 {
		storeID = s.allocID(c)
	}

	return &metapb.Store{
		Id:      storeID,
		Address: addr,
		Version: version,
	}
}

func (s *baseCluster) newRegion(c *C, regionID uint64, startKey []byte,
	endKey []byte, peers []*metapb.Peer, epoch *metapb.RegionEpoch) *metapb.Region {
	if regionID == 0 {
		regionID = s.allocID(c)
	}

	if epoch == nil {
		epoch = &metapb.RegionEpoch{
			ConfVer: initEpochConfVer,
			Version: initEpochVersion,
		}
	}

	for _, peer := range peers {
		peerID := peer.GetId()
		c.Assert(peerID, Greater, uint64(0))
	}

	return &metapb.Region{
		Id:          regionID,
		StartKey:    startKey,
		EndKey:      endKey,
		RegionEpoch: epoch,
		Peers:       peers,
	}
}

func (s *testClusterSuite) TestBootstrap(c *C) {
	var err error
	var cleanup func()
	_, s.svr, cleanup, err = NewTestServer(c)
	defer cleanup()
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = testutil.MustNewGrpcClient(c, s.svr.GetAddr())
	clusterID := s.svr.clusterID

	// IsBootstrapped returns false.
	req := s.newIsBootstrapRequest(clusterID)
	resp, err := s.grpcPDClient.IsBootstrapped(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	c.Assert(resp.GetBootstrapped(), IsFalse)

	// Bootstrap the cluster.
	storeAddr := "127.0.0.1:0"
	s.bootstrapCluster(c, clusterID, storeAddr)

	// IsBootstrapped returns true.
	req = s.newIsBootstrapRequest(clusterID)
	resp, err = s.grpcPDClient.IsBootstrapped(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetBootstrapped(), IsTrue)

	// check bootstrapped error.
	reqBoot := s.newBootstrapRequest(c, clusterID, storeAddr)
	respBoot, err := s.grpcPDClient.Bootstrap(context.Background(), reqBoot)
	c.Assert(err, IsNil)
	c.Assert(respBoot.GetHeader().GetError(), NotNil)
	c.Assert(respBoot.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_ALREADY_BOOTSTRAPPED)
}

func (s *baseCluster) newIsBootstrapRequest(clusterID uint64) *pdpb.IsBootstrappedRequest {
	req := &pdpb.IsBootstrappedRequest{
		Header: testutil.NewRequestHeader(clusterID),
	}

	return req
}

func (s *baseCluster) newBootstrapRequest(c *C, clusterID uint64, storeAddr string) *pdpb.BootstrapRequest {
	store := s.newStore(c, 0, storeAddr, "2.1.0")
	peer := s.newPeer(c, store.GetId(), 0)
	region := s.newRegion(c, 0, []byte{}, []byte{}, []*metapb.Peer{peer}, nil)

	req := &pdpb.BootstrapRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  store,
		Region: region,
	}

	return req
}

// helper function to check and bootstrap.
func (s *baseCluster) bootstrapCluster(c *C, clusterID uint64, storeAddr string) {
	req := s.newBootstrapRequest(c, clusterID, storeAddr)
	_, err := s.grpcPDClient.Bootstrap(context.Background(), req)
	c.Assert(err, IsNil)
}

func (s *baseCluster) getStore(c *C, clusterID uint64, storeID uint64) *metapb.Store {
	req := &pdpb.GetStoreRequest{
		Header:  testutil.NewRequestHeader(clusterID),
		StoreId: storeID,
	}
	resp, err := s.grpcPDClient.GetStore(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetStore().GetId(), Equals, storeID)

	return resp.GetStore()
}

func (s *baseCluster) getRegion(c *C, clusterID uint64, regionKey []byte) *metapb.Region {
	req := &pdpb.GetRegionRequest{
		Header:    testutil.NewRequestHeader(clusterID),
		RegionKey: regionKey,
	}

	resp, err := s.grpcPDClient.GetRegion(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetRegion(), NotNil)

	return resp.GetRegion()
}

func (s *baseCluster) getRegionByID(c *C, clusterID uint64, regionID uint64) *metapb.Region {
	req := &pdpb.GetRegionByIDRequest{
		Header:   testutil.NewRequestHeader(clusterID),
		RegionId: regionID,
	}

	resp, err := s.grpcPDClient.GetRegionByID(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetRegion(), NotNil)

	return resp.GetRegion()
}

func (s *baseCluster) getRaftCluster(c *C) *RaftCluster {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)
	return cluster
}

func (s *baseCluster) getClusterConfig(c *C, clusterID uint64) *metapb.Cluster {
	req := &pdpb.GetClusterConfigRequest{
		Header: testutil.NewRequestHeader(clusterID),
	}

	resp, err := s.grpcPDClient.GetClusterConfig(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetCluster(), NotNil)

	return resp.GetCluster()
}

func (s *testClusterSuite) TestGetPutConfig(c *C) {
	var err error
	var cleanup func()
	_, s.svr, cleanup, err = NewTestServer(c)
	defer cleanup()
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = testutil.MustNewGrpcClient(c, s.svr.GetAddr())
	clusterID := s.svr.clusterID

	storeAddr := "127.0.0.1:0"
	_, err = s.svr.bootstrapCluster(s.newBootstrapRequest(c, s.svr.clusterID, storeAddr))
	c.Assert(err, IsNil)

	// Get region.
	region := s.getRegion(c, clusterID, []byte("abc"))
	c.Assert(region.GetPeers(), HasLen, 1)
	peer := region.GetPeers()[0]

	// Get region by id.
	regionByID := s.getRegionByID(c, clusterID, region.GetId())
	c.Assert(region, DeepEquals, regionByID)

	// Get store.
	storeID := peer.GetStoreId()
	store := s.getStore(c, clusterID, storeID)

	// Update store.
	store.Address = "127.0.0.1:1"
	s.testPutStore(c, clusterID, store)

	// Remove store.
	s.testRemoveStore(c, clusterID, store)

	// Update cluster config.
	req := &pdpb.PutClusterConfigRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Cluster: &metapb.Cluster{
			Id:           clusterID,
			MaxPeerCount: 5,
		},
	}
	resp, err := s.grpcPDClient.PutClusterConfig(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	meta := s.getClusterConfig(c, clusterID)
	c.Assert(meta.GetMaxPeerCount(), Equals, uint32(5))
}

func putStore(c *C, grpcPDClient pdpb.PDClient, clusterID uint64, store *metapb.Store) (*pdpb.PutStoreResponse, error) {
	req := &pdpb.PutStoreRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  store,
	}
	resp, err := grpcPDClient.PutStore(context.Background(), req)
	return resp, err
}

func (s *baseCluster) testPutStore(c *C, clusterID uint64, store *metapb.Store) {
	// Update store.
	_, err := putStore(c, s.grpcPDClient, clusterID, store)
	c.Assert(err, IsNil)
	updatedStore := s.getStore(c, clusterID, store.GetId())
	c.Assert(updatedStore, DeepEquals, store)

	// Update store again.
	_, err = putStore(c, s.grpcPDClient, clusterID, store)
	c.Assert(err, IsNil)

	// Put new store with a duplicated address when old store is up will fail.
	_, err = putStore(c, s.grpcPDClient, clusterID, s.newStore(c, 0, store.GetAddress(), "2.1.0"))
	c.Assert(err, NotNil)

	// Put new store with a duplicated address when old store is offline will fail.
	s.resetStoreState(c, store.GetId(), metapb.StoreState_Offline)
	_, err = putStore(c, s.grpcPDClient, clusterID, s.newStore(c, 0, store.GetAddress(), "2.1.0"))
	c.Assert(err, NotNil)

	// Put new store with a duplicated address when old store is tombstone is OK.
	s.resetStoreState(c, store.GetId(), metapb.StoreState_Tombstone)
	_, err = putStore(c, s.grpcPDClient, clusterID, s.newStore(c, 0, store.GetAddress(), "2.1.0"))
	c.Assert(err, IsNil)

	// Put a new store.
	_, err = putStore(c, s.grpcPDClient, clusterID, s.newStore(c, 0, "127.0.0.1:12345", "2.1.0"))
	c.Assert(err, IsNil)

	// Put an existed store with duplicated address with other old stores.
	s.resetStoreState(c, store.GetId(), metapb.StoreState_Up)
	_, err = putStore(c, s.grpcPDClient, clusterID, s.newStore(c, store.GetId(), "127.0.0.1:12345", "2.1.0"))
	c.Assert(err, NotNil)
}

func (s *baseCluster) resetStoreState(c *C, storeID uint64, state metapb.StoreState) {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)
	store := cluster.GetStore(storeID)
	c.Assert(store, NotNil)
	newStore := store.Clone(core.SetStoreState(state))
	cluster.Lock()
	err := cluster.putStoreLocked(newStore)
	cluster.Unlock()
	c.Assert(err, IsNil)
}

func (s *baseCluster) testRemoveStore(c *C, clusterID uint64, store *metapb.Store) {
	cluster := s.getRaftCluster(c)

	// When store is up:
	{
		// Case 1: RemoveStore should be OK;
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Up)
		err := cluster.RemoveStore(store.GetId())
		c.Assert(err, IsNil)
		removedStore := s.getStore(c, clusterID, store.GetId())
		c.Assert(removedStore.GetState(), Equals, metapb.StoreState_Offline)
		// Case 2: BuryStore w/ force should be OK;
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Up)
		err = cluster.BuryStore(store.GetId(), true)
		c.Assert(err, IsNil)
		buriedStore := s.getStore(c, clusterID, store.GetId())
		c.Assert(buriedStore.GetState(), Equals, metapb.StoreState_Tombstone)
		// Case 3: BuryStore w/o force should fail.
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Up)
		err = cluster.BuryStore(store.GetId(), false)
		c.Assert(err, NotNil)
	}

	// When store is offline:
	{
		// Case 1: RemoveStore should be OK;
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Offline)
		err := cluster.RemoveStore(store.GetId())
		c.Assert(err, IsNil)
		removedStore := s.getStore(c, clusterID, store.GetId())
		c.Assert(removedStore.GetState(), Equals, metapb.StoreState_Offline)
		// Case 2: BuryStore w/ or w/o force should be OK.
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Offline)
		err = cluster.BuryStore(store.GetId(), false)
		c.Assert(err, IsNil)
		buriedStore := s.getStore(c, clusterID, store.GetId())
		c.Assert(buriedStore.GetState(), Equals, metapb.StoreState_Tombstone)
	}

	// When store is tombstone:
	{
		// Case 1: RemoveStore should should fail;
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Tombstone)
		err := cluster.RemoveStore(store.GetId())
		c.Assert(err, NotNil)
		// Case 2: BuryStore w/ or w/o force should be OK.
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Tombstone)
		err = cluster.BuryStore(store.GetId(), false)
		c.Assert(err, IsNil)
		buriedStore := s.getStore(c, clusterID, store.GetId())
		c.Assert(buriedStore.GetState(), Equals, metapb.StoreState_Tombstone)
	}

	{
		// Put after removed should return tombstone error.
		resp, err := putStore(c, s.grpcPDClient, clusterID, store)
		c.Assert(err, IsNil)
		c.Assert(resp.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_STORE_TOMBSTONE)
	}
	{
		// Update after removed should return tombstone error.
		req := &pdpb.StoreHeartbeatRequest{
			Header: testutil.NewRequestHeader(clusterID),
			Stats:  &pdpb.StoreStats{StoreId: store.GetId()},
		}
		resp, err := s.grpcPDClient.StoreHeartbeat(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(resp.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_STORE_TOMBSTONE)
	}
}

// Make sure PD will not panic if it start and stop again and again.
func (s *testClusterSuite) TestRaftClusterRestart(c *C) {
	var err error
	var cleanup func()
	_, s.svr, cleanup, err = NewTestServer(c)
	defer cleanup()
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
	_, err = s.svr.bootstrapCluster(s.newBootstrapRequest(c, s.svr.clusterID, "127.0.0.1:0"))
	c.Assert(err, IsNil)

	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)
	cluster.stop()

	err = s.svr.createRaftCluster()
	c.Assert(err, IsNil)

	cluster = s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)
	cluster.stop()
}

// Make sure PD will not deadlock if it start and stop again and again.
func (s *testClusterSuite) TestRaftClusterMultipleRestart(c *C) {
	var err error
	var cleanup func()
	_, s.svr, cleanup, err = NewTestServer(c)
	defer cleanup()
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
	_, err = s.svr.bootstrapCluster(s.newBootstrapRequest(c, s.svr.clusterID, "127.0.0.1:0"))
	c.Assert(err, IsNil)
	// add an offline store
	store := s.newStore(c, s.allocID(c), "127.0.0.1:4", "2.1.0")
	store.State = metapb.StoreState_Offline
	cluster := s.svr.GetRaftCluster()
	err = cluster.putStore(store)
	c.Assert(err, IsNil)
	c.Assert(cluster, NotNil)

	// let the job run at small interval
	c.Assert(failpoint.Enable("github.com/pingcap/pd/server/highFrequencyClusterJobs", `return(true)`), IsNil)
	for i := 0; i < 100; i++ {
		err = s.svr.createRaftCluster()
		c.Assert(err, IsNil)
		time.Sleep(time.Millisecond)
		cluster = s.svr.GetRaftCluster()
		c.Assert(cluster, NotNil)
		cluster.stop()
	}
}

func (s *testClusterSuite) TestGetPDMembers(c *C) {
	var err error
	var cleanup func()
	_, s.svr, cleanup, err = NewTestServer(c)
	defer cleanup()
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = testutil.MustNewGrpcClient(c, s.svr.GetAddr())
	req := &pdpb.GetMembersRequest{
		Header: testutil.NewRequestHeader(s.svr.ClusterID()),
	}

	resp, err := s.grpcPDClient.GetMembers(context.Background(), req)
	c.Assert(err, IsNil)
	// A more strict test can be found at api/member_test.go
	c.Assert(len(resp.GetMembers()), Not(Equals), 0)
}

func (s *testClusterSuite) TestStoreVersionChange(c *C) {
	var err error
	var cleanup func()
	_, s.svr, cleanup, err = NewTestServer(c)
	defer cleanup()
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = testutil.MustNewGrpcClient(c, s.svr.GetAddr())
	_, err = s.svr.bootstrapCluster(s.newBootstrapRequest(c, s.svr.clusterID, "127.0.0.1:0"))
	c.Assert(err, IsNil)
	s.svr.SetClusterVersion("2.0.0")
	store := s.newStore(c, s.allocID(c), "127.0.0.1:4", "2.1.0")
	store.State = metapb.StoreState_Up
	var wg sync.WaitGroup
	c.Assert(failpoint.Enable("github.com/pingcap/pd/server/versionChangeConcurrency", `return(true)`), IsNil)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err = putStore(c, s.grpcPDClient, s.svr.clusterID, store)
		c.Assert(err, IsNil)
	}()
	time.Sleep(100 * time.Millisecond)
	s.svr.SetClusterVersion("1.0.0")
	wg.Wait()
	v, err := semver.NewVersion("1.0.0")
	c.Assert(err, IsNil)
	c.Assert(s.svr.GetClusterVersion(), Equals, *v)
	c.Assert(failpoint.Disable("github.com/pingcap/pd/server/versionChangeConcurrency"), IsNil)
}

func (s *testClusterSuite) TestConcurrentHandleRegion(c *C) {
	var err error
	_, s.svr, _, err = NewTestServer(c)
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = testutil.MustNewGrpcClient(c, s.svr.GetAddr())
	storeAddrs := []string{"127.0.1.1:0", "127.0.1.1:1", "127.0.1.1:2"}
	_, err = s.svr.bootstrapCluster(s.newBootstrapRequest(c, s.svr.clusterID, "127.0.0.1:0"))
	c.Assert(err, IsNil)
	s.svr.cluster.Lock()
	s.svr.cluster.storage = core.NewStorage(kv.NewMemoryKV())
	s.svr.cluster.Unlock()
	var stores []*metapb.Store
	for _, addr := range storeAddrs {
		store := s.newStore(c, 0, addr, "2.1.0")
		stores = append(stores, store)
		_, err := putStore(c, s.grpcPDClient, s.svr.clusterID, store)
		c.Assert(err, IsNil)
	}

	var wg sync.WaitGroup
	// register store and bind stream
	for i, store := range stores {
		req := &pdpb.StoreHeartbeatRequest{
			Header: testutil.NewRequestHeader(s.svr.clusterID),
			Stats: &pdpb.StoreStats{
				StoreId:   store.GetId(),
				Capacity:  1000 * (1 << 20),
				Available: 1000 * (1 << 20),
			},
		}
		_, err := s.svr.StoreHeartbeat(context.TODO(), req)
		c.Assert(err, IsNil)
		stream, err := s.grpcPDClient.RegionHeartbeat(context.Background())
		c.Assert(err, IsNil)
		peer := &metapb.Peer{Id: s.allocID(c), StoreId: store.GetId()}
		regionReq := &pdpb.RegionHeartbeatRequest{
			Header: testutil.NewRequestHeader(s.svr.clusterID),
			Region: &metapb.Region{
				Id:    s.allocID(c),
				Peers: []*metapb.Peer{peer},
			},
			Leader: peer,
		}
		err = stream.Send(regionReq)
		c.Assert(err, IsNil)
		// make sure the first store can receive one response
		if i == 0 {
			wg.Add(1)
		}
		go func(isReciver bool) {
			if isReciver {
				_, err := stream.Recv()
				c.Assert(err, IsNil)
				wg.Done()
			}
			for {
				_, err := stream.Recv()
				c.Assert(err, IsNil)
			}
		}(i == 0)
	}
	concurrent := 2000
	for i := 0; i < concurrent; i++ {
		region := &metapb.Region{
			Id:       s.allocID(c),
			StartKey: []byte(fmt.Sprintf("%5d", i)),
			EndKey:   []byte(fmt.Sprintf("%5d", i+1)),
			Peers:    []*metapb.Peer{{Id: s.allocID(c), StoreId: stores[0].GetId()}},
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: initEpochConfVer,
				Version: initEpochVersion,
			},
		}
		if i == 0 {
			region.StartKey = []byte("")
		} else if i == concurrent-1 {
			region.EndKey = []byte("")
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.svr.cluster.HandleRegionHeartbeat(core.NewRegionInfo(region, region.Peers[0]))
			c.Assert(err, IsNil)
		}()
	}
	wg.Wait()
}

var _ = Suite(&testGetStoresSuite{})

type testGetStoresSuite struct {
	cluster *RaftCluster
}

func (s *testGetStoresSuite) SetUpSuite(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := createTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()))
	s.cluster = cluster

	stores := newTestStores(200)

	for _, store := range stores {
		c.Assert(s.cluster.putStoreLocked(store), IsNil)
	}
}

func (s *testGetStoresSuite) BenchmarkGetStores(c *C) {
	for i := 0; i < c.N; i++ {
		// Logic to benchmark
		s.cluster.core.Stores.GetStores()
	}
}

func (s *testClusterSuite) TestSetScheduleOpt(c *C) {
	var err error
	var cleanup func()
	_, s.svr, cleanup, err = NewTestServer(c)
	defer cleanup()
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = testutil.MustNewGrpcClient(c, s.svr.GetAddr())
	clusterID := s.svr.clusterID

	storeAddr := "127.0.0.1:0"
	_, err = s.svr.bootstrapCluster(s.newBootstrapRequest(c, clusterID, storeAddr))
	c.Assert(err, IsNil)

	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)

	scheduleCfg := opt.Load()
	replicateCfg := s.svr.GetReplicationConfig()
	pdServerCfg := s.svr.scheduleOpt.LoadPDServerConfig()

	//PUT GET DELETE succeed
	replicateCfg.MaxReplicas = 5
	scheduleCfg.MaxSnapshotCount = 10
	pdServerCfg.UseRegionStorage = true
	typ, labelKey, labelValue := "testTyp", "testKey", "testValue"
	nsConfig := config.NamespaceConfig{LeaderScheduleLimit: uint64(200)}

	c.Assert(s.svr.SetScheduleConfig(*scheduleCfg), IsNil)
	c.Assert(s.svr.SetPDServerConfig(*pdServerCfg), IsNil)
	c.Assert(s.svr.SetLabelProperty(typ, labelKey, labelValue), IsNil)
	c.Assert(s.svr.SetNamespaceConfig("testNS", nsConfig), IsNil)
	c.Assert(s.svr.SetReplicationConfig(*replicateCfg), IsNil)

	c.Assert(s.svr.GetReplicationConfig().MaxReplicas, Equals, uint64(5))
	c.Assert(s.svr.scheduleOpt.GetMaxSnapshotCount(), Equals, uint64(10))
	c.Assert(s.svr.scheduleOpt.LoadPDServerConfig().UseRegionStorage, Equals, true)
	c.Assert(s.svr.scheduleOpt.LoadLabelPropertyConfig()[typ][0].Key, Equals, "testKey")
	c.Assert(s.svr.scheduleOpt.LoadLabelPropertyConfig()[typ][0].Value, Equals, "testValue")
	c.Assert(s.svr.GetNamespaceConfig("testNS").LeaderScheduleLimit, Equals, uint64(200))

	c.Assert(s.svr.DeleteNamespaceConfig("testNS"), IsNil)
	c.Assert(s.svr.DeleteLabelProperty(typ, labelKey, labelValue), IsNil)

	c.Assert(s.svr.GetNamespaceConfig("testNS").LeaderScheduleLimit, Equals, uint64(0))
	c.Assert(len(s.svr.scheduleOpt.LoadLabelPropertyConfig()[typ]), Equals, 0)

	//PUT GET failed
	oldStorage := s.svr.storage
	s.svr.storage = core.NewStorage(&testErrorKV{})
	replicateCfg.MaxReplicas = 7
	scheduleCfg.MaxSnapshotCount = 20
	pdServerCfg.UseRegionStorage = false

	c.Assert(s.svr.SetScheduleConfig(*scheduleCfg), NotNil)
	c.Assert(s.svr.SetReplicationConfig(*replicateCfg), NotNil)
	c.Assert(s.svr.SetPDServerConfig(*pdServerCfg), NotNil)
	c.Assert(s.svr.SetLabelProperty(typ, labelKey, labelValue), NotNil)
	c.Assert(s.svr.SetNamespaceConfig("testNS", nsConfig), NotNil)

	c.Assert(s.svr.GetReplicationConfig().MaxReplicas, Equals, uint64(5))
	c.Assert(s.svr.scheduleOpt.GetMaxSnapshotCount(), Equals, uint64(10))
	c.Assert(s.svr.scheduleOpt.LoadPDServerConfig().UseRegionStorage, Equals, true)
	c.Assert(s.svr.GetNamespaceConfig("testNS").LeaderScheduleLimit, Equals, uint64(0))
	c.Assert(len(s.svr.scheduleOpt.LoadLabelPropertyConfig()[typ]), Equals, 0)

	//DELETE failed
	s.svr.storage = oldStorage
	c.Assert(s.svr.SetNamespaceConfig("testNS", nsConfig), IsNil)
	c.Assert(s.svr.SetReplicationConfig(*replicateCfg), IsNil)

	s.svr.storage = core.NewStorage(&testErrorKV{})
	c.Assert(s.svr.DeleteLabelProperty(typ, labelKey, labelValue), NotNil)
	c.Assert(s.svr.GetNamespaceConfig("testNS").LeaderScheduleLimit, Equals, uint64(200))
	c.Assert(s.svr.DeleteNamespaceConfig("testNS"), NotNil)

	c.Assert(s.svr.GetNamespaceConfig("testNS").LeaderScheduleLimit, Equals, uint64(200))
	c.Assert(s.svr.scheduleOpt.LoadLabelPropertyConfig()[typ][0].Key, Equals, "testKey")
	c.Assert(s.svr.scheduleOpt.LoadLabelPropertyConfig()[typ][0].Value, Equals, "testValue")
}

var _ = Suite(&testStoresInfoSuite{})

type testStoresInfoSuite struct{}

func checkStaleRegion(origin *metapb.Region, region *metapb.Region) error {
	o := origin.GetRegionEpoch()
	e := region.GetRegionEpoch()

	if e.GetVersion() < o.GetVersion() || e.GetConfVer() < o.GetConfVer() {
		return ErrRegionIsStale(region, origin)
	}

	return nil
}

// Create n stores (0..n).
func newTestStores(n uint64) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, n)
	for i := uint64(1); i <= n; i++ {
		store := &metapb.Store{
			Id: i,
		}
		stores = append(stores, core.NewStoreInfo(store))
	}
	return stores
}

func (s *testStoresInfoSuite) TestStores(c *C) {
	n := uint64(10)
	cache := core.NewStoresInfo()
	stores := newTestStores(n)

	for i, store := range stores {
		id := store.GetID()
		c.Assert(cache.GetStore(id), IsNil)
		c.Assert(cache.BlockStore(id), NotNil)
		cache.SetStore(store)
		c.Assert(cache.GetStore(id), DeepEquals, store)
		c.Assert(cache.GetStoreCount(), Equals, i+1)
		c.Assert(cache.BlockStore(id), IsNil)
		c.Assert(cache.GetStore(id).IsBlocked(), IsTrue)
		c.Assert(cache.BlockStore(id), NotNil)
		cache.UnblockStore(id)
		c.Assert(cache.GetStore(id).IsBlocked(), IsFalse)
	}
	c.Assert(cache.GetStoreCount(), Equals, int(n))

	for _, store := range cache.GetStores() {
		c.Assert(store, DeepEquals, stores[store.GetID()-1])
	}
	for _, store := range cache.GetMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()-1].GetMeta())
	}

	c.Assert(cache.GetStoreCount(), Equals, int(n))
}

var _ = Suite(&testRegionsInfoSuite{})

type testRegionsInfoSuite struct{}

// Create n regions (0..n) of n stores (0..n).
// Each region contains np peers, the first peer is the leader.
func newTestRegions(n, np uint64) []*core.RegionInfo {
	regions := make([]*core.RegionInfo, 0, n)
	for i := uint64(0); i < n; i++ {
		peers := make([]*metapb.Peer, 0, np)
		for j := uint64(0); j < np; j++ {
			peer := &metapb.Peer{
				Id: i*np + j,
			}
			peer.StoreId = (i + j) % n
			peers = append(peers, peer)
		}
		region := &metapb.Region{
			Id:          i,
			Peers:       peers,
			StartKey:    []byte{byte(i)},
			EndKey:      []byte{byte(i + 1)},
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
		}
		regions = append(regions, core.NewRegionInfo(region, peers[0]))
	}
	return regions
}

func (s *testRegionsInfoSuite) Test(c *C) {
	n, np := uint64(10), uint64(3)
	cache := core.NewRegionsInfo()
	regions := newTestRegions(n, np)

	for i := uint64(0); i < n; i++ {
		region := regions[i]
		regionKey := []byte{byte(i)}

		c.Assert(cache.GetRegion(i), IsNil)
		c.Assert(cache.SearchRegion(regionKey), IsNil)
		checkRegions(c, cache, regions[0:i])

		cache.AddRegion(region)
		checkRegion(c, cache.GetRegion(i), region)
		checkRegion(c, cache.SearchRegion(regionKey), region)
		checkRegions(c, cache, regions[0:(i+1)])
		// previous region
		if i == 0 {
			c.Assert(cache.SearchPrevRegion(regionKey), IsNil)
		} else {
			checkRegion(c, cache.SearchPrevRegion(regionKey), regions[i-1])
		}
		// Update leader to peer np-1.
		newRegion := region.Clone(core.WithLeader(region.GetPeers()[np-1]))
		regions[i] = newRegion
		cache.SetRegion(newRegion)
		checkRegion(c, cache.GetRegion(i), newRegion)
		checkRegion(c, cache.SearchRegion(regionKey), newRegion)
		checkRegions(c, cache, regions[0:(i+1)])

		cache.RemoveRegion(region)
		c.Assert(cache.GetRegion(i), IsNil)
		c.Assert(cache.SearchRegion(regionKey), IsNil)
		checkRegions(c, cache, regions[0:i])

		// Reset leader to peer 0.
		newRegion = region.Clone(core.WithLeader(region.GetPeers()[0]))
		regions[i] = newRegion
		cache.AddRegion(newRegion)
		checkRegion(c, cache.GetRegion(i), newRegion)
		checkRegions(c, cache, regions[0:(i+1)])
		checkRegion(c, cache.SearchRegion(regionKey), newRegion)
	}

	for i := uint64(0); i < n; i++ {
		region := cache.RandLeaderRegion(i, core.HealthRegion())
		c.Assert(region.GetLeader().GetStoreId(), Equals, i)

		region = cache.RandFollowerRegion(i, core.HealthRegion())
		c.Assert(region.GetLeader().GetStoreId(), Not(Equals), i)

		c.Assert(region.GetStorePeer(i), NotNil)
	}

	// check overlaps
	// clone it otherwise there are two items with the same key in the tree
	overlapRegion := regions[n-1].Clone(core.WithStartKey(regions[n-2].GetStartKey()))
	cache.AddRegion(overlapRegion)
	c.Assert(cache.GetRegion(n-2), IsNil)
	c.Assert(cache.GetRegion(n-1), NotNil)

	// All regions will be filtered out if they have pending peers.
	for i := uint64(0); i < n; i++ {
		for j := 0; j < cache.GetStoreLeaderCount(i); j++ {
			region := cache.RandLeaderRegion(i, core.HealthRegion())
			newRegion := region.Clone(core.WithPendingPeers(region.GetPeers()))
			cache.SetRegion(newRegion)
		}
		c.Assert(cache.RandLeaderRegion(i, core.HealthRegion()), IsNil)
	}
	for i := uint64(0); i < n; i++ {
		c.Assert(cache.RandFollowerRegion(i, core.HealthRegion()), IsNil)
	}
}

func checkRegion(c *C, a *core.RegionInfo, b *core.RegionInfo) {
	c.Assert(a, DeepEquals, b)
	c.Assert(a.GetMeta(), DeepEquals, b.GetMeta())
	c.Assert(a.GetLeader(), DeepEquals, b.GetLeader())
	c.Assert(a.GetPeers(), DeepEquals, b.GetPeers())
	if len(a.GetDownPeers()) > 0 || len(b.GetDownPeers()) > 0 {
		c.Assert(a.GetDownPeers(), DeepEquals, b.GetDownPeers())
	}
	if len(a.GetPendingPeers()) > 0 || len(b.GetPendingPeers()) > 0 {
		c.Assert(a.GetPendingPeers(), DeepEquals, b.GetPendingPeers())
	}
}

func checkRegionsKV(c *C, s *core.Storage, regions []*core.RegionInfo) {
	if s != nil {
		for _, region := range regions {
			var meta metapb.Region
			ok, err := s.LoadRegion(region.GetID(), &meta)
			c.Assert(ok, IsTrue)
			c.Assert(err, IsNil)
			c.Assert(&meta, DeepEquals, region.GetMeta())
		}
	}
}

func checkRegions(c *C, cache *core.RegionsInfo, regions []*core.RegionInfo) {
	regionCount := make(map[uint64]int)
	leaderCount := make(map[uint64]int)
	followerCount := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.GetPeers() {
			regionCount[peer.StoreId]++
			if peer.Id == region.GetLeader().Id {
				leaderCount[peer.StoreId]++
				checkRegion(c, cache.GetLeader(peer.StoreId, region), region)
			} else {
				followerCount[peer.StoreId]++
				checkRegion(c, cache.GetFollower(peer.StoreId, region), region)
			}
		}
	}

	c.Assert(cache.GetRegionCount(), Equals, len(regions))
	for id, count := range regionCount {
		c.Assert(cache.GetStoreRegionCount(id), Equals, count)
	}
	for id, count := range leaderCount {
		c.Assert(cache.GetStoreLeaderCount(id), Equals, count)
	}
	for id, count := range followerCount {
		c.Assert(cache.GetStoreFollowerCount(id), Equals, count)
	}

	for _, region := range cache.GetRegions() {
		checkRegion(c, region, regions[region.GetID()])
	}
	for _, region := range cache.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()].GetMeta())
	}
}

var _ = Suite(&testClusterInfoSuite{})

type testClusterInfoSuite struct{}

func (s *testClusterInfoSuite) TestLoadClusterInfo(c *C) {
	server, cleanup := mustRunTestServer(c)
	defer cleanup()

	storage := server.storage
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)

	raftCluster := createTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()))
	// Cluster is not bootstrapped.
	cluster, err := raftCluster.loadClusterInfo()
	c.Assert(err, IsNil)
	c.Assert(cluster, IsNil)

	// Save meta, stores and regions.
	n := 10
	meta := &metapb.Cluster{Id: 123}
	c.Assert(storage.SaveMeta(meta), IsNil)
	stores := mustSaveStores(c, storage, n)
	regions := mustSaveRegions(c, storage, n)

	raftCluster = createTestRaftCluster(server.idAllocator, opt, storage)
	cluster, err = raftCluster.loadClusterInfo()
	c.Assert(err, IsNil)
	c.Assert(cluster, NotNil)

	// Check meta, stores, and regions.
	c.Assert(cluster.GetConfig(), DeepEquals, meta)
	c.Assert(cluster.getStoreCount(), Equals, n)
	for _, store := range cluster.GetMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()])
	}
	c.Assert(cluster.core.Regions.GetRegionCount(), Equals, n)
	for _, region := range cluster.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
	}
}

func (s *testClusterInfoSuite) TestStoreHeartbeat(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := createTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()))

	n, np := uint64(3), uint64(3)
	stores := newTestStores(n)
	regions := newTestRegions(n, np)

	for _, region := range regions {
		c.Assert(cluster.putRegion(region), IsNil)
	}
	c.Assert(cluster.core.Regions.GetRegionCount(), Equals, int(n))

	for i, store := range stores {
		storeStats := &pdpb.StoreStats{
			StoreId:     store.GetID(),
			Capacity:    100,
			Available:   50,
			RegionCount: 1,
		}
		c.Assert(cluster.handleStoreHeartbeat(storeStats), NotNil)

		c.Assert(cluster.putStoreLocked(store), IsNil)
		c.Assert(cluster.getStoreCount(), Equals, i+1)

		c.Assert(store.GetLastHeartbeatTS().IsZero(), IsTrue)

		c.Assert(cluster.handleStoreHeartbeat(storeStats), IsNil)

		s := cluster.GetStore(store.GetID())
		c.Assert(s.GetLastHeartbeatTS().IsZero(), IsFalse)
		c.Assert(s.GetStoreStats(), DeepEquals, storeStats)
	}

	c.Assert(cluster.getStoreCount(), Equals, int(n))

	for _, store := range stores {
		tmp := &metapb.Store{}
		ok, err := cluster.storage.LoadStore(store.GetID(), tmp)
		c.Assert(ok, IsTrue)
		c.Assert(err, IsNil)
		c.Assert(tmp, DeepEquals, store.GetMeta())
	}
}

func (s *testClusterInfoSuite) TestRegionHeartbeat(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := createTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()))

	n, np := uint64(3), uint64(3)

	stores := newTestStores(3)
	regions := newTestRegions(n, np)

	for _, store := range stores {
		c.Assert(cluster.putStoreLocked(store), IsNil)
	}

	for i, region := range regions {
		// region does not exist.
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is the same, not updated.
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])
		origin := region
		// region is updated.
		region = origin.Clone(core.WithIncVersion())
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is stale (Version).
		stale := origin.Clone(core.WithIncConfVer())
		c.Assert(cluster.processRegionHeartbeat(stale), NotNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is updated.
		region = origin.Clone(
			core.WithIncVersion(),
			core.WithIncConfVer(),
		)
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is stale (ConfVer).
		stale = origin.Clone(core.WithIncConfVer())
		c.Assert(cluster.processRegionHeartbeat(stale), NotNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// Add a down peer.
		region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{
			{
				Peer:        region.GetPeers()[rand.Intn(len(region.GetPeers()))],
				DownSeconds: 42,
			},
		}))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Add a pending peer.
		region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetPeers()[rand.Intn(len(region.GetPeers()))]}))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Clear down peers.
		region = region.Clone(core.WithDownPeers(nil))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Clear pending peers.
		region = region.Clone(core.WithPendingPeers(nil))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Remove peers.
		origin = region
		region = origin.Clone(core.SetPeers(region.GetPeers()[:1]))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])
		// Add peers.
		region = origin
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// Change leader.
		region = region.Clone(core.WithLeader(region.GetPeers()[1]))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change ApproximateSize.
		region = region.Clone(core.SetApproximateSize(144))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change ApproximateKeys.
		region = region.Clone(core.SetApproximateKeys(144000))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change bytes written.
		region = region.Clone(core.SetWrittenBytes(24000))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change keys written.
		region = region.Clone(core.SetWrittenKeys(240))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change bytes read.
		region = region.Clone(core.SetReadBytes(1080000))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change keys read.
		region = region.Clone(core.SetReadKeys(1080))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
	}

	regionCounts := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.GetPeers() {
			regionCounts[peer.GetStoreId()]++
		}
	}
	for id, count := range regionCounts {
		c.Assert(cluster.GetStoreRegionCount(id), Equals, count)
	}

	for _, region := range cluster.GetRegions() {
		checkRegion(c, region, regions[region.GetID()])
	}
	for _, region := range cluster.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()].GetMeta())
	}

	for _, region := range regions {
		for _, store := range cluster.GetRegionStores(region) {
			c.Assert(region.GetStorePeer(store.GetID()), NotNil)
		}
		for _, store := range cluster.GetFollowerStores(region) {
			peer := region.GetStorePeer(store.GetID())
			c.Assert(peer.GetId(), Not(Equals), region.GetLeader().GetId())
		}
	}

	for _, store := range cluster.core.Stores.GetStores() {
		c.Assert(store.GetLeaderCount(), Equals, cluster.core.Regions.GetStoreLeaderCount(store.GetID()))
		c.Assert(store.GetRegionCount(), Equals, cluster.core.Regions.GetStoreRegionCount(store.GetID()))
		c.Assert(store.GetLeaderSize(), Equals, cluster.core.Regions.GetStoreLeaderRegionSize(store.GetID()))
		c.Assert(store.GetRegionSize(), Equals, cluster.core.Regions.GetStoreRegionSize(store.GetID()))
	}

	// Test with storage.
	if storage := cluster.storage; storage != nil {
		for _, region := range regions {
			tmp := &metapb.Region{}
			ok, err := storage.LoadRegion(region.GetID(), tmp)
			c.Assert(ok, IsTrue)
			c.Assert(err, IsNil)
			c.Assert(tmp, DeepEquals, region.GetMeta())
		}

		// Check overlap with stale version
		overlapRegion := regions[n-1].Clone(
			core.WithStartKey([]byte("")),
			core.WithEndKey([]byte("")),
			core.WithNewRegionID(10000),
			core.WithDecVersion(),
		)
		c.Assert(cluster.processRegionHeartbeat(overlapRegion), NotNil)
		region := &metapb.Region{}
		ok, err := storage.LoadRegion(regions[n-1].GetID(), region)
		c.Assert(ok, IsTrue)
		c.Assert(err, IsNil)
		c.Assert(region, DeepEquals, regions[n-1].GetMeta())
		ok, err = storage.LoadRegion(regions[n-2].GetID(), region)
		c.Assert(ok, IsTrue)
		c.Assert(err, IsNil)
		c.Assert(region, DeepEquals, regions[n-2].GetMeta())
		ok, err = storage.LoadRegion(overlapRegion.GetID(), region)
		c.Assert(ok, IsFalse)
		c.Assert(err, IsNil)

		// Check overlap
		overlapRegion = regions[n-1].Clone(
			core.WithStartKey(regions[n-2].GetStartKey()),
			core.WithNewRegionID(regions[n-1].GetID()+1),
		)
		c.Assert(cluster.processRegionHeartbeat(overlapRegion), IsNil)
		region = &metapb.Region{}
		ok, err = storage.LoadRegion(regions[n-1].GetID(), region)
		c.Assert(ok, IsFalse)
		c.Assert(err, IsNil)
		ok, err = storage.LoadRegion(regions[n-2].GetID(), region)
		c.Assert(ok, IsFalse)
		c.Assert(err, IsNil)
		ok, err = storage.LoadRegion(overlapRegion.GetID(), region)
		c.Assert(ok, IsTrue)
		c.Assert(err, IsNil)
		c.Assert(region, DeepEquals, overlapRegion.GetMeta())
	}
}

func heartbeatRegions(c *C, cluster *RaftCluster, regions []*core.RegionInfo) {
	// Heartbeat and check region one by one.
	for _, r := range regions {
		c.Assert(cluster.processRegionHeartbeat(r), IsNil)

		checkRegion(c, cluster.GetRegion(r.GetID()), r)
		checkRegion(c, cluster.GetRegionInfoByKey(r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkRegion(c, cluster.GetRegionInfoByKey([]byte{end - 1}), r)
		}
	}

	// Check all regions after handling all heartbeats.
	for _, r := range regions {
		checkRegion(c, cluster.GetRegion(r.GetID()), r)
		checkRegion(c, cluster.GetRegionInfoByKey(r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkRegion(c, cluster.GetRegionInfoByKey([]byte{end - 1}), r)
			result := cluster.GetRegionInfoByKey([]byte{end + 1})
			c.Assert(result.GetID(), Not(Equals), r.GetID())
		}
	}
}

func (s *testClusterInfoSuite) TestHeartbeatSplit(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := createTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()))

	// 1: [nil, nil)
	region1 := core.NewRegionInfo(&metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, nil)
	c.Assert(cluster.processRegionHeartbeat(region1), IsNil)
	checkRegion(c, cluster.GetRegionInfoByKey([]byte("foo")), region1)

	// split 1 to 2: [nil, m) 1: [m, nil), sync 2 first.
	region1 = region1.Clone(
		core.WithStartKey([]byte("m")),
		core.WithIncVersion(),
	)
	region2 := core.NewRegionInfo(&metapb.Region{Id: 2, EndKey: []byte("m"), RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, nil)
	c.Assert(cluster.processRegionHeartbeat(region2), IsNil)
	checkRegion(c, cluster.GetRegionInfoByKey([]byte("a")), region2)
	// [m, nil) is missing before r1's heartbeat.
	c.Assert(cluster.GetRegionInfoByKey([]byte("z")), IsNil)

	c.Assert(cluster.processRegionHeartbeat(region1), IsNil)
	checkRegion(c, cluster.GetRegionInfoByKey([]byte("z")), region1)

	// split 1 to 3: [m, q) 1: [q, nil), sync 1 first.
	region1 = region1.Clone(
		core.WithStartKey([]byte("q")),
		core.WithIncVersion(),
	)
	region3 := core.NewRegionInfo(&metapb.Region{Id: 3, StartKey: []byte("m"), EndKey: []byte("q"), RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, nil)
	c.Assert(cluster.processRegionHeartbeat(region1), IsNil)
	checkRegion(c, cluster.GetRegionInfoByKey([]byte("z")), region1)
	checkRegion(c, cluster.GetRegionInfoByKey([]byte("a")), region2)
	// [m, q) is missing before r3's heartbeat.
	c.Assert(cluster.GetRegionInfoByKey([]byte("n")), IsNil)
	c.Assert(cluster.processRegionHeartbeat(region3), IsNil)
	checkRegion(c, cluster.GetRegionInfoByKey([]byte("n")), region3)
}

func (s *testClusterInfoSuite) TestRegionSplitAndMerge(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := createTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()))

	regions := []*core.RegionInfo{core.NewTestRegionInfo([]byte{}, []byte{})}

	// Byte will underflow/overflow if n > 7.
	n := 7

	// Split.
	for i := 0; i < n; i++ {
		regions = core.SplitRegions(regions)
		heartbeatRegions(c, cluster, regions)
	}

	// Merge.
	for i := 0; i < n; i++ {
		regions = core.MergeRegions(regions)
		heartbeatRegions(c, cluster, regions)
	}

	// Split twice and merge once.
	for i := 0; i < n*2; i++ {
		if (i+1)%3 == 0 {
			regions = core.MergeRegions(regions)
		} else {
			regions = core.SplitRegions(regions)
		}
		heartbeatRegions(c, cluster, regions)
	}
}

func (s *testClusterInfoSuite) TestUpdateStorePendingPeerCount(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	stores := newTestStores(5)
	for _, s := range stores {
		c.Assert(tc.putStoreLocked(s), IsNil)
	}
	peers := []*metapb.Peer{
		{
			Id:      2,
			StoreId: 1,
		},
		{
			Id:      3,
			StoreId: 2,
		},
		{
			Id:      3,
			StoreId: 3,
		},
		{
			Id:      4,
			StoreId: 4,
		},
	}
	origin := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers[:3]}, peers[0], core.WithPendingPeers(peers[1:3]))
	c.Assert(tc.processRegionHeartbeat(origin), IsNil)
	checkPendingPeerCount([]int{0, 1, 1, 0}, tc.RaftCluster, c)
	newRegion := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers[1:]}, peers[1], core.WithPendingPeers(peers[3:4]))
	c.Assert(tc.processRegionHeartbeat(newRegion), IsNil)
	checkPendingPeerCount([]int{0, 0, 0, 1}, tc.RaftCluster, c)
}

func checkPendingPeerCount(expect []int, cluster *RaftCluster, c *C) {
	for i, e := range expect {
		s := cluster.core.Stores.GetStore(uint64(i + 1))
		c.Assert(s.GetPendingPeerCount(), Equals, e)
	}
}

var _ = Suite(&testClusterUtilSuite{})

type testClusterUtilSuite struct{}

func (s *testClusterUtilSuite) TestCheckStaleRegion(c *C) {
	// (0, 0) v.s. (0, 0)
	region := core.NewTestRegionInfo([]byte{}, []byte{})
	origin := core.NewTestRegionInfo([]byte{}, []byte{})
	c.Assert(checkStaleRegion(region.GetMeta(), origin.GetMeta()), IsNil)
	c.Assert(checkStaleRegion(origin.GetMeta(), region.GetMeta()), IsNil)

	// (1, 0) v.s. (0, 0)
	region.GetRegionEpoch().Version++
	c.Assert(checkStaleRegion(origin.GetMeta(), region.GetMeta()), IsNil)
	c.Assert(checkStaleRegion(region.GetMeta(), origin.GetMeta()), NotNil)

	// (1, 1) v.s. (0, 0)
	region.GetRegionEpoch().ConfVer++
	c.Assert(checkStaleRegion(origin.GetMeta(), region.GetMeta()), IsNil)
	c.Assert(checkStaleRegion(region.GetMeta(), origin.GetMeta()), NotNil)

	// (0, 1) v.s. (0, 0)
	region.GetRegionEpoch().Version--
	c.Assert(checkStaleRegion(origin.GetMeta(), region.GetMeta()), IsNil)
	c.Assert(checkStaleRegion(region.GetMeta(), origin.GetMeta()), NotNil)
}

func mustSaveStores(c *C, s *core.Storage, n int) []*metapb.Store {
	stores := make([]*metapb.Store, 0, n)
	for i := 0; i < n; i++ {
		store := &metapb.Store{Id: uint64(i)}
		stores = append(stores, store)
	}

	for _, store := range stores {
		c.Assert(s.SaveStore(store), IsNil)
	}

	return stores
}

func mustSaveRegions(c *C, s *core.Storage, n int) []*metapb.Region {
	regions := make([]*metapb.Region, 0, n)
	for i := 0; i < n; i++ {
		region := newTestRegionMeta(uint64(i))
		regions = append(regions, region)
	}

	for _, region := range regions {
		c.Assert(s.SaveRegion(region), IsNil)
	}
	c.Assert(s.Flush(), IsNil)

	return regions
}
