package server

import (
	"net"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

const (
	initEpochVersion uint64 = 1
	initEpochConfVer uint64 = 1
)

var _ = Suite(&testClusterSuite{})

type testClusterBaseSuite struct {
	client *clientv3.Client
	svr    *Server
}

type testClusterSuite struct {
	testClusterBaseSuite
}

func (s *testClusterSuite) getRootPath() string {
	return "test_cluster"
}

func (s *testClusterSuite) SetUpSuite(c *C) {
	s.svr = newTestServer(c, s.getRootPath())
	s.client = newEtcdClient(c)
	deleteRoot(c, s.client, s.getRootPath())

	go s.svr.Run()
}

func (s *testClusterSuite) TearDownSuite(c *C) {
	s.svr.Close()
	s.client.Close()
}

func (s *testClusterBaseSuite) allocID(c *C) uint64 {
	id, err := s.svr.idAlloc.Alloc()
	c.Assert(err, IsNil)
	return id
}

func newRequestHeader(clusterID uint64) *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: proto.Uint64(clusterID),
	}
}

func (s *testClusterBaseSuite) newStore(c *C, storeID uint64, addr string) *metapb.Store {
	if storeID == 0 {
		storeID = s.allocID(c)
	}

	return &metapb.Store{
		Id:      proto.Uint64(storeID),
		Address: proto.String(addr),
	}
}

func (s *testClusterBaseSuite) newRegion(c *C, regionID uint64, startKey []byte,
	endKey []byte, storeIDs []uint64, epoch *metapb.RegionEpoch) *metapb.Region {
	if regionID == 0 {
		regionID = s.allocID(c)
	}

	if epoch == nil {
		epoch = &metapb.RegionEpoch{
			ConfVer: proto.Uint64(initEpochConfVer),
			Version: proto.Uint64(initEpochVersion),
		}
	}

	return &metapb.Region{
		Id:          proto.Uint64(regionID),
		StartKey:    startKey,
		EndKey:      endKey,
		RegionEpoch: epoch,
		StoreIds:    storeIDs,
	}
}

func (s *testClusterSuite) TestBootstrap(c *C) {
	leader := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	conn, err := net.Dial("tcp", leader.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	clusterID := uint64(0)

	// IsBootstrapped returns false.
	req := s.newIsBootstrapRequest(clusterID)
	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.IsBootstrapped, NotNil)
	c.Assert(resp.IsBootstrapped.GetBootstrapped(), IsFalse)

	// Bootstrap the cluster.
	storeAddr := "127.0.0.1:0"
	s.bootstrapCluster(c, conn, clusterID, storeAddr)

	// IsBootstrapped returns true.
	req = s.newIsBootstrapRequest(clusterID)
	sendRequest(c, conn, 0, req)
	_, resp = recvResponse(c, conn)
	c.Assert(resp.IsBootstrapped, NotNil)
	c.Assert(resp.IsBootstrapped.GetBootstrapped(), IsTrue)

	// check bootstrapped error.
	req = s.newBootstrapRequest(c, clusterID, storeAddr)
	sendRequest(c, conn, 0, req)
	_, resp = recvResponse(c, conn)
	c.Assert(resp.Bootstrap, IsNil)
	c.Assert(resp.Header.Error, NotNil)
	c.Assert(resp.Header.Error.Bootstrapped, NotNil)
}

func (s *testClusterBaseSuite) newIsBootstrapRequest(clusterID uint64) *pdpb.Request {
	req := &pdpb.Request{
		Header:         newRequestHeader(clusterID),
		CmdType:        pdpb.CommandType_IsBootstrapped.Enum(),
		IsBootstrapped: &pdpb.IsBootstrappedRequest{},
	}

	return req
}

func (s *testClusterBaseSuite) newBootstrapRequest(c *C, clusterID uint64, storeAddr string) *pdpb.Request {
	store := s.newStore(c, 0, storeAddr)
	region := s.newRegion(c, 0, []byte{}, []byte{}, []uint64{store.GetId()}, nil)

	req := &pdpb.Request{
		Header:  newRequestHeader(clusterID),
		CmdType: pdpb.CommandType_Bootstrap.Enum(),
		Bootstrap: &pdpb.BootstrapRequest{
			Store:  store,
			Region: region,
		},
	}

	return req
}

// helper function to check and bootstrap.
func (s *testClusterBaseSuite) bootstrapCluster(c *C, conn net.Conn, clusterID uint64, storeAddr string) {
	req := s.newBootstrapRequest(c, clusterID, storeAddr)
	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.Bootstrap, NotNil)
}

func (s *testClusterBaseSuite) tryBootstrapCluster(c *C, conn net.Conn, clusterID uint64, storeAddr string) {
	req := s.newBootstrapRequest(c, clusterID, storeAddr)
	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	if resp.Bootstrap == nil {
		c.Assert(resp.Header.Error, NotNil)
		c.Assert(resp.Header.Error.Bootstrapped, NotNil)
	}
}

func (s *testClusterBaseSuite) getStore(c *C, conn net.Conn, clusterID uint64, storeID uint64) *metapb.Store {
	req := &pdpb.Request{
		Header:  newRequestHeader(clusterID),
		CmdType: pdpb.CommandType_GetStore.Enum(),
		GetStore: &pdpb.GetStoreRequest{
			StoreId: proto.Uint64(storeID),
		},
	}

	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.GetStore, NotNil)
	c.Assert(resp.GetStore.GetStore().GetId(), Equals, uint64(storeID))

	return resp.GetStore.GetStore()
}

func (s *testClusterBaseSuite) getRegion(c *C, conn net.Conn, clusterID uint64, regionKey []byte) *metapb.Region {
	req := &pdpb.Request{
		Header:  newRequestHeader(clusterID),
		CmdType: pdpb.CommandType_GetRegion.Enum(),
		GetRegion: &pdpb.GetRegionRequest{
			RegionKey: regionKey,
		},
	}

	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.GetRegion, NotNil)
	c.Assert(resp.GetRegion.GetRegion(), NotNil)

	return resp.GetRegion.GetRegion()
}

func (s *testClusterBaseSuite) getClusterConfig(c *C, conn net.Conn, clusterID uint64) *metapb.Cluster {
	req := &pdpb.Request{
		Header:  newRequestHeader(clusterID),
		CmdType: pdpb.CommandType_GetClusterConfig.Enum(),
		GetClusterConfig: &pdpb.GetClusterConfigRequest{
			ClusterId: proto.Uint64(clusterID),
		},
	}

	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.GetClusterConfig, NotNil)
	c.Assert(resp.GetClusterConfig.GetCluster(), NotNil)

	return resp.GetClusterConfig.GetCluster()
}

func (s *testClusterSuite) TestGetPutConfig(c *C) {
	leader := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	conn, err := net.Dial("tcp", leader.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	clusterID := uint64(0)

	storeAddr := "127.0.0.1:0"
	s.tryBootstrapCluster(c, conn, clusterID, storeAddr)

	// Get region.
	region := s.getRegion(c, conn, clusterID, []byte("abc"))
	c.Assert(region.GetStoreIds(), HasLen, 1)

	// Get store.
	storeID := region.GetStoreIds()[0]
	store := s.getStore(c, conn, clusterID, storeID)
	c.Assert(store.GetAddress(), Equals, storeAddr)

	// Update store.
	storeAddr = "127.0.0.1:1"
	req := &pdpb.Request{
		Header:  newRequestHeader(clusterID),
		CmdType: pdpb.CommandType_PutStore.Enum(),
		PutStore: &pdpb.PutStoreRequest{
			Store: s.newStore(c, storeID, storeAddr),
		},
	}

	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.PutStore, NotNil)

	store = s.getStore(c, conn, clusterID, storeID)
	c.Assert(store.GetAddress(), Equals, storeAddr)

	// Update cluster config.
	req = &pdpb.Request{
		Header:  newRequestHeader(clusterID),
		CmdType: pdpb.CommandType_PutClusterConfig.Enum(),
		PutClusterConfig: &pdpb.PutClusterConfigRequest{
			Cluster: &metapb.Cluster{
				Id:            proto.Uint64(clusterID),
				MaxPeerNumber: proto.Uint32(5),
			},
		},
	}
	sendRequest(c, conn, 0, req)
	_, resp = recvResponse(c, conn)
	c.Assert(resp.PutClusterConfig, NotNil)
	meta := s.getClusterConfig(c, conn, clusterID)
	c.Assert(meta.GetMaxPeerNumber(), Equals, uint32(5))
}

var _ = Suite(&testClusterCacheSuite{})

type testClusterCacheSuite struct {
	testClusterBaseSuite
}

func (s *testClusterCacheSuite) getRootPath() string {
	return "test_cluster_cache"
}

func (s *testClusterCacheSuite) SetUpSuite(c *C) {
	s.svr = newTestServer(c, s.getRootPath())
	s.client = newEtcdClient(c)
	deleteRoot(c, s.client, s.getRootPath())

	go s.svr.Run()
}

func (s *testClusterCacheSuite) TearDownSuite(c *C) {
	s.svr.Close()
	s.client.Close()
}

func (s *testClusterCacheSuite) TestCache(c *C) {
	mustGetLeader(c, s.client, s.svr.getLeaderPath())

	clusterID := uint64(0)

	req := s.newBootstrapRequest(c, clusterID, "127.0.0.1:1")
	store1 := req.Bootstrap.Store

	_, err := s.svr.bootstrapCluster(req.Bootstrap)
	c.Assert(err, IsNil)

	cluster, err := s.svr.getRaftCluster()
	c.Assert(err, IsNil)

	// Add another store.
	store2 := s.newStore(c, 0, "127.0.0.1:2")
	err = cluster.PutStore(store2)
	c.Assert(err, IsNil)

	stores := map[uint64]*metapb.Store{
		store1.GetId(): store1,
		store2.GetId(): store2,
	}

	s.svr.cluster.Stop()

	cluster, err = s.svr.getRaftCluster()
	c.Assert(err, IsNil)
	c.Assert(cluster, IsNil)

	allStores, err := s.svr.cluster.GetAllStores()
	c.Assert(err, IsNil)
	c.Assert(allStores, HasLen, 2)
	for _, store := range allStores {
		c.Assert(stores, HasKey, store.GetId())
	}
}
