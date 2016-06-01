package server

import (
	"net"

	"github.com/golang/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raftpb"
)

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
	leaderPd := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	conn, err := net.Dial("tcp", leaderPd.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	clusterID := uint64(0)

	req := s.newBootstrapRequest(c, clusterID, "127.0.0.1:1")
	store1 := req.Bootstrap.Store

	_, err = s.svr.bootstrapCluster(req.Bootstrap)
	c.Assert(err, IsNil)

	cluster, err := s.svr.getRaftCluster()
	c.Assert(err, IsNil)

	// Check cachedCluster.
	c.Assert(cluster.cachedCluster.getMeta().GetId(), Equals, clusterID)
	c.Assert(cluster.cachedCluster.getMeta().GetMaxPeerCount(), Equals, defaultMaxPeerCount)

	cacheStore := cluster.cachedCluster.getStore(store1.GetId())
	c.Assert(cacheStore.store, DeepEquals, store1)
	c.Assert(cluster.cachedCluster.regions.regions, HasLen, 1)
	c.Assert(cluster.cachedCluster.regions.searchRegions.Len(), Equals, 1)
	c.Assert(cluster.cachedCluster.regions.leaders.storeRegions, HasLen, 0)
	c.Assert(cluster.cachedCluster.regions.leaders.regionStores, HasLen, 0)

	// Add another store.
	store2 := s.newStore(c, 0, "127.0.0.1:2")
	err = cluster.PutStore(store2)
	c.Assert(err, IsNil)

	// Check cachedCluster.
	c.Assert(cluster.cachedCluster.getMeta().GetId(), Equals, clusterID)
	c.Assert(cluster.cachedCluster.getMeta().GetMaxPeerCount(), Equals, defaultMaxPeerCount)

	cacheStore = cluster.cachedCluster.getStore(store1.GetId())
	c.Assert(cacheStore.store, DeepEquals, store1)
	cacheStore = cluster.cachedCluster.getStore(store2.GetId())
	c.Assert(cacheStore.store, DeepEquals, store2)
	cacheStores := cluster.cachedCluster.getStores()
	c.Assert(cacheStores, HasLen, 2)
	c.Assert(cluster.cachedCluster.regions.regions, HasLen, 1)

	// There is only one region now, directly use it for test.
	regionKey := []byte("a")
	region, err := cluster.GetRegion(regionKey)
	c.Assert(err, IsNil)
	c.Assert(region.Peers, HasLen, 1)

	cacheRegions := cluster.cachedCluster.regions
	c.Assert(cacheRegions.regions, HasLen, 1)
	c.Assert(cacheRegions.searchRegions.Len(), Equals, 1)
	c.Assert(cacheRegions.leaders.storeRegions, HasLen, 0)
	c.Assert(cacheRegions.leaders.regionStores, HasLen, 0)

	leaderPeer := region.GetPeers()[0]
	res := heartbeatRegion(c, conn, clusterID, 0, region, leaderPeer)
	c.Assert(res.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(leaderPeer.GetId(), Not(Equals), res.GetPeer().GetId())

	cacheStores = cluster.cachedCluster.getStores()
	c.Assert(cacheStores, HasLen, 2)
	c.Assert(cacheRegions.regions, HasLen, 1)
	cacheRegion := cacheRegions.regions[region.GetId()]
	c.Assert(cacheRegion, DeepEquals, region)

	c.Assert(cacheRegions.leaders.storeRegions, HasKey, store1.GetId())
	c.Assert(cacheRegions.leaders.storeRegions, Not(HasKey), store2.GetId())

	c.Assert(cacheRegions.leaders.storeRegions[store1.GetId()], HasKey, region.GetId())
	c.Assert(cacheRegions.leaders.regionStores[region.GetId()], Equals, store1.GetId())

	// Add another peer.
	region.Peers = append(region.Peers, res.GetPeer())
	region.RegionEpoch.ConfVer = proto.Uint64(region.GetRegionEpoch().GetConfVer() + 1)
	res = heartbeatRegion(c, conn, clusterID, 0, region, leaderPeer)
	c.Assert(res, IsNil)

	c.Assert(cluster.cachedCluster.regions.regions, HasLen, 1)

	oldRegionID := region.GetId()
	cacheRegion = cacheRegions.regions[oldRegionID]
	region, err = cluster.GetRegion(regionKey)
	c.Assert(err, IsNil)
	c.Assert(region.GetPeers(), HasLen, 2)
	c.Assert(cacheRegion, DeepEquals, region)

	c.Assert(cacheRegions.leaders.storeRegions, HasKey, store1.GetId())
	c.Assert(cacheRegions.leaders.storeRegions, Not(HasKey), store2.GetId())

	c.Assert(cacheRegions.leaders.storeRegions[store1.GetId()], HasKey, region.GetId())
	c.Assert(cacheRegions.leaders.regionStores[region.GetId()], Equals, store1.GetId())

	// Test change leader peer.
	newLeaderPeer := region.GetPeers()[1]
	c.Assert(leaderPeer.GetId(), Not(Equals), newLeaderPeer.GetId())

	// There is no store to add peer, so the return res is nil.
	res = heartbeatRegion(c, conn, clusterID, 0, region, newLeaderPeer)
	c.Assert(res, IsNil)

	region, err = cluster.GetRegion(regionKey)
	c.Assert(err, IsNil)
	c.Assert(region.GetPeers(), HasLen, 2)
	c.Assert(cacheRegion, DeepEquals, region)

	c.Assert(cluster.cachedCluster.stores, HasLen, 2)
	c.Assert(cacheRegions.regions, HasLen, 1)
	c.Assert(cacheRegions.leaders.storeRegions, Not(HasKey), store1.GetId())
	c.Assert(cacheRegions.leaders.storeRegions, HasKey, store2.GetId())

	c.Assert(cacheRegions.leaders.storeRegions[store2.GetId()], HasKey, region.GetId())
	c.Assert(cacheRegions.leaders.regionStores[region.GetId()], Equals, store2.GetId())

	region, err = cluster.GetRegion(regionKey)
	c.Assert(err, IsNil)
	c.Assert(cacheRegion, DeepEquals, region)

	s.svr.cluster.Stop()

	// Check GetAllStores.
	stores := map[uint64]*metapb.Store{
		store1.GetId(): store1,
		store2.GetId(): store2,
	}

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
