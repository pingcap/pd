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

func mustEqualStore(c *C, store1 *metapb.Store, store2 *metapb.Store) {
	c.Assert(store1.GetId(), Equals, store2.GetId())
	c.Assert(store1.GetAddress(), Equals, store2.GetAddress())
}

func mustEqualRegion(c *C, region1 *metapb.Region, region2 *metapb.Region) {
	c.Assert(region1.GetId(), Equals, region2.GetId())
	c.Assert(region1.GetRegionEpoch().GetVersion(), Equals, region2.GetRegionEpoch().GetVersion())
	c.Assert(region1.GetRegionEpoch().GetConfVer(), Equals, region2.GetRegionEpoch().GetConfVer())
	c.Assert(region1.GetStartKey(), BytesEquals, region2.GetStartKey())
	c.Assert(region1.GetEndKey(), BytesEquals, region2.GetEndKey())
	c.Assert(len(region1.GetPeers()), Equals, len(region2.GetPeers()))

	for i := range region1.GetPeers() {
		mustEqualPeer(c, region1.GetPeers()[i], region2.GetPeers()[i])
	}
}

func mustEqualPeer(c *C, peer1 *metapb.Peer, peer2 *metapb.Peer) {
	c.Assert(peer1.GetId(), Equals, peer2.GetId())
	c.Assert(peer1.GetStoreId(), Equals, peer2.GetStoreId())
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
	mustEqualStore(c, cacheStore.store, store1)
	c.Assert(cluster.cachedCluster.regions, HasLen, 0)

	// Add another store.
	store2 := s.newStore(c, 0, "127.0.0.1:2")
	err = cluster.PutStore(store2)
	c.Assert(err, IsNil)

	c.Assert(cluster.cachedCluster.getMeta().GetId(), Equals, clusterID)
	c.Assert(cluster.cachedCluster.getMeta().GetMaxPeerCount(), Equals, defaultMaxPeerCount)

	cacheStore = cluster.cachedCluster.getStore(store1.GetId())
	mustEqualStore(c, cacheStore.store, store1)
	cacheStore = cluster.cachedCluster.getStore(store2.GetId())
	mustEqualStore(c, cacheStore.store, store2)
	cacheStores := cluster.cachedCluster.getStores()
	c.Assert(cacheStores, HasLen, 2)
	c.Assert(cluster.cachedCluster.regions, HasLen, 0)

	// There is only one region now, directly use it for test.
	regionKey := []byte("a")
	region, err := cluster.GetRegion(regionKey)
	c.Assert(err, IsNil)
	c.Assert(region.Peers, HasLen, 1)

	leaderPeer := region.GetPeers()[0]
	res := heartbeatRegion(c, conn, clusterID, 0, region, leaderPeer)
	c.Assert(res.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)

	cacheStores = cluster.cachedCluster.getStores()
	c.Assert(cacheStores, HasLen, 2)
	c.Assert(cluster.cachedCluster.regions, HasLen, 1)

	cacheRegion := cluster.cachedCluster.regions[region.GetId()]
	mustEqualRegion(c, cacheRegion.region, region)

	region.RegionEpoch.ConfVer = proto.Uint64(region.GetRegionEpoch().GetConfVer() + 1)
	res = heartbeatRegion(c, conn, clusterID, 0, region, leaderPeer)
	c.Assert(res, IsNil)

	c.Assert(cluster.cachedCluster.regions, HasLen, 1)

	oldLeaderPeerID := region.GetId()
	cacheRegion = cluster.cachedCluster.regions[oldLeaderPeerID]
	region, err = cluster.GetRegion(regionKey)
	c.Assert(err, IsNil)
	c.Assert(region.GetPeers(), HasLen, 1)
	mustEqualRegion(c, cacheRegion.region, region)

	cacheStore, ok := cluster.cachedCluster.stores[leaderPeer.GetStoreId()]
	c.Assert(ok, IsTrue)
	cachePeer, ok := cacheStore.regions[region.GetId()]
	c.Logf("[cacheStore]:%v, [leaderPeer]:%v", cacheStore, leaderPeer)
	c.Assert(ok, IsTrue)
	mustEqualPeer(c, cachePeer, leaderPeer)

	// Test change leader peer.
	newLeaderPeer := region.GetPeers()[0]
	if newLeaderPeer.GetId() == oldLeaderPeerID {
		newLeaderPeer = region.GetPeers()[1]
	}

	res = heartbeatRegion(c, conn, clusterID, 0, region, newLeaderPeer)
	c.Assert(res, NotNil)

	region, err = cluster.GetRegion(regionKey)
	c.Assert(err, IsNil)
	c.Assert(region.GetPeers(), HasLen, 1)
	mustEqualRegion(c, cacheRegion.region, region)

	c.Assert(cluster.cachedCluster.stores, HasLen, 2)
	cacheStore, ok = cluster.cachedCluster.stores[newLeaderPeer.GetStoreId()]
	c.Assert(ok, IsTrue)
	c.Assert(cacheStore.regions, HasLen, 1)
	cachePeer, ok = cacheStore.regions[region.GetId()]
	c.Assert(ok, IsTrue)
	mustEqualPeer(c, cachePeer, newLeaderPeer)

	s.svr.cluster.Stop()

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
