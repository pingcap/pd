package server

import (
	"bytes"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/kvproto/pkg/raftpb"
	"github.com/pingcap/kvproto/pkg/util"
)

var _ = Suite(&testClusterWorkerSuite{})

type testClusterWorkerSuite struct {
	testClusterBaseSuite

	clusterID uint64

	storeLock sync.Mutex
	stores    map[uint64]*mockRaftStore

	regionLeaderLock sync.Mutex
	regionLeaders    map[uint64]metapb.Peer

	quitCh chan struct{}
}

func (s *testClusterWorkerSuite) getRootPath() string {
	return "test_cluster_worker"
}

type mockRaftPeer struct {
	peer   metapb.Peer
	region metapb.Region
}

type mockRaftMsg struct {
	peer   metapb.Peer
	region metapb.Region
	req    *raft_cmdpb.RaftCmdRequest
}

type mockRaftStore struct {
	sync.Mutex

	s *testClusterWorkerSuite

	listener net.Listener

	store *metapb.Store

	peers map[uint64]*mockRaftPeer

	raftMsgCh chan *mockRaftMsg
}

func (s *testClusterWorkerSuite) bootstrap(c *C) *mockRaftStore {
	req := s.newBootstrapRequest(c, s.clusterID, "127.0.0.1:0")
	store := req.Bootstrap.Store
	region := req.Bootstrap.Region

	_, err := s.svr.bootstrapCluster(req.Bootstrap)
	c.Assert(err, IsNil)

	raftStore := s.newMockRaftStore(c, store)
	raftStore.addRegion(c, region)
	return raftStore
}

func (s *testClusterWorkerSuite) newMockRaftStore(c *C, metaStore *metapb.Store) *mockRaftStore {
	if metaStore == nil {
		metaStore = s.newStore(c, 0, "127.0.0.1:0")
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, IsNil)

	addr := l.Addr().String()
	metaStore.Address = proto.String(addr)
	store := &mockRaftStore{
		s:         s,
		listener:  l,
		raftMsgCh: make(chan *mockRaftMsg, 1024),
		store:     metaStore,
		peers:     make(map[uint64]*mockRaftPeer),
	}

	go store.runCmd(c)
	go store.runRaft(c)

	cluster, err := s.svr.getRaftCluster()
	c.Assert(err, IsNil)

	cluster.PutStore(metaStore)

	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	s.stores[metaStore.GetId()] = store

	return store
}

func (s *testClusterWorkerSuite) sendRaftMsg(c *C, msg *mockRaftMsg) {
	storeID := msg.peer.GetStoreId()

	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	store, ok := s.stores[storeID]
	if !ok {
		return
	}

	select {
	case store.raftMsgCh <- msg:
	default:
		c.Logf("can not send msg to %v", msg.peer)
	}
}

func (s *testClusterWorkerSuite) broadcastRaftMsg(c *C, leader *mockRaftPeer,
	req *raft_cmdpb.RaftCmdRequest) {
	region := leader.region
	for _, peer := range region.Peers {
		if peer.GetId() != leader.peer.GetId() {
			msg := &mockRaftMsg{
				peer:   *peer,
				region: *proto.Clone(&region).(*metapb.Region),
				req:    req,
			}
			s.sendRaftMsg(c, msg)
		}
	}

	// We should handle ConfChangeType_AddNode specially, because here the leader's
	// region doesn't contain this peer.
	if req.AdminRequest != nil && req.AdminRequest.ChangePeer != nil {
		changePeer := req.AdminRequest.ChangePeer
		if changePeer.GetChangeType() == raftpb.ConfChangeType_AddNode {
			c.Assert(changePeer.Peer.GetId(), Not(Equals), leader.peer.GetId())
			msg := &mockRaftMsg{
				peer:   *changePeer.Peer,
				region: *proto.Clone(&region).(*metapb.Region),
				req:    req,
			}
			s.sendRaftMsg(c, msg)
		}
	}
}

func (s *testClusterWorkerSuite) clearRegionLeader(c *C, regionID uint64) {
	s.regionLeaderLock.Lock()
	defer s.regionLeaderLock.Unlock()

	delete(s.regionLeaders, regionID)
}

func (s *testClusterWorkerSuite) chooseRegionLeader(c *C, region *metapb.Region) *metapb.Peer {
	// randomly select a peer in the region as the leader
	peer := region.Peers[rand.Intn(len(region.Peers))]

	s.regionLeaderLock.Lock()
	defer s.regionLeaderLock.Unlock()

	s.regionLeaders[region.GetId()] = *peer
	return peer
}

func (s *mockRaftStore) addRegion(c *C, region *metapb.Region) {
	s.Lock()
	defer s.Unlock()

	storeID := s.store.GetId()
	var (
		peer  metapb.Peer
		found = false
	)

	for _, p := range region.Peers {
		if p.GetStoreId() == storeID {
			peer = *p
			found = true
			break
		}
	}
	c.Assert(found, IsTrue)
	s.peers[region.GetId()] = &mockRaftPeer{
		peer:   peer,
		region: *proto.Clone(region).(*metapb.Region),
	}
}

func (s *mockRaftStore) runCmd(c *C) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			c.Logf("accept err %v", err)
			return
		}

		go func() {
			for {
				msg := &msgpb.Message{}
				msgID, err := util.ReadMessage(conn, msg)
				if err != nil {
					c.Log(err)
					return
				}

				req := msg.GetCmdReq()
				c.Assert(req, NotNil)

				resp := s.proposeCommand(c, req)
				if resp.Header == nil {
					resp.Header = &raft_cmdpb.RaftResponseHeader{}
				}
				resp.Header.Uuid = req.Header.Uuid

				respMsg := &msgpb.Message{
					MsgType: msgpb.MessageType_CmdResp.Enum(),
					CmdResp: resp,
				}

				if rand.Intn(2) == 1 && resp.StatusResponse == nil {
					// randomly close the connection to force
					// cluster work retry
					conn.Close()
					return
				}

				err = util.WriteMessage(conn, msgID, respMsg)
				if err != nil {
					c.Log(err)
				}
			}
		}()
	}
}

func (s *mockRaftStore) runRaft(c *C) {
	for {
		select {
		case msg := <-s.raftMsgCh:
			s.handleRaftMsg(c, msg)
		case <-s.s.quitCh:
			return
		}
	}
}

func (s *mockRaftStore) handleRaftMsg(c *C, msg *mockRaftMsg) {
	s.Lock()
	defer s.Unlock()

	regionID := msg.region.GetId()
	_, ok := s.peers[regionID]
	if !ok {
		// No peer, create it.
		s.peers[regionID] = &mockRaftPeer{
			peer:   msg.peer,
			region: msg.region,
		}
	}

	// TODO: all stores must have same response, check later.
	s.handleWriteCommand(c, msg.req)
}

func newErrorCmdResponse(err error) *raft_cmdpb.RaftCmdResponse {
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{
			Error: &errorpb.Error{
				Message: proto.String(err.Error()),
			},
		},
	}
	return resp
}

func (s *mockRaftStore) proposeCommand(c *C, req *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
	s.Lock()
	defer s.Unlock()

	regionID := req.Header.GetRegionId()
	peer, ok := s.peers[regionID]
	if !ok {
		resp := newErrorCmdResponse(errors.New("region not found"))
		resp.Header.Error.RegionNotFound = &errorpb.RegionNotFound{
			RegionId: proto.Uint64(regionID),
		}
		return resp
	}

	if req.StatusRequest != nil {
		return s.handleStatusRequest(c, req)
	}

	// lock leader to prevent outer test change it.
	s.s.regionLeaderLock.Lock()
	defer s.s.regionLeaderLock.Unlock()

	leader, ok := s.s.regionLeaders[regionID]
	if !ok || leader.GetId() != peer.peer.GetId() {
		resp := newErrorCmdResponse(errors.New("peer not leader"))
		resp.Header.Error.NotLeader = &errorpb.NotLeader{
			RegionId: proto.Uint64(regionID),
		}

		if ok {
			resp.Header.Error.NotLeader.Leader = &leader
		}

		return resp
	}

	// send the request to other peers.
	s.s.broadcastRaftMsg(c, peer, req)
	resp := s.handleWriteCommand(c, req)

	// update the region leader.
	s.s.regionLeaders[regionID] = peer.peer

	return resp
}

func (s *mockRaftStore) handleWriteCommand(c *C, req *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
	if req.AdminRequest != nil {
		return s.handleAdminRequest(c, req)
	}
	return newErrorCmdResponse(errors.Errorf("unsupported request %v", req))
}

func (s *mockRaftStore) handleStatusRequest(c *C, req *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
	regionID := req.Header.GetRegionId()
	status := req.StatusRequest

	peer := s.peers[regionID]

	var leader *metapb.Peer
	s.s.regionLeaderLock.Lock()
	l, ok := s.s.regionLeaders[regionID]
	s.s.regionLeaderLock.Unlock()
	if ok {
		leader = &l
	}

	switch status.GetCmdType() {
	case raft_cmdpb.StatusCmdType_RegionLeader:
		return &raft_cmdpb.RaftCmdResponse{
			StatusResponse: &raft_cmdpb.StatusResponse{
				CmdType: raft_cmdpb.StatusCmdType_RegionLeader.Enum(),
				RegionLeader: &raft_cmdpb.RegionLeaderResponse{
					Leader: leader,
				},
			},
		}
	case raft_cmdpb.StatusCmdType_RegionDetail:
		return &raft_cmdpb.RaftCmdResponse{
			StatusResponse: &raft_cmdpb.StatusResponse{
				CmdType: raft_cmdpb.StatusCmdType_RegionDetail.Enum(),
				RegionDetail: &raft_cmdpb.RegionDetailResponse{
					Leader: leader,
					Region: proto.Clone(&peer.region).(*metapb.Region),
				},
			},
		}
	default:
		return newErrorCmdResponse(errors.Errorf("unsupported request %v", req))
	}
}

func (s *mockRaftStore) handleAdminRequest(c *C, req *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
	var resp *raft_cmdpb.RaftCmdResponse
	switch req.AdminRequest.GetCmdType() {
	case raft_cmdpb.AdminCmdType_ChangePeer:
		resp = s.handleChangePeer(c, req)
	case raft_cmdpb.AdminCmdType_Split:
		resp = s.handleSplit(c, req)
	}

	if resp.AdminResponse != nil {
		resp.AdminResponse.CmdType = req.AdminRequest.CmdType
	}
	return resp
}

func (s *mockRaftStore) handleChangePeer(c *C, req *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
	changePeer := req.AdminRequest.ChangePeer
	confType := changePeer.GetChangeType()
	peer := changePeer.Peer

	raftPeer := s.peers[req.Header.GetRegionId()]
	region := raftPeer.region
	c.Assert(region.GetId(), Equals, req.Header.GetRegionId())

	if region.RegionEpoch.GetConfVer() > req.Header.RegionEpoch.GetConfVer() {
		return newErrorCmdResponse(errors.Errorf("stale message with epoch %v < %v",
			region.RegionEpoch, req.Header.RegionEpoch))
	}

	if confType == raftpb.ConfChangeType_AddNode {
		for _, p := range region.Peers {
			if p.GetId() == peer.GetId() || p.GetStoreId() == peer.GetStoreId() {
				return newErrorCmdResponse(errors.Errorf("add duplicated peer %v for region %v", peer, region))
			}
		}

		region.Peers = append(region.Peers, peer)
		raftPeer.region = region
	} else {
		foundIndex := -1
		for i, p := range region.Peers {
			if p.GetId() == peer.GetId() {
				foundIndex = i
				break
			}
		}

		if foundIndex == -1 {
			return newErrorCmdResponse(errors.Errorf("remove missing peer %v for region %v", peer, region))
		}

		region.Peers = append(region.Peers[:foundIndex], region.Peers[foundIndex+1:]...)
		raftPeer.region = region

		// remove itself
		if peer.GetStoreId() == s.store.GetId() {
			delete(s.peers, region.GetId())
		}
	}

	region.RegionEpoch.ConfVer = proto.Uint64(region.RegionEpoch.GetConfVer() + 1)

	resp := &raft_cmdpb.RaftCmdResponse{
		AdminResponse: &raft_cmdpb.AdminResponse{
			ChangePeer: &raft_cmdpb.ChangePeerResponse{
				Region: &region,
			},
		},
	}
	return resp
}

func (s *mockRaftStore) handleSplit(c *C, req *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
	split := req.AdminRequest.Split
	raftPeer := s.peers[req.Header.GetRegionId()]
	splitKey := split.SplitKey
	newRegionID := split.GetNewRegionId()
	newPeerIDs := split.GetNewPeerIds()

	region := raftPeer.region
	c.Assert(newPeerIDs, HasLen, len(region.Peers))

	version := region.RegionEpoch.GetVersion()
	if version > req.Header.RegionEpoch.GetVersion() {
		return newErrorCmdResponse(errors.Errorf("stale message with epoch %v < %v",
			region.RegionEpoch, req.Header.RegionEpoch))
	}

	if bytes.Equal(splitKey, region.GetEndKey()) {
		return newErrorCmdResponse(errors.Errorf("region %v is already split for key %q", region, splitKey))
	}

	c.Assert(string(splitKey), Greater, string(region.GetStartKey()))
	if len(region.GetEndKey()) > 0 {
		c.Assert(string(splitKey), Less, string(region.GetEndKey()))
	}

	region.RegionEpoch.Version = proto.Uint64(version + 1)
	newRegion := &metapb.Region{
		Id:       proto.Uint64(newRegionID),
		Peers:    make([]*metapb.Peer, len(newPeerIDs)),
		StartKey: splitKey,
		EndKey:   append([]byte(nil), region.GetEndKey()...),
		RegionEpoch: &metapb.RegionEpoch{
			Version: proto.Uint64(version + 1),
			ConfVer: proto.Uint64(region.RegionEpoch.GetConfVer()),
		},
	}

	var newPeer metapb.Peer
	for i, id := range newPeerIDs {
		peer := *region.Peers[i]
		peer.Id = proto.Uint64(id)

		if peer.GetStoreId() == s.store.GetId() {
			newPeer = peer
		}

		newRegion.Peers[i] = &peer
	}

	region.EndKey = append([]byte(nil), splitKey...)

	raftPeer.region = region
	s.peers[newRegionID] = &mockRaftPeer{
		peer:   newPeer,
		region: *newRegion,
	}

	resp := &raft_cmdpb.RaftCmdResponse{
		AdminResponse: &raft_cmdpb.AdminResponse{
			Split: &raft_cmdpb.SplitResponse{
				Left:  &region,
				Right: newRegion,
			},
		},
	}
	return resp
}

func (s *testClusterWorkerSuite) SetUpSuite(c *C) {
	s.clusterID = 0

	s.stores = make(map[uint64]*mockRaftStore)

	s.svr = newTestServer(c, s.getRootPath())
	s.svr.cfg.nextRetryDelay = 50 * time.Millisecond

	s.client = newEtcdClient(c)

	s.regionLeaders = make(map[uint64]metapb.Peer)

	s.quitCh = make(chan struct{})

	deleteRoot(c, s.client, s.getRootPath())

	go s.svr.Run()

	mustGetLeader(c, s.client, s.svr.getLeaderPath())

	// Construct the raft cluster 5 stores.
	s.bootstrap(c)
	s.newMockRaftStore(c, nil)
	s.newMockRaftStore(c, nil)
	s.newMockRaftStore(c, nil)
	s.newMockRaftStore(c, nil)

	cluster, err := s.svr.getRaftCluster()
	c.Assert(err, IsNil)
	cluster.PutConfig(&metapb.Cluster{
		Id:            proto.Uint64(s.clusterID),
		MaxPeerNumber: proto.Uint32(5),
	})

	stores, err := cluster.GetAllStores()
	c.Assert(err, IsNil)
	c.Assert(stores, HasLen, 5)
}

func (s *testClusterWorkerSuite) TearDownSuite(c *C) {
	s.svr.Close()
	s.client.Close()

	close(s.quitCh)
}

func (s *testClusterWorkerSuite) checkRegionPeerNumber(c *C, regionKey []byte, expectNumber int) *metapb.Region {
	cluster, err := s.svr.getRaftCluster()
	c.Assert(err, IsNil)

	for i := 0; i < 10; i++ {
		region, err1 := cluster.GetRegion(regionKey)
		c.Assert(err1, IsNil)
		if len(region.Peers) == expectNumber {
			return region
		}
		time.Sleep(100 * time.Millisecond)
	}
	region, err := cluster.GetRegion(regionKey)
	c.Assert(err, IsNil)
	c.Assert(region.Peers, HasLen, expectNumber)
	return region
}

func (s *testClusterWorkerSuite) regionPeerExisted(c *C, regionID uint64, peer *metapb.Peer) bool {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	storeID := peer.GetStoreId()
	store, ok := s.stores[storeID]
	if !ok {
		return false
	}

	store.Lock()
	defer store.Unlock()
	p, ok := store.peers[regionID]
	if !ok {
		return false
	}

	c.Assert(p.peer.GetId(), Equals, peer.GetId())
	return true
}

func (s *testClusterWorkerSuite) TestChangePeer(c *C) {
	cluster, err := s.svr.getRaftCluster()
	c.Assert(err, IsNil)

	meta, err := cluster.GetConfig()
	c.Assert(err, IsNil)
	c.Assert(meta.GetMaxPeerNumber(), Equals, uint32(5))

	regionKey := []byte("a")
	region, err := cluster.GetRegion(regionKey)
	c.Assert(err, IsNil)

	c.Assert(region.Peers, HasLen, 1)

	leaderPd := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	conn, err := net.Dial("tcp", leaderPd.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	// add another 4 peers.
	for i := 0; i < 4; i++ {
		leaderPeer := s.chooseRegionLeader(c, region)

		askChangePeer := &pdpb.Request{
			Header:  newRequestHeader(s.clusterID),
			CmdType: pdpb.CommandType_AskChangePeer.Enum(),
			AskChangePeer: &pdpb.AskChangePeerRequest{
				Leader: leaderPeer,
				Region: region,
			},
		}

		if rand.Intn(2) == 1 {
			// randomly change leader
			s.clearRegionLeader(c, region.GetId())
			s.chooseRegionLeader(c, region)
		}

		sendRequest(c, conn, 0, askChangePeer)
		_, resp := recvResponse(c, conn)
		c.Assert(resp.GetCmdType(), Equals, pdpb.CommandType_AskChangePeer)

		region = s.checkRegionPeerNumber(c, regionKey, i+2)
	}

	region = s.checkRegionPeerNumber(c, regionKey, 5)

	regionID := region.GetId()
	for _, peer := range region.Peers {
		ok := s.regionPeerExisted(c, regionID, peer)
		c.Assert(ok, IsTrue)
	}

	err = cluster.PutConfig(&metapb.Cluster{
		Id:            proto.Uint64(s.clusterID),
		MaxPeerNumber: proto.Uint32(3),
	})
	c.Assert(err, IsNil)

	oldRegion := proto.Clone(region).(*metapb.Region)

	// remove 2 peers
	for i := 0; i < 2; i++ {
		leaderPeer := s.chooseRegionLeader(c, region)

		askChangePeer := &pdpb.Request{
			Header:  newRequestHeader(s.clusterID),
			CmdType: pdpb.CommandType_AskChangePeer.Enum(),
			AskChangePeer: &pdpb.AskChangePeerRequest{
				Leader: leaderPeer,
				Region: region,
			},
		}
		sendRequest(c, conn, 0, askChangePeer)
		_, resp := recvResponse(c, conn)
		c.Assert(resp.GetCmdType(), Equals, pdpb.CommandType_AskChangePeer)

		region = s.checkRegionPeerNumber(c, regionKey, 4-i)
	}

	region = s.checkRegionPeerNumber(c, regionKey, 3)

	for _, peer := range region.Peers {
		ok := s.regionPeerExisted(c, regionID, peer)
		c.Assert(ok, IsTrue)
	}

	// check removed peer
	for _, oldPeer := range oldRegion.Peers {
		found := false
		for _, peer := range region.Peers {
			if oldPeer.GetId() == peer.GetId() {
				found = true
				break
			}
		}

		if found {
			continue
		}

		ok := s.regionPeerExisted(c, regionID, oldPeer)
		c.Assert(ok, IsFalse)
	}
}

func (s *testClusterWorkerSuite) TestSplit(c *C) {
	cluster, err := s.svr.getRaftCluster()
	c.Assert(err, IsNil)

	leaderPd := mustGetLeader(c, s.client, s.svr.getLeaderPath())
	conn, err := net.Dial("tcp", leaderPd.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	tbl := []struct {
		searchKey string
		startKey  string
		endKey    string
		splitKey  string
	}{
		{"a", "", "", "b"},
		{"c", "b", "", "d"},
		{"e", "d", "", "f"},
	}

	firstRegionStartKey := tbl[0].startKey
	firstRegionEndKey := tbl[0].splitKey
	for _, t := range tbl {
		regionKey := []byte(t.searchKey)
		region, err := cluster.GetRegion(regionKey)
		c.Assert(err, IsNil)
		c.Assert(region.GetStartKey(), BytesEquals, []byte(t.startKey))
		c.Assert(region.GetEndKey(), BytesEquals, []byte(t.endKey))

		// Now we treat the first peer in region as leader.
		leaderPeer := s.chooseRegionLeader(c, region)
		if rand.Intn(2) == 1 {
			// randomly change leader
			s.clearRegionLeader(c, region.GetId())
			s.chooseRegionLeader(c, region)
		}

		askSplit := &pdpb.Request{
			Header:  newRequestHeader(s.clusterID),
			CmdType: pdpb.CommandType_AskSplit.Enum(),
			AskSplit: &pdpb.AskSplitRequest{
				Region:   region,
				Leader:   leaderPeer,
				SplitKey: []byte(t.splitKey),
			},
		}

		sendRequest(c, conn, 0, askSplit)
		_, resp := recvResponse(c, conn)
		c.Assert(resp.GetCmdType(), Equals, pdpb.CommandType_AskSplit)

		time.Sleep(500 * time.Millisecond)
		left, err := cluster.GetRegion([]byte(t.searchKey))
		c.Assert(err, IsNil)
		c.Assert(left.GetStartKey(), BytesEquals, []byte(t.startKey))
		c.Assert(left.GetEndKey(), BytesEquals, []byte(t.splitKey))
		c.Assert(left.GetId(), Equals, region.GetId())

		for _, peer := range left.Peers {
			ok := s.regionPeerExisted(c, left.GetId(), peer)
			c.Assert(ok, IsTrue)
		}

		right, err := cluster.GetRegion([]byte(t.splitKey))
		c.Assert(err, IsNil)
		c.Assert(right.GetStartKey(), BytesEquals, []byte(t.splitKey))
		c.Assert(right.GetEndKey(), BytesEquals, []byte(t.endKey))

		// Test get first region.
		regionKey = []byte{}
		region, err = cluster.GetRegion(regionKey)
		c.Assert(err, IsNil)
		c.Assert(region.GetStartKey(), BytesEquals, []byte(firstRegionStartKey))
		c.Assert(region.GetEndKey(), BytesEquals, []byte(firstRegionEndKey))
	}
}
