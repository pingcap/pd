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
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
)

var (
	_ Balancer = &capacityBalancer{}
	_ Balancer = &defaultBalancer{}
	_ Balancer = &leaderBalancer{}
)

// Balancer is an interface to select store regions for auto-balance.
type Balancer interface {
	// Balance selects one store to do balance.
	Balance(cluster *clusterInfo) (*score, *balanceOperator, error)
	// ScoreType returns score type.
	ScoreType() scoreType
}

func selectFromStore(stores []*storeInfo, excluded map[uint64]struct{}, filters []Filter, st scoreType) *storeInfo {
	score := 0
	scorer := newScorer(st)
	if scorer == nil {
		return nil
	}

	var resultStore *storeInfo
	for _, store := range stores {
		if store == nil {
			continue
		}

		if _, ok := excluded[store.store.GetId()]; ok {
			continue
		}

		if filterFromStore(store, filters) {
			continue
		}

		currScore := scorer.Score(store)
		if resultStore == nil {
			resultStore = store
			score = currScore
			continue
		}

		if currScore > score {
			score = currScore
			resultStore = store
		}
	}

	return resultStore
}

func selectToStore(stores []*storeInfo, excluded map[uint64]struct{}, filters []Filter, st scoreType) *storeInfo {
	score := 0
	scorer := newScorer(st)
	if scorer == nil {
		return nil
	}

	var resultStore *storeInfo
	for _, store := range stores {
		if store == nil {
			continue
		}

		if _, ok := excluded[store.store.GetId()]; ok {
			continue
		}

		if filterToStore(store, filters) {
			continue
		}

		currScore := scorer.Score(store)
		if resultStore == nil {
			resultStore = store
			score = currScore
			continue
		}

		if currScore < score {
			score = currScore
			resultStore = store
		}
	}

	return resultStore
}

type capacityBalancer struct {
	filters []Filter
	st      scoreType

	cfg *BalanceConfig
}

func newCapacityBalancer(cfg *BalanceConfig) *capacityBalancer {
	cb := &capacityBalancer{cfg: cfg, st: capacityScore}
	cb.filters = append(cb.filters, newCapacityFilter(cfg))
	cb.filters = append(cb.filters, newSnapCountFilter(cfg))
	return cb
}

func (cb *capacityBalancer) ScoreType() scoreType {
	return cb.st
}

// selectBalanceRegion tries to select a store follower region to do balance.
func (cb *capacityBalancer) selectBalanceRegion(cluster *clusterInfo, stores []*storeInfo) (*metapb.Region, *metapb.Peer, *metapb.Peer) {
	store := selectFromStore(stores, cluster.getUnknownStores(), cb.filters, cb.st)
	if store == nil {
		log.Warn("from store cannot be found to select balance region")
		return nil, nil, nil
	}

	storeID := store.store.GetId()
	meta := cluster.getMeta()
	if meta.GetMaxPeerCount() == 1 {
		region := cluster.regions.randLeaderRegion(storeID)
		if region == nil {
			return nil, nil, nil
		}

		leader := leaderPeer(region, storeID)
		return region, leader, leader
	}

	// Random select one follower region from store.
	return cluster.regions.randRegion(storeID)
}

func (cb *capacityBalancer) selectNewLeaderPeer(cluster *clusterInfo, peers map[uint64]*metapb.Peer) *metapb.Peer {
	stores := make([]*storeInfo, 0, len(peers))
	for storeID := range peers {
		stores = append(stores, cluster.getStore(storeID))
	}

	store := selectToStore(stores, cluster.getUnknownStores(), nil, cb.st)
	if store == nil {
		log.Warn("find no store to get new leader peer for region")
		return nil
	}

	storeID := store.store.GetId()
	return peers[storeID]
}

func (cb *capacityBalancer) selectAddPeer(cluster *clusterInfo, stores []*storeInfo, excluded map[uint64]struct{}) (*metapb.Peer, error) {
	store := selectToStore(stores, excluded, cb.filters, cb.st)
	if store == nil {
		log.Warn("to store cannot be found to add peer")
		return nil, nil
	}

	peerID, err := cluster.idAlloc.Alloc()
	if err != nil {
		return nil, errors.Trace(err)
	}

	peer := &metapb.Peer{
		Id:      proto.Uint64(peerID),
		StoreId: proto.Uint64(store.store.GetId()),
	}

	return peer, nil
}

func (cb *capacityBalancer) selectRemovePeer(cluster *clusterInfo, peers map[uint64]*metapb.Peer) (*metapb.Peer, error) {
	stores := make([]*storeInfo, 0, len(peers))
	for storeID := range peers {
		stores = append(stores, cluster.getStore(storeID))
	}

	store := selectFromStore(stores, nil, nil, cb.st)
	if store == nil {
		log.Warn("from store cannot be found to remove peer")
		return nil, nil
	}

	storeID := store.store.GetId()
	return peers[storeID], nil
}

// Balance tries to select a store region to do balance.
// The balance type is follower balance.
func (cb *capacityBalancer) Balance(cluster *clusterInfo) (*score, *balanceOperator, error) {
	stores := cluster.getStores()
	region, leader, peer := cb.selectBalanceRegion(cluster, stores)
	if region == nil || leader == nil || peer == nil {
		log.Warn("region cannot be found to do balance")
		return nil, nil, nil
	}

	// If region peer count is not equal to max peer count, no need to do balance.
	if len(region.GetPeers()) != int(cluster.getMeta().GetMaxPeerCount()) {
		log.Warnf("region peer count %d not equals to max peer count %d, no need to do balance",
			len(region.GetPeers()), cluster.getMeta().GetMaxPeerCount())
		return nil, nil, nil
	}

	_, excludedStores := getFollowerPeers(region, leader)
	excludedStores = mergeMap(excludedStores, cluster.getUnknownStores())

	// Select one store to add new peer.
	newPeer, err := cb.selectAddPeer(cluster, stores, excludedStores)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if newPeer == nil {
		log.Warn("new peer cannot be found to do balance")
		return nil, nil, nil
	}

	// Check and get diff score.
	score, ok := checkAndGetDiffScore(cluster, peer, newPeer, cb.st, cb.cfg)
	if !ok {
		return nil, nil, nil
	}

	addPeerOperator := newAddPeerOperator(region.GetId(), newPeer)
	removePeerOperator := newRemovePeerOperator(region.GetId(), peer)
	return score, newBalanceOperator(region, addPeerOperator, removePeerOperator), nil
}

type leaderBalancer struct {
	filters []Filter
	st      scoreType

	cfg *BalanceConfig
}

func newLeaderBalancer(cfg *BalanceConfig) *leaderBalancer {
	lb := &leaderBalancer{cfg: cfg, st: leaderScore}
	lb.filters = append(lb.filters, newLeaderCountFilter(cfg))
	return lb
}

func (lb *leaderBalancer) ScoreType() scoreType {
	return lb.st
}

// selectBalanceRegion tries to select a store leader region to do balance.
func (lb *leaderBalancer) selectBalanceRegion(cluster *clusterInfo, stores []*storeInfo) (*metapb.Region, *metapb.Peer, *metapb.Peer) {
	store := selectFromStore(stores, cluster.getUnknownStores(), lb.filters, lb.st)
	if store == nil {
		log.Warn("from store cannot be found to select balance region")
		return nil, nil, nil
	}

	// Random select one leader region from store.
	storeID := store.store.GetId()
	region := cluster.regions.randLeaderRegion(storeID)
	if region == nil {
		return nil, nil, nil
	}

	leader := leaderPeer(region, storeID)
	if leader == nil {
		return nil, nil, nil
	}

	followerPeers, _ := getFollowerPeers(region, leader)
	newLeader := lb.selectNewLeaderPeer(cluster, followerPeers)
	if newLeader == nil {
		log.Warn("new leader peer cannot be found to do leader transfer")
		return nil, nil, nil
	}

	return region, leader, newLeader
}

func (lb *leaderBalancer) selectNewLeaderPeer(cluster *clusterInfo, peers map[uint64]*metapb.Peer) *metapb.Peer {
	stores := make([]*storeInfo, 0, len(peers))
	for storeID := range peers {
		stores = append(stores, cluster.getStore(storeID))
	}

	store := selectToStore(stores, cluster.getUnknownStores(), nil, lb.st)
	if store == nil {
		log.Warn("find no store to get new leader peer for region")
		return nil
	}

	storeID := store.store.GetId()
	return peers[storeID]
}

// Balance tries to select a store region to do balance.
// The balance type is leader transfer.
func (lb *leaderBalancer) Balance(cluster *clusterInfo) (*score, *balanceOperator, error) {
	// If cluster max peer count config is 1, we cannot do leader transfer,
	meta := cluster.getMeta()
	if meta.GetMaxPeerCount() == 1 {
		return nil, nil, nil
	}

	stores := cluster.getStores()
	region, leader, newLeader := lb.selectBalanceRegion(cluster, stores)
	if region == nil || leader == nil || newLeader == nil {
		log.Warn("region cannot be found to do leader transfer")
		return nil, nil, nil
	}

	// If region peer count is not equal to max peer count, no need to do leader transfer.
	if len(region.GetPeers()) != int(cluster.getMeta().GetMaxPeerCount()) {
		log.Warnf("region peer count %d not equals to max peer count %d, no need to do leader transfer",
			len(region.GetPeers()), cluster.getMeta().GetMaxPeerCount())
		return nil, nil, nil
	}

	score, ok := checkAndGetDiffScore(cluster, leader, newLeader, lb.st, lb.cfg)
	if !ok {
		return nil, nil, nil
	}

	regionID := region.GetId()
	transferLeaderOperator := newTransferLeaderOperator(regionID, leader, newLeader, lb.cfg)
	return score, newBalanceOperator(region, transferLeaderOperator), nil
}

// defaultBalancer is used for default config change, like add/remove peer.
type defaultBalancer struct {
	*capacityBalancer
	region *metapb.Region
	leader *metapb.Peer
}

func newDefaultBalancer(region *metapb.Region, leader *metapb.Peer, cfg *BalanceConfig) *defaultBalancer {
	return &defaultBalancer{
		region:           region,
		leader:           leader,
		capacityBalancer: newCapacityBalancer(cfg),
	}
}

func (db *defaultBalancer) addPeer(cluster *clusterInfo) (*balanceOperator, error) {
	stores := cluster.getStores()
	excludedStores := make(map[uint64]struct{}, len(db.region.GetPeers()))
	for _, peer := range db.region.GetPeers() {
		storeID := peer.GetStoreId()
		excludedStores[storeID] = struct{}{}
	}

	excludedStores = mergeMap(excludedStores, cluster.getUnknownStores())

	peer, err := db.selectAddPeer(cluster, stores, excludedStores)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if peer == nil {
		log.Warnf("find no store to add peer for region %v", db.region)
		return nil, nil
	}

	addPeerOperator := newAddPeerOperator(db.region.GetId(), peer)
	return newBalanceOperator(db.region, newOnceOperator(addPeerOperator)), nil
}

func (db *defaultBalancer) removePeer(cluster *clusterInfo) (*balanceOperator, error) {
	followerPeers := make(map[uint64]*metapb.Peer, len(db.region.GetPeers()))
	for _, peer := range db.region.GetPeers() {
		if peer.GetId() == db.leader.GetId() {
			continue
		}

		storeID := peer.GetStoreId()
		followerPeers[storeID] = peer
	}

	peer, err := db.selectRemovePeer(cluster, followerPeers)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if peer == nil {
		log.Warnf("find no store to remove peer for region %v", db.region)
		return nil, nil
	}

	removePeerOperator := newRemovePeerOperator(db.region.GetId(), peer)
	return newBalanceOperator(db.region, newOnceOperator(removePeerOperator)), nil
}

func (db *defaultBalancer) Balance(cluster *clusterInfo) (*score, *balanceOperator, error) {
	peerCount := len(db.region.GetPeers())
	clusterMeta := cluster.getMeta()
	maxPeerCount := int(clusterMeta.GetMaxPeerCount())

	var (
		bop *balanceOperator
		err error
	)

	if peerCount < maxPeerCount {
		bop, err = db.addPeer(cluster)
	} else if peerCount > maxPeerCount {
		bop, err = db.removePeer(cluster)
	}

	return nil, bop, errors.Trace(err)
}
