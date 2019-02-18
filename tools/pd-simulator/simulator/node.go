// Copyright 2017 PingCAP, Inc.
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

package simulator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/tools/pd-simulator/simulator/cases"
	"github.com/pingcap/pd/tools/pd-simulator/simulator/simutil"
)

const (
	storeHeartBeatPeriod  = 10
	regionHeartBeatPeriod = 60
)

// Node simulates a TiKV.
type Node struct {
	*metapb.Store
	sync.RWMutex
	stats                    *pdpb.StoreStats
	tick                     uint64
	wg                       sync.WaitGroup
	tasks                    map[uint64]Task
	client                   Client
	receiveRegionHeartbeatCh <-chan *pdpb.RegionHeartbeatResponse
	ctx                      context.Context
	cancel                   context.CancelFunc
	raftEngine               *RaftEngine
	ioRate                   int64
}

// NewNode returns a Node.
func NewNode(s *cases.Store, pdAddr string, ioRate int64) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &metapb.Store{
		Id:      s.ID,
		Address: fmt.Sprintf("mock:://tikv-%d", s.ID),
		Version: s.Version,
		Labels:  s.Labels,
		State:   s.Status,
	}
	stats := &pdpb.StoreStats{
		StoreId:   s.ID,
		Capacity:  s.Capacity,
		Available: s.Available,
		StartTime: uint32(time.Now().Unix()),
	}
	tag := fmt.Sprintf("store %d", s.ID)
	client, receiveRegionHeartbeatCh, err := NewClient(pdAddr, tag)
	if err != nil {
		cancel()
		return nil, err
	}
	return &Node{
		Store:                    store,
		stats:                    stats,
		client:                   client,
		ctx:                      ctx,
		cancel:                   cancel,
		tasks:                    make(map[uint64]Task),
		receiveRegionHeartbeatCh: receiveRegionHeartbeatCh,
		ioRate:                   ioRate * cases.MB,
	}, nil
}

// Start starts the node.
func (n *Node) Start() error {
	ctx, cancel := context.WithTimeout(n.ctx, pdTimeout)
	err := n.client.PutStore(ctx, n.Store)
	cancel()
	if err != nil {
		return err
	}
	n.wg.Add(1)
	go n.receiveRegionHeartbeat()
	n.Store.State = metapb.StoreState_Up
	return nil
}

func (n *Node) receiveRegionHeartbeat() {
	defer n.wg.Done()
	for {
		select {
		case resp := <-n.receiveRegionHeartbeatCh:
			task := responseToTask(resp, n.raftEngine)
			if task != nil {
				n.AddTask(task)
			}
		case <-n.ctx.Done():
			return
		}
	}
}

// Tick steps node status change.
func (n *Node) Tick() {
	if n.GetState() != metapb.StoreState_Up {
		return
	}
	n.stepHeartBeat()
	n.stepTask()
	n.tick++
}

// GetState returns current node state.
func (n *Node) GetState() metapb.StoreState {
	return n.Store.State
}

func (n *Node) stepTask() {
	n.Lock()
	defer n.Unlock()
	for _, task := range n.tasks {
		task.Step(n.raftEngine)
		if task.IsFinished() {
			simutil.Logger.Debug("task finished",
				zap.Uint64("node-id", n.Id),
				zap.Uint64("region-id", task.RegionID()),
				zap.String("task", task.Desc()))
			delete(n.tasks, task.RegionID())
		}
	}
}

func (n *Node) stepHeartBeat() {
	if n.tick%storeHeartBeatPeriod == 0 {
		n.storeHeartBeat()
	}
	if n.tick%regionHeartBeatPeriod == 0 {
		n.regionHeartBeat()
	}
}

func (n *Node) storeHeartBeat() {
	if n.GetState() != metapb.StoreState_Up {
		return
	}
	ctx, cancel := context.WithTimeout(n.ctx, pdTimeout)
	err := n.client.StoreHeartbeat(ctx, n.stats)
	if err != nil {
		simutil.Logger.Info("report heartbeat error",
			zap.Uint64("node-id", n.GetId()),
			zap.Error(err))
	}
	cancel()
}

func (n *Node) regionHeartBeat() {
	if n.GetState() != metapb.StoreState_Up {
		return
	}
	regions := n.raftEngine.GetRegions()
	for _, region := range regions {
		if region.GetLeader() != nil && region.GetLeader().GetStoreId() == n.Id {
			ctx, cancel := context.WithTimeout(n.ctx, pdTimeout)
			err := n.client.RegionHeartbeat(ctx, region)
			if err != nil {
				simutil.Logger.Info("report heartbeat error",
					zap.Uint64("node-id", n.Id),
					zap.Uint64("region-id", region.GetID()),
					zap.Error(err))
			}
			cancel()
		}
	}
}

func (n *Node) reportRegionChange() {
	for _, regionID := range n.raftEngine.regionChange[n.Id] {
		region := n.raftEngine.GetRegion(regionID)
		ctx, cancel := context.WithTimeout(n.ctx, pdTimeout)
		err := n.client.RegionHeartbeat(ctx, region)
		if err != nil {
			simutil.Logger.Info("report heartbeat error",
				zap.Uint64("node-id", n.Id),
				zap.Uint64("region-id", region.GetID()),
				zap.Error(err))
		}
		cancel()
	}
	delete(n.raftEngine.regionChange, n.Id)
}

// AddTask adds task in this node.
func (n *Node) AddTask(task Task) {
	n.Lock()
	defer n.Unlock()
	if t, ok := n.tasks[task.RegionID()]; ok {
		simutil.Logger.Info("task has already existed",
			zap.Uint64("node-id", n.Id),
			zap.Uint64("region-id", task.RegionID()),
			zap.String("task", t.Desc()))
		return
	}
	n.tasks[task.RegionID()] = task
}

// Stop stops this node.
func (n *Node) Stop() {
	n.cancel()
	n.client.Close()
	n.wg.Wait()
	simutil.Logger.Info("node stopped", zap.Uint64("node-id", n.Id))
}

func (n *Node) incUsedSize(size uint64) {
	n.stats.Available -= size
	n.stats.UsedSize += size
}

func (n *Node) decUsedSize(size uint64) {
	n.stats.Available += size
	n.stats.UsedSize -= size
}
