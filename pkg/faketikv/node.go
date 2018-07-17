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

package faketikv

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/faketikv/simutil"
)

// NodeState node's state.
type NodeState int

// some state
const (
	Up NodeState = iota
	Down
	LossConnect
	Block
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
	state                    NodeState
	raftEngine               *RaftEngine
}

// NewNode returns a Node.
func NewNode(id uint64, addr string, pdAddr string) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &metapb.Store{
		Id:      id,
		Address: addr,
	}
	stats := &pdpb.StoreStats{
		StoreId: id,
		// TODO: configurable
		Capacity:  1000000000000,
		Available: 1000000000000,
		StartTime: uint32(time.Now().Unix()),
	}
	tag := fmt.Sprintf("store %d", id)
	client, receiveRegionHeartbeatCh, err := NewClient(pdAddr, tag)
	if err != nil {
		cancel()
		return nil, err
	}
	return &Node{
		Store:  store,
		stats:  stats,
		client: client,
		ctx:    ctx,
		cancel: cancel,
		tasks:  make(map[uint64]Task),
		state:  Down,
		receiveRegionHeartbeatCh: receiveRegionHeartbeatCh,
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
	n.state = Up
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
	if n.state != Up {
		return
	}
	n.stepHeartBeat()
	n.stepTask()
	n.tick++
}

// GetState returns current node state.
func (n *Node) GetState() NodeState {
	return n.state
}

func (n *Node) stepTask() {
	n.Lock()
	defer n.Unlock()
	for _, task := range n.tasks {
		task.Step(n.raftEngine)
		if task.IsFinished() {
			simutil.Logger.Infof("[store %d][region %d] task finished: %s final: %v", n.GetId(), task.RegionID(), task.Desc(), n.raftEngine.GetRegion(task.RegionID()))
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
	if n.state != Up {
		return
	}
	ctx, cancel := context.WithTimeout(n.ctx, pdTimeout)
	err := n.client.StoreHeartbeat(ctx, n.stats)
	if err != nil {
		simutil.Logger.Infof("[store %d] report heartbeat error: %s", n.GetId(), err)
	}
	cancel()
}

func (n *Node) regionHeartBeat() {
	if n.state != Up {
		return
	}
	regions := n.raftEngine.GetRegions()
	for _, region := range regions {
		if region.Leader != nil && region.Leader.GetStoreId() == n.Id {
			ctx, cancel := context.WithTimeout(n.ctx, pdTimeout)
			err := n.client.RegionHeartbeat(ctx, region)
			if err != nil {
				simutil.Logger.Infof("[node %d][region %d] report heartbeat error: %s", n.Id, region.GetId(), err)
			}
			cancel()
		}
	}
}

func (n *Node) reportRegionChange() {
	for _, regionID := range n.raftEngine.regionchange[n.Id] {
		region := n.raftEngine.GetRegion(regionID)
		ctx, cancel := context.WithTimeout(n.ctx, pdTimeout)
		err := n.client.RegionHeartbeat(ctx, region)
		if err != nil {
			simutil.Logger.Infof("[node %d][region %d] report heartbeat error: %s", n.Id, region.GetId(), err)
		}
		cancel()
	}
	delete(n.raftEngine.regionchange, n.Id)
}

// AddTask adds task in this node.
func (n *Node) AddTask(task Task) {
	n.Lock()
	defer n.Unlock()
	if t, ok := n.tasks[task.RegionID()]; ok {
		simutil.Logger.Infof("[node %d][region %d] already exists task : %s", n.Id, task.RegionID(), t.Desc())
		return
	}
	n.tasks[task.RegionID()] = task
}

// Stop stops this node.
func (n *Node) Stop() {
	n.cancel()
	n.client.Close()
	n.wg.Wait()
	simutil.Logger.Infof("node %d stopped", n.Id)
}
