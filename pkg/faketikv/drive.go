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

	log "github.com/Sirupsen/logrus"
)

type Driver struct {
	clusterInfo *ClusterInfo
	addr        string
	client      Client
}

func NewDriver(addr string) *Driver {
	return &Driver{addr: addr}
}

func (c *Driver) Prepare() error {
	initCase := NewTiltCase()
	clusterInfo := initCase.Init(c.addr, "./case1.toml")
	c.clusterInfo = clusterInfo
	store, region := clusterInfo.GetBootstrapInfo()
	c.client = clusterInfo.Nodes[store.GetId()].client

	ctx, cancel := context.WithTimeout(context.Background(), pdTimeout)
	err := c.client.Bootstrap(ctx, store, region)
	cancel()
	if err != nil {
		log.Fatal("bootstrapped error: ", err)
	} else {
		log.Info("Bootstrap sucess")
	}
	for _, n := range c.clusterInfo.Nodes {
		err := n.Start()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Driver) Tick() {
	for _, n := range c.clusterInfo.Nodes {
		n.Tick()
	}
}

func (c *Driver) Stop() {
	for _, n := range c.clusterInfo.Nodes {
		n.Stop()
	}
}

func (c *Driver) AddNode() {
	id, err := c.client.AllocID(context.Background())
	n, err := NewNode(id, fmt.Sprintf("mock://tikv-%d", id), c.addr)
	if err != nil {
		log.Info("Add node failed:", err)
	}
	err = n.Start()
	if err != nil {
		log.Info("Start node failed:", err)
	}
	n.clusterInfo = c.clusterInfo
	c.clusterInfo.Nodes[n.Id] = n
}

func (c *Driver) DeleteNode() {

}
