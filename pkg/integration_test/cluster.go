// Copyright 2018 PingCAP, Inc.
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

package integration

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
	"github.com/pkg/errors"
)

// testServer states.
const (
	Initial int32 = iota
	Running
	Stop
	Destroy
)

type testServer struct {
	sync.RWMutex
	server *server.Server
	state  int32
}

var initHTTPClientOnce sync.Once

func newTestServer(cfg *server.Config) (*testServer, error) {
	err := server.PrepareJoinCluster(cfg)
	if err != nil {
		return nil, err
	}
	svr, err := server.CreateServer(cfg, api.NewHandler)
	if err != nil {
		return nil, err
	}
	initHTTPClientOnce.Do(func() {
		err = server.InitHTTPClient(svr)
	})
	if err != nil {
		return nil, err
	}
	return &testServer{
		server: svr,
		state:  Initial,
	}, nil
}

func (s *testServer) Run(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()
	if s.state != Initial && s.state != Stop {
		return errors.Errorf("server(state%d) cannot run", s.state)
	}
	if err := s.server.Run(ctx); err != nil {
		return err
	}
	s.state = Running
	return nil
}

func (s *testServer) Stop() error {
	s.Lock()
	defer s.Unlock()
	if s.state != Running {
		return errors.Errorf("server(state%d) cannot stop", s.state)
	}
	s.server.Close()
	s.state = Stop
	return nil
}

func (s *testServer) Destroy() error {
	s.Lock()
	defer s.Unlock()
	if s.state == Running {
		s.server.Close()
	}
	os.RemoveAll(s.server.GetConfig().DataDir)
	s.state = Destroy
	return nil
}

func (s *testServer) State() int32 {
	s.RLock()
	defer s.RUnlock()
	return s.state
}

func (s *testServer) GetConfig() *server.Config {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetConfig()
}

func (s *testServer) GetClusterID() uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.server.ClusterID()
}

func (s *testServer) GetLeader() *pdpb.Member {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetLeader()
}

func (s *testServer) GetCluster() *metapb.Cluster {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetCluster()
}

func (s *testServer) GetClusterVersion() semver.Version {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetClusterVersion()
}

func (s *testServer) GetServerID() uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.server.ID()
}

func (s *testServer) IsLeader() bool {
	s.RLock()
	defer s.RUnlock()
	return s.server.IsLeader()
}

func (s *testServer) GetEtcdLeader() (string, error) {
	s.RLock()
	defer s.RUnlock()
	req := &pdpb.GetMembersRequest{Header: &pdpb.RequestHeader{ClusterId: s.server.ClusterID()}}
	members, err := s.server.GetMembers(context.TODO(), req)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return members.GetEtcdLeader().GetName(), nil
}

func (s *testServer) GetEtcdClient() *clientv3.Client {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetClient()
}

func (s *testServer) GetStores() []*metapb.Store {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetRaftCluster().GetStores()
}

func (s *testServer) CheckHealth(members []*pdpb.Member) map[uint64]*pdpb.Member {
	s.RLock()
	defer s.RUnlock()
	return s.server.CheckHealth(members)
}

type testCluster struct {
	config  *clusterConfig
	servers map[string]*testServer
}

// ConfigOption is used to define customize settings in test.
type ConfigOption func(conf *server.Config)

func newTestCluster(initialServerCount int, opts ...ConfigOption) (*testCluster, error) {
	config := newClusterConfig(initialServerCount)
	servers := make(map[string]*testServer)
	for _, conf := range config.InitialServers {
		serverConf, err := conf.Generate(opts...)
		if err != nil {
			return nil, err
		}
		s, err := newTestServer(serverConf)
		if err != nil {
			return nil, err
		}
		servers[conf.Name] = s
	}
	return &testCluster{
		config:  config,
		servers: servers,
	}, nil
}

func (c *testCluster) RunServer(ctx context.Context, server *testServer) <-chan error {
	resC := make(chan error)
	go func() { resC <- server.Run(ctx) }()
	return resC
}

func (c *testCluster) RunServers(ctx context.Context, servers []*testServer) error {
	res := make([]<-chan error, len(servers))
	for i, s := range servers {
		res[i] = c.RunServer(ctx, s)
	}
	for _, c := range res {
		if err := <-c; err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (c *testCluster) RunInitialServers() error {
	var servers []*testServer
	for _, conf := range c.config.InitialServers {
		servers = append(servers, c.GetServer(conf.Name))
	}
	return c.RunServers(context.Background(), servers)
}

func (c *testCluster) StopAll() error {
	for _, s := range c.servers {
		if err := s.Stop(); err != nil {
			return err
		}
	}
	return nil
}

func (c *testCluster) GetServer(name string) *testServer {
	return c.servers[name]
}

func (c *testCluster) GetLeader() string {
	for name, s := range c.servers {
		if s.IsLeader() {
			return name
		}
	}
	return ""
}

func (c *testCluster) WaitLeader() string {
	for i := 0; i < 100; i++ {
		if leader := c.GetLeader(); leader != "" {
			return leader
		}
		time.Sleep(500 * time.Millisecond)
	}
	return ""
}

func (c *testCluster) GetCluster() *metapb.Cluster {
	leader := c.GetLeader()
	return c.servers[leader].GetCluster()
}

func (c *testCluster) GetEtcdClient() *clientv3.Client {
	leader := c.GetLeader()
	return c.servers[leader].GetEtcdClient()
}

func (c *testCluster) CheckHealth(members []*pdpb.Member) map[uint64]*pdpb.Member {
	leader := c.GetLeader()
	return c.servers[leader].CheckHealth(members)
}

func (c *testCluster) Join() (*testServer, error) {
	conf, err := c.config.Join().Generate()
	if err != nil {
		return nil, err
	}
	s, err := newTestServer(conf)
	if err != nil {
		return nil, err
	}
	c.servers[conf.Name] = s
	return s, nil
}

func (c *testCluster) Destroy() error {
	for _, s := range c.servers {
		err := s.Destroy()
		if err != nil {
			return err
		}
	}
	return nil
}
