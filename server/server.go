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
	"net"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	statsd "gopkg.in/alexcesaro/statsd.v2"
)

const (
	etcdTimeout = time.Second * 3
)

// Server is the pd server.
type Server struct {
	cfg *Config

	listener net.Listener

	client *clientv3.Client

	rootPath string

	isLeader int64
	// leader value saved in etcd leader key.
	// Every write will use this to check leader validation.
	leaderValue string

	wg sync.WaitGroup

	connsLock sync.Mutex
	conns     map[*conn]struct{}

	closed int64

	// for tso
	ts            atomic.Value
	lastSavedTime time.Time

	// for id allocator, we can use one allocator for
	// store, region and peer, because we just need
	// a unique ID.
	idAlloc *idAllocator

	// for raft cluster
	clusterLock sync.RWMutex
	cluster     *raftCluster

	msgID uint64

	stats *statsd.Client
}

// NewServer creates the pd server with given configuration.
func NewServer(cfg *Config) (*Server, error) {
	cfg.adjust()

	log.Infof("create etcd v3 client with endpoints %v", cfg.EtcdAddrs)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdAddrs,
		DialTimeout: etcdTimeout,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Infof("listening address %s", cfg.Addr)
	l, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		client.Close()
		return nil, errors.Trace(err)
	}

	// If advertise addr not set, using default listening address.
	if len(cfg.AdvertiseAddr) == 0 {
		cfg.AdvertiseAddr = l.Addr().String()
	}

	stats, err := initStatsClient(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s := &Server{
		cfg:      cfg,
		listener: l,
		client:   client,
		isLeader: 0,
		conns:    make(map[*conn]struct{}),
		closed:   0,
		rootPath: path.Join(cfg.RootPath, strconv.FormatUint(cfg.ClusterID, 10)),
		stats:    stats,
	}

	s.idAlloc = &idAllocator{s: s}
	s.cluster = &raftCluster{
		s:           s,
		running:     false,
		clusterID:   cfg.ClusterID,
		clusterRoot: s.getClusterRootPath(),
	}

	return s, nil
}

func initStatsClient(cfg *Config) (*statsd.Client, error) {
	var statsOpts []statsd.Option
	if len(cfg.MetricAddr) > 0 {
		statsOpts = append(statsOpts, statsd.Address(cfg.MetricAddr))
	} else {
		statsOpts = append(statsOpts, statsd.Mute(true))
	}

	statsOpts = append(statsOpts, statsd.Prefix(cfg.MetricPrefix))

	stats, err := statsd.New(statsOpts...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return stats, nil
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.closed, 0, 1) {
		// server is already closed
		return
	}

	log.Info("closing server")

	s.enableLeader(false)

	if s.listener != nil {
		s.listener.Close()
	}

	if s.client != nil {
		s.client.Close()
	}

	if s.stats != nil {
		s.stats.Close()
	}

	s.wg.Wait()
}

// IsClosed checks whether server is closed or not.
func (s *Server) IsClosed() bool {
	return atomic.LoadInt64(&s.closed) == 1
}

// ListeningAddr returns listen address.
func (s *Server) ListeningAddr() string {
	return s.listener.Addr().String()
}

// Run runs the pd server.
func (s *Server) Run() error {
	// We use "127.0.0.1:0" for test and will set correct listening
	// address before run, so we set leader value here.
	s.leaderValue = s.marshalLeader()

	s.wg.Add(1)
	go s.leaderLoop()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Errorf("accept err %s", err)
			break
		}

		c, err := newConn(s, conn)
		if err != nil {
			log.Warn(err)
			conn.Close()
			continue
		}

		s.wg.Add(1)
		go c.run()
	}

	return nil
}

func (s *Server) closeAllConnections() {
	s.connsLock.Lock()
	defer s.connsLock.Unlock()

	if len(s.conns) == 0 {
		return
	}

	for conn := range s.conns {
		err := conn.Close()
		if err != nil {
			log.Warnf("close conn failed - %v", err)
		}
	}

	s.conns = make(map[*conn]struct{})
}
