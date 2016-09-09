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

package pd

import (
	"time"

	"github.com/coreos/etcd/clientv3"
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testLeaderChangeSuite{})

type testLeaderChangeSuite struct{}

func mustGetEtcdClient(c *C, svrs map[string]*server.Server) *clientv3.Client {
	for _, svr := range svrs {
		return svr.GetClient()
	}
	c.Fatal("etcd client none available")
	return nil
}

func (s *testLeaderChangeSuite) TestLeaderChange(c *C) {
	cfgs := server.NewTestMultiConfig(3)

	ch := make(chan *server.Server, 3)

	for i := 0; i < 3; i++ {
		cfg := cfgs[i]

		go func() {
			svr, err := server.NewServer(cfg)
			c.Assert(err, IsNil)
			ch <- svr
		}()
	}

	svrs := make(map[string]*server.Server, 3)
	for i := 0; i < 3; i++ {
		svr := <-ch
		svrs[svr.GetAddr()] = svr
	}

	for _, svr := range svrs {
		go svr.Run()
	}

	// wait etcds start ok.
	time.Sleep(5 * time.Second)

	defer func() {
		for _, svr := range svrs {
			svr.Close()
		}
		for _, cfg := range cfgs {
			cleanServer(cfg)
		}
	}()

	defaultWatchLeaderTimeout = 500 * time.Millisecond

	etcdClient := mustGetEtcdClient(c, svrs)
	cli, err := NewClient(etcdClient.Endpoints(), 0)
	c.Assert(err, IsNil)
	defer cli.Close()

	p1, l1, err := cli.GetTS()
	c.Assert(err, IsNil)

	leaderPath := getLeaderPath(0)

	leaderAddr, _, err := getLeader(etcdClient, leaderPath)
	c.Assert(err, IsNil)
	svrs[leaderAddr].Close()
	delete(svrs, leaderAddr)

	etcdClient = mustGetEtcdClient(c, svrs)

	// wait leader changes
	changed := false
	for i := 0; i < 20; i++ {
		leaderAddr1, _, _ := getLeader(etcdClient, leaderPath)
		if len(leaderAddr1) != 0 && leaderAddr1 != leaderAddr {
			changed = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.Assert(changed, IsTrue)

	cli, err = NewClient(etcdClient.Endpoints(), 0)
	c.Assert(err, IsNil)
	defer cli.Close()

	for i := 0; i < 20; i++ {
		p2, l2, err := cli.GetTS()
		if err == nil {
			c.Assert(p1<<18+l1, Less, p2<<18+l2)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.Error("failed getTS from new leader after 10 seconds")
}
