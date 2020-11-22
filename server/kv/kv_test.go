// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"testing"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func TestKV(t *testing.T) {
	TestingT(t)
}

type testKVSuite struct{}

var _ = Suite(&testKVSuite{})

func (s *testKVSuite) TestEtcd(c *C) {
	cfg := newTestSingleConfig()
	defer cleanConfig(cfg)
	etcd, err := embed.StartEtcd(cfg)
	c.Assert(err, IsNil)
	defer etcd.Close()

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	c.Assert(err, IsNil)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))

	kv := NewEtcdKVBase(client, rootPath)
	s.testReadWrite(c, kv)
	s.testRange(c, kv)
	s.testTxn(c, kv)
}

func (s *testKVSuite) TestLevelDB(c *C) {
	dir, err := ioutil.TempDir("/tmp", "leveldb_kv")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)
	kv, err := NewLeveldbKV(dir)
	c.Assert(err, IsNil)

	s.testReadWrite(c, kv)
	s.testRange(c, kv)
}

func (s *testKVSuite) TestMemKV(c *C) {
	kv := NewMemoryKV()
	s.testReadWrite(c, kv)
	s.testRange(c, kv)
}

func (s *testKVSuite) testReadWrite(c *C, kv Base) {
	v, err := kv.Load("key")
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "")
	err = kv.Save("key", "value")
	c.Assert(err, IsNil)
	v, err = kv.Load("key")
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "value")
	err = kv.Remove("key")
	c.Assert(err, IsNil)
	v, err = kv.Load("key")
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "")
	err = kv.Remove("key")
	c.Assert(err, IsNil)
}

func (s *testKVSuite) testRange(c *C, kv Base) {
	keys := []string{
		"test-a", "test-a/a", "test-a/ab",
		"test", "test/a", "test/ab",
		"testa", "testa/a", "testa/ab",
	}
	for _, k := range keys {
		err := kv.Save(k, k)
		c.Assert(err, IsNil)
	}
	sortedKeys := append(keys[:0:0], keys...)
	sort.Strings(sortedKeys)

	testCases := []struct {
		start, end string
		limit      int
		expect     []string
	}{
		{start: "", end: "z", limit: 100, expect: sortedKeys},
		{start: "", end: "z", limit: 3, expect: sortedKeys[:3]},
		{start: "testa", end: "z", limit: 3, expect: []string{"testa", "testa/a", "testa/ab"}},
		{start: "test/", end: clientv3.GetPrefixRangeEnd("test/"), limit: 100, expect: []string{"test/a", "test/ab"}},
		{start: "test-a/", end: clientv3.GetPrefixRangeEnd("test-a/"), limit: 100, expect: []string{"test-a/a", "test-a/ab"}},
		{start: "test", end: clientv3.GetPrefixRangeEnd("test"), limit: 100, expect: sortedKeys},
		{start: "test", end: clientv3.GetPrefixRangeEnd("test/"), limit: 100, expect: []string{"test", "test-a", "test-a/a", "test-a/ab", "test/a", "test/ab"}},
	}

	for _, tc := range testCases {
		ks, vs, err := kv.LoadRange(tc.start, tc.end, tc.limit)
		c.Assert(err, IsNil)
		c.Assert(ks, DeepEquals, tc.expect)
		c.Assert(vs, DeepEquals, tc.expect)
	}
}

func (s *testKVSuite) testTxn(c *C, kv TxnBase) {
	testCases := []struct {
		_if      []Cmp
		then     []Op
		_else    []Op
		expect   map[string]string
		willFail bool
	}{
		{then: []Op{OpSave("key", "value")}, expect: map[string]string{"key": "value"}},
		{
			_if:    []Cmp{Eq(Value("key"), "value")},
			then:   []Op{OpSave("foo", "bar1")},
			_else:  []Op{OpSave("foo", "bar2")},
			expect: map[string]string{"foo": "bar1"},
		},
		{
			_if:      []Cmp{Eq(Value("foo"), "bar2")},
			then:     []Op{OpSave("foo", "bar3")},
			_else:    []Op{OpSave("foo", "bar4")},
			expect:   map[string]string{"foo": "bar4"},
			willFail: true,
		},
		{
			_if:      []Cmp{Eq(Value("foo"), "boo")},
			then:     []Op{OpSave("foo", "bar3")},
			willFail: true,
		},
		{
			_if:    []Cmp{Eq(Version("foo"), 2)},
			then:   []Op{OpSave("test_version_eq", "pass")},
			expect: map[string]string{"test_version_eq": "pass"},
		},
		{
			_if:    []Cmp{Gt(Version("foo"), 1)},
			then:   []Op{OpSave("test_version_gt", "pass")},
			expect: map[string]string{"test_version_gt": "pass"},
		},
		{
			_if:    []Cmp{Lt(Version("foo"), 3)},
			then:   []Op{OpSave("test_version_lt", "pass")},
			expect: map[string]string{"test_version_lt": "pass"},
		},
		{
			_if:    []Cmp{Neq(Version("foo"), 0)},
			then:   []Op{OpSave("test_version_neq", "pass")},
			expect: map[string]string{"test_version_neq": "pass"},
		},
		{
			then:   []Op{OpSave("key/1", "1"), OpSave("key/2", "2"), OpSave("key/3", "3")},
			expect: map[string]string{"key/1": "1", "key/2": "2", "key/3": "3"},
		},
		{
			then:   []Op{OpRemoveRange("key/", clientv3.GetPrefixRangeEnd("key/"), 0)},
			expect: map[string]string{"key": "value", "key/1": "", "key/2": "", "key/3": ""},
		},
		{
			then:   []Op{OpRemove("key")},
			expect: map[string]string{"key": ""},
		},
	}

	for _, testCases := range testCases {
		_, err := kv.NewTxn().If(testCases._if...).Then(testCases.then...).Else(testCases._else...).Commit()
		if testCases.willFail {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
		for k, v := range testCases.expect {
			value, err := kv.Load(k)
			c.Assert(err, IsNil)
			c.Assert(value, Equals, v)
		}
	}
}

func newTestSingleConfig() *embed.Config {
	cfg := embed.NewConfig()
	cfg.Name = "test_etcd"
	cfg.Dir, _ = ioutil.TempDir("/tmp", "test_etcd")
	cfg.WalDir = ""
	cfg.Logger = "zap"
	cfg.LogOutputs = []string{"stdout"}

	pu, _ := url.Parse(tempurl.Alloc())
	cfg.LPUrls = []url.URL{*pu}
	cfg.APUrls = cfg.LPUrls
	cu, _ := url.Parse(tempurl.Alloc())
	cfg.LCUrls = []url.URL{*cu}
	cfg.ACUrls = cfg.LCUrls

	cfg.StrictReconfigCheck = false
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, &cfg.LPUrls[0])
	cfg.ClusterState = embed.ClusterStateFlagNew
	return cfg
}

func cleanConfig(cfg *embed.Config) {
	// Clean data directory
	os.RemoveAll(cfg.Dir)
}
