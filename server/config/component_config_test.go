// Copyright 2019 PingCAP, Inc.
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

package config

import (
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/kv"
)

var _ = Suite(&testComponentsConfigSuite{})

type testComponentsConfigSuite struct{}

func (s *testComponentsConfigSuite) TestDecodeAndEncode(c *C) {
	cfgData := `
log-level = "debug"
panic-when-unexpected-key-or-data = true

[pd]
endpoints = [
    "example.com:443",
]

[coprocessor]
split-region-on-table = true
batch-split-limit = 1
region-max-size = "12MB"

[rocksdb]
wal-recovery-mode = 1
wal-dir = "/var"
create-if-missing = false

[rocksdb.titan]
enabled = true
dirname = "bar"
max-background-gc = 9

[rocksdb.defaultcf]
block-size = "12KB"
disable-block-cache = false
bloom-filter-bits-per-key = 123
compression-per-level = [
    "no",
    "lz4",
]

[rocksdb.defaultcf.titan]
min-blob-size = "2018B"
discardable-ratio = 0.00156

[rocksdb.writecf]
block-size = "12KB"
disable-block-cache = false
bloom-filter-bits-per-key = 123
compression-per-level = [
    "no",
    "zstd",
]
`
	cfg := make(map[string]interface{})
	err := decodeConfigs(cfgData, cfg)
	c.Assert(err, IsNil)
	decoded := make(map[string]interface{})
	decoded["log-level"] = "debug"
	decoded["panic-when-unexpected-key-or-data"] = true
	pdMap := map[string]interface{}{"endpoints": []interface{}{"example.com:443"}}
	decoded["pd"] = pdMap
	copMap := map[string]interface{}{
		"split-region-on-table": true,
		"batch-split-limit":     int64(1),
		"region-max-size":       "12MB",
	}
	decoded["coprocessor"] = copMap
	titanMap := map[string]interface{}{
		"enabled":           true,
		"dirname":           "bar",
		"max-background-gc": int64(9),
	}
	defaultcfTitanMap := map[string]interface{}{
		"min-blob-size":     "2018B",
		"discardable-ratio": 0.00156,
	}
	defaultcfMap := map[string]interface{}{
		"block-size":                "12KB",
		"disable-block-cache":       false,
		"bloom-filter-bits-per-key": int64(123),
		"compression-per-level":     []interface{}{"no", "lz4"},
		"titan":                     defaultcfTitanMap,
	}
	writecfMap := map[string]interface{}{
		"block-size":                "12KB",
		"disable-block-cache":       false,
		"bloom-filter-bits-per-key": int64(123),
		"compression-per-level":     []interface{}{"no", "zstd"},
	}
	rocksdbMap := map[string]interface{}{
		"wal-recovery-mode": int64(1),
		"wal-dir":           "/var",
		"create-if-missing": false,
		"titan":             titanMap,
		"defaultcf":         defaultcfMap,
		"writecf":           writecfMap,
	}
	decoded["rocksdb"] = rocksdbMap
	c.Assert(cfg, DeepEquals, decoded)

	str, err := encodeConfigs(decoded)
	c.Assert(err, IsNil)
	encodedStr := `log-level = "debug"
panic-when-unexpected-key-or-data = true

[coprocessor]
  batch-split-limit = 1
  region-max-size = "12MB"
  split-region-on-table = true

[pd]
  endpoints = ["example.com:443"]

[rocksdb]
  create-if-missing = false
  wal-dir = "/var"
  wal-recovery-mode = 1
  [rocksdb.defaultcf]
    block-size = "12KB"
    bloom-filter-bits-per-key = 123
    compression-per-level = ["no", "lz4"]
    disable-block-cache = false
    [rocksdb.defaultcf.titan]
      discardable-ratio = 0.00156
      min-blob-size = "2018B"
  [rocksdb.titan]
    dirname = "bar"
    enabled = true
    max-background-gc = 9
  [rocksdb.writecf]
    block-size = "12KB"
    bloom-filter-bits-per-key = 123
    compression-per-level = ["no", "zstd"]
    disable-block-cache = false
`
	c.Assert(str, Equals, encodedStr)
}

func (s *testComponentsConfigSuite) TestUpdateConfig(c *C) {
	cfg := make(map[string]interface{})
	defaultcfTitanMap := map[string]interface{}{
		"discardable-ratio": 0.00156,
	}
	defaultcfMap := map[string]interface{}{
		"block-size":            "12KB",
		"compression-per-level": []interface{}{"no", "lz4"},
		"titan":                 defaultcfTitanMap,
	}
	rocksdbMap := map[string]interface{}{
		"wal-recovery-mode": int64(1),
		"defaultcf":         defaultcfMap,
	}
	cfg["rocksdb"] = rocksdbMap
	update(cfg, strings.Split("rocksdb.defaultcf.titan.discardable-ratio", "."), "0.002")
	c.Assert(defaultcfTitanMap["discardable-ratio"], Equals, 0.002)
}

func (s *testComponentsConfigSuite) TestReloadConfig(c *C) {
	cfgData := `
[rocksdb]
wal-recovery-mode = 1

[rocksdb.defaultcf]
block-size = "12KB"
disable-block-cache = false
compression-per-level = [
    "no",
    "lz4",
]

[rocksdb.defaultcf.titan]
discardable-ratio = 0.00156
`
	cfg := NewComponentsConfig()
	lc, err := NewLocalConfig(cfgData)
	c.Assert(err, IsNil)
	gc := NewGlobalConfig([]*configpb.ConfigEntry{{
		Name: "rocksdb.defaultcf.disable-block-cache", Value: "true"},
	})
	cfg.GlobalCfgs["tikv"] = gc
	cfg.LocalCfgs["tikv1"] = lc

	storage := core.NewStorage(kv.NewMemoryKV())
	err = cfg.Persist(storage)
	c.Assert(err, IsNil)

	cfg1 := NewComponentsConfig()
	err = cfg1.Reload(storage)
	c.Assert(err, IsNil)
	c.Assert(cfg1.LocalCfgs["tikv1"], DeepEquals, lc)
	c.Assert(cfg1.GlobalCfgs["tikv"], DeepEquals, gc)

	// test cover config
	cfgData1 := `
[rocksdb]
wal-recovery-mode = 1

[rocksdb.defaultcf]
block-size = "20KB"
disable-block-cache = true
compression-per-level = [
    "zstd",
    "zstd",
]
`
	lc1, err := NewLocalConfig(cfgData1)
	c.Assert(err, IsNil)
	gc1 := NewGlobalConfig([]*configpb.ConfigEntry{{
		Name: "rocksdb.defaultcf.disable-block-cache", Value: "true"},
	})
	cfg.GlobalCfgs["tikv"] = gc1
	cfg.LocalCfgs["tikv1"] = lc1
	err = cfg.Persist(storage)
	c.Assert(err, IsNil)
	err = cfg1.Reload(storage)
	c.Assert(err, IsNil)
	c.Assert(cfg1.LocalCfgs["tikv1"], DeepEquals, lc1)
	c.Assert(cfg1.GlobalCfgs["tikv"], DeepEquals, gc1)
}

func (s *testComponentsConfigSuite) TestGetConfig(c *C) {
	cfgData := `
[rocksdb]
wal-recovery-mode = 1

[rocksdb.defaultcf]
block-size = "12KB"
disable-block-cache = false
compression-per-level = [
    "no",
    "lz4",
]

[rocksdb.defaultcf.titan]
discardable-ratio = 0.00156
`
	cfg := NewComponentsConfig()
	lc, err := NewLocalConfig(cfgData)
	c.Assert(err, IsNil)
	gc := NewGlobalConfig([]*configpb.ConfigEntry{{
		Name: "rocksdb.defaultcf.disable-block-cache", Value: "true"},
	})
	cfg.GlobalCfgs["tikv"] = gc
	cfg.LocalCfgs["tikv1"] = lc
	str, err := cfg.getComponentCfg("tikv", "tikv1")
	c.Assert(err, IsNil)
	expect := `[rocksdb]
  wal-recovery-mode = 1
  [rocksdb.defaultcf]
    block-size = "12KB"
    compression-per-level = ["no", "lz4"]
    disable-block-cache = true
    [rocksdb.defaultcf.titan]
      discardable-ratio = 0.00156
`
	c.Assert(str, Equals, expect)
}

func (s *testComponentsConfigSuite) TestCreate(c *C) {
	cfgData := `
log-level = "debug"
`
	cfg := NewComponentsConfig()
	v, config, status := cfg.Create(&configpb.Version{Global: 0, Local: 0}, "tikv", "tikv1", cfgData)
	c.Assert(v, DeepEquals, &configpb.Version{Global: 0, Local: 0})
	c.Assert(status.GetCode(), Equals, configpb.Status_OK)
	expect := `log-level = "debug"
`
	c.Assert(config, Equals, expect)
	v, config, status = cfg.Create(&configpb.Version{Global: 0, Local: 0}, "tikv", "tikv1", cfgData)
	c.Assert(v, DeepEquals, &configpb.Version{Global: 0, Local: 0})
	c.Assert(status.GetCode(), Equals, configpb.Status_NOT_CHANGE)
	c.Assert(config, Equals, "")
	v, status = cfg.Update(
		&configpb.ConfigKind{Kind: &configpb.ConfigKind_Local{Local: &configpb.Local{ComponentId: "tikv1"}}},
		&configpb.Version{Global: 0, Local: 0},
		[]*configpb.ConfigEntry{{Name: "log-level", Value: "info"}},
	)
	c.Assert(v, DeepEquals, &configpb.Version{Global: 0, Local: 1})
	c.Assert(status.GetCode(), Equals, configpb.Status_OK)
	v, config, status = cfg.Create(&configpb.Version{Global: 0, Local: 0}, "tikv", "tikv1", cfgData)
	c.Assert(v, DeepEquals, &configpb.Version{Global: 0, Local: 1})
	c.Assert(status.GetCode(), Equals, configpb.Status_STALE_VERSION)
	expect1 := `log-level = "info"
`
	c.Assert(config, Equals, expect1)
	v, config, status = cfg.Create(&configpb.Version{Global: 10, Local: 10}, "tikv", "tikv1", cfgData)
	c.Assert(v, DeepEquals, &configpb.Version{Global: 0, Local: 1})
	c.Assert(status.GetCode(), Equals, configpb.Status_FAILED)
	c.Assert(config, Equals, expect1)
}

func (s *testComponentsConfigSuite) TestUpdate(c *C) {
	cfgData := `
log-level = "debug"
`
	cfg := NewComponentsConfig()
	v, config, status := cfg.Create(&configpb.Version{Global: 0, Local: 0}, "tikv", "tikv1", cfgData)
	c.Assert(v, DeepEquals, &configpb.Version{Global: 0, Local: 0})
	c.Assert(status.GetCode(), Equals, configpb.Status_OK)
	expect := `log-level = "debug"
`
	c.Assert(config, Equals, expect)
	v, status = cfg.Update(
		&configpb.ConfigKind{Kind: &configpb.ConfigKind_Local{Local: &configpb.Local{ComponentId: "tikv1"}}},
		&configpb.Version{Global: 0, Local: 0},
		[]*configpb.ConfigEntry{{Name: "log-level", Value: "info"}},
	)
	c.Assert(v, DeepEquals, &configpb.Version{Global: 0, Local: 1})
	c.Assert(status.GetCode(), Equals, configpb.Status_OK)
	v, status = cfg.Update(
		&configpb.ConfigKind{Kind: &configpb.ConfigKind_Local{Local: &configpb.Local{ComponentId: "tikv1"}}},
		&configpb.Version{Global: 0, Local: 0},
		[]*configpb.ConfigEntry{{Name: "log-level", Value: "info"}},
	)
	c.Assert(v, DeepEquals, &configpb.Version{Global: 0, Local: 1})
	c.Assert(status.GetCode(), Equals, configpb.Status_STALE_VERSION)
	v, status = cfg.Update(
		&configpb.ConfigKind{Kind: &configpb.ConfigKind_Global{Global: &configpb.Global{Component: "tikv"}}},
		&configpb.Version{Global: 10, Local: 0},
		[]*configpb.ConfigEntry{{Name: "log-level", Value: "info"}},
	)
	c.Assert(v, DeepEquals, &configpb.Version{Global: 11, Local: 0})
	c.Assert(status.GetCode(), Equals, configpb.Status_OK)
	v, status = cfg.Update(
		&configpb.ConfigKind{Kind: &configpb.ConfigKind_Global{Global: &configpb.Global{Component: "tikv"}}},
		&configpb.Version{Global: 0, Local: 0},
		[]*configpb.ConfigEntry{{Name: "log-level", Value: "info"}},
	)
	c.Assert(v, DeepEquals, &configpb.Version{Global: 11, Local: 0})
	c.Assert(status.GetCode(), Equals, configpb.Status_STALE_VERSION)

	// nil case
	v, status = cfg.Update(nil, nil, nil)
	c.Assert(v, DeepEquals, &configpb.Version{Global: 0, Local: 0})
	c.Assert(status.GetCode(), Equals, configpb.Status_FAILED)
}

func (s *testComponentsConfigSuite) TestGet(c *C) {
	cfgData := `
log-level = "debug"
`
	cfg := NewComponentsConfig()
	v, config, status := cfg.Create(&configpb.Version{Global: 0, Local: 0}, "tikv", "tikv1", cfgData)
	c.Assert(v, DeepEquals, &configpb.Version{Global: 0, Local: 0})
	c.Assert(status.GetCode(), Equals, configpb.Status_OK)
	expect := `log-level = "debug"
`
	c.Assert(config, Equals, expect)
	v, status = cfg.Update(
		&configpb.ConfigKind{Kind: &configpb.ConfigKind_Local{Local: &configpb.Local{ComponentId: "tikv1"}}},
		&configpb.Version{Global: 0, Local: 0},
		[]*configpb.ConfigEntry{{Name: "log-level", Value: "info"}},
	)
	c.Assert(v, DeepEquals, &configpb.Version{Global: 0, Local: 1})
	c.Assert(status.GetCode(), Equals, configpb.Status_OK)
	v, config, status = cfg.Get(&configpb.Version{Global: 0, Local: 0}, "tikv", "tikv1")
	c.Assert(v, DeepEquals, &configpb.Version{Global: 0, Local: 1})
	c.Assert(status.GetCode(), Equals, configpb.Status_STALE_VERSION)
	expect1 := `log-level = "info"
`
	c.Assert(config, Equals, expect1)
	v, config, status = cfg.Get(&configpb.Version{Global: 10, Local: 0}, "tikv", "tikv1")
	c.Assert(v, DeepEquals, &configpb.Version{Global: 0, Local: 1})
	c.Assert(status.GetCode(), Equals, configpb.Status_STALE_VERSION)
	c.Assert(config, Equals, expect1)
	v, config, status = cfg.Get(&configpb.Version{Global: 10, Local: 1}, "tikv", "tikv1")
	c.Assert(v, DeepEquals, &configpb.Version{Global: 0, Local: 1})
	c.Assert(status.GetCode(), Equals, configpb.Status_FAILED)
	c.Assert(config, Equals, expect1)
}
