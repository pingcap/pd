// Copyright 2018 TiKV Project Authors.
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

package statistics

import (
	"fmt"

	"github.com/tikv/pd/server/core"

)

const (
	// StoreHeartBeatReportInterval is the heartbeat report interval of a store.
	StoreHeartBeatReportInterval = 10
	// RegionHeartBeatReportInterval is the heartbeat report interval of a region.
	RegionHeartBeatReportInterval = 60
)

func storeTag(id uint64) string {
	return fmt.Sprintf("store-%d", id)
}

func GetLoads(r *core.RegionInfo) []float64 {
	return []float64{
		RegionWriteBytes: float64(r.GetBytesWritten()),
		RegionWriteKeys:  float64(r.GetKeysWritten()),
		RegionReadBytes:  float64(r.GetBytesRead()),
		RegionReadKeys:   float64(r.GetKeysRead()),
	}
}

