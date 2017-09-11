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

package namespace

import "github.com/pingcap/pd/server/core"

var (
	// DefaultNamespace is the namespace all the store and region belong to by
	// default.
	DefaultNamespace = "global"

	// DefaultClassifer is a classifier that classifies all Regions and stores to
	// DefaultNamespace.
	DefaultClassifer = defaultClassifer{}
)

// Classifier is used to determine the namespace which the store or region
// belongs.
type Classifier interface {
	GetStoreNamespace(*core.StoreInfo) string
	GetRegionNamespace(*core.RegionInfo) string
}

type defaultClassifer struct{}

func (c defaultClassifer) GetStoreNamespace(*core.StoreInfo) string {
	return DefaultNamespace
}

func (c defaultClassifer) GetRegionNamespace(*core.RegionInfo) string {
	return DefaultNamespace
}
