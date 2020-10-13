// Copyright 2020 TiKV Project Authors.
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

package schedule

import (
	"bytes"
	"errors"
	"math"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
)

// SplitRegionsHandler used to handle region splitting
type SplitRegionsHandler interface {
	SplitRegionByKeys(region *core.RegionInfo, splitKeys [][]byte) error
	WatchRegionsByKeyRange(startKey, endKey []byte, expectRegionsCount int, timeout, watchInterval time.Duration) []uint64
}

// NewSplitRegionsHandler return SplitRegionsHandler
func NewSplitRegionsHandler(cluster opt.Cluster, oc *OperatorController) SplitRegionsHandler {
	return &splitRegionsHandler{
		cluster: cluster,
		oc:      oc,
	}
}

// RegionSplitter handles split regions
type RegionSplitter struct {
	cluster opt.Cluster
	handler SplitRegionsHandler
}

// NewRegionSplitter return a region splitter
func NewRegionSplitter(cluster opt.Cluster, handler SplitRegionsHandler) *RegionSplitter {
	return &RegionSplitter{
		cluster: cluster,
		handler: handler,
	}
}

// SplitRegions support splitRegions by given split keys.
func (r *RegionSplitter) SplitRegions(splitKeys [][]byte, retryLimit int) (int, []uint64) {
	if len(splitKeys) < 1 {
		return 0, nil
	}
	unprocessedKeys := splitKeys
	newRegions := make(map[uint64]struct{}, len(splitKeys))
	for currentRetry := 0; currentRetry < retryLimit; currentRetry++ {
		unprocessedKeys = r.splitRegionsByKeys(unprocessedKeys, newRegions)
		if len(unprocessedKeys) < 1 {
			break
		}
		// Wait for a while if there are some regions failed to be relocated
		time.Sleep(typeutil.MinDuration(maxSleepDuration, time.Duration(math.Pow(2, float64(currentRetry)))*initialSleepDuration))
	}
	returned := make([]uint64, 0, len(newRegions))
	for regionID := range newRegions {
		returned = append(returned, regionID)
	}
	return 100 - len(unprocessedKeys)*100/len(splitKeys), returned
}

func (r *RegionSplitter) splitRegionsByKeys(splitKeys [][]byte, newRegions map[uint64]struct{}) [][]byte {
	//TODO: support batch limit
	groupKeys, unProcessedKeys := r.groupKeysByRegion(splitKeys)
	for regionID, keys := range groupKeys {
		region := r.cluster.GetRegion(regionID)
		if region == nil {
			unProcessedKeys = append(unProcessedKeys, keys...)
			continue
		}
		if !r.checkRegionValid(region) {
			unProcessedKeys = append(unProcessedKeys, keys...)
			continue
		}
		err := r.handler.SplitRegionByKeys(region, keys)
		if err != nil {
			unProcessedKeys = append(unProcessedKeys, keys...)
			continue
		}
		// TODO: use goroutine to run watchRegionsByKeyRange asynchronously
		// TODO: support configure timeout and interval
		splittedRegionsID := r.handler.WatchRegionsByKeyRange(region.GetStartKey(), region.GetEndKey(),
			len(keys)+1, time.Minute, 100*time.Millisecond)
		for _, id := range splittedRegionsID {
			newRegions[id] = struct{}{}
		}
	}
	return unProcessedKeys
}

// GroupKeysByRegion separates keys into groups by their belonging Regions.
// If any key failed to be found its Region, it will be placed into unProcessed key.
// If the key is exactly the start key of its region, the key would be discarded directly.
func (r *RegionSplitter) groupKeysByRegion(keys [][]byte) (map[uint64][][]byte, [][]byte) {
	unProcessedKeys := make([][]byte, 0, len(keys))
	groupKeys := make(map[uint64][][]byte, len(keys))
	for _, key := range keys {
		region := r.cluster.GetRegionByKey(key)
		if region == nil {
			unProcessedKeys = append(unProcessedKeys, key)
			continue
		}
		// filter start key
		if bytes.Equal(region.GetStartKey(), key) {
			continue
		}
		group, ok := groupKeys[region.GetID()]
		if !ok {
			groupKeys[region.GetID()] = [][]byte{}
		}
		log.Info("found region",
			zap.Uint64("regionID", region.GetID()),
			zap.String("key", logutil.RedactString(string(key[:]))))
		groupKeys[region.GetID()] = append(group, key)
	}
	return groupKeys, unProcessedKeys
}

func (r *RegionSplitter) checkRegionValid(region *core.RegionInfo) bool {
	if r.cluster.IsRegionHot(region) {
		return false
	}
	if !opt.IsRegionReplicated(r.cluster, region) {
		r.cluster.AddSuspectRegions(region.GetID())
		return false
	}
	if region.GetLeader() == nil {
		return false
	}
	return true
}

type splitRegionsHandler struct {
	cluster opt.Cluster
	oc      *OperatorController
}

func (h *splitRegionsHandler) SplitRegionByKeys(region *core.RegionInfo, splitKeys [][]byte) error {
	op := operator.CreateSplitRegionOperator("region-splitter", region, 0, pdpb.CheckPolicy_USEKEY, splitKeys)
	if ok := h.oc.AddOperator(op); !ok {
		log.Info("add region split operator failed", zap.Uint64("regionID", region.GetID()))
		return errors.New("add region split operator failed")
	}
	return nil
}

func (h *splitRegionsHandler) WatchRegionsByKeyRange(startKey, endKey []byte, expectRegionsCount int, timeout, watchInterval time.Duration) []uint64 {
	after := time.After(timeout)
	ticker := time.NewTicker(watchInterval)
	defer ticker.Stop()
	var regionsID []uint64
	for {
		select {
		case <-ticker.C:
			regions := h.cluster.ScanRegions(startKey, endKey, expectRegionsCount)
			regionsID = make([]uint64, 0, expectRegionsCount)
			for _, region := range regions {
				regionsID = append(regionsID, region.GetID())
			}
			if len(regions) < expectRegionsCount {
				continue
			}
			return regionsID
		case <-after:
			return regionsID
		}
	}
}
