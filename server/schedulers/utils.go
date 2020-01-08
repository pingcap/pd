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

package schedulers

import (
	"math"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	// adjustRatio is used to adjust TolerantSizeRatio according to region count.
	adjustRatio             float64 = 0.005
	leaderTolerantSizeRatio float64 = 5.0
	minTolerantSizeRatio    float64 = 1.0
)

// ErrScheduleConfigNotExist the config is not correct.
var ErrScheduleConfigNotExist = errors.New("the config does not exist")

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func shouldBalance(cluster opt.Cluster, source, target *core.StoreInfo, region *core.RegionInfo, kind core.ScheduleKind, opInfluence operator.OpInfluence, scheduleName string) bool {
	// The reason we use max(regionSize, averageRegionSize) to check is:
	// 1. prevent moving small regions between stores with close scores, leading to unnecessary balance.
	// 2. prevent moving huge regions, leading to over balance.
	sourceID := source.GetID()
	targetID := target.GetID()
	tolerantResource := getTolerantResource(cluster, region, kind)
	sourceInfluence := opInfluence.GetStoreInfluence(sourceID).ResourceProperty(kind)
	targetInfluence := opInfluence.GetStoreInfluence(targetID).ResourceProperty(kind)
	sourceScore := source.ResourceScore(kind, cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), sourceInfluence-tolerantResource)
	targetScore := target.ResourceScore(kind, cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), targetInfluence+tolerantResource)
	if cluster.IsDebugMetricsEnabled() {
		opInfluenceStatus.WithLabelValues(scheduleName, strconv.FormatUint(sourceID, 10), "source").Set(float64(sourceInfluence))
		opInfluenceStatus.WithLabelValues(scheduleName, strconv.FormatUint(targetID, 10), "target").Set(float64(targetInfluence))
		tolerantResourceStatus.WithLabelValues(scheduleName, strconv.FormatUint(sourceID, 10), strconv.FormatUint(targetID, 10)).Set(float64(tolerantResource))
	}
	// Make sure after move, source score is still greater than target score.
	shouldBalance := sourceScore > targetScore

	if !shouldBalance {
		log.Debug("skip balance "+kind.Resource.String(),
			zap.String("scheduler", scheduleName), zap.Uint64("region-id", region.GetID()), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID),
			zap.Int64("source-size", source.GetRegionSize()), zap.Float64("source-score", sourceScore),
			zap.Int64("source-influence", sourceInfluence),
			zap.Int64("target-size", target.GetRegionSize()), zap.Float64("target-score", targetScore),
			zap.Int64("target-influence", targetInfluence),
			zap.Int64("average-region-size", cluster.GetAverageRegionSize()),
			zap.Int64("tolerant-resource", tolerantResource))
	}
	return shouldBalance
}

func getTolerantResource(cluster opt.Cluster, region *core.RegionInfo, kind core.ScheduleKind) int64 {
	if kind.Resource == core.LeaderKind && kind.Policy == core.ByCount {
		tolerantSizeRatio := cluster.GetTolerantSizeRatio()
		if tolerantSizeRatio == 0 {
			tolerantSizeRatio = leaderTolerantSizeRatio
		}
		leaderCount := int64(1.0 * tolerantSizeRatio)
		return leaderCount
	}

	regionSize := region.GetApproximateSize()
	if regionSize < cluster.GetAverageRegionSize() {
		regionSize = cluster.GetAverageRegionSize()
	}
	regionSize = int64(float64(regionSize) * adjustTolerantRatio(cluster))
	return regionSize
}

func adjustTolerantRatio(cluster opt.Cluster) float64 {
	tolerantSizeRatio := cluster.GetTolerantSizeRatio()
	if tolerantSizeRatio == 0 {
		var maxRegionCount float64
		stores := cluster.GetStores()
		for _, store := range stores {
			regionCount := float64(cluster.GetStoreRegionCount(store.GetID()))
			if maxRegionCount < regionCount {
				maxRegionCount = regionCount
			}
		}
		tolerantSizeRatio = maxRegionCount * adjustRatio
		if tolerantSizeRatio < minTolerantSizeRatio {
			tolerantSizeRatio = minTolerantSizeRatio
		}
	}
	return tolerantSizeRatio
}

func adjustBalanceLimit(cluster opt.Cluster, kind core.ResourceKind) uint64 {
	stores := cluster.GetStores()
	counts := make([]float64, 0, len(stores))
	for _, s := range stores {
		if s.IsUp() {
			counts = append(counts, float64(s.ResourceCount(kind)))
		}
	}
	limit, _ := stats.StandardDeviation(counts)
	return maxUint64(1, uint64(limit))
}

// ScoreInfo stores storeID and score of a store.
type ScoreInfo struct {
	storeID uint64
	score   float64
}

// NewScoreInfo returns a ScoreInfo.
func NewScoreInfo(storeID uint64, score float64) *ScoreInfo {
	return &ScoreInfo{
		storeID: storeID,
		score:   score,
	}
}

// GetStoreID returns the storeID.
func (s *ScoreInfo) GetStoreID() uint64 {
	return s.storeID
}

// GetScore returns the score.
func (s *ScoreInfo) GetScore() float64 {
	return s.score
}

// SetScore sets the score.
func (s *ScoreInfo) SetScore(score float64) {
	s.score = score
}

// ScoreInfos is used for sorting ScoreInfo.
type ScoreInfos struct {
	scoreInfos []*ScoreInfo
	isSorted   bool
}

// NewScoreInfos returns a ScoreInfos.
func NewScoreInfos() *ScoreInfos {
	return &ScoreInfos{
		scoreInfos: make([]*ScoreInfo, 0),
		isSorted:   true,
	}
}

// Add adds a scoreInfo into the slice.
func (s *ScoreInfos) Add(scoreInfo *ScoreInfo) {
	infosLen := len(s.scoreInfos)
	if s.isSorted && infosLen != 0 && s.scoreInfos[infosLen-1].score > scoreInfo.score {
		s.isSorted = false
	}
	s.scoreInfos = append(s.scoreInfos, scoreInfo)
}

// Len returns length of slice.
func (s *ScoreInfos) Len() int { return len(s.scoreInfos) }

// Less returns if one number is less than another.
func (s *ScoreInfos) Less(i, j int) bool { return s.scoreInfos[i].score < s.scoreInfos[j].score }

// Swap switches out two numbers in slice.
func (s *ScoreInfos) Swap(i, j int) {
	s.scoreInfos[i], s.scoreInfos[j] = s.scoreInfos[j], s.scoreInfos[i]
}

// Sort sorts the slice.
func (s *ScoreInfos) Sort() {
	if !s.isSorted {
		sort.Sort(s)
		s.isSorted = true
	}
}

// ToSlice returns the scoreInfo slice.
func (s *ScoreInfos) ToSlice() []*ScoreInfo {
	return s.scoreInfos
}

// Min returns the min score of the slice.
func (s *ScoreInfos) Min() float64 {
	if len(s.scoreInfos) == 0 {
		return 0
	}
	s.Sort()
	return s.scoreInfos[0].score
}

// Max returns the Max score of the slice.
func (s *ScoreInfos) Max() float64 {
	if len(s.scoreInfos) == 0 {
		return 0
	}
	s.Sort()
	return s.scoreInfos[len(s.scoreInfos)-1].score
}

// Variation uses slice's stddev/mean,which is coefficient of variation, to measure the data imbalance.
func (s *ScoreInfos) Variation() float64 {
	mean := s.Mean()
	if mean == 0 {
		return 0
	}

	return s.StdDev() / mean
}

// Mean returns the mean of the slice.
func (s *ScoreInfos) Mean() float64 {
	if s.Len() == 0 {
		return 0
	}

	var sum float64
	for _, info := range s.scoreInfos {
		sum += info.score
	}

	return sum / float64(s.Len())
}

// StdDev returns the standard deviation of the slice.
func (s *ScoreInfos) StdDev() float64 {
	if s.Len() == 0 {
		return 0
	}

	var res float64
	mean := s.Mean()
	for _, info := range s.ToSlice() {
		diff := info.GetScore() - mean
		res += diff * diff
	}
	res /= float64(s.Len())
	res = math.Sqrt(res)

	return res
}

// MeanStoresStats returns the mean of stores' stats.
func MeanStoresStats(storesStats map[uint64]float64) float64 {
	if len(storesStats) == 0 {
		return 0.0
	}

	var sum float64
	for _, storeStat := range storesStats {
		sum += storeStat
	}
	return sum / float64(len(storesStats))
}

// ConvertStoresStats converts a map to a ScoreInfos.
func ConvertStoresStats(storesStats map[uint64]float64) *ScoreInfos {
	scoreInfos := NewScoreInfos()
	for storeID, score := range storesStats {
		scoreInfos.Add(NewScoreInfo(storeID, score))
	}
	scoreInfos.Sort()
	return scoreInfos
}

func getKeyRanges(args []string) ([]core.KeyRange, error) {
	var ranges []core.KeyRange
	for len(args) > 1 {
		startKey, err := url.QueryUnescape(args[0])
		if err != nil {
			return nil, err
		}
		endKey, err := url.QueryUnescape(args[1])
		if err != nil {
			return nil, err
		}
		args = args[2:]
		ranges = append(ranges, core.NewKeyRange(startKey, endKey))
	}
	if len(ranges) == 0 {
		return []core.KeyRange{core.NewKeyRange("", "")}, nil
	}
	return ranges, nil
}

// Influence records operator influence.
type Influence struct {
	ByteRate float64
}

func (infl Influence) add(rhs *Influence, w float64) Influence {
	infl.ByteRate += rhs.ByteRate * w
	return infl
}

// TODO: merge it into OperatorInfluence.
type pendingInfluence struct {
	op               *operator.Operator
	from, to         uint64
	origin           Influence
	isTransferLeader bool
}

func (p *pendingInfluence) isDone() bool {
	return p.op.IsEnd()
}

func newPendingInfluence(op *operator.Operator, from, to uint64, infl Influence, isTransferLeader bool) *pendingInfluence {
	return &pendingInfluence{
		op:               op,
		from:             from,
		to:               to,
		origin:           infl,
		isTransferLeader: isTransferLeader,
	}
}

func summaryPendingInfluence(pendings map[*pendingInfluence]struct{}, f func(*operator.Operator) float64) map[uint64]Influence {
	ret := map[uint64]Influence{}
	for p := range pendings {
		w := f(p.op)
		if w == 0 {
			delete(pendings, p)
		}
		ret[p.to] = ret[p.to].add(&p.origin, w)
		ret[p.from] = ret[p.from].add(&p.origin, -w)
	}
	return ret
}
