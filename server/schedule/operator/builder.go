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

package operator

import (
	"fmt"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/versioninfo"
)

// Builder is used to create operators. Usage:
//     op, err := NewBuilder(desc, cluster, region).
//                 RemovePeer(store1).
//                 AddPeer(peer1).
//                 SetLeader(store2).
//                 Build(kind)
// The generated Operator will choose the most appropriate execution order
// according to various constraints.
type Builder struct {
	// basic info
	desc        string
	cluster     opt.Cluster
	regionID    uint64
	regionEpoch *metapb.RegionEpoch
	rules       []*placement.Rule

	// operation record
	originPeers         peersMap
	originLeaderStoreID uint64
	targetPeers         peersMap
	targetLeaderStoreID uint64
	err                 error

	// flags
	useJointConsensus bool
	isLightWeight     bool
	forceTargetLeader bool

	// intermediate states
	currentPeers                         peersMap
	currentLeaderStoreID                 uint64
	toAdd, toRemove, toPromote, toDemote peersMap       // pending tasks.
	steps                                []OpStep       // generated steps.
	peerAddStep                          map[uint64]int // record at which step a peer is created.

	// comparison function
	stepPlanPreferFuncs []func(stepPlan) int // for buildStepsWithoutJointConsensus
}

// newBuilderWithBasicCheck creates a Builder with some basic checks.
func newBuilderWithBasicCheck(desc string, cluster opt.Cluster, region *core.RegionInfo) *Builder {
	var err error
	originPeers := newPeersMap()

	for _, p := range region.GetPeers() {
		if p == nil || p.GetStoreId() == 0 {
			err = errors.Errorf("cannot build operator for region with nil peer")
			break
		}
		originPeers.Set(p)
	}

	originLeaderStoreID := region.GetLeader().GetStoreId()
	if _, ok := originPeers[originLeaderStoreID]; err == nil && !ok {
		err = errors.Errorf("cannot build operator for region with no leader")
	}

	var rules []*placement.Rule
	if err == nil && cluster.GetOpts().IsPlacementRulesEnabled() {
		fit := cluster.FitRegion(region)
		for _, rf := range fit.RuleFits {
			rules = append(rules, rf.Rule)
		}
		if len(rules) == 0 {
			err = errors.Errorf("cannot build operator for region match no placement rule")
		}
	}

	return &Builder{
		desc:                desc,
		cluster:             cluster,
		regionID:            region.GetID(),
		regionEpoch:         region.GetRegionEpoch(),
		rules:               rules,
		originPeers:         originPeers,
		originLeaderStoreID: originLeaderStoreID,
		targetPeers:         originPeers.Copy(),
		useJointConsensus:   cluster.IsFeatureSupported(versioninfo.JointConsensus),
		err:                 err,
	}
}

// NewBuilder creates a Builder.
func NewBuilder(desc string, cluster opt.Cluster, region *core.RegionInfo) *Builder {
	b := newBuilderWithBasicCheck(desc, cluster, region)

	if b.err == nil && core.IsInJointState(region.GetPeers()...) {
		b.err = errors.Errorf("cannot build operator for region which is in joint state")
	}

	return b
}

// AddPeer records an add Peer operation in Builder. If peer.Id is 0, the builder
// will allocate a new peer ID later.
func (b *Builder) AddPeer(peer *metapb.Peer) *Builder {
	if b.err != nil {
		return b
	}
	if peer == nil || peer.GetStoreId() == 0 {
		b.err = errors.Errorf("cannot add nil peer")
	} else if core.IsInJointState(peer) {
		b.err = errors.Errorf("cannot add peer %s: is in joint state", peer)
	} else if old, ok := b.targetPeers[peer.GetStoreId()]; ok {
		b.err = errors.Errorf("cannot add peer %s: already have peer %s", peer, old)
	} else {
		b.targetPeers.Set(peer)
	}
	return b
}

// RemovePeer records a remove peer operation in Builder.
func (b *Builder) RemovePeer(storeID uint64) *Builder {
	if b.err != nil {
		return b
	}
	if _, ok := b.targetPeers[storeID]; !ok {
		b.err = errors.Errorf("cannot remove peer from %d: not found", storeID)
	} else if b.targetLeaderStoreID == storeID {
		b.err = errors.Errorf("cannot remove peer from %d: is target leader", storeID)
	} else {
		delete(b.targetPeers, storeID)
	}
	return b
}

// PromoteLearner records a promote learner operation in Builder.
func (b *Builder) PromoteLearner(storeID uint64) *Builder {
	if b.err != nil {
		return b
	}
	if peer, ok := b.targetPeers[storeID]; !ok {
		b.err = errors.Errorf("cannot promote peer %d: not found", storeID)
	} else if !core.IsLearner(peer) {
		b.err = errors.Errorf("cannot promote peer %d: is not learner", storeID)
	} else {
		b.targetPeers.Set(&metapb.Peer{
			Id:      peer.GetId(),
			StoreId: peer.GetStoreId(),
			Role:    metapb.PeerRole_Voter,
		})
	}
	return b
}

// DemoteVoter records a demote voter operation in Builder.
func (b *Builder) DemoteVoter(storeID uint64) *Builder {
	if b.err != nil {
		return b
	}
	if peer, ok := b.targetPeers[storeID]; !ok {
		b.err = errors.Errorf("cannot demote voter %d: not found", storeID)
	} else if core.IsLearner(peer) {
		b.err = errors.Errorf("cannot demote voter %d: is already learner", storeID)
	} else {
		b.targetPeers.Set(&metapb.Peer{
			Id:      peer.GetId(),
			StoreId: peer.GetStoreId(),
			Role:    metapb.PeerRole_Learner,
		})
	}
	return b
}

// SetLeader records the target leader in Builder.
func (b *Builder) SetLeader(storeID uint64) *Builder {
	if b.err != nil {
		return b
	}
	if peer, ok := b.targetPeers[storeID]; !ok {
		b.err = errors.Errorf("cannot transfer leader to %d: not found", storeID)
	} else if core.IsLearner(peer) {
		b.err = errors.Errorf("cannot transfer leader to %d: not voter", storeID)
	} else {
		b.targetLeaderStoreID = storeID
	}
	return b
}

// SetPeers resets the target peer list.
//
// If peer's ID is 0, the builder will allocate a new ID later. If current
// target leader does not exist in peers, it will be reset.
func (b *Builder) SetPeers(peers map[uint64]*metapb.Peer) *Builder {
	if b.err != nil {
		return b
	}

	for key, peer := range peers {
		if peer == nil || key == 0 || peer.GetStoreId() != key || core.IsInJointState(peer) {
			b.err = errors.Errorf("setPeers with mismatch peers: %v", peers)
			return b
		}
	}

	if _, ok := peers[b.targetLeaderStoreID]; !ok {
		b.targetLeaderStoreID = 0
	}

	b.targetPeers = peersMap(peers).Copy()
	return b
}

// EnableLightWeight marks the region as light weight. It is used for scatter regions.
func (b *Builder) EnableLightWeight() *Builder {
	b.isLightWeight = true
	return b
}

// EnableForceTargetLeader marks the step of transferring leader to target is forcible. It is used for grant leader.
func (b *Builder) EnableForceTargetLeader() *Builder {
	b.forceTargetLeader = true
	return b
}

func (b *Builder) setUseJointConsensus(useJointConsensus bool) *Builder {
	b.useJointConsensus = useJointConsensus
	return b
}

// Build creates the Operator.
func (b *Builder) Build(kind OpKind) (*Operator, error) {
	var brief string

	if b.err != nil {
		return nil, b.err
	}

	if brief, b.err = b.prepareBuild(); b.err != nil {
		return nil, b.err
	}

	if b.useJointConsensus {
		kind, b.err = b.buildStepsWithJointConsensus(kind)
	} else {
		kind, b.err = b.buildStepsWithoutJointConsensus(kind)
	}
	if b.err != nil {
		return nil, b.err
	}

	return NewOperator(b.desc, brief, b.regionID, b.regionEpoch, kind, b.steps...), nil
}

// Initialize intermediate states.
// TODO: simplify the code
func (b *Builder) prepareBuild() (string, error) {
	b.toAdd = newPeersMap()
	b.toRemove = newPeersMap()
	b.toPromote = newPeersMap()
	b.toDemote = newPeersMap()

	voterCount := 0
	for _, peer := range b.targetPeers {
		if !core.IsLearner(peer) {
			voterCount++
		}
	}
	if voterCount == 0 {
		return "", errors.New("cannot create operator: target peers have no voter")
	}

	// Diff `originPeers` and `targetPeers` to initialize `toAdd`, `toRemove`, `toPromote`, `toDemote`.
	// Note: Use `toDemote` only when `useJointConsensus` is true. Otherwise use `toAdd`, `toRemove` instead.
	for _, o := range b.originPeers {
		n := b.targetPeers[o.GetStoreId()]
		if n == nil {
			b.toRemove.Set(o)
			continue
		}

		// If the peer id in the target is different from that in the origin,
		// modify it to the peer id of the origin.
		if o.GetId() != n.GetId() {
			n = &metapb.Peer{
				Id:      o.GetId(),
				StoreId: o.GetStoreId(),
				Role:    n.GetRole(),
			}
		}

		if core.IsLearner(o) {
			if !core.IsLearner(n) {
				// learner -> voter
				b.toPromote.Set(n)
			}
		} else {
			if core.IsLearner(n) {
				// voter -> learner
				if b.useJointConsensus {
					b.toDemote.Set(n)
				} else {
					b.toRemove.Set(o)
					// Need to add `b.toAdd.Set(n)` in the later targetPeers loop
				}
			}
		}
	}
	for _, n := range b.targetPeers {
		// old peer not exists, or target is learner while old one is voter.
		o := b.originPeers[n.GetStoreId()]
		if o == nil || (!b.useJointConsensus && !core.IsLearner(o) && core.IsLearner(n)) {
			if n.GetId() == 0 {
				// Allocate peer ID if need.
				id, err := b.cluster.AllocID()
				if err != nil {
					return "", err
				}
				n = &metapb.Peer{
					Id:      id,
					StoreId: n.GetStoreId(),
					Role:    n.GetRole(),
				}
			}
			// It is a pair with `b.toRemove.Set(o)` when `o != nil`.
			b.toAdd.Set(n)
		}
	}

	// If the target leader does not exist or is a Learner, the target is cancelled.
	if peer, ok := b.targetPeers[b.targetLeaderStoreID]; !ok || core.IsLearner(peer) {
		b.targetLeaderStoreID = 0
	}

	// If no target leader is specified, try not to change the leader as much as possible.
	if b.targetLeaderStoreID == 0 {
		if peer, ok := b.targetPeers[b.originLeaderStoreID]; ok && !core.IsLearner(peer) {
			b.targetLeaderStoreID = b.originLeaderStoreID
		}
	}

	b.currentPeers, b.currentLeaderStoreID = b.originPeers.Copy(), b.originLeaderStoreID

	if b.targetLeaderStoreID != 0 {
		targetLeader := b.targetPeers[b.targetLeaderStoreID]
		if b.forceTargetLeader {
			if !b.hasAbilityLeader(targetLeader) {
				return "", errors.New("cannot create operator: target leader is impossible")
			}
		} else if !b.allowLeader(targetLeader) {
			return "", errors.New("cannot create operator: target leader is not allowed")
		}
	}

	if len(b.toAdd)+len(b.toRemove)+len(b.toPromote)+len(b.toDemote) <= 1 {
		// If only one peer changed, joint consensus is not used.
		b.useJointConsensus = false
	}

	b.peerAddStep = make(map[uint64]int)

	return b.brief(), nil
}

// generate brief description of the operator.
func (b *Builder) brief() string {
	switch {
	case len(b.toAdd) > 0 && len(b.toRemove) > 0:
		op := "mv peer"
		if b.isLightWeight {
			op = "mv light peer"
		}
		return fmt.Sprintf("%s: store %s to %s", op, b.toRemove, b.toAdd)
	case len(b.toAdd) > 0:
		return fmt.Sprintf("add peer: store %s", b.toAdd)
	case len(b.toRemove) > 0:
		return fmt.Sprintf("rm peer: store %s", b.toRemove)
	case len(b.toPromote) > 0:
		return fmt.Sprintf("promote peer: store %s", b.toPromote)
	case len(b.toDemote) > 0:
		return fmt.Sprintf("demote peer: store %s", b.toDemote)
	case b.originLeaderStoreID != b.targetLeaderStoreID:
		return fmt.Sprintf("transfer leader: store %d to %d", b.originLeaderStoreID, b.targetLeaderStoreID)
	default:
		return ""
	}
}

// Using Joint Consensus can ensure the replica safety and reduce the number of steps.
func (b *Builder) buildStepsWithJointConsensus(kind OpKind) (OpKind, error) {
	return kind, errors.New("not implemented")
}

// Some special cases, and stores that do not support using joint consensus.
func (b *Builder) buildStepsWithoutJointConsensus(kind OpKind) (OpKind, error) {
	b.initStepPlanPreferFuncs()

	for len(b.toAdd) > 0 || len(b.toRemove) > 0 || len(b.toPromote) > 0 || len(b.toDemote) > 0 {
		plan := b.peerPlan()
		if plan.Empty() {
			return kind, errors.New("fail to build operator: plan is empty, maybe no valid leader")
		}
		if plan.leaderBeforeAdd != 0 && plan.leaderBeforeAdd != b.currentLeaderStoreID {
			b.execTransferLeader(plan.leaderBeforeAdd)
			kind |= OpLeader
		}
		if plan.add != nil {
			b.execAddPeer(plan.add)
			kind |= OpRegion
		}
		if plan.promote != nil {
			b.execPromoteLearner(plan.promote)
		}
		if plan.leaderBeforeRemove != 0 && plan.leaderBeforeRemove != b.currentLeaderStoreID {
			b.execTransferLeader(plan.leaderBeforeRemove)
			kind |= OpLeader
		}
		if plan.demote != nil {
			b.execDemoteFollower(plan.demote)
		}
		if plan.remove != nil {
			b.execRemovePeer(plan.remove)
			kind |= OpRegion
		}
	}

	if b.targetLeaderStoreID != 0 &&
		b.currentLeaderStoreID != b.targetLeaderStoreID &&
		b.currentPeers[b.targetLeaderStoreID] != nil {
		// Transfer only when target leader is legal.
		b.execTransferLeader(b.targetLeaderStoreID)
		kind |= OpLeader
	}

	if len(b.steps) == 0 {
		return kind, errors.New("no operator step is built")
	}
	return kind, nil
}

func (b *Builder) execTransferLeader(id uint64) {
	b.steps = append(b.steps, TransferLeader{FromStore: b.currentLeaderStoreID, ToStore: id})
	b.currentLeaderStoreID = id
}

func (b *Builder) execPromoteLearner(peer *metapb.Peer) {
	b.steps = append(b.steps, PromoteLearner{ToStore: peer.GetStoreId(), PeerID: peer.GetId()})
	b.currentPeers.Set(peer)
	delete(b.toPromote, peer.GetStoreId())
}

func (b *Builder) execDemoteFollower(peer *metapb.Peer) {
	b.steps = append(b.steps, DemoteFollower{ToStore: peer.GetStoreId(), PeerID: peer.GetId()})
	b.currentPeers.Set(peer)
	delete(b.toDemote, peer.GetStoreId())
}

func (b *Builder) execAddPeer(peer *metapb.Peer) {
	if b.isLightWeight {
		b.steps = append(b.steps, AddLightLearner{ToStore: peer.GetStoreId(), PeerID: peer.GetId()})
	} else {
		b.steps = append(b.steps, AddLearner{ToStore: peer.GetStoreId(), PeerID: peer.GetId()})
	}
	if !core.IsLearner(peer) {
		b.steps = append(b.steps, PromoteLearner{ToStore: peer.GetStoreId(), PeerID: peer.GetId()})
	}
	b.currentPeers.Set(peer)
	b.peerAddStep[peer.GetStoreId()] = len(b.steps)
	delete(b.toAdd, peer.GetStoreId())
}

func (b *Builder) execRemovePeer(peer *metapb.Peer) {
	b.steps = append(b.steps, RemovePeer{FromStore: peer.GetStoreId(), PeerID: peer.GetId()})
	delete(b.currentPeers, peer.GetStoreId())
	delete(b.toRemove, peer.GetStoreId())
}

var stateFilter = filter.StoreStateFilter{ActionScope: "operator-builder", TransferLeader: true}

// check if the peer has the ability to become a leader.
func (b *Builder) hasAbilityLeader(peer *metapb.Peer) bool {
	// these roles are not allowed to become leaders.
	switch peer.GetRole() {
	case metapb.PeerRole_Learner, metapb.PeerRole_DemotingVoter:
		return false
	}

	// store does not exist
	if peer.GetStoreId() == b.currentLeaderStoreID {
		return true
	}
	store := b.cluster.GetStore(peer.GetStoreId())
	return store != nil
}

// check if the peer is allowed to become the leader.
func (b *Builder) allowLeader(peer *metapb.Peer) bool {
	// these roles are not allowed to become leaders.
	switch peer.GetRole() {
	case metapb.PeerRole_Learner, metapb.PeerRole_DemotingVoter:
		return false
	}

	// store does not exist
	if peer.GetStoreId() == b.currentLeaderStoreID {
		return true
	}
	store := b.cluster.GetStore(peer.GetStoreId())
	if store == nil {
		return false
	}

	// filter and rules
	if !stateFilter.Target(b.cluster.GetOpts(), store) {
		return false
	}
	if len(b.rules) == 0 {
		return true
	}
	for _, r := range b.rules {
		if (r.Role == placement.Leader || r.Role == placement.Voter) &&
			placement.MatchLabelConstraints(store, r.LabelConstraints) {
			return true
		}
	}

	return false
}

// stepPlan is exec step. It can be:
// 1. add voter + remove voter.
// 2. add learner + remove learner.
// 3. add learner + promote learner + remove voter.
// 4. promote learner.
// 5. demote voter.
// 6. remove voter/learner.
// 7. add voter/learner.
// Plan 1-3 (replace plans) do not change voter/learner count, so they have higher priority.
type stepPlan struct {
	leaderBeforeAdd    uint64 // leader before adding peer.
	leaderBeforeRemove uint64 // leader before removing peer.
	add                *metapb.Peer
	remove             *metapb.Peer
	promote            *metapb.Peer
	demote             *metapb.Peer
}

func (p stepPlan) String() string {
	return fmt.Sprintf("stepPlan{leaderBeforeAdd=%v,add={%s},promote={%s},leaderBeforeRemove=%v,demote={%s},remove={%s}}",
		p.leaderBeforeAdd, p.add, p.promote, p.leaderBeforeRemove, p.demote, p.remove)
}

func (p stepPlan) Empty() bool {
	return p.promote == nil && p.demote == nil && p.add == nil && p.remove == nil
}

func (b *Builder) peerPlan() stepPlan {
	// Replace has the highest priority because it does not change region's
	// voter/learner count.
	if p := b.planReplace(); !p.Empty() {
		return p
	}
	if p := b.planPromotePeer(); !p.Empty() {
		return p
	}
	if p := b.planDemotePeer(); !p.Empty() {
		return p
	}
	if p := b.planRemovePeer(); !p.Empty() {
		return p
	}
	if p := b.planAddPeer(); !p.Empty() {
		return p
	}
	return stepPlan{}
}

func (b *Builder) planReplace() stepPlan {
	var best stepPlan
	// add voter + remove voter OR add learner + remove learner.
	for _, i := range b.toAdd.IDs() {
		add := b.toAdd[i]
		for _, j := range b.toRemove.IDs() {
			remove := b.toRemove[j]
			if core.IsLearner(remove) == core.IsLearner(add) {
				best = b.planReplaceLeaders(best, stepPlan{add: add, remove: remove})
			}
		}
	}
	// add learner + promote learner + remove voter
	for _, i := range b.toPromote.IDs() {
		promote := b.toPromote[i]
		for _, j := range b.toAdd.IDs() {
			if add := b.toAdd[j]; core.IsLearner(add) {
				for _, k := range b.toRemove.IDs() {
					if remove := b.toRemove[k]; !core.IsLearner(remove) && j != k {
						best = b.planReplaceLeaders(best, stepPlan{promote: promote, add: add, remove: remove})
					}
				}
			}
		}
	}
	// demote voter + remove learner + add voter
	for _, i := range b.toDemote.IDs() {
		demote := b.toDemote[i]
		for _, j := range b.toRemove.IDs() {
			if remove := b.toRemove[j]; core.IsLearner(remove) {
				for _, k := range b.toAdd.IDs() {
					if add := b.toAdd[k]; !core.IsLearner(add) && j != k {
						best = b.planReplaceLeaders(best, stepPlan{demote: demote, add: add, remove: remove})
					}
				}
			}
		}
	}
	return best
}

func (b *Builder) planReplaceLeaders(best, next stepPlan) stepPlan {
	// Brute force all possible leader combinations to find the best plan.
	for _, leaderBeforeAdd := range b.currentPeers.IDs() {
		if !b.allowLeader(b.currentPeers[leaderBeforeAdd]) {
			continue
		}
		next.leaderBeforeAdd = leaderBeforeAdd
		for _, leaderBeforeRemove := range b.currentPeers.IDs() {
			if leaderBeforeRemove != next.demote.GetStoreId() &&
				leaderBeforeRemove != next.remove.GetStoreId() &&
				b.allowLeader(b.currentPeers[leaderBeforeRemove]) {
				// leaderBeforeRemove does not select nodes to be demote or removed.
				next.leaderBeforeRemove = leaderBeforeRemove
				best = b.comparePlan(best, next)
			}
		}
		if next.promote != nil &&
			next.promote.GetStoreId() != next.demote.GetStoreId() &&
			next.promote.GetStoreId() != next.remove.GetStoreId() &&
			b.allowLeader(next.promote) {
			// leaderBeforeRemove does not select nodes to be demote or removed.
			next.leaderBeforeRemove = next.promote.GetStoreId()
			best = b.comparePlan(best, next)
		}
		if next.add != nil &&
			next.add.GetStoreId() != next.demote.GetStoreId() &&
			next.add.GetStoreId() != next.remove.GetStoreId() &&
			b.allowLeader(next.add) {
			// leaderBeforeRemove does not select nodes to be demote or removed.
			next.leaderBeforeRemove = next.add.GetStoreId()
			best = b.comparePlan(best, next)
		}
	}
	return best
}

func (b *Builder) planPromotePeer() stepPlan {
	for _, i := range b.toPromote.IDs() {
		peer := b.toPromote[i]
		return stepPlan{promote: peer}
	}
	return stepPlan{}
}

func (b *Builder) planDemotePeer() stepPlan {
	var best stepPlan
	for _, i := range b.toDemote.IDs() {
		d := b.toDemote[i]
		for _, leader := range b.currentPeers.IDs() {
			if b.allowLeader(b.currentPeers[leader]) && leader != d.GetStoreId() {
				best = b.comparePlan(best, stepPlan{demote: d, leaderBeforeRemove: leader})
			}
		}
	}
	return best
}

func (b *Builder) planRemovePeer() stepPlan {
	var best stepPlan
	for _, i := range b.toRemove.IDs() {
		r := b.toRemove[i]
		for _, leader := range b.currentPeers.IDs() {
			if b.allowLeader(b.currentPeers[leader]) && leader != r.GetStoreId() {
				best = b.comparePlan(best, stepPlan{remove: r, leaderBeforeRemove: leader})
			}
		}
	}
	return best
}

func (b *Builder) planAddPeer() stepPlan {
	var best stepPlan
	for _, i := range b.toAdd.IDs() {
		a := b.toAdd[i]
		for _, leader := range b.currentPeers.IDs() {
			if b.allowLeader(b.currentPeers[leader]) {
				best = b.comparePlan(best, stepPlan{add: a, leaderBeforeAdd: leader})
			}
		}
	}
	return best
}

func (b *Builder) initStepPlanPreferFuncs() {
	b.stepPlanPreferFuncs = []func(stepPlan) int{
		b.planPreferReplaceByNearest, // 1. violate it affects replica safety.
		// 2-3 affects operator execution speed.
		b.planPreferUpStoreAsLeader, // 2. compare to 3, it is more likely to affect execution speed.
		b.planPreferOldPeerAsLeader, // 3. violate it may or may not affect execution speed.
		// 4-6 are less important as they are only trying to build the
		// operator with less leader transfer steps.
		b.planPreferAddOrPromoteTargetLeader, // 4. it is precondition of 5 so goes first.
		b.planPreferTargetLeader,             // 5. it may help 6 in later steps.
		b.planPreferLessLeaderTransfer,       // 6. trivial optimization to make the operator more tidy.
	}
}

// Pick the better plan from 2 candidates.
func (b *Builder) comparePlan(best, next stepPlan) stepPlan {
	if best.Empty() {
		return next
	}
	for _, f := range b.stepPlanPreferFuncs {
		if scoreBest, scoreNext := f(best), f(next); scoreBest > scoreNext {
			return best
		} else if scoreBest < scoreNext {
			return next
		}
	}
	return best
}

func (b *Builder) labelMatch(x, y uint64) int {
	sx, sy := b.cluster.GetStore(x), b.cluster.GetStore(y)
	if sx == nil || sy == nil {
		return 0
	}
	labels := b.cluster.GetOpts().GetLocationLabels()
	for i, l := range labels {
		if sx.GetLabelValue(l) != sy.GetLabelValue(l) {
			return i
		}
	}
	return len(labels)
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// return matched label count.
func (b *Builder) planPreferReplaceByNearest(p stepPlan) int {
	m := 0
	if p.add != nil && p.remove != nil {
		m = b.labelMatch(p.add.GetStoreId(), p.remove.GetStoreId())
		if p.promote != nil {
			// add learner + promote learner + remove voter
			if m2 := b.labelMatch(p.promote.GetStoreId(), p.add.GetStoreId()); m2 < m {
				return m2
			}
		} else if p.demote != nil {
			// demote voter + remove learner + add voter
			if m2 := b.labelMatch(p.demote.GetStoreId(), p.remove.GetStoreId()); m2 < m {
				return m2
			}
		}
	}
	return m
}

// Avoid generating snapshots from offline stores.
func (b *Builder) planPreferUpStoreAsLeader(p stepPlan) int {
	if p.add != nil {
		store := b.cluster.GetStore(p.leaderBeforeAdd)
		return b2i(store != nil && store.IsUp())
	}
	return 1
}

// Newly created peer may reject the leader. See https://github.com/tikv/tikv/issues/3819
func (b *Builder) planPreferOldPeerAsLeader(p stepPlan) int {
	ret := -b.peerAddStep[p.leaderBeforeAdd]
	if p.add != nil && p.add.GetStoreId() == p.leaderBeforeRemove {
		ret -= len(b.steps) + 1
	} else {
		ret -= b.peerAddStep[p.leaderBeforeRemove]
	}
	return ret
}

// It is better to avoid transferring leader.
func (b *Builder) planPreferLessLeaderTransfer(p stepPlan) int {
	if p.leaderBeforeAdd == 0 || p.leaderBeforeAdd == b.currentLeaderStoreID {
		// 3: current == leaderBeforeAdd == leaderBeforeRemove
		// 2: current == leaderBeforeAdd != leaderBeforeRemove
		return 2 + b2i(p.leaderBeforeRemove == 0 || p.leaderBeforeRemove == b.currentLeaderStoreID)
	}
	// 1: current != leaderBeforeAdd == leaderBeforeRemove
	// 0: current != leaderBeforeAdd != leaderBeforeRemove
	return b2i(p.leaderBeforeRemove == 0 || p.leaderBeforeRemove == p.leaderBeforeAdd)
}

// It is better to transfer leader to the target leader.
func (b *Builder) planPreferTargetLeader(p stepPlan) int {
	return b2i(b.targetLeaderStoreID == 0 ||
		(p.leaderBeforeRemove != 0 && p.leaderBeforeRemove == b.targetLeaderStoreID) ||
		(p.leaderBeforeRemove == 0 && p.leaderBeforeAdd == b.targetLeaderStoreID))
}

// It is better to add target leader as early as possible.
func (b *Builder) planPreferAddOrPromoteTargetLeader(p stepPlan) int {
	if b.targetLeaderStoreID == 0 {
		return 0
	}
	addTarget := p.add != nil && !core.IsLearner(p.add) && p.add.GetStoreId() == b.targetLeaderStoreID
	promoteTarget := p.promote != nil && p.promote.GetStoreId() == b.targetLeaderStoreID
	return b2i(addTarget || promoteTarget)
}

// Peers indexed by storeID.
type peersMap map[uint64]*metapb.Peer

func newPeersMap() peersMap {
	return make(map[uint64]*metapb.Peer)
}

// IDs is used for iteration in order.
func (pm peersMap) IDs() []uint64 {
	ids := make([]uint64, 0, len(pm))
	for id := range pm {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (pm peersMap) Set(peer *metapb.Peer) {
	pm[peer.GetStoreId()] = peer
}

func (pm peersMap) String() string {
	ids := make([]uint64, 0, len(pm))
	for _, p := range pm {
		ids = append(ids, p.GetStoreId())
	}
	return fmt.Sprintf("%v", ids)
}

func (pm peersMap) Copy() peersMap {
	var pm2 peersMap = make(map[uint64]*metapb.Peer, len(pm))
	for _, p := range pm {
		pm2.Set(p)
	}
	return pm2
}
