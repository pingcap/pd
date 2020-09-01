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

package core

import (
	"github.com/pingcap/kvproto/pkg/metapb"
)

// IsLearner judges whether the Peer's Role is Learner.
func IsLearner(peer *metapb.Peer) bool {
	return peer.GetRole() == metapb.PeerRole_Learner
}

// IsInJointState judges whether the Peer is in joint state.
func IsInJointState(peers ...*metapb.Peer) bool {
	for _, peer := range peers {
		switch peer.GetRole() {
		case metapb.PeerRole_IncomingVoter, metapb.PeerRole_DemotingVoter:
			return true
		default:
		}
	}
	return false
}

// CountInJointState count the peers are in joint state.
func CountInJointState(peers ...*metapb.Peer) int {
	count := 0
	for _, peer := range peers {
		switch peer.GetRole() {
		case metapb.PeerRole_IncomingVoter, metapb.PeerRole_DemotingVoter:
			count++
		default:
		}
	}
	return count
}
