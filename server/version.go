// Copyright 2018 PingCAP, Inc.
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
	"github.com/coreos/go-semver/semver"
	log "github.com/sirupsen/logrus"
)

// Feature supported features.
type Feature int

// Fetures list.
// The cluster provides corresponding new features if the cluster version
// greater than or equal to the required minimum version of the feature.
const (
	Base Feature = iota
	Version2_0
	// RegionMerge supports the adjacent regions to be merged.
	// and PD will periodically check if there is enough small
	// region to be merged. if there is, will send the corresponding
	// merge command to the TiKV.
	RegionMerge
	// RaftLearner supports add a non-voting member in raft members.
	// and PD scheduling strategy will replace `addPeer` to `addLearner`,`promotoLearner`.
	RaftLearner
	// BatchSplit can speed up the region split.
	// and PD will response the BatchSplit request.
	BatchSplit
)

var featuresDict = map[Feature]string{
	Base:        "1.0.0",
	Version2_0:  "2.0.0",
	RegionMerge: "2.0.0",
	RaftLearner: "2.0.0",
	BatchSplit:  "2.1.0-rc.1",
}

// MinSupportedVersion returns the minimum support version for the specified feature.
func MinSupportedVersion(v Feature) semver.Version {
	target, ok := featuresDict[v]
	if !ok {
		log.Fatalf("the corresponding version of the feature %d doesn't exist", v)
	}
	version := MustParseVersion(target)
	return *version
}

// ParseVersion wraps semver.NewVersion and handles compatibility issues.
func ParseVersion(v string) (*semver.Version, error) {
	// for compatibility with old version which not support `version` mechanism.
	if v == "" {
		return semver.New(featuresDict[Base]), nil
	}
	if v[0] == 'v' {
		v = v[1:]
	}
	return semver.NewVersion(v)
}

// MustParseVersion wraps ParseVersion and will panic if error is not nil.
func MustParseVersion(v string) *semver.Version {
	ver, err := ParseVersion(v)
	if err != nil {
		log.Fatalf("version string is illegal: %s", err)
	}
	return ver
}

// IsCompatible checks if the clusterVersion is compatible with the specified version.
func IsCompatible(clusterVersion, v semver.Version) bool {
	if clusterVersion.LessThan(v) {
		return true
	}
	return clusterVersion.Major == v.Major && clusterVersion.Minor == v.Minor
}
