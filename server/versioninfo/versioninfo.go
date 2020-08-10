// Copyright 2020 PingCAP, Inc.
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

package versioninfo

import (
	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	// CommunityEdition is the default edition for building.
	CommunityEdition = "Community"
)

// Version information.
var (
	PDReleaseVersion = "None"
	PDBuildTS        = "None"
	PDGitHash        = "None"
	PDGitBranch      = "None"
	PDEdition        = CommunityEdition
)

// ParseVersion wraps semver.NewVersion and handles compatibility issues.
func ParseVersion(v string) (*semver.Version, error) {
	// for compatibility with old version which not support `version` mechanism.
	if v == "" {
		return semver.New(featuresDict[Base]), nil
	}
	if v[0] == 'v' {
		v = v[1:]
	}
	ver, err := semver.NewVersion(v)
	return ver, errors.WithStack(err)
}

// MustParseVersion wraps ParseVersion and will panic if error is not nil.
func MustParseVersion(v string) *semver.Version {
	ver, err := ParseVersion(v)
	if err != nil {
		log.Fatal("version string is illegal", zap.Error(err))
	}
	return ver
}

// IsCompatible checks if the current version is compatible with the specified version.
func IsCompatible(current, specified semver.Version) bool {
	if current.LessThan(specified) {
		return true
	}
	return current.Major == specified.Major && current.Minor == specified.Minor
}
