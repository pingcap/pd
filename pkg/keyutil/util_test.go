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

package keyutil

import (
	"testing"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testKeyUtilSuite{})

type testKeyUtilSuite struct {
}

func (s *testKeyUtilSuite) TestKeyUtil(c *C) {
	startKey := []byte("a")
	endKey := []byte("b")
	key := BuildKeyRangeKey(startKey, endKey)
	c.Assert(key, Equals, "61-62")
}
