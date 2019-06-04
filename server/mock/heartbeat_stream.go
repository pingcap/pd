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

package mock

import (
	"context"
	"errors"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
)

// HeartbeatStream is used to mock HeartbeatStream for test use.
type HeartbeatStream struct {
	ch chan *pdpb.RegionHeartbeatResponse
}

// NewHeartbeatStream creates a new HeartbeatStream.
func NewHeartbeatStream() HeartbeatStream {
	return HeartbeatStream{
		ch: make(chan *pdpb.RegionHeartbeatResponse),
	}
}

// Send mocks method.
func (s HeartbeatStream) Send(m *pdpb.RegionHeartbeatResponse) error {
	select {
	case <-time.After(time.Second):
		return errors.New("timeout")
	case s.ch <- m:
	}
	return nil
}

// SendMsg is used to send the message.
func (s HeartbeatStream) SendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse) {
	return
}

// Recv mocks method.
func (s HeartbeatStream) Recv() *pdpb.RegionHeartbeatResponse {
	select {
	case <-time.After(time.Millisecond * 10):
		return nil
	case res := <-s.ch:
		return res
	}
}

// HeartbeatStreams is used to mock heartbeatstreams for test use.
type HeartbeatStreams struct {
	ctx       context.Context
	cancel    context.CancelFunc
	clusterID uint64
	msgCh     chan *pdpb.RegionHeartbeatResponse
}

// NewHeartbeatStreams creates a new HeartbeatStreams.
func NewHeartbeatStreams(clusterID uint64) *HeartbeatStreams {
	ctx, cancel := context.WithCancel(context.Background())
	hs := &HeartbeatStreams{
		ctx:       ctx,
		cancel:    cancel,
		clusterID: clusterID,
		msgCh:     make(chan *pdpb.RegionHeartbeatResponse, 1024),
	}
	return hs
}

// SendMsg is used to send the message.
func (mhs *HeartbeatStreams) SendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse) {
	if region.GetLeader() == nil {
		return
	}

	msg.Header = &pdpb.ResponseHeader{ClusterId: mhs.clusterID}
	msg.RegionId = region.GetID()
	msg.RegionEpoch = region.GetRegionEpoch()
	msg.TargetPeer = region.GetLeader()

	select {
	case mhs.msgCh <- msg:
	case <-mhs.ctx.Done():
	}
}
