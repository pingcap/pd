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

package syncer

import (
	"context"
	"io"
	"math"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	msgSize                  = 8 * 1024 * 1024
	maxSyncRegionBatchSize   = 100
	syncerKeepAliveInterval  = 10 * time.Second
	defaultHistoryBufferSize = 10000
)

// ClientStream is the client side of the region syncer.
type ClientStream interface {
	Recv() (*pdpb.SyncRegionResponse, error)
	CloseSend() error
}

// ServerStream is the server side of the region syncer.
type ServerStream interface {
	Send(regions *pdpb.SyncRegionResponse) error
}

// Server is the abstraction of the syncer storage server.
type Server interface {
	Context() context.Context
	ClusterID() uint64
	GetMemberInfo() *pdpb.Member
	GetLeader() *pdpb.Member
	GetStorage() *core.KV
}

// RegionSyncer is used to sync the region information without raft.
type RegionSyncer struct {
	sync.RWMutex
	streams map[string]ServerStream
	ctx     context.Context
	cancel  context.CancelFunc
	server  Server
	closed  chan struct{}
	wg      sync.WaitGroup
	history *historyBuffer
}

// NewRegionSyncer returns a region syncer.
// The final consistency is ensured by the heartbeat.
// Strong consistency is not guaranteed.
// Usually open the region syncer in huge cluster and the server
// no longer etcd but go-leveldb.
func NewRegionSyncer(s Server) *RegionSyncer {
	return &RegionSyncer{
		streams: make(map[string]ServerStream),
		server:  s,
		closed:  make(chan struct{}),
		history: newHistoryBuffer(defaultHistoryBufferSize, s.GetStorage().GetRegionKV()),
	}
}

// RunServer runs the server of the region syncer.
// regionNitifier is used to get the changed regions.
func (s *RegionSyncer) RunServer(regionNotifier <-chan *core.RegionInfo, quit chan struct{}) {
	var requests []*metapb.Region
	ticker := time.NewTicker(syncerKeepAliveInterval)
	for {
		select {
		case <-quit:
			log.Info("exit region syncer")
			return
		case first := <-regionNotifier:
			requests = append(requests, first.GetMeta())
			startIndex := s.history.GetNextIndex()
			s.history.Record(first)
			pending := len(regionNotifier)
			for i := 0; i < pending && i < maxSyncRegionBatchSize; i++ {
				region := <-regionNotifier
				requests = append(requests, region.GetMeta())
				s.history.Record(region)
			}
			regions := &pdpb.SyncRegionResponse{
				Header:     &pdpb.ResponseHeader{ClusterId: s.server.ClusterID()},
				Regions:    requests,
				StartIndex: startIndex,
			}
			s.broadcast(regions)
		case <-ticker.C:
			alive := &pdpb.SyncRegionResponse{
				Header: &pdpb.ResponseHeader{ClusterId: s.server.ClusterID()},
				// use max uint64 to mark keepalive.
				StartIndex: math.MaxUint64,
			}
			s.broadcast(alive)
		}
		requests = requests[:0]
	}
}

// Sync fistlly try to sync the history records to client.
// then to sync the newly records.
func (s *RegionSyncer) Sync(stream pdpb.PD_SyncRegionsServer) error {
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		if request.GetHeader().GetClusterId() != s.server.ClusterID() {
			return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.server.ClusterID(), request.GetHeader().GetClusterId())
		}
		log.Infof("establish sync region stream with %s [%s]", request.GetMember().GetName(), request.GetMember().GetClientUrls()[0])

		err = s.syncHistoryRegion(request.GetStartIndex(), stream)
		if err != nil {
			return err
		}
		s.bindStream(request.GetMember().GetName(), stream)
	}
}

func (s *RegionSyncer) syncHistoryRegion(startIndex uint64, stream pdpb.PD_SyncRegionsServer) error {
	records := s.history.RecordsFrom(startIndex)
	if len(records) == 0 {
		log.Warnf("no history regions form index %d, the leader maybe restarted", startIndex)
		// TODO: Full synchronization
		// if startIndex == 0 {}
		return nil
	}
	log.Infof("syncer server sync the history regions from index:%d, got records length: %d", startIndex, len(records))
	regions := make([]*metapb.Region, len(records))
	for i, r := range records {
		regions[i] = r.GetMeta()
	}
	resp := &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: s.server.ClusterID()},
		Regions:    regions,
		StartIndex: startIndex,
	}
	return stream.Send(resp)
}

// BindStream binds the established server stream.
func (s *RegionSyncer) bindStream(name string, stream ServerStream) {
	s.Lock()
	defer s.Unlock()
	s.streams[name] = stream
}

func (s *RegionSyncer) broadcast(regions *pdpb.SyncRegionResponse) {
	var failed []string
	s.RLock()
	for name, sender := range s.streams {
		err := sender.Send(regions)
		if err != nil {
			log.Error("region syncer send data meet error:", err)
			failed = append(failed, name)
		}
	}
	s.RUnlock()
	if len(failed) > 0 {
		s.Lock()
		for _, name := range failed {
			delete(s.streams, name)
			log.Infof("region syncer delete the stream of %s", name)
		}
		s.Unlock()
	}
}
