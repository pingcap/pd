// Copyright 2016 PingCAP, Inc.
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
	"context"
	"fmt"
	"math/rand"
	"path"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/etcdutil"
	"github.com/pingcap/pd/pkg/logutil"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

// IsLeader returns whether the server is leader or not.
func (s *Server) IsLeader() bool {
	// If server is not started. Both leaderID and ID could be 0.
	return !s.isClosed() && s.GetLeaderID() == s.ID()
}

// GetLeaderID returns current leader's member ID.
func (s *Server) GetLeaderID() uint64 {
	return s.GetLeader().GetMemberId()
}

// GetLeader returns current leader of PD cluster.
func (s *Server) GetLeader() *pdpb.Member {
	leader := s.leader.Load()
	if leader == nil {
		return nil
	}
	member := leader.(*pdpb.Member)
	if member.GetMemberId() == 0 {
		return nil
	}
	return member
}

func (s *Server) enableLeader() {
	s.leader.Store(s.member)
}

func (s *Server) disableLeader() {
	s.leader.Store(&pdpb.Member{})
}

func (s *Server) getLeaderPath() string {
	return path.Join(s.rootPath, "leader")
}

func (s *Server) leaderLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	for {
		if s.isClosed() {
			log.Infof("server is closed, return leader loop")
			return
		}

		if s.GetEtcdLeader() == 0 {
			log.Error("no etcd leader, check leader later")
			time.Sleep(200 * time.Millisecond)
			continue
		}

		leader, rev, err := getLeader(s.client, s.getLeaderPath())
		if err != nil {
			log.Errorf("get leader err %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if leader != nil {
			if s.isSameLeader(leader) {
				// oh, we are already leader, we may meet something wrong
				// in previous campaignLeader. we can delete and campaign again.
				log.Warnf("leader is still %s, delete and campaign again", leader)
				if err = s.deleteLeaderKey(); err != nil {
					log.Errorf("delete leader key err %s", err)
					time.Sleep(200 * time.Millisecond)
					continue
				}
			} else {
				log.Infof("leader is %s, watch it", leader)
				s.watchLeader(leader, rev)
				log.Info("leader changed, try to campaign leader")
			}
		}

		etcdLeader := s.GetEtcdLeader()
		if etcdLeader != s.ID() {
			log.Infof("%v is not etcd leader, skip campaign leader and check later", s.Name())
			time.Sleep(200 * time.Millisecond)
			continue
		}

		if err = s.campaignLeader(); err != nil {
			log.Errorf("campaign leader err %s", fmt.Sprintf("%+v", err))
		}
	}
}

func (s *Server) etcdLeaderLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	for {
		select {
		case <-time.After(s.cfg.LeaderPriorityCheckInterval.Duration):
			etcdLeader := s.GetEtcdLeader()
			if etcdLeader == s.ID() || etcdLeader == 0 {
				break
			}
			myPriority, err := s.GetMemberLeaderPriority(s.ID())
			if err != nil {
				log.Errorf("failed to load leader priority: %v", err)
				break
			}
			leaderPriority, err := s.GetMemberLeaderPriority(etcdLeader)
			if err != nil {
				log.Errorf("failed to load leader priority: %v", err)
				break
			}
			if myPriority > leaderPriority {
				err := s.etcd.Server.MoveLeader(ctx, etcdLeader, s.ID())
				if err != nil {
					log.Errorf("failed to transfer etcd leader: %v", err)
				} else {
					log.Infof("etcd leader moved from %v to %v", etcdLeader, s.ID())
				}
			}
		case <-ctx.Done():
			log.Info("server is closed, exit etcd leader loop")
			return
		}
	}
}

// getLeader gets server leader from etcd.
func getLeader(c *clientv3.Client, leaderPath string) (*pdpb.Member, int64, error) {
	leader := &pdpb.Member{}
	ok, rev, err := getProtoMsgWithModRev(c, leaderPath, leader)
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		return nil, 0, nil
	}

	return leader, rev, nil
}

// GetEtcdLeader returns the etcd leader ID.
func (s *Server) GetEtcdLeader() uint64 {
	return s.etcd.Server.Lead()
}

func (s *Server) isSameLeader(leader *pdpb.Member) bool {
	return leader.GetMemberId() == s.ID()
}

func (s *Server) memberInfo() (member *pdpb.Member, marshalStr string) {
	leader := &pdpb.Member{
		Name:       s.Name(),
		MemberId:   s.ID(),
		ClientUrls: strings.Split(s.cfg.AdvertiseClientUrls, ","),
		PeerUrls:   strings.Split(s.cfg.AdvertisePeerUrls, ","),
	}

	data, err := leader.Marshal()
	if err != nil {
		// can't fail, so panic here.
		log.Fatalf("marshal leader %s err %v", leader, err)
	}

	return leader, string(data)
}

func (s *Server) campaignLeader() error {
	log.Debugf("begin to campaign leader %s", s.Name())

	lessor := clientv3.NewLease(s.client)
	defer lessor.Close()

	start := time.Now()
	ctx, cancel := context.WithTimeout(s.client.Ctx(), requestTimeout)
	leaseResp, err := lessor.Grant(ctx, s.cfg.LeaderLease)
	cancel()

	if cost := time.Since(start); cost > slowRequestTime {
		log.Warnf("lessor grants too slow, cost %s", cost)
	}

	if err != nil {
		return errors.WithStack(err)
	}

	leaderKey := s.getLeaderPath()
	// The leader key must not exist, so the CreateRevision is 0.
	resp, err := s.txn().
		If(clientv3.Compare(clientv3.CreateRevision(leaderKey), "=", 0)).
		Then(clientv3.OpPut(leaderKey, s.memberValue, clientv3.WithLease(leaseResp.ID))).
		Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !resp.Succeeded {
		return errors.New("campaign leader failed, other server may campaign ok")
	}

	// Make the leader keepalived.
	ctx, cancel = context.WithCancel(s.serverLoopCtx)
	defer cancel()

	ch, err := lessor.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("campaign leader ok %s", s.Name())

	err = s.reloadConfigFromKV()
	if err != nil {
		return err
	}
	// Try to create raft cluster.
	err = s.createRaftCluster()
	if err != nil {
		return err
	}
	defer s.stopRaftCluster()

	log.Debug("sync timestamp for tso")
	if err = s.syncTimestamp(); err != nil {
		return err
	}
	defer s.ts.Store(&atomicObject{
		physical: zeroTime,
	})

	s.enableLeader()
	defer s.disableLeader()

	log.Infof("cluster version is %s", s.scheduleOpt.loadClusterVersion())
	log.Infof("PD cluster leader %s is ready to serve", s.Name())
	CheckPDVersion(s.scheduleOpt)

	tsTicker := time.NewTicker(updateTimestampStep)
	defer tsTicker.Stop()

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				log.Info("keep alive channel is closed")
				return nil
			}
		case <-tsTicker.C:
			if err = s.updateTimestamp(); err != nil {
				return err
			}
			etcdLeader := s.GetEtcdLeader()
			if etcdLeader != s.ID() {
				log.Infof("etcd leader changed, %s resigns leadership", s.Name())
				return nil
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Info("server is closed")
			return nil
		}
	}
}

func (s *Server) watchLeader(leader *pdpb.Member, revision int64) {
	s.leader.Store(leader)
	defer s.leader.Store(&pdpb.Member{})

	watcher := clientv3.NewWatcher(s.client)
	defer watcher.Close()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	err := s.reloadConfigFromKV()
	if err != nil {
		log.Error("reload config failed:", err)
		return
	}
	if s.scheduleOpt.loadPDServerConfig().UseRegionStorage {
		s.cluster.regionSyncer.StartSyncWithLeader(leader.GetClientUrls()[0])
		defer s.cluster.regionSyncer.StopSyncWithLeader()
	}

	// The revision is the revision of last modification on this key.
	// If the revision is compacted, will meet required revision has been compacted error.
	// In this case, use the compact revision to re-watch the key.
	for {
		// gofail: var delayWatcher struct{}
		rch := watcher.Watch(ctx, s.getLeaderPath(), clientv3.WithRev(revision))
		for wresp := range rch {
			// meet compacted error, use the compact revision.
			if wresp.CompactRevision != 0 {
				log.Warnf("required revision %d has been compacted, use the compact revision %d", revision, wresp.CompactRevision)
				revision = wresp.CompactRevision
				break
			}
			if wresp.Canceled {
				log.Errorf("leader watcher is canceled with revision: %d, error: %s", revision, wresp.Err())
				return
			}

			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					log.Info("leader is deleted")
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			// server closed, return
			return
		default:
		}
	}
}

// ResignLeader resigns current PD's leadership. If nextLeader is empty, all
// other pd-servers can campaign.
func (s *Server) ResignLeader(nextLeader string) error {
	log.Infof("%s tries to resign leader with next leader directive: %v", s.Name(), nextLeader)
	// Determine next leaders.
	var leaderIDs []uint64
	res, err := etcdutil.ListEtcdMembers(s.client)
	if err != nil {
		return err
	}
	for _, member := range res.Members {
		if (nextLeader == "" && member.ID != s.id) || (nextLeader != "" && member.Name == nextLeader) {
			leaderIDs = append(leaderIDs, member.GetID())
		}
	}
	if len(leaderIDs) == 0 {
		return errors.New("no valid pd to transfer leader")
	}
	nextLeaderID := leaderIDs[rand.Intn(len(leaderIDs))]
	log.Infof("%s ready to resign leader, next leader: %v", s.Name(), nextLeaderID)
	err = s.etcd.Server.MoveLeader(s.serverLoopCtx, s.ID(), nextLeaderID)
	return errors.WithStack(err)
}

func (s *Server) deleteLeaderKey() error {
	// delete leader itself and let others start a new election again.
	leaderKey := s.getLeaderPath()
	resp, err := s.leaderTxn().Then(clientv3.OpDelete(leaderKey)).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !resp.Succeeded {
		return errors.New("resign leader failed, we are not leader already")
	}

	return nil
}

func (s *Server) leaderCmp() clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(s.getLeaderPath()), "=", s.memberValue)
}

func (s *Server) reloadConfigFromKV() error {
	err := s.scheduleOpt.reload(s.kv)
	if err != nil {
		return err
	}
	if s.scheduleOpt.loadPDServerConfig().UseRegionStorage {
		s.kv.SwitchToRegionStorage()
		log.Info("server enable region storage")
	} else {
		s.kv.SwitchToDefaultStorage()
		log.Info("server disable region storage")
	}
	return nil
}
