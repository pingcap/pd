// Copyright 2016 TiKV Project Authors.
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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/sysutil"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/grpcutil"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/id"
	"github.com/tikv/pd/server/kv"
	"github.com/tikv/pd/server/member"
	syncer "github.com/tikv/pd/server/region_syncer"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/tso"
	"github.com/tikv/pd/server/versioninfo"
	"github.com/urfave/negroni"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	etcdTimeout           = time.Second * 3
	serverMetricsInterval = time.Minute
	leaderTickInterval    = 50 * time.Millisecond
	// pdRootPath for all pd servers.
	pdRootPath      = "/pd"
	pdAPIPrefix     = "/pd/"
	pdClusterIDPath = "/pd/cluster_id"
)

var (
	// EnableZap enable the zap logger in embed etcd.
	EnableZap = false
	// EtcdStartTimeout the timeout of the startup etcd.
	EtcdStartTimeout = time.Minute * 5
)

// Server is the pd server.
type Server struct {
	diagnosticspb.DiagnosticsServer

	// Server state.
	isServing int64

	// Server start timestamp
	startTimestamp int64

	// Configs and initial fields.
	cfg            *config.Config
	etcdCfg        *embed.Config
	persistOptions *config.PersistOptions
	handler        *Handler

	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	// for PD leader election.
	member *member.Member
	// etcd client
	client *clientv3.Client
	// http client
	httpClient *http.Client
	clusterID  uint64 // pd cluster id.
	rootPath   string

	// Server services.
	// for id allocator, we can use one allocator for
	// store, region and peer, because we just need
	// a unique ID.
	idAllocator *id.AllocatorImpl
	// for storage operation.
	storage *core.Storage
	// for basicCluster operation.
	basicCluster *core.BasicCluster
	// for tso.
	tsoAllocatorManager *tso.AllocatorManager
	// for raft cluster
	cluster *cluster.RaftCluster
	// For async region heartbeat.
	hbStreams *heartbeatStreams
	// Zap logger
	lg       *zap.Logger
	logProps *log.ZapProperties

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()

	// serviceSafePointLock is a lock for UpdateServiceGCSafePoint
	serviceSafePointLock sync.Mutex
}

// HandlerBuilder builds a server HTTP handler.
type HandlerBuilder func(context.Context, *Server) (http.Handler, ServiceGroup, error)

// ServiceGroup used to register the service.
type ServiceGroup struct {
	Name       string
	Version    string
	IsCore     bool
	PathPrefix string
}

const (
	// CorePath the core group, is at REST path `/pd/api/v1`.
	CorePath = "/pd/api/v1"
	// ExtensionsPath the named groups are REST at `/pd/apis/{GROUP_NAME}/{Version}`.
	ExtensionsPath = "/pd/apis"
)

func combineBuilderServerHTTPService(ctx context.Context, svr *Server, serviceBuilders ...HandlerBuilder) (map[string]http.Handler, error) {
	userHandlers := make(map[string]http.Handler)
	registerMap := make(map[string]struct{})

	apiService := negroni.New()
	recovery := negroni.NewRecovery()
	apiService.Use(recovery)
	router := mux.NewRouter()

	for _, build := range serviceBuilders {
		handler, info, err := build(ctx, svr)
		if err != nil {
			return nil, err
		}
		if !info.IsCore && len(info.PathPrefix) == 0 && (len(info.Name) == 0 || len(info.Version) == 0) {
			return nil, errs.ErrAPIInformationInvalid.FastGenByArgs(info.Name, info.Version)
		}
		var pathPrefix string
		if len(info.PathPrefix) != 0 {
			pathPrefix = info.PathPrefix
		} else if info.IsCore {
			pathPrefix = CorePath
		} else {
			pathPrefix = path.Join(ExtensionsPath, info.Name, info.Version)
		}
		if _, ok := registerMap[pathPrefix]; ok {
			return nil, errs.ErrServiceRegistered.FastGenByArgs(pathPrefix)
		}

		log.Info("register REST path", zap.String("path", pathPrefix))
		registerMap[pathPrefix] = struct{}{}
		if len(info.PathPrefix) != 0 {
			// If PathPrefix is specified, register directly into userHandlers
			userHandlers[pathPrefix] = handler
		} else {
			// If PathPrefix is not specified, register into apiService,
			// and finally apiService is registered in userHandlers.
			router.PathPrefix(pathPrefix).Handler(handler)
			if info.IsCore {
				// Deprecated
				router.Path("/pd/health").Handler(handler)
				// Deprecated
				router.Path("/pd/diagnose").Handler(handler)
				// Deprecated
				router.Path("/pd/ping").Handler(handler)
			}
		}
	}
	apiService.UseHandler(router)
	userHandlers[pdAPIPrefix] = apiService

	return userHandlers, nil
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(ctx context.Context, cfg *config.Config, serviceBuilders ...HandlerBuilder) (*Server, error) {
	log.Info("PD Config", zap.Reflect("config", cfg))
	rand.Seed(time.Now().UnixNano())

	s := &Server{
		cfg:               cfg,
		persistOptions:    config.NewPersistOptions(cfg),
		member:            &member.Member{},
		ctx:               ctx,
		startTimestamp:    time.Now().Unix(),
		DiagnosticsServer: sysutil.NewDiagnosticsServer(cfg.Log.File.Filename),
	}

	s.handler = newHandler(s)

	// Adjust etcd config.
	etcdCfg, err := s.cfg.GenEmbedEtcdConfig()
	if err != nil {
		return nil, err
	}
	if len(serviceBuilders) != 0 {
		userHandlers, err := combineBuilderServerHTTPService(ctx, s, serviceBuilders...)
		if err != nil {
			return nil, err
		}
		etcdCfg.UserHandlers = userHandlers
	}
	etcdCfg.ServiceRegister = func(gs *grpc.Server) {
		pdpb.RegisterPDServer(gs, s)
		diagnosticspb.RegisterDiagnosticsServer(gs, s)
	}
	s.etcdCfg = etcdCfg
	if EnableZap {
		// The etcd master version has removed embed.Config.SetupLogging.
		// Now logger is set up automatically based on embed.Config.Logger,
		// Use zap logger in the test, otherwise will panic.
		// Reference: https://go.etcd.io/etcd/blob/master/embed/config_logging.go#L45
		s.etcdCfg.Logger = "zap"
		s.etcdCfg.LogOutputs = []string{"stdout"}
	}
	s.lg = cfg.GetZapLogger()
	s.logProps = cfg.GetZapLogProperties()
	return s, nil
}

func (s *Server) startEtcd(ctx context.Context) error {
	newCtx, cancel := context.WithTimeout(ctx, EtcdStartTimeout)
	defer cancel()

	etcd, err := embed.StartEtcd(s.etcdCfg)
	if err != nil {
		return errs.ErrStartEtcd.Wrap(err).GenWithStackByCause()
	}

	// Check cluster ID
	urlMap, err := types.NewURLsMap(s.cfg.InitialCluster)
	if err != nil {
		return errs.ErrEtcdURLMap.Wrap(err).GenWithStackByCause()
	}
	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}

	if err = etcdutil.CheckClusterID(etcd.Server.Cluster().ID(), urlMap, tlsConfig); err != nil {
		return err
	}

	select {
	// Wait etcd until it is ready to use
	case <-etcd.Server.ReadyNotify():
	case <-newCtx.Done():
		return errs.ErrCancelStartEtcd.FastGenByArgs()
	}

	endpoints := []string{s.etcdCfg.ACUrls[0].String()}
	log.Info("create etcd v3 client", zap.Strings("endpoints", endpoints), zap.Reflect("cert", s.cfg.Security))

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
		TLS:         tlsConfig,
	})
	if err != nil {
		return errs.ErrNewEtcdClient.Wrap(err).GenWithStackByCause()
	}

	etcdServerID := uint64(etcd.Server.ID())

	// update advertise peer urls.
	etcdMembers, err := etcdutil.ListEtcdMembers(client)
	if err != nil {
		return err
	}
	for _, m := range etcdMembers.Members {
		if etcdServerID == m.ID {
			etcdPeerURLs := strings.Join(m.PeerURLs, ",")
			if s.cfg.AdvertisePeerUrls != etcdPeerURLs {
				log.Info("update advertise peer urls", zap.String("from", s.cfg.AdvertisePeerUrls), zap.String("to", etcdPeerURLs))
				s.cfg.AdvertisePeerUrls = etcdPeerURLs
			}
		}
	}
	s.client = client
	s.httpClient = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
			TLSClientConfig:   tlsConfig,
		},
	}

	failpoint.Inject("memberNil", func() {
		time.Sleep(1500 * time.Millisecond)
	})
	s.member = member.NewMember(etcd, client, etcdServerID)
	return nil
}

// AddStartCallback adds a callback in the startServer phase.
func (s *Server) AddStartCallback(callbacks ...func()) {
	s.startCallbacks = append(s.startCallbacks, callbacks...)
}

func (s *Server) startServer(ctx context.Context) error {
	var err error
	if err = s.initClusterID(); err != nil {
		return err
	}
	log.Info("init cluster id", zap.Uint64("cluster-id", s.clusterID))
	// It may lose accuracy if use float64 to store uint64. So we store the
	// cluster id in label.
	metadataGauge.WithLabelValues(fmt.Sprintf("cluster%d", s.clusterID)).Set(0)

	s.rootPath = path.Join(pdRootPath, strconv.FormatUint(s.clusterID, 10))
	s.member.MemberInfo(s.cfg, s.Name(), s.rootPath)
	s.member.SetMemberDeployPath(s.member.ID())
	s.member.SetMemberBinaryVersion(s.member.ID(), versioninfo.PDReleaseVersion)
	s.member.SetMemberGitHash(s.member.ID(), versioninfo.PDGitHash)
	s.idAllocator = id.NewAllocatorImpl(s.client, s.rootPath, s.member.MemberValue())
	s.tsoAllocatorManager = tso.NewAllocatorManager(
		s.member.Etcd(), s.client, s.rootPath, s.cfg.TsoSaveInterval.Duration,
		func() time.Duration { return s.persistOptions.GetMaxResetTSGap() },
	)
	kvBase := kv.NewEtcdKVBase(s.client, s.rootPath)
	path := filepath.Join(s.cfg.DataDir, "region-meta")
	regionStorage, err := core.NewRegionStorage(ctx, path)
	if err != nil {
		return err
	}
	s.storage = core.NewStorage(kvBase).SetRegionStorage(regionStorage)
	s.basicCluster = core.NewBasicCluster()
	s.cluster = cluster.NewRaftCluster(ctx, s.GetClusterRootPath(), s.clusterID, syncer.NewRegionSyncer(s), s.client, s.httpClient)
	s.hbStreams = newHeartbeatStreams(ctx, s.clusterID, s.cluster)

	// Run callbacks
	for _, cb := range s.startCallbacks {
		cb()
	}

	// Server has started.
	atomic.StoreInt64(&s.isServing, 1)
	return nil
}

func (s *Server) initClusterID() error {
	// Get any cluster key to parse the cluster ID.
	resp, err := etcdutil.EtcdKVGet(s.client, pdClusterIDPath)
	if err != nil {
		return err
	}

	// If no key exist, generate a random cluster ID.
	if len(resp.Kvs) == 0 {
		s.clusterID, err = initOrGetClusterID(s.client, pdClusterIDPath)
		return err
	}
	s.clusterID, err = typeutil.BytesToUint64(resp.Kvs[0].Value)
	return err
}

// AddCloseCallback adds a callback in the Close phase.
func (s *Server) AddCloseCallback(callbacks ...func()) {
	s.closeCallbacks = append(s.closeCallbacks, callbacks...)
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.isServing, 1, 0) {
		// server is already closed
		return
	}

	log.Info("closing server")

	s.stopServerLoop()

	if s.client != nil {
		if err := s.client.Close(); err != nil {
			log.Error("close etcd client meet error", errs.ZapError(errs.ErrCloseEtcdClient, err))
		}
	}

	if s.httpClient != nil {
		s.httpClient.CloseIdleConnections()
	}

	if s.member.Etcd() != nil {
		s.member.Close()
	}

	if s.hbStreams != nil {
		s.hbStreams.Close()
	}
	if err := s.storage.Close(); err != nil {
		log.Error("close storage meet error", errs.ZapError(err))
	}

	// Run callbacks
	for _, cb := range s.closeCallbacks {
		cb()
	}

	log.Info("close server")
}

// IsClosed checks whether server is closed or not.
func (s *Server) IsClosed() bool {
	return atomic.LoadInt64(&s.isServing) == 0
}

// Run runs the pd server.
func (s *Server) Run() error {
	go StartMonitor(s.ctx, time.Now, func() {
		log.Error("system time jumps backward", errs.ZapError(errs.ErrIncorrectSystemTime))
		timeJumpBackCounter.Inc()
	})
	if err := s.startEtcd(s.ctx); err != nil {
		return err
	}

	if err := s.startServer(s.ctx); err != nil {
		return err
	}

	s.startServerLoop(s.ctx)

	return nil
}

// Context returns the context of server.
func (s *Server) Context() context.Context {
	return s.ctx
}

// LoopContext returns the loop context of server.
func (s *Server) LoopContext() context.Context {
	return s.serverLoopCtx
}

func (s *Server) startServerLoop(ctx context.Context) {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(ctx)
	s.serverLoopWg.Add(4)
	go s.leaderLoop()
	go s.etcdLeaderLoop()
	go s.serverMetricsLoop()
	go s.tsoAllocatorLoop()
}

func (s *Server) stopServerLoop() {
	s.serverLoopCancel()
	s.serverLoopWg.Wait()
}

func (s *Server) serverMetricsLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	for {
		select {
		case <-time.After(serverMetricsInterval):
			s.collectEtcdStateMetrics()
		case <-ctx.Done():
			log.Info("server is closed, exit metrics loop")
			return
		}
	}
}

// tsoAllocatorLoop is used to run the TSO Allocator updating daemon.
func (s *Server) tsoAllocatorLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	s.tsoAllocatorManager.AllocatorDaemon(ctx)
	log.Info("server is closed, exit allocator loop")
}

func (s *Server) collectEtcdStateMetrics() {
	etcdStateGauge.WithLabelValues("term").Set(float64(s.member.Etcd().Server.Term()))
	etcdStateGauge.WithLabelValues("appliedIndex").Set(float64(s.member.Etcd().Server.AppliedIndex()))
	etcdStateGauge.WithLabelValues("committedIndex").Set(float64(s.member.Etcd().Server.CommittedIndex()))
}

func (s *Server) bootstrapCluster(req *pdpb.BootstrapRequest) (*pdpb.BootstrapResponse, error) {
	clusterID := s.clusterID

	log.Info("try to bootstrap raft cluster",
		zap.Uint64("cluster-id", clusterID),
		zap.String("request", fmt.Sprintf("%v", req)))

	if err := checkBootstrapRequest(clusterID, req); err != nil {
		return nil, err
	}

	clusterMeta := metapb.Cluster{
		Id:           clusterID,
		MaxPeerCount: uint32(s.persistOptions.GetMaxReplicas()),
	}

	// Set cluster meta
	clusterValue, err := clusterMeta.Marshal()
	if err != nil {
		return nil, errs.ErrBootstrapClusterMarshal.Wrap(err).GenWithStackByCause()
	}
	clusterRootPath := s.GetClusterRootPath()

	var ops []clientv3.Op
	ops = append(ops, clientv3.OpPut(clusterRootPath, string(clusterValue)))

	// Set bootstrap time
	bootstrapKey := makeBootstrapTimeKey(clusterRootPath)
	nano := time.Now().UnixNano()

	timeData := typeutil.Uint64ToBytes(uint64(nano))
	ops = append(ops, clientv3.OpPut(bootstrapKey, string(timeData)))

	// Set store meta
	storeMeta := req.GetStore()
	storePath := makeStoreKey(clusterRootPath, storeMeta.GetId())
	storeValue, err := storeMeta.Marshal()
	if err != nil {
		return nil, errs.ErrBootstrapClusterMarshal.Wrap(err).GenWithStackByCause()
	}
	ops = append(ops, clientv3.OpPut(storePath, string(storeValue)))

	regionValue, err := req.GetRegion().Marshal()
	if err != nil {
		return nil, errs.ErrBootstrapClusterMarshal.Wrap(err).GenWithStackByCause()
	}

	// Set region meta with region id.
	regionPath := makeRegionKey(clusterRootPath, req.GetRegion().GetId())
	ops = append(ops, clientv3.OpPut(regionPath, string(regionValue)))

	// TODO: we must figure out a better way to handle bootstrap failed, maybe intervene manually.
	bootstrapCmp := clientv3.Compare(clientv3.CreateRevision(clusterRootPath), "=", 0)
	resp, err := kv.NewSlowLogTxn(s.client).If(bootstrapCmp).Then(ops...).Commit()
	if err != nil {
		return nil, errs.ErrEtcdTxn.Wrap(err).GenWithStackByCause()
	}
	if !resp.Succeeded {
		log.Warn("cluster already bootstrapped", zap.Uint64("cluster-id", clusterID))
		return nil, errs.ErrEtcdTxn.FastGenByArgs()
	}

	log.Info("bootstrap cluster ok", zap.Uint64("cluster-id", clusterID))
	err = s.storage.SaveRegion(req.GetRegion())
	if err != nil {
		log.Warn("save the bootstrap region failed", errs.ZapError(err))
	}
	err = s.storage.Flush()
	if err != nil {
		log.Warn("flush the bootstrap region failed", errs.ZapError(err))
	}

	if err := s.cluster.Start(s); err != nil {
		return nil, err
	}

	return &pdpb.BootstrapResponse{
		ReplicationStatus: s.cluster.GetReplicationMode().GetReplicationStatus(),
	}, nil
}

func (s *Server) createRaftCluster() error {
	if s.cluster.IsRunning() {
		return nil
	}

	return s.cluster.Start(s)
}

func (s *Server) stopRaftCluster() {
	s.cluster.Stop()
}

// GetAddr returns the server urls for clients.
func (s *Server) GetAddr() string {
	return s.cfg.AdvertiseClientUrls
}

// GetClientScheme returns the client URL scheme
func (s *Server) GetClientScheme() string {
	if len(s.cfg.Security.CertPath) == 0 && len(s.cfg.Security.KeyPath) == 0 {
		return "http"
	}
	return "https"
}

// GetMemberInfo returns the server member information.
func (s *Server) GetMemberInfo() *pdpb.Member {
	return proto.Clone(s.member.Member()).(*pdpb.Member)
}

// GetHandler returns the handler for API.
func (s *Server) GetHandler() *Handler {
	return s.handler
}

// GetEndpoints returns the etcd endpoints for outer use.
func (s *Server) GetEndpoints() []string {
	return s.client.Endpoints()
}

// GetClient returns builtin etcd client.
func (s *Server) GetClient() *clientv3.Client {
	return s.client
}

// GetHTTPClient returns builtin etcd client.
func (s *Server) GetHTTPClient() *http.Client {
	return s.httpClient
}

// GetLeader returns the leader of PD cluster(i.e the PD leader).
func (s *Server) GetLeader() *pdpb.Member {
	return s.member.GetLeader()
}

// GetMember returns the member of server.
func (s *Server) GetMember() *member.Member {
	return s.member
}

// GetStorage returns the backend storage of server.
func (s *Server) GetStorage() *core.Storage {
	return s.storage
}

// SetStorage changes the storage only for test purpose.
// When we use it, we should prevent calling GetStorage, otherwise, it may cause a data race problem.
func (s *Server) SetStorage(storage *core.Storage) {
	s.storage = storage
}

// GetBasicCluster returns the basic cluster of server.
func (s *Server) GetBasicCluster() *core.BasicCluster {
	return s.basicCluster
}

// GetPersistOptions returns the schedule option.
func (s *Server) GetPersistOptions() *config.PersistOptions {
	return s.persistOptions
}

// GetHBStreams returns the heartbeat streams.
func (s *Server) GetHBStreams() opt.HeartbeatStreams {
	return s.hbStreams
}

// GetAllocator returns the ID allocator of server.
func (s *Server) GetAllocator() *id.AllocatorImpl {
	return s.idAllocator
}

// Name returns the unique etcd Name for this server in etcd cluster.
func (s *Server) Name() string {
	return s.cfg.Name
}

// ClusterID returns the cluster ID of this server.
func (s *Server) ClusterID() uint64 {
	return s.clusterID
}

// StartTimestamp returns the start timestamp of this server
func (s *Server) StartTimestamp() int64 {
	return s.startTimestamp
}

// GetConfig gets the config information.
func (s *Server) GetConfig() *config.Config {
	cfg := s.cfg.Clone()
	cfg.Schedule = *s.persistOptions.GetScheduleConfig()
	cfg.Replication = *s.persistOptions.GetReplicationConfig()
	cfg.PDServerCfg = *s.persistOptions.GetPDServerConfig()
	cfg.ReplicationMode = *s.persistOptions.GetReplicationModeConfig()
	cfg.LabelProperty = s.persistOptions.GetLabelPropertyConfig().Clone()
	cfg.ClusterVersion = *s.persistOptions.GetClusterVersion()
	storage := s.GetStorage()
	if storage == nil {
		return cfg
	}
	sches, configs, err := storage.LoadAllScheduleConfig()
	if err != nil {
		return cfg
	}
	payload := make(map[string]interface{})
	for i, sche := range sches {
		var config interface{}
		err := schedule.DecodeConfig([]byte(configs[i]), &config)
		if err != nil {
			log.Error("failed to decode scheduler config",
				zap.String("config", configs[i]),
				zap.String("scheduler", sche),
				errs.ZapError(errs.ErrDecodeSchedulerConfig, err))
			continue
		}
		payload[sche] = config
	}
	cfg.Schedule.SchedulersPayload = payload
	return cfg
}

// GetScheduleConfig gets the balance config information.
func (s *Server) GetScheduleConfig() *config.ScheduleConfig {
	cfg := &config.ScheduleConfig{}
	*cfg = *s.persistOptions.GetScheduleConfig()
	return cfg
}

// SetScheduleConfig sets the balance config information.
func (s *Server) SetScheduleConfig(cfg config.ScheduleConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := cfg.Deprecated(); err != nil {
		return err
	}
	old := s.persistOptions.GetScheduleConfig()
	cfg.SchedulersPayload = nil
	s.persistOptions.SetScheduleConfig(&cfg)
	if err := s.persistOptions.Persist(s.storage); err != nil {
		s.persistOptions.SetScheduleConfig(old)
		log.Error("failed to update schedule config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", old),
			errs.ZapError(err))
		return err
	}
	log.Info("schedule config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// GetReplicationConfig get the replication config.
func (s *Server) GetReplicationConfig() *config.ReplicationConfig {
	cfg := &config.ReplicationConfig{}
	*cfg = *s.persistOptions.GetReplicationConfig()
	return cfg
}

// SetReplicationConfig sets the replication config.
func (s *Server) SetReplicationConfig(cfg config.ReplicationConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	old := s.persistOptions.GetReplicationConfig()
	if cfg.EnablePlacementRules != old.EnablePlacementRules {
		raftCluster := s.GetRaftCluster()
		if raftCluster == nil {
			return errs.ErrNotBootstrapped.GenWithStackByArgs()
		}
		if cfg.EnablePlacementRules {
			// initialize rule manager.
			if err := raftCluster.GetRuleManager().Initialize(int(cfg.MaxReplicas), cfg.LocationLabels); err != nil {
				return err
			}
		} else {
			// NOTE: can be removed after placement rules feature is enabled by default.
			for _, s := range raftCluster.GetStores() {
				if !s.IsTombstone() && core.IsTiFlashStore(s.GetMeta()) {
					return errors.New("cannot disable placement rules with TiFlash nodes")
				}
			}
		}
	}

	s.persistOptions.SetReplicationConfig(&cfg)
	if err := s.persistOptions.Persist(s.storage); err != nil {
		s.persistOptions.SetReplicationConfig(old)
		log.Error("failed to update replication config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", old),
			errs.ZapError(err))
		return err
	}
	log.Info("replication config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// GetPDServerConfig gets the balance config information.
func (s *Server) GetPDServerConfig() *config.PDServerConfig {
	cfg := &config.PDServerConfig{}
	*cfg = *s.persistOptions.GetPDServerConfig()
	return cfg
}

// SetPDServerConfig sets the server config.
func (s *Server) SetPDServerConfig(cfg config.PDServerConfig) error {
	switch cfg.DashboardAddress {
	case "auto":
	case "none":
	default:
		if !strings.HasPrefix(cfg.DashboardAddress, "http") {
			cfg.DashboardAddress = fmt.Sprintf("%s://%s", s.GetClientScheme(), cfg.DashboardAddress)
		}
		if !cluster.IsClientURL(cfg.DashboardAddress, s.client) {
			return errors.Errorf("%s is not the client url of any member", cfg.DashboardAddress)
		}
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	old := s.persistOptions.GetPDServerConfig()
	s.persistOptions.SetPDServerConfig(&cfg)
	if err := s.persistOptions.Persist(s.storage); err != nil {
		s.persistOptions.SetPDServerConfig(old)
		log.Error("failed to update PDServer config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", old),
			errs.ZapError(err))
		return err
	}
	log.Info("PD server config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// SetLabelPropertyConfig sets the label property config.
func (s *Server) SetLabelPropertyConfig(cfg config.LabelPropertyConfig) error {
	old := s.persistOptions.GetLabelPropertyConfig()
	s.persistOptions.SetLabelPropertyConfig(cfg)
	if err := s.persistOptions.Persist(s.storage); err != nil {
		s.persistOptions.SetLabelPropertyConfig(old)
		log.Error("failed to update label property config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", &old),
			errs.ZapError(err))
		return err
	}
	log.Info("label property config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// SetLabelProperty inserts a label property config.
func (s *Server) SetLabelProperty(typ, labelKey, labelValue string) error {
	s.persistOptions.SetLabelProperty(typ, labelKey, labelValue)
	err := s.persistOptions.Persist(s.storage)
	if err != nil {
		s.persistOptions.DeleteLabelProperty(typ, labelKey, labelValue)
		log.Error("failed to update label property config",
			zap.String("typ", typ),
			zap.String("labelKey", labelKey),
			zap.String("labelValue", labelValue),
			zap.Reflect("config", s.persistOptions.GetLabelPropertyConfig()),
			errs.ZapError(err))
		return err
	}

	log.Info("label property config is updated", zap.Reflect("config", s.persistOptions.GetLabelPropertyConfig()))
	return nil
}

// DeleteLabelProperty deletes a label property config.
func (s *Server) DeleteLabelProperty(typ, labelKey, labelValue string) error {
	s.persistOptions.DeleteLabelProperty(typ, labelKey, labelValue)
	err := s.persistOptions.Persist(s.storage)
	if err != nil {
		s.persistOptions.SetLabelProperty(typ, labelKey, labelValue)
		log.Error("failed to delete label property config",
			zap.String("typ", typ),
			zap.String("labelKey", labelKey),
			zap.String("labelValue", labelValue),
			zap.Reflect("config", s.persistOptions.GetLabelPropertyConfig()),
			errs.ZapError(err))
		return err
	}

	log.Info("label property config is deleted", zap.Reflect("config", s.persistOptions.GetLabelPropertyConfig()))
	return nil
}

// GetLabelProperty returns the whole label property config.
func (s *Server) GetLabelProperty() config.LabelPropertyConfig {
	return s.persistOptions.GetLabelPropertyConfig().Clone()
}

// SetClusterVersion sets the version of cluster.
func (s *Server) SetClusterVersion(v string) error {
	version, err := versioninfo.ParseVersion(v)
	if err != nil {
		return err
	}
	old := s.persistOptions.GetClusterVersion()
	s.persistOptions.SetClusterVersion(version)
	err = s.persistOptions.Persist(s.storage)
	if err != nil {
		s.persistOptions.SetClusterVersion(old)
		log.Error("failed to update cluster version",
			zap.String("old-version", old.String()),
			zap.String("new-version", v),
			errs.ZapError(err))
		return err
	}
	log.Info("cluster version is updated", zap.String("new-version", v))
	return nil
}

// GetClusterVersion returns the version of cluster.
func (s *Server) GetClusterVersion() semver.Version {
	return *s.persistOptions.GetClusterVersion()
}

// GetSecurityConfig get the security config.
func (s *Server) GetSecurityConfig() *grpcutil.SecurityConfig {
	return &s.cfg.Security
}

// GetClusterRootPath returns the cluster root path.
func (s *Server) GetClusterRootPath() string {
	return path.Join(s.rootPath, "raft")
}

// GetRaftCluster gets Raft cluster.
// If cluster has not been bootstrapped, return nil.
func (s *Server) GetRaftCluster() *cluster.RaftCluster {
	if s.IsClosed() || !s.cluster.IsRunning() {
		return nil
	}
	return s.cluster
}

// GetCluster gets cluster.
func (s *Server) GetCluster() *metapb.Cluster {
	return &metapb.Cluster{
		Id:           s.clusterID,
		MaxPeerCount: uint32(s.persistOptions.GetMaxReplicas()),
	}
}

// GetServerOption gets the option of the server.
func (s *Server) GetServerOption() *config.PersistOptions {
	return s.persistOptions
}

// GetMetaRegions gets meta regions from cluster.
func (s *Server) GetMetaRegions() []*metapb.Region {
	cluster := s.GetRaftCluster()
	if cluster != nil {
		return cluster.GetMetaRegions()
	}
	return nil
}

// GetRegions gets regions from cluster.
func (s *Server) GetRegions() []*core.RegionInfo {
	cluster := s.GetRaftCluster()
	if cluster != nil {
		return cluster.GetRegions()
	}
	return nil
}

// GetClusterStatus gets cluster status.
func (s *Server) GetClusterStatus() (*cluster.Status, error) {
	s.cluster.Lock()
	defer s.cluster.Unlock()
	return s.cluster.LoadClusterStatus()
}

// SetLogLevel sets log level.
func (s *Server) SetLogLevel(level string) error {
	if !isLevelLegal(level) {
		return errors.Errorf("log level %s is illegal", level)
	}
	s.cfg.Log.Level = level
	log.SetLevel(logutil.StringToZapLogLevel(level))
	log.Warn("log level changed", zap.String("level", log.GetLevel().String()))
	return nil
}

func isLevelLegal(level string) bool {
	switch strings.ToLower(level) {
	case "fatal", "error", "warn", "warning", "debug", "info":
		return true
	default:
		return false
	}
}

// GetReplicationModeConfig returns the replication mode config.
func (s *Server) GetReplicationModeConfig() *config.ReplicationModeConfig {
	return s.persistOptions.GetReplicationModeConfig()
}

// SetReplicationModeConfig sets the replication mode.
func (s *Server) SetReplicationModeConfig(cfg config.ReplicationModeConfig) error {
	if config.NormalizeReplicationMode(cfg.ReplicationMode) == "" {
		return errors.Errorf("invalid replication mode: %v", cfg.ReplicationMode)
	}

	old := s.persistOptions.GetReplicationModeConfig()
	s.persistOptions.SetReplicationModeConfig(&cfg)
	if err := s.persistOptions.Persist(s.storage); err != nil {
		s.persistOptions.SetReplicationModeConfig(old)
		log.Error("failed to update replication mode config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", &old),
			errs.ZapError(err))
		return err
	}
	log.Info("replication mode config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))

	cluster := s.GetRaftCluster()
	if cluster != nil {
		err := cluster.GetReplicationMode().UpdateConfig(cfg)
		if err != nil {
			log.Warn("failed to update replication mode", errs.ZapError(err))
			// revert to old config
			// NOTE: since we can't put the 2 storage mutations in a batch, it
			// is possible that memory and persistent data become different
			// (when below revert fail). They will become the same after PD is
			// restart or PD leader is changed.
			s.persistOptions.SetReplicationModeConfig(old)
			revertErr := s.persistOptions.Persist(s.storage)
			if revertErr != nil {
				log.Error("failed to revert replication mode persistent config", errs.ZapError(revertErr))
			}
		}
		return err
	}

	return nil
}

func (s *Server) leaderLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	for {
		if s.IsClosed() {
			log.Info("server is closed, return pd leader loop")
			return
		}

		leader, rev, checkAgain := s.member.CheckLeader()
		if checkAgain {
			continue
		}
		if leader != nil {
			err := s.reloadConfigFromKV()
			if err != nil {
				log.Error("reload config failed", errs.ZapError(err))
				continue
			}
			syncer := s.cluster.GetRegionSyncer()
			if s.persistOptions.IsUseRegionStorage() {
				syncer.StartSyncWithLeader(leader.GetClientUrls()[0])
			}
			log.Info("start to watch pd leader", zap.Stringer("pd-leader", leader))
			// WatchLeader will keep looping and never return unless the PD leader has changed.
			s.member.WatchLeader(s.serverLoopCtx, leader, rev)
			syncer.StopSyncWithLeader()
			log.Info("pd leader has changed, try to re-campaign a pd leader")
		}

		// To make sure the etcd leader and PD leader are on the same server.
		etcdLeader := s.member.GetEtcdLeader()
		if etcdLeader != s.member.ID() {
			log.Info("skip campaigning of pd leader and check later",
				zap.String("server-name", s.Name()),
				zap.Uint64("etcd-leader-id", etcdLeader),
				zap.Uint64("member-id", s.member.ID()))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		s.campaignLeader()
	}
}

func (s *Server) campaignLeader() {
	log.Info("start to campaign pd leader", zap.String("campaign-pd-leader-name", s.Name()))
	if err := s.member.CampaignLeader(s.cfg.LeaderLease); err != nil {
		log.Error("campaign pd leader meet error", errs.ZapError(err))
		return
	}

	// Start keepalive the leadership and enable TSO service.
	// TSO service is strictly enabled/disabled by PD leader lease for 2 reasons:
	//   1. lease based approach is not affected by thread pause, slow runtime schedule, etc.
	//   2. load region could be slow. Based on lease we can recover TSO service faster.

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	defer s.member.ResetLeader()
	// maintain the PD leader
	go s.member.KeepLeader(ctx)
	log.Info("campaign pd leader ok", zap.String("campaign-pd-leader-name", s.Name()))

	log.Info("setting up the global TSO allocator")
	if err := s.tsoAllocatorManager.SetUpAllocator(ctx, cancel, tso.GlobalDCLocation, s.member.GetLeadership()); err != nil {
		log.Error("failed to set up the global TSO allocator", errs.ZapError(err))
		return
	}

	if err := s.reloadConfigFromKV(); err != nil {
		log.Error("failed to reload configuration", errs.ZapError(err))
		return
	}
	// Try to create raft cluster.
	if err := s.createRaftCluster(); err != nil {
		log.Error("failed to create raft cluster", errs.ZapError(err))
		return
	}
	defer s.stopRaftCluster()

	s.member.EnableLeader()

	CheckPDVersion(s.persistOptions)
	log.Info("PD cluster leader is ready to serve", zap.String("pd-leader-name", s.Name()))

	leaderTicker := time.NewTicker(leaderTickInterval)
	defer leaderTicker.Stop()

	for {
		select {
		case <-leaderTicker.C:
			if !s.member.IsStillLeader() {
				log.Info("no longer a leader because lease has expired, pd leader will step down")
				return
			}
			etcdLeader := s.member.GetEtcdLeader()
			if etcdLeader != s.member.ID() {
				log.Info("etcd leader changed, resigns pd leadership", zap.String("old-pd-leader-name", s.Name()))
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Info("server is closed")
			return
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
			s.member.CheckPriority(ctx)
		case <-ctx.Done():
			log.Info("server is closed, exit etcd leader loop")
			return
		}
	}
}

func (s *Server) reloadConfigFromKV() error {
	err := s.persistOptions.Reload(s.storage)
	if err != nil {
		return err
	}
	if s.persistOptions.IsUseRegionStorage() {
		s.storage.SwitchToRegionStorage()
		log.Info("server enable region storage")
	} else {
		s.storage.SwitchToDefaultStorage()
		log.Info("server disable region storage")
	}
	return nil
}

// ReplicateFileToAllMembers is used to synchronize state among all members.
// Each member will write `data` to a local file named `name`.
func (s *Server) ReplicateFileToAllMembers(ctx context.Context, name string, data []byte) error {
	resp, err := s.GetMembers(ctx, nil)
	if err != nil {
		return err
	}
	for _, member := range resp.Members {
		clientUrls := member.GetClientUrls()
		if len(clientUrls) == 0 {
			log.Warn("failed to replicate file", zap.String("name", name), zap.String("member", member.GetName()), errs.ZapError(err))
			return errs.ErrClientURLEmpty.FastGenByArgs()
		}
		url := clientUrls[0] + filepath.Join("/pd/api/v1/admin/persist-file", name)
		req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
		req.Header.Set("PD-Allow-follower-handle", "true")
		res, err := s.httpClient.Do(req)
		if err != nil {
			log.Warn("failed to replicate file", zap.String("name", name), zap.String("member", member.GetName()), errs.ZapError(err))
			return errs.ErrHTTPPostFile.Wrap(err).GenWithStackByCause()
		}
		if res.StatusCode != http.StatusOK {
			log.Warn("failed to replicate file", zap.String("name", name), zap.String("member", member.GetName()), zap.Int("status-code", res.StatusCode))
			return errs.ErrHTTPPostFile.FastGenByArgs()
		}
	}
	return nil
}

// PersistFile saves a file in DataDir.
func (s *Server) PersistFile(name string, data []byte) error {
	return ioutil.WriteFile(filepath.Join(s.GetConfig().DataDir, name), data, 0644)
}
