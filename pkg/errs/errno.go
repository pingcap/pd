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

package errs

import "github.com/pingcap/errors"

// The internal error which is generated in PD project.
// tso errors
var (
	ErrSetAllocator        = errors.Normalize("set allocator failed, %s", errors.RFCCodeText("PD:tso:ErrSetAllocator"))
	ErrGetAllocator        = errors.Normalize("get allocator failed, %s", errors.RFCCodeText("PD:tso:ErrGetAllocator"))
	ErrInvalidTimestamp    = errors.Normalize("invalid timestamp", errors.RFCCodeText("PD:tso:ErrInvalidTimestamp"))
	ErrLogicOverflow       = errors.Normalize("logic part overflow", errors.RFCCodeText("PD:tso:ErrLogicOverflow"))
	ErrIncorrectSystemTime = errors.Normalize("incorrect system time", errors.RFCCodeText("PD:tso:ErrIncorrectSystemTime"))
)

// member errors
var (
	ErrEtcdLeaderNotFound = errors.Normalize("etcd leader not found", errors.RFCCodeText("PD:member:ErrEtcdLeaderNotFound"))
	ErrMarshalLeader      = errors.Normalize("marshal leader failed", errors.RFCCodeText("PD:member:ErrMarshalLeader"))
)

// client errors
var (
	ErrClientCreateTSOStream = errors.Normalize("create TSO stream failed", errors.RFCCodeText("PD:client:ErrClientCreateTSOStream"))
	ErrClientGetTSOTimeout   = errors.Normalize("get TSO timeout", errors.RFCCodeText("PD:client:ErrClientGetTSOTimeout"))
	ErrClientGetTSO          = errors.Normalize("get TSO failed", errors.RFCCodeText("PD:client:ErrClientGetTSO"))
	ErrClientGetLeader       = errors.Normalize("get leader from %v error", errors.RFCCodeText("PD:client:ErrClientGetLeader"))
	ErrClientGetMember       = errors.Normalize("get member failed", errors.RFCCodeText("PD:client:ErrClientGetMember"))
)

// scheduler errors
var (
	ErrGetSourceStore         = errors.Normalize("failed to get the source store", errors.RFCCodeText("PD:scheduler:ErrGetSourceStore"))
	ErrSchedulerExisted       = errors.Normalize("scheduler existed", errors.RFCCodeText("PD:scheduler:ErrSchedulerExisted"))
	ErrSchedulerNotFound      = errors.Normalize("scheduler not found", errors.RFCCodeText("PD:scheduler:ErrSchedulerNotFound"))
	ErrScheduleConfigNotExist = errors.Normalize("the config does not exist", errors.RFCCodeText("PD:scheduler:ErrScheduleConfigNotExist"))
	ErrSchedulerConfig        = errors.Normalize("wrong scheduler config %s", errors.RFCCodeText("PD:scheduler:ErrSchedulerConfig"))
	ErrCacheOverflow          = errors.Normalize("cache overflow", errors.RFCCodeText("PD:scheduler:ErrCacheOverflow"))
	ErrInternalGrowth         = errors.Normalize("unknown interval growth type error", errors.RFCCodeText("PD:scheduler:ErrInternalGrowth"))
)

// placement errors
var (
	ErrRuleContent   = errors.Normalize("invalid rule content, %s", errors.RFCCodeText("PD:placement:ErrRuleContent"))
	ErrLoadRule      = errors.Normalize("load rule failed", errors.RFCCodeText("PD:placement:ErrLoadRule"))
	ErrLoadRuleGroup = errors.Normalize("load rule group failed", errors.RFCCodeText("PD:placement:ErrLoadRuleGroup"))
	ErrBuildRuleList = errors.Normalize("build rule list failed, %s", errors.RFCCodeText("PD:placement:ErrBuildRuleList"))
)

// cluster errors
var (
	ErrPersistStore          = errors.Normalize("failed to persist store", errors.RFCCodeText("PD:cluster:ErrPersistStore"))
	ErrDeleteRegion          = errors.Normalize("failed to delete region from storage", errors.RFCCodeText("PD:cluster:ErrDeleteRegion"))
	ErrSaveRegion            = errors.Normalize("failed to save region from storage", errors.RFCCodeText("PD:cluster:ErrSaveRegion"))
	ErrBuryStore             = errors.Normalize("failed to bury store", errors.RFCCodeText("PD:cluster:ErrBuryStore"))
	ErrDeleteStore           = errors.Normalize("failed to delete store", errors.RFCCodeText("PD:cluster:ErrDeleteStore"))
	ErrPersistClusterVersion = errors.Normalize("persist cluster version meet error", errors.RFCCodeText("PD:cluster:ErrPersistClusterVersion"))
	ErrGetMembers            = errors.Normalize("get members failed", errors.RFCCodeText("PD:cluster:ErrGetMembers"))
)

// grpcutil errors
var (
	ErrSecurityConfig = errors.Normalize("security config error: %s", errors.RFCCodeText("PD:grpcutil:ErrSecurityConfig"))
)

// The third-party project error.
// url errors
var (
	ErrURLParse      = errors.Normalize("parse url error", errors.RFCCodeText("PD:url:ErrURLParse"))
	ErrQueryUnescape = errors.Normalize("inverse transformation of QueryEscape error", errors.RFCCodeText("PD:url:ErrQueryUnescape"))
)

// grpc errors
var (
	ErrGRPCDial      = errors.Normalize("dial error", errors.RFCCodeText("PD:grpc:ErrGRPCDial"))
	ErrCloseGRPCConn = errors.Normalize("close gRPC connection failed", errors.RFCCodeText("PD:grpc:ErrCloseGRPCConn"))
)

// proto errors
var (
	ErrProtoUnmarshal = errors.Normalize("failed to unmarshal proto", errors.RFCCodeText("PD:proto:ErrProtoUnmarshal"))
)

// etcd errors
var (
	ErrEtcdTxn                   = errors.Normalize("etcd Txn failed", errors.RFCCodeText("PD:etcd:ErrEtcdTxn"))
	ErrEtcdKVPut                 = errors.Normalize("etcd KV put failed", errors.RFCCodeText("PD:etcd:ErrEtcdKVPut"))
	ErrEtcdKVDelete              = errors.Normalize("etcd KV delete failed", errors.RFCCodeText("PD:etcd:ErrEtcdKVDelete"))
	ErrEtcdKVGet                 = errors.Normalize("etcd KV get failed", errors.RFCCodeText("PD:etcd:ErrEtcdKVGet"))
	ErrEtcdKVGetResponse         = errors.Normalize("etcd invalid get value response %v, must only one", errors.RFCCodeText("PD:etcd:ErrEtcdKVGetResponse"))
	ErrEtcdGetCluster            = errors.Normalize("etcd get cluster from remote peer failed", errors.RFCCodeText("PD:etcd:ErrEtcdGetCluster"))
	ErrEtcdMoveLeader            = errors.Normalize("etcd move leader error", errors.RFCCodeText("PD:etcd:ErrEtcdMoveLeader"))
	ErrEtcdTLSConfig             = errors.Normalize("etcd TLS config error", errors.RFCCodeText("PD:etcd:ErrEtcdTLSConfig"))
	ErrEtcdGetProtoMsgWithModRev = errors.Normalize("etcd get proto message with mod rev error", errors.RFCCodeText("PD:etcd:ErrEtcdGetProtoMsgWithModRev"))
	ErrEtcdWatcherCancel         = errors.Normalize("watcher canceled", errors.RFCCodeText("PD:etcd:ErrEtcdWatcherCancel"))
	ErrCloseEtcdClient           = errors.Normalize("close etcd client failed", errors.RFCCodeText("PD:etcd:ErrCloseEtcdClient"))
)

// dashboard errors
var (
	ErrDashboardStart = errors.Normalize("start dashboard failed", errors.RFCCodeText("PD:dashboard:ErrDashboardStart"))
	ErrDashboardStop  = errors.Normalize("stop dashboard failed", errors.RFCCodeText("PD:dashboard:ErrDashboardStop"))
)

// strconv errors
var (
	ErrStrconvParseInt  = errors.Normalize("parse int error", errors.RFCCodeText("PD:strconv:ErrStrconvParseInt"))
	ErrStrconvParseUint = errors.Normalize("parse uint error", errors.RFCCodeText("PD:strconv:ErrStrconvParseUint"))
)

// prometheus errors
var (
	ErrPrometheusPushMetrics = errors.Normalize("push metrics to gateway failed", errors.RFCCodeText("PD:prometheus:ErrPrometheusPushMetrics"))
)

// http errors
var (
	ErrSendRequest    = errors.Normalize("send HTTP request failed", errors.RFCCodeText("PD:http:ErrSendRequest"))
	ErrWriteHTTPBody  = errors.Normalize("write HTTP body failed", errors.RFCCodeText("PD:http:ErrWriteHTTPBody"))
	ErrNewHTTPRequest = errors.Normalize("new HTTP request failed", errors.RFCCodeText("PD:http:ErrNewHTTPRequest"))
)

// ioutil error
var (
	ErrIORead = errors.Normalize("IO read error", errors.RFCCodeText("PD:ioutil:ErrIORead"))
)

// netstat error
var (
	ErrNetstatTCPSocks = errors.Normalize("TCP socks error", errors.RFCCodeText("PD:netstat:ErrNetstatTCPSocks"))
)

// hex error
var (
	ErrHexDecodingString = errors.Normalize("decode string %s error", errors.RFCCodeText("PD:hex:ErrHexDecodingString"))
)
