// Code generated by protoc-gen-go.
// source: kvrpcpb.proto
// DO NOT EDIT!

/*
Package kvrpcpb is a generated protocol buffer package.

It is generated from these files:
	kvrpcpb.proto

It has these top-level messages:
	LockInfo
	KeyError
	Context
	CmdGetRequest
	CmdGetResponse
	CmdScanRequest
	KvPair
	CmdScanResponse
	Mutation
	CmdPrewriteRequest
	CmdPrewriteResponse
	CmdCommitRequest
	CmdCommitResponse
	CmdBatchRollbackRequest
	CmdBatchRollbackResponse
	CmdCleanupRequest
	CmdCleanupResponse
	CmdRollbackThenGetRequest
	CmdRollbackThenGetResponse
	CmdCommitThenGetRequest
	CmdCommitThenGetResponse
	CmdBatchGetRequest
	CmdBatchGetResponse
	Request
	Response
*/
package kvrpcpb

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import metapb "github.com/pingcap/kvproto/pkg/metapb"
import errorpb "github.com/pingcap/kvproto/pkg/errorpb"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type MessageType int32

const (
	MessageType_CmdGet      MessageType = 1
	MessageType_CmdScan     MessageType = 2
	MessageType_CmdPrewrite MessageType = 3
	MessageType_CmdCommit   MessageType = 4
	MessageType_CmdCleanup  MessageType = 5
	// Below types both use for Get failed. If Get failed, it may be locked.
	// So it tries to clean primary lock(CmdCleanup), and then server will return
	// either committed or rolled back. Finally, client will commit/rollback
	// primary lock and then Get again.
	MessageType_CmdRollbackThenGet MessageType = 6
	MessageType_CmdCommitThenGet   MessageType = 7
	MessageType_CmdBatchGet        MessageType = 8
	MessageType_CmdBatchRollback   MessageType = 9
)

var MessageType_name = map[int32]string{
	1: "CmdGet",
	2: "CmdScan",
	3: "CmdPrewrite",
	4: "CmdCommit",
	5: "CmdCleanup",
	6: "CmdRollbackThenGet",
	7: "CmdCommitThenGet",
	8: "CmdBatchGet",
	9: "CmdBatchRollback",
}
var MessageType_value = map[string]int32{
	"CmdGet":             1,
	"CmdScan":            2,
	"CmdPrewrite":        3,
	"CmdCommit":          4,
	"CmdCleanup":         5,
	"CmdRollbackThenGet": 6,
	"CmdCommitThenGet":   7,
	"CmdBatchGet":        8,
	"CmdBatchRollback":   9,
}

func (x MessageType) Enum() *MessageType {
	p := new(MessageType)
	*p = x
	return p
}
func (x MessageType) String() string {
	return proto.EnumName(MessageType_name, int32(x))
}
func (x *MessageType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(MessageType_value, data, "MessageType")
	if err != nil {
		return err
	}
	*x = MessageType(value)
	return nil
}
func (MessageType) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

type Op int32

const (
	Op_Put  Op = 1
	Op_Del  Op = 2
	Op_Lock Op = 3
)

var Op_name = map[int32]string{
	1: "Put",
	2: "Del",
	3: "Lock",
}
var Op_value = map[string]int32{
	"Put":  1,
	"Del":  2,
	"Lock": 3,
}

func (x Op) Enum() *Op {
	p := new(Op)
	*p = x
	return p
}
func (x Op) String() string {
	return proto.EnumName(Op_name, int32(x))
}
func (x *Op) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(Op_value, data, "Op")
	if err != nil {
		return err
	}
	*x = Op(value)
	return nil
}
func (Op) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

type LockInfo struct {
	PrimaryLock      []byte  `protobuf:"bytes,1,opt,name=primary_lock" json:"primary_lock,omitempty"`
	LockVersion      *uint64 `protobuf:"varint,2,opt,name=lock_version" json:"lock_version,omitempty"`
	Key              []byte  `protobuf:"bytes,3,opt,name=key" json:"key,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *LockInfo) Reset()                    { *m = LockInfo{} }
func (m *LockInfo) String() string            { return proto.CompactTextString(m) }
func (*LockInfo) ProtoMessage()               {}
func (*LockInfo) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *LockInfo) GetPrimaryLock() []byte {
	if m != nil {
		return m.PrimaryLock
	}
	return nil
}

func (m *LockInfo) GetLockVersion() uint64 {
	if m != nil && m.LockVersion != nil {
		return *m.LockVersion
	}
	return 0
}

func (m *LockInfo) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

type KeyError struct {
	Locked           *LockInfo `protobuf:"bytes,1,opt,name=locked" json:"locked,omitempty"`
	Retryable        *string   `protobuf:"bytes,2,opt,name=retryable" json:"retryable,omitempty"`
	Abort            *string   `protobuf:"bytes,3,opt,name=abort" json:"abort,omitempty"`
	XXX_unrecognized []byte    `json:"-"`
}

func (m *KeyError) Reset()                    { *m = KeyError{} }
func (m *KeyError) String() string            { return proto.CompactTextString(m) }
func (*KeyError) ProtoMessage()               {}
func (*KeyError) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *KeyError) GetLocked() *LockInfo {
	if m != nil {
		return m.Locked
	}
	return nil
}

func (m *KeyError) GetRetryable() string {
	if m != nil && m.Retryable != nil {
		return *m.Retryable
	}
	return ""
}

func (m *KeyError) GetAbort() string {
	if m != nil && m.Abort != nil {
		return *m.Abort
	}
	return ""
}

type Context struct {
	RegionId         *uint64             `protobuf:"varint,1,opt,name=region_id" json:"region_id,omitempty"`
	RegionEpoch      *metapb.RegionEpoch `protobuf:"bytes,2,opt,name=region_epoch" json:"region_epoch,omitempty"`
	Peer             *metapb.Peer        `protobuf:"bytes,3,opt,name=peer" json:"peer,omitempty"`
	XXX_unrecognized []byte              `json:"-"`
}

func (m *Context) Reset()                    { *m = Context{} }
func (m *Context) String() string            { return proto.CompactTextString(m) }
func (*Context) ProtoMessage()               {}
func (*Context) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

func (m *Context) GetRegionId() uint64 {
	if m != nil && m.RegionId != nil {
		return *m.RegionId
	}
	return 0
}

func (m *Context) GetRegionEpoch() *metapb.RegionEpoch {
	if m != nil {
		return m.RegionEpoch
	}
	return nil
}

func (m *Context) GetPeer() *metapb.Peer {
	if m != nil {
		return m.Peer
	}
	return nil
}

type CmdGetRequest struct {
	Key              []byte  `protobuf:"bytes,1,opt,name=key" json:"key,omitempty"`
	Version          *uint64 `protobuf:"varint,2,opt,name=version" json:"version,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *CmdGetRequest) Reset()                    { *m = CmdGetRequest{} }
func (m *CmdGetRequest) String() string            { return proto.CompactTextString(m) }
func (*CmdGetRequest) ProtoMessage()               {}
func (*CmdGetRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{3} }

func (m *CmdGetRequest) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *CmdGetRequest) GetVersion() uint64 {
	if m != nil && m.Version != nil {
		return *m.Version
	}
	return 0
}

type CmdGetResponse struct {
	Error            *KeyError `protobuf:"bytes,1,opt,name=error" json:"error,omitempty"`
	Value            []byte    `protobuf:"bytes,2,opt,name=value" json:"value,omitempty"`
	XXX_unrecognized []byte    `json:"-"`
}

func (m *CmdGetResponse) Reset()                    { *m = CmdGetResponse{} }
func (m *CmdGetResponse) String() string            { return proto.CompactTextString(m) }
func (*CmdGetResponse) ProtoMessage()               {}
func (*CmdGetResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{4} }

func (m *CmdGetResponse) GetError() *KeyError {
	if m != nil {
		return m.Error
	}
	return nil
}

func (m *CmdGetResponse) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

type CmdScanRequest struct {
	StartKey         []byte  `protobuf:"bytes,1,opt,name=start_key" json:"start_key,omitempty"`
	Limit            *uint32 `protobuf:"varint,2,opt,name=limit" json:"limit,omitempty"`
	Version          *uint64 `protobuf:"varint,3,opt,name=version" json:"version,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *CmdScanRequest) Reset()                    { *m = CmdScanRequest{} }
func (m *CmdScanRequest) String() string            { return proto.CompactTextString(m) }
func (*CmdScanRequest) ProtoMessage()               {}
func (*CmdScanRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{5} }

func (m *CmdScanRequest) GetStartKey() []byte {
	if m != nil {
		return m.StartKey
	}
	return nil
}

func (m *CmdScanRequest) GetLimit() uint32 {
	if m != nil && m.Limit != nil {
		return *m.Limit
	}
	return 0
}

func (m *CmdScanRequest) GetVersion() uint64 {
	if m != nil && m.Version != nil {
		return *m.Version
	}
	return 0
}

type KvPair struct {
	Error            *KeyError `protobuf:"bytes,1,opt,name=error" json:"error,omitempty"`
	Key              []byte    `protobuf:"bytes,2,opt,name=key" json:"key,omitempty"`
	Value            []byte    `protobuf:"bytes,3,opt,name=value" json:"value,omitempty"`
	XXX_unrecognized []byte    `json:"-"`
}

func (m *KvPair) Reset()                    { *m = KvPair{} }
func (m *KvPair) String() string            { return proto.CompactTextString(m) }
func (*KvPair) ProtoMessage()               {}
func (*KvPair) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{6} }

func (m *KvPair) GetError() *KeyError {
	if m != nil {
		return m.Error
	}
	return nil
}

func (m *KvPair) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *KvPair) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

type CmdScanResponse struct {
	Pairs            []*KvPair `protobuf:"bytes,1,rep,name=pairs" json:"pairs,omitempty"`
	XXX_unrecognized []byte    `json:"-"`
}

func (m *CmdScanResponse) Reset()                    { *m = CmdScanResponse{} }
func (m *CmdScanResponse) String() string            { return proto.CompactTextString(m) }
func (*CmdScanResponse) ProtoMessage()               {}
func (*CmdScanResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{7} }

func (m *CmdScanResponse) GetPairs() []*KvPair {
	if m != nil {
		return m.Pairs
	}
	return nil
}

type Mutation struct {
	Op               *Op    `protobuf:"varint,1,opt,name=op,enum=kvrpcpb.Op" json:"op,omitempty"`
	Key              []byte `protobuf:"bytes,2,opt,name=key" json:"key,omitempty"`
	Value            []byte `protobuf:"bytes,3,opt,name=value" json:"value,omitempty"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *Mutation) Reset()                    { *m = Mutation{} }
func (m *Mutation) String() string            { return proto.CompactTextString(m) }
func (*Mutation) ProtoMessage()               {}
func (*Mutation) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{8} }

func (m *Mutation) GetOp() Op {
	if m != nil && m.Op != nil {
		return *m.Op
	}
	return Op_Put
}

func (m *Mutation) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *Mutation) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

type CmdPrewriteRequest struct {
	Mutations []*Mutation `protobuf:"bytes,1,rep,name=mutations" json:"mutations,omitempty"`
	// primary_lock_key
	PrimaryLock      []byte  `protobuf:"bytes,2,opt,name=primary_lock" json:"primary_lock,omitempty"`
	StartVersion     *uint64 `protobuf:"varint,3,opt,name=start_version" json:"start_version,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *CmdPrewriteRequest) Reset()                    { *m = CmdPrewriteRequest{} }
func (m *CmdPrewriteRequest) String() string            { return proto.CompactTextString(m) }
func (*CmdPrewriteRequest) ProtoMessage()               {}
func (*CmdPrewriteRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{9} }

func (m *CmdPrewriteRequest) GetMutations() []*Mutation {
	if m != nil {
		return m.Mutations
	}
	return nil
}

func (m *CmdPrewriteRequest) GetPrimaryLock() []byte {
	if m != nil {
		return m.PrimaryLock
	}
	return nil
}

func (m *CmdPrewriteRequest) GetStartVersion() uint64 {
	if m != nil && m.StartVersion != nil {
		return *m.StartVersion
	}
	return 0
}

type CmdPrewriteResponse struct {
	Errors           []*KeyError `protobuf:"bytes,1,rep,name=errors" json:"errors,omitempty"`
	XXX_unrecognized []byte      `json:"-"`
}

func (m *CmdPrewriteResponse) Reset()                    { *m = CmdPrewriteResponse{} }
func (m *CmdPrewriteResponse) String() string            { return proto.CompactTextString(m) }
func (*CmdPrewriteResponse) ProtoMessage()               {}
func (*CmdPrewriteResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{10} }

func (m *CmdPrewriteResponse) GetErrors() []*KeyError {
	if m != nil {
		return m.Errors
	}
	return nil
}

type CmdCommitRequest struct {
	StartVersion     *uint64  `protobuf:"varint,1,opt,name=start_version" json:"start_version,omitempty"`
	Keys             [][]byte `protobuf:"bytes,2,rep,name=keys" json:"keys,omitempty"`
	CommitVersion    *uint64  `protobuf:"varint,3,opt,name=commit_version" json:"commit_version,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *CmdCommitRequest) Reset()                    { *m = CmdCommitRequest{} }
func (m *CmdCommitRequest) String() string            { return proto.CompactTextString(m) }
func (*CmdCommitRequest) ProtoMessage()               {}
func (*CmdCommitRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{11} }

func (m *CmdCommitRequest) GetStartVersion() uint64 {
	if m != nil && m.StartVersion != nil {
		return *m.StartVersion
	}
	return 0
}

func (m *CmdCommitRequest) GetKeys() [][]byte {
	if m != nil {
		return m.Keys
	}
	return nil
}

func (m *CmdCommitRequest) GetCommitVersion() uint64 {
	if m != nil && m.CommitVersion != nil {
		return *m.CommitVersion
	}
	return 0
}

type CmdCommitResponse struct {
	Error            *KeyError `protobuf:"bytes,1,opt,name=error" json:"error,omitempty"`
	XXX_unrecognized []byte    `json:"-"`
}

func (m *CmdCommitResponse) Reset()                    { *m = CmdCommitResponse{} }
func (m *CmdCommitResponse) String() string            { return proto.CompactTextString(m) }
func (*CmdCommitResponse) ProtoMessage()               {}
func (*CmdCommitResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{12} }

func (m *CmdCommitResponse) GetError() *KeyError {
	if m != nil {
		return m.Error
	}
	return nil
}

type CmdBatchRollbackRequest struct {
	StartVersion     *uint64  `protobuf:"varint,1,opt,name=start_version" json:"start_version,omitempty"`
	Keys             [][]byte `protobuf:"bytes,2,rep,name=keys" json:"keys,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *CmdBatchRollbackRequest) Reset()                    { *m = CmdBatchRollbackRequest{} }
func (m *CmdBatchRollbackRequest) String() string            { return proto.CompactTextString(m) }
func (*CmdBatchRollbackRequest) ProtoMessage()               {}
func (*CmdBatchRollbackRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{13} }

func (m *CmdBatchRollbackRequest) GetStartVersion() uint64 {
	if m != nil && m.StartVersion != nil {
		return *m.StartVersion
	}
	return 0
}

func (m *CmdBatchRollbackRequest) GetKeys() [][]byte {
	if m != nil {
		return m.Keys
	}
	return nil
}

type CmdBatchRollbackResponse struct {
	Error            *KeyError `protobuf:"bytes,1,opt,name=error" json:"error,omitempty"`
	XXX_unrecognized []byte    `json:"-"`
}

func (m *CmdBatchRollbackResponse) Reset()                    { *m = CmdBatchRollbackResponse{} }
func (m *CmdBatchRollbackResponse) String() string            { return proto.CompactTextString(m) }
func (*CmdBatchRollbackResponse) ProtoMessage()               {}
func (*CmdBatchRollbackResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{14} }

func (m *CmdBatchRollbackResponse) GetError() *KeyError {
	if m != nil {
		return m.Error
	}
	return nil
}

type CmdCleanupRequest struct {
	Key              []byte  `protobuf:"bytes,1,opt,name=key" json:"key,omitempty"`
	StartVersion     *uint64 `protobuf:"varint,2,opt,name=start_version" json:"start_version,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *CmdCleanupRequest) Reset()                    { *m = CmdCleanupRequest{} }
func (m *CmdCleanupRequest) String() string            { return proto.CompactTextString(m) }
func (*CmdCleanupRequest) ProtoMessage()               {}
func (*CmdCleanupRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{15} }

func (m *CmdCleanupRequest) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *CmdCleanupRequest) GetStartVersion() uint64 {
	if m != nil && m.StartVersion != nil {
		return *m.StartVersion
	}
	return 0
}

type CmdCleanupResponse struct {
	Error            *KeyError `protobuf:"bytes,1,opt,name=error" json:"error,omitempty"`
	CommitVersion    *uint64   `protobuf:"varint,2,opt,name=commit_version" json:"commit_version,omitempty"`
	XXX_unrecognized []byte    `json:"-"`
}

func (m *CmdCleanupResponse) Reset()                    { *m = CmdCleanupResponse{} }
func (m *CmdCleanupResponse) String() string            { return proto.CompactTextString(m) }
func (*CmdCleanupResponse) ProtoMessage()               {}
func (*CmdCleanupResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{16} }

func (m *CmdCleanupResponse) GetError() *KeyError {
	if m != nil {
		return m.Error
	}
	return nil
}

func (m *CmdCleanupResponse) GetCommitVersion() uint64 {
	if m != nil && m.CommitVersion != nil {
		return *m.CommitVersion
	}
	return 0
}

type CmdRollbackThenGetRequest struct {
	Key              []byte  `protobuf:"bytes,1,opt,name=key" json:"key,omitempty"`
	LockVersion      *uint64 `protobuf:"varint,2,opt,name=lock_version" json:"lock_version,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *CmdRollbackThenGetRequest) Reset()                    { *m = CmdRollbackThenGetRequest{} }
func (m *CmdRollbackThenGetRequest) String() string            { return proto.CompactTextString(m) }
func (*CmdRollbackThenGetRequest) ProtoMessage()               {}
func (*CmdRollbackThenGetRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{17} }

func (m *CmdRollbackThenGetRequest) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *CmdRollbackThenGetRequest) GetLockVersion() uint64 {
	if m != nil && m.LockVersion != nil {
		return *m.LockVersion
	}
	return 0
}

type CmdRollbackThenGetResponse struct {
	Error            *KeyError `protobuf:"bytes,1,opt,name=error" json:"error,omitempty"`
	Value            []byte    `protobuf:"bytes,2,opt,name=value" json:"value,omitempty"`
	XXX_unrecognized []byte    `json:"-"`
}

func (m *CmdRollbackThenGetResponse) Reset()                    { *m = CmdRollbackThenGetResponse{} }
func (m *CmdRollbackThenGetResponse) String() string            { return proto.CompactTextString(m) }
func (*CmdRollbackThenGetResponse) ProtoMessage()               {}
func (*CmdRollbackThenGetResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{18} }

func (m *CmdRollbackThenGetResponse) GetError() *KeyError {
	if m != nil {
		return m.Error
	}
	return nil
}

func (m *CmdRollbackThenGetResponse) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

type CmdCommitThenGetRequest struct {
	Key              []byte  `protobuf:"bytes,1,opt,name=key" json:"key,omitempty"`
	LockVersion      *uint64 `protobuf:"varint,2,opt,name=lock_version" json:"lock_version,omitempty"`
	CommitVersion    *uint64 `protobuf:"varint,3,opt,name=commit_version" json:"commit_version,omitempty"`
	GetVersion       *uint64 `protobuf:"varint,4,opt,name=get_version" json:"get_version,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *CmdCommitThenGetRequest) Reset()                    { *m = CmdCommitThenGetRequest{} }
func (m *CmdCommitThenGetRequest) String() string            { return proto.CompactTextString(m) }
func (*CmdCommitThenGetRequest) ProtoMessage()               {}
func (*CmdCommitThenGetRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{19} }

func (m *CmdCommitThenGetRequest) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *CmdCommitThenGetRequest) GetLockVersion() uint64 {
	if m != nil && m.LockVersion != nil {
		return *m.LockVersion
	}
	return 0
}

func (m *CmdCommitThenGetRequest) GetCommitVersion() uint64 {
	if m != nil && m.CommitVersion != nil {
		return *m.CommitVersion
	}
	return 0
}

func (m *CmdCommitThenGetRequest) GetGetVersion() uint64 {
	if m != nil && m.GetVersion != nil {
		return *m.GetVersion
	}
	return 0
}

type CmdCommitThenGetResponse struct {
	Error            *KeyError `protobuf:"bytes,1,opt,name=error" json:"error,omitempty"`
	Value            []byte    `protobuf:"bytes,2,opt,name=value" json:"value,omitempty"`
	XXX_unrecognized []byte    `json:"-"`
}

func (m *CmdCommitThenGetResponse) Reset()                    { *m = CmdCommitThenGetResponse{} }
func (m *CmdCommitThenGetResponse) String() string            { return proto.CompactTextString(m) }
func (*CmdCommitThenGetResponse) ProtoMessage()               {}
func (*CmdCommitThenGetResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{20} }

func (m *CmdCommitThenGetResponse) GetError() *KeyError {
	if m != nil {
		return m.Error
	}
	return nil
}

func (m *CmdCommitThenGetResponse) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

type CmdBatchGetRequest struct {
	Keys             [][]byte `protobuf:"bytes,1,rep,name=keys" json:"keys,omitempty"`
	Version          *uint64  `protobuf:"varint,2,opt,name=version" json:"version,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *CmdBatchGetRequest) Reset()                    { *m = CmdBatchGetRequest{} }
func (m *CmdBatchGetRequest) String() string            { return proto.CompactTextString(m) }
func (*CmdBatchGetRequest) ProtoMessage()               {}
func (*CmdBatchGetRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{21} }

func (m *CmdBatchGetRequest) GetKeys() [][]byte {
	if m != nil {
		return m.Keys
	}
	return nil
}

func (m *CmdBatchGetRequest) GetVersion() uint64 {
	if m != nil && m.Version != nil {
		return *m.Version
	}
	return 0
}

type CmdBatchGetResponse struct {
	Pairs            []*KvPair `protobuf:"bytes,1,rep,name=pairs" json:"pairs,omitempty"`
	XXX_unrecognized []byte    `json:"-"`
}

func (m *CmdBatchGetResponse) Reset()                    { *m = CmdBatchGetResponse{} }
func (m *CmdBatchGetResponse) String() string            { return proto.CompactTextString(m) }
func (*CmdBatchGetResponse) ProtoMessage()               {}
func (*CmdBatchGetResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{22} }

func (m *CmdBatchGetResponse) GetPairs() []*KvPair {
	if m != nil {
		return m.Pairs
	}
	return nil
}

type Request struct {
	Type                *MessageType               `protobuf:"varint,1,opt,name=type,enum=kvrpcpb.MessageType" json:"type,omitempty"`
	Context             *Context                   `protobuf:"bytes,2,opt,name=context" json:"context,omitempty"`
	CmdGetReq           *CmdGetRequest             `protobuf:"bytes,3,opt,name=cmd_get_req" json:"cmd_get_req,omitempty"`
	CmdScanReq          *CmdScanRequest            `protobuf:"bytes,4,opt,name=cmd_scan_req" json:"cmd_scan_req,omitempty"`
	CmdPrewriteReq      *CmdPrewriteRequest        `protobuf:"bytes,5,opt,name=cmd_prewrite_req" json:"cmd_prewrite_req,omitempty"`
	CmdCommitReq        *CmdCommitRequest          `protobuf:"bytes,6,opt,name=cmd_commit_req" json:"cmd_commit_req,omitempty"`
	CmdCleanupReq       *CmdCleanupRequest         `protobuf:"bytes,7,opt,name=cmd_cleanup_req" json:"cmd_cleanup_req,omitempty"`
	CmdRbGetReq         *CmdRollbackThenGetRequest `protobuf:"bytes,8,opt,name=cmd_rb_get_req" json:"cmd_rb_get_req,omitempty"`
	CmdCommitGetReq     *CmdCommitThenGetRequest   `protobuf:"bytes,9,opt,name=cmd_commit_get_req" json:"cmd_commit_get_req,omitempty"`
	CmdBatchGetReq      *CmdBatchGetRequest        `protobuf:"bytes,10,opt,name=cmd_batch_get_req" json:"cmd_batch_get_req,omitempty"`
	CmdBatchRollbackReq *CmdBatchRollbackRequest   `protobuf:"bytes,11,opt,name=cmd_batch_rollback_req" json:"cmd_batch_rollback_req,omitempty"`
	XXX_unrecognized    []byte                     `json:"-"`
}

func (m *Request) Reset()                    { *m = Request{} }
func (m *Request) String() string            { return proto.CompactTextString(m) }
func (*Request) ProtoMessage()               {}
func (*Request) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{23} }

func (m *Request) GetType() MessageType {
	if m != nil && m.Type != nil {
		return *m.Type
	}
	return MessageType_CmdGet
}

func (m *Request) GetContext() *Context {
	if m != nil {
		return m.Context
	}
	return nil
}

func (m *Request) GetCmdGetReq() *CmdGetRequest {
	if m != nil {
		return m.CmdGetReq
	}
	return nil
}

func (m *Request) GetCmdScanReq() *CmdScanRequest {
	if m != nil {
		return m.CmdScanReq
	}
	return nil
}

func (m *Request) GetCmdPrewriteReq() *CmdPrewriteRequest {
	if m != nil {
		return m.CmdPrewriteReq
	}
	return nil
}

func (m *Request) GetCmdCommitReq() *CmdCommitRequest {
	if m != nil {
		return m.CmdCommitReq
	}
	return nil
}

func (m *Request) GetCmdCleanupReq() *CmdCleanupRequest {
	if m != nil {
		return m.CmdCleanupReq
	}
	return nil
}

func (m *Request) GetCmdRbGetReq() *CmdRollbackThenGetRequest {
	if m != nil {
		return m.CmdRbGetReq
	}
	return nil
}

func (m *Request) GetCmdCommitGetReq() *CmdCommitThenGetRequest {
	if m != nil {
		return m.CmdCommitGetReq
	}
	return nil
}

func (m *Request) GetCmdBatchGetReq() *CmdBatchGetRequest {
	if m != nil {
		return m.CmdBatchGetReq
	}
	return nil
}

func (m *Request) GetCmdBatchRollbackReq() *CmdBatchRollbackRequest {
	if m != nil {
		return m.CmdBatchRollbackReq
	}
	return nil
}

type Response struct {
	Type                 *MessageType                `protobuf:"varint,1,opt,name=type,enum=kvrpcpb.MessageType" json:"type,omitempty"`
	RegionError          *errorpb.Error              `protobuf:"bytes,2,opt,name=region_error" json:"region_error,omitempty"`
	CmdGetResp           *CmdGetResponse             `protobuf:"bytes,3,opt,name=cmd_get_resp" json:"cmd_get_resp,omitempty"`
	CmdScanResp          *CmdScanResponse            `protobuf:"bytes,4,opt,name=cmd_scan_resp" json:"cmd_scan_resp,omitempty"`
	CmdPrewriteResp      *CmdPrewriteResponse        `protobuf:"bytes,5,opt,name=cmd_prewrite_resp" json:"cmd_prewrite_resp,omitempty"`
	CmdCommitResp        *CmdCommitResponse          `protobuf:"bytes,6,opt,name=cmd_commit_resp" json:"cmd_commit_resp,omitempty"`
	CmdCleanupResp       *CmdCleanupResponse         `protobuf:"bytes,7,opt,name=cmd_cleanup_resp" json:"cmd_cleanup_resp,omitempty"`
	CmdRbGetResp         *CmdRollbackThenGetResponse `protobuf:"bytes,8,opt,name=cmd_rb_get_resp" json:"cmd_rb_get_resp,omitempty"`
	CmdCommitGetResp     *CmdCommitThenGetResponse   `protobuf:"bytes,9,opt,name=cmd_commit_get_resp" json:"cmd_commit_get_resp,omitempty"`
	CmdBatchGetResp      *CmdBatchGetResponse        `protobuf:"bytes,10,opt,name=cmd_batch_get_resp" json:"cmd_batch_get_resp,omitempty"`
	CmdBatchRollbackResp *CmdBatchRollbackResponse   `protobuf:"bytes,11,opt,name=cmd_batch_rollback_resp" json:"cmd_batch_rollback_resp,omitempty"`
	XXX_unrecognized     []byte                      `json:"-"`
}

func (m *Response) Reset()                    { *m = Response{} }
func (m *Response) String() string            { return proto.CompactTextString(m) }
func (*Response) ProtoMessage()               {}
func (*Response) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{24} }

func (m *Response) GetType() MessageType {
	if m != nil && m.Type != nil {
		return *m.Type
	}
	return MessageType_CmdGet
}

func (m *Response) GetRegionError() *errorpb.Error {
	if m != nil {
		return m.RegionError
	}
	return nil
}

func (m *Response) GetCmdGetResp() *CmdGetResponse {
	if m != nil {
		return m.CmdGetResp
	}
	return nil
}

func (m *Response) GetCmdScanResp() *CmdScanResponse {
	if m != nil {
		return m.CmdScanResp
	}
	return nil
}

func (m *Response) GetCmdPrewriteResp() *CmdPrewriteResponse {
	if m != nil {
		return m.CmdPrewriteResp
	}
	return nil
}

func (m *Response) GetCmdCommitResp() *CmdCommitResponse {
	if m != nil {
		return m.CmdCommitResp
	}
	return nil
}

func (m *Response) GetCmdCleanupResp() *CmdCleanupResponse {
	if m != nil {
		return m.CmdCleanupResp
	}
	return nil
}

func (m *Response) GetCmdRbGetResp() *CmdRollbackThenGetResponse {
	if m != nil {
		return m.CmdRbGetResp
	}
	return nil
}

func (m *Response) GetCmdCommitGetResp() *CmdCommitThenGetResponse {
	if m != nil {
		return m.CmdCommitGetResp
	}
	return nil
}

func (m *Response) GetCmdBatchGetResp() *CmdBatchGetResponse {
	if m != nil {
		return m.CmdBatchGetResp
	}
	return nil
}

func (m *Response) GetCmdBatchRollbackResp() *CmdBatchRollbackResponse {
	if m != nil {
		return m.CmdBatchRollbackResp
	}
	return nil
}

func init() {
	proto.RegisterType((*LockInfo)(nil), "kvrpcpb.LockInfo")
	proto.RegisterType((*KeyError)(nil), "kvrpcpb.KeyError")
	proto.RegisterType((*Context)(nil), "kvrpcpb.Context")
	proto.RegisterType((*CmdGetRequest)(nil), "kvrpcpb.CmdGetRequest")
	proto.RegisterType((*CmdGetResponse)(nil), "kvrpcpb.CmdGetResponse")
	proto.RegisterType((*CmdScanRequest)(nil), "kvrpcpb.CmdScanRequest")
	proto.RegisterType((*KvPair)(nil), "kvrpcpb.KvPair")
	proto.RegisterType((*CmdScanResponse)(nil), "kvrpcpb.CmdScanResponse")
	proto.RegisterType((*Mutation)(nil), "kvrpcpb.Mutation")
	proto.RegisterType((*CmdPrewriteRequest)(nil), "kvrpcpb.CmdPrewriteRequest")
	proto.RegisterType((*CmdPrewriteResponse)(nil), "kvrpcpb.CmdPrewriteResponse")
	proto.RegisterType((*CmdCommitRequest)(nil), "kvrpcpb.CmdCommitRequest")
	proto.RegisterType((*CmdCommitResponse)(nil), "kvrpcpb.CmdCommitResponse")
	proto.RegisterType((*CmdBatchRollbackRequest)(nil), "kvrpcpb.CmdBatchRollbackRequest")
	proto.RegisterType((*CmdBatchRollbackResponse)(nil), "kvrpcpb.CmdBatchRollbackResponse")
	proto.RegisterType((*CmdCleanupRequest)(nil), "kvrpcpb.CmdCleanupRequest")
	proto.RegisterType((*CmdCleanupResponse)(nil), "kvrpcpb.CmdCleanupResponse")
	proto.RegisterType((*CmdRollbackThenGetRequest)(nil), "kvrpcpb.CmdRollbackThenGetRequest")
	proto.RegisterType((*CmdRollbackThenGetResponse)(nil), "kvrpcpb.CmdRollbackThenGetResponse")
	proto.RegisterType((*CmdCommitThenGetRequest)(nil), "kvrpcpb.CmdCommitThenGetRequest")
	proto.RegisterType((*CmdCommitThenGetResponse)(nil), "kvrpcpb.CmdCommitThenGetResponse")
	proto.RegisterType((*CmdBatchGetRequest)(nil), "kvrpcpb.CmdBatchGetRequest")
	proto.RegisterType((*CmdBatchGetResponse)(nil), "kvrpcpb.CmdBatchGetResponse")
	proto.RegisterType((*Request)(nil), "kvrpcpb.Request")
	proto.RegisterType((*Response)(nil), "kvrpcpb.Response")
	proto.RegisterEnum("kvrpcpb.MessageType", MessageType_name, MessageType_value)
	proto.RegisterEnum("kvrpcpb.Op", Op_name, Op_value)
}

func init() { proto.RegisterFile("kvrpcpb.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 1015 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x9c, 0x56, 0xff, 0x72, 0xdb, 0x44,
	0x10, 0x1e, 0xdb, 0xb2, 0x65, 0xaf, 0x6c, 0x47, 0x51, 0x4a, 0xec, 0x1a, 0x86, 0x49, 0x45, 0xff,
	0x80, 0x32, 0x0d, 0xd3, 0x74, 0x42, 0x3b, 0x4c, 0x26, 0x40, 0x43, 0x61, 0x98, 0x50, 0x9a, 0x09,
	0xfd, 0xdf, 0x23, 0x2b, 0x47, 0xe2, 0x89, 0x6d, 0xa9, 0xa7, 0xb3, 0xc1, 0xcf, 0xc4, 0x43, 0xf0,
	0x14, 0xbc, 0x0f, 0xab, 0xbd, 0x3b, 0xfd, 0xf0, 0x29, 0x19, 0xb7, 0xff, 0xd9, 0xa7, 0xfd, 0xf6,
	0xfb, 0x76, 0xf7, 0xbb, 0x95, 0xa0, 0x77, 0xbb, 0xe2, 0x71, 0x18, 0x4f, 0x0e, 0x63, 0x1e, 0x89,
	0xc8, 0xb3, 0xd5, 0xdf, 0x51, 0x77, 0xce, 0x44, 0xa0, 0x8f, 0x47, 0x3d, 0xc6, 0x79, 0xc4, 0xf5,
	0x5f, 0xff, 0x0c, 0xda, 0xbf, 0x45, 0xe1, 0xed, 0xaf, 0x8b, 0x3f, 0x23, 0xef, 0x01, 0x74, 0x63,
	0x3e, 0x9d, 0x07, 0x7c, 0x3d, 0x9e, 0xe1, 0xd9, 0xb0, 0x76, 0x50, 0xfb, 0xb2, 0x9b, 0x9e, 0xa6,
	0xff, 0xc6, 0x2b, 0xc6, 0x93, 0x69, 0xb4, 0x18, 0xd6, 0xf1, 0xd4, 0xf2, 0x1c, 0x68, 0xdc, 0xb2,
	0xf5, 0xb0, 0x91, 0x86, 0xf8, 0x6f, 0xa1, 0x7d, 0xce, 0xd6, 0xaf, 0xd3, 0xc4, 0xde, 0x23, 0x68,
	0xa5, 0xe1, 0xec, 0x8a, 0xe0, 0xce, 0xd1, 0xee, 0xa1, 0x96, 0x95, 0xf1, 0xec, 0x42, 0x87, 0x33,
	0xc1, 0xd7, 0xc1, 0x64, 0xc6, 0x28, 0x5d, 0xc7, 0xeb, 0x41, 0x33, 0x98, 0x44, 0x5c, 0x50, 0xc2,
	0x8e, 0x1f, 0x82, 0x7d, 0x16, 0x2d, 0x04, 0xfb, 0x5b, 0xc8, 0xe0, 0x6b, 0x24, 0x1e, 0x4f, 0x65,
	0x4a, 0xcb, 0xfb, 0x0a, 0xba, 0xea, 0x88, 0xc5, 0x51, 0x78, 0x43, 0x29, 0x9c, 0xa3, 0xbd, 0x43,
	0x55, 0xe7, 0x25, 0x3d, 0x7b, 0x9d, 0x3e, 0xf2, 0x46, 0x60, 0xc5, 0x8c, 0x71, 0x4a, 0xeb, 0x1c,
	0x75, 0x75, 0xc8, 0x05, 0x9e, 0xf9, 0x4f, 0xa1, 0x77, 0x36, 0xbf, 0xfa, 0x85, 0x89, 0x4b, 0xf6,
	0x7e, 0xc9, 0x12, 0xa1, 0x6b, 0x92, 0x65, 0xef, 0x80, 0x5d, 0xaa, 0xd8, 0xff, 0x11, 0xfa, 0x3a,
	0x3c, 0x89, 0xa3, 0x45, 0xc2, 0xbc, 0x03, 0x68, 0x52, 0x33, 0x8d, 0x4a, 0xb3, 0x66, 0x60, 0x59,
	0xab, 0x60, 0xb6, 0x94, 0x55, 0x76, 0xb1, 0xd9, 0x69, 0x8a, 0x3f, 0xc2, 0x60, 0xa1, 0x29, 0xb1,
	0xba, 0x44, 0x04, 0x5c, 0x8c, 0x73, 0x62, 0xc4, 0xcc, 0xa6, 0xf3, 0xa9, 0x20, 0x4c, 0xaf, 0xa8,
	0xa3, 0x41, 0x3a, 0x7e, 0x86, 0xd6, 0xf9, 0xea, 0x22, 0x98, 0xf2, 0x2d, 0xf8, 0x55, 0x45, 0x75,
	0x9d, 0x58, 0x8a, 0x91, 0x43, 0x7b, 0x06, 0x3b, 0x99, 0x18, 0x55, 0xd0, 0xe7, 0xd0, 0x8c, 0x31,
	0x71, 0x82, 0x09, 0x1b, 0x98, 0x70, 0x27, 0x4f, 0x48, 0x84, 0xfe, 0xf7, 0xd0, 0x7e, 0xb3, 0x14,
	0x81, 0x40, 0x31, 0xde, 0x00, 0xea, 0x51, 0x4c, 0xcc, 0xfd, 0x23, 0x27, 0x0b, 0x7c, 0x1b, 0xdf,
	0xcb, 0x79, 0x0d, 0x1e, 0x72, 0x5e, 0x70, 0xf6, 0x17, 0x9f, 0x0a, 0xa6, 0x9b, 0xf0, 0x18, 0x3a,
	0x73, 0x95, 0x56, 0x53, 0xe7, 0xb5, 0x64, 0x84, 0x9b, 0xee, 0x94, 0x04, 0x9f, 0x40, 0x4f, 0x36,
	0xb0, 0xdc, 0xa4, 0x97, 0xb0, 0x57, 0x22, 0x52, 0x05, 0xa2, 0x39, 0xa9, 0x63, 0x26, 0x8d, 0x6e,
	0x19, 0x7a, 0xd9, 0x45, 0xe4, 0x59, 0x34, 0xc7, 0x11, 0x68, 0x81, 0x06, 0x89, 0xf4, 0x61, 0x17,
	0x2c, 0xac, 0x34, 0x41, 0x25, 0x0d, 0x54, 0xb2, 0x0f, 0xfd, 0x90, 0x50, 0x1b, 0x52, 0x8e, 0x61,
	0xb7, 0x90, 0x70, 0x5b, 0xeb, 0xf8, 0xa7, 0x30, 0x40, 0xd8, 0xab, 0x40, 0x84, 0x37, 0x97, 0xd1,
	0x6c, 0x36, 0x09, 0xc2, 0xdb, 0x0f, 0x91, 0xe3, 0x9f, 0xc0, 0xd0, 0xc4, 0x6f, 0xcd, 0xfe, 0x42,
	0x8a, 0x9e, 0xb1, 0x60, 0xb1, 0x8c, 0x2b, 0xef, 0x87, 0x21, 0x42, 0xde, 0x92, 0xdf, 0x69, 0xc2,
	0x19, 0x70, 0xeb, 0x9b, 0x62, 0x76, 0x4f, 0xe6, 0x3b, 0x85, 0x87, 0x98, 0x4f, 0x57, 0xf0, 0xee,
	0x86, 0x2d, 0xee, 0xba, 0xb0, 0x95, 0x7b, 0xca, 0x7f, 0x03, 0xa3, 0x2a, 0xfc, 0xc7, 0xde, 0xe0,
	0x29, 0x4d, 0x45, 0x0e, 0xf3, 0x83, 0xc5, 0xdc, 0x65, 0x11, 0x6f, 0x0f, 0x9c, 0x6b, 0x96, 0x1f,
	0x5a, 0xa4, 0xfc, 0x9c, 0x06, 0xb8, 0x41, 0xf5, 0xb1, 0xba, 0x9f, 0xd3, 0x58, 0xc8, 0x0d, 0x05,
	0xc9, 0xda, 0x31, 0x35, 0x32, 0xb0, 0xb1, 0xf1, 0x8e, 0xe9, 0x12, 0xe5, 0xa0, 0x2d, 0xb7, 0xc4,
	0xbf, 0x16, 0xd8, 0x9a, 0xc1, 0x07, 0x4b, 0xac, 0x63, 0xa6, 0xf6, 0xc4, 0x83, 0xfc, 0x56, 0xb3,
	0x24, 0x09, 0xae, 0xd9, 0x3b, 0x7c, 0x86, 0x97, 0xd2, 0x0e, 0xe5, 0xb2, 0x57, 0x9b, 0xdc, 0xcd,
	0xc2, 0xf4, 0x4b, 0xe0, 0x6b, 0x70, 0xc2, 0xf9, 0xd5, 0x38, 0x6d, 0x12, 0x67, 0xef, 0xd5, 0x36,
	0xdf, 0xcf, 0xc3, 0x4a, 0x6b, 0xfc, 0x29, 0x74, 0xd3, 0xe0, 0x04, 0x37, 0x1b, 0x45, 0x5b, 0x14,
	0x3d, 0x28, 0x46, 0x17, 0x57, 0xf0, 0x31, 0xb8, 0x69, 0x78, 0xac, 0x76, 0x05, 0x41, 0x9a, 0x04,
	0xf9, 0xb4, 0x08, 0xd9, 0x5c, 0x5a, 0xcf, 0x70, 0x96, 0x08, 0x53, 0xf3, 0x4c, 0x41, 0x2d, 0x02,
	0x3d, 0x2c, 0x82, 0xca, 0x6b, 0xe4, 0x39, 0xec, 0x10, 0x44, 0x5e, 0x0e, 0xc2, 0xd8, 0x84, 0x19,
	0x95, 0x30, 0xe5, 0x4b, 0xf7, 0x9d, 0xe4, 0xe1, 0x93, 0xac, 0xfa, 0x36, 0x61, 0xfc, 0x22, 0xe6,
	0x8e, 0xfb, 0x71, 0x02, 0x5e, 0x41, 0xa3, 0xc6, 0x77, 0x08, 0x7f, 0x60, 0xea, 0xdc, 0x40, 0x7f,
	0x0b, 0xbb, 0x29, 0x7a, 0x92, 0xce, 0x3f, 0x03, 0x83, 0xd9, 0x99, 0x4d, 0x57, 0xfd, 0x00, 0xfb,
	0x39, 0x8e, 0x2b, 0x65, 0x04, 0x76, 0x4c, 0xe6, 0xaa, 0x05, 0xe7, 0xff, 0x67, 0x41, 0x3b, 0xb3,
	0xdb, 0x36, 0x16, 0x7a, 0x9c, 0x7f, 0x11, 0xd0, 0xb5, 0x90, 0x3e, 0xea, 0x1f, 0xea, 0x6f, 0x1d,
	0x79, 0x27, 0x94, 0x31, 0x64, 0x29, 0x49, 0xac, 0x6c, 0x34, 0x30, 0x6c, 0xa4, 0x88, 0xbf, 0x81,
	0x5e, 0xc1, 0x47, 0x18, 0x2f, 0x8d, 0x34, 0x34, 0x8d, 0xa4, 0x00, 0x2f, 0x64, 0xc3, 0x0a, 0x4e,
	0x42, 0x90, 0xb4, 0xd2, 0x67, 0xd5, 0x56, 0x52, 0x40, 0x6d, 0x0c, 0xed, 0x25, 0x84, 0xb5, 0x2a,
	0x8c, 0x51, 0x7e, 0x85, 0x28, 0xdf, 0xe6, 0x6e, 0x42, 0x94, 0x6d, 0x4e, 0x67, 0x73, 0x15, 0x9f,
	0x48, 0xae, 0xcc, 0x4f, 0x88, 0x92, 0x86, 0xfa, 0xe2, 0x5e, 0x43, 0x29, 0xf4, 0x29, 0xec, 0x19,
	0x8e, 0xc2, 0x0c, 0xd2, 0x52, 0x8f, 0xee, 0xb1, 0x94, 0xc2, 0xbf, 0x94, 0x8e, 0x2c, 0x7a, 0x0a,
	0xe1, 0x60, 0xf6, 0xc8, 0xd8, 0x3a, 0xaf, 0x60, 0x50, 0xe9, 0x2a, 0x84, 0x3b, 0x26, 0x7b, 0xe5,
	0x7b, 0xef, 0xc9, 0x3f, 0x35, 0x70, 0x8a, 0xb6, 0x01, 0x68, 0xc9, 0x99, 0xbb, 0x35, 0x5c, 0xdf,
	0xb6, 0x9a, 0xa7, 0x5b, 0xc7, 0x55, 0xe8, 0x14, 0xe6, 0xe4, 0x36, 0x70, 0x9d, 0x76, 0xb2, 0x9a,
	0x5c, 0xcb, 0xeb, 0x03, 0xe4, 0xad, 0x75, 0x9b, 0xb8, 0xd8, 0x3d, 0xb3, 0x69, 0x6e, 0x0b, 0x5f,
	0x03, 0xee, 0x66, 0x2b, 0x5c, 0x5b, 0x65, 0xd7, 0x15, 0xba, 0x6d, 0x15, 0x56, 0xd2, 0xec, 0x76,
	0x9e, 0x1c, 0x40, 0x1d, 0x3f, 0xa7, 0x6c, 0x68, 0x5c, 0x2c, 0x53, 0x81, 0xf8, 0xe3, 0x27, 0x36,
	0x43, 0x71, 0x6d, 0xb0, 0xd2, 0x4f, 0x69, 0xb7, 0xf1, 0x7f, 0x00, 0x00, 0x00, 0xff, 0xff, 0x35,
	0xdb, 0x4d, 0xa9, 0xf2, 0x0b, 0x00, 0x00,
}
