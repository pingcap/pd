// Code generated by protoc-gen-go.
// source: pdpb.proto
// DO NOT EDIT!

/*
Package pdpb is a generated protocol buffer package.

It is generated from these files:
	pdpb.proto

It has these top-level messages:
	Leader
	TsoRequest
	Timestamp
	TsoResponse
	BootstrapRequest
	BootstrapResponse
	IsBootstrappedRequest
	IsBootstrappedResponse
	AllocIdRequest
	AllocIdResponse
	GetMetaRequest
	GetMetaResponse
	PutMetaRequest
	PutMetaResponse
	AskChangePeerRequest
	AskChangePeerResponse
	AskSplitRequest
	AskSplitResponse
	RequestHeader
	ResponseHeader
	Request
	Response
	BootstrappedError
	Error
*/
package pdpb

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import metapb "github.com/pingcap/kvproto/pkg/metapb"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
const _ = proto.ProtoPackageIsVersion1

type CommandType int32

const (
	CommandType_Invalid        CommandType = 0
	CommandType_Tso            CommandType = 1
	CommandType_Bootstrap      CommandType = 2
	CommandType_IsBootstrapped CommandType = 3
	CommandType_AllocId        CommandType = 4
	CommandType_GetMeta        CommandType = 5
	CommandType_PutMeta        CommandType = 6
	CommandType_DeleteMeta     CommandType = 7
	CommandType_AskChangePeer  CommandType = 8
	CommandType_AskSplit       CommandType = 9
)

var CommandType_name = map[int32]string{
	0: "Invalid",
	1: "Tso",
	2: "Bootstrap",
	3: "IsBootstrapped",
	4: "AllocId",
	5: "GetMeta",
	6: "PutMeta",
	7: "DeleteMeta",
	8: "AskChangePeer",
	9: "AskSplit",
}
var CommandType_value = map[string]int32{
	"Invalid":        0,
	"Tso":            1,
	"Bootstrap":      2,
	"IsBootstrapped": 3,
	"AllocId":        4,
	"GetMeta":        5,
	"PutMeta":        6,
	"DeleteMeta":     7,
	"AskChangePeer":  8,
	"AskSplit":       9,
}

func (x CommandType) Enum() *CommandType {
	p := new(CommandType)
	*p = x
	return p
}
func (x CommandType) String() string {
	return proto.EnumName(CommandType_name, int32(x))
}
func (x *CommandType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(CommandType_value, data, "CommandType")
	if err != nil {
		return err
	}
	*x = CommandType(value)
	return nil
}
func (CommandType) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

type MetaType int32

const (
	MetaType_InvalidMeta MetaType = 0
	MetaType_StoreType   MetaType = 1
	MetaType_RegionType  MetaType = 2
	MetaType_PeerType    MetaType = 3
	MetaType_ClusterType MetaType = 4
)

var MetaType_name = map[int32]string{
	0: "InvalidMeta",
	1: "StoreType",
	2: "RegionType",
	3: "PeerType",
	4: "ClusterType",
}
var MetaType_value = map[string]int32{
	"InvalidMeta": 0,
	"StoreType":   1,
	"RegionType":  2,
	"PeerType":    3,
	"ClusterType": 4,
}

func (x MetaType) Enum() *MetaType {
	p := new(MetaType)
	*p = x
	return p
}
func (x MetaType) String() string {
	return proto.EnumName(MetaType_name, int32(x))
}
func (x *MetaType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(MetaType_value, data, "MetaType")
	if err != nil {
		return err
	}
	*x = MetaType(value)
	return nil
}
func (MetaType) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

type Leader struct {
	Addr             *string `protobuf:"bytes,1,opt,name=addr" json:"addr,omitempty"`
	Pid              *int64  `protobuf:"varint,2,opt,name=pid" json:"pid,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Leader) Reset()                    { *m = Leader{} }
func (m *Leader) String() string            { return proto.CompactTextString(m) }
func (*Leader) ProtoMessage()               {}
func (*Leader) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Leader) GetAddr() string {
	if m != nil && m.Addr != nil {
		return *m.Addr
	}
	return ""
}

func (m *Leader) GetPid() int64 {
	if m != nil && m.Pid != nil {
		return *m.Pid
	}
	return 0
}

type TsoRequest struct {
	Number           *uint32 `protobuf:"varint,1,opt,name=number" json:"number,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *TsoRequest) Reset()                    { *m = TsoRequest{} }
func (m *TsoRequest) String() string            { return proto.CompactTextString(m) }
func (*TsoRequest) ProtoMessage()               {}
func (*TsoRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *TsoRequest) GetNumber() uint32 {
	if m != nil && m.Number != nil {
		return *m.Number
	}
	return 0
}

type Timestamp struct {
	Physical         *int64 `protobuf:"varint,1,opt,name=physical" json:"physical,omitempty"`
	Logical          *int64 `protobuf:"varint,2,opt,name=logical" json:"logical,omitempty"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *Timestamp) Reset()                    { *m = Timestamp{} }
func (m *Timestamp) String() string            { return proto.CompactTextString(m) }
func (*Timestamp) ProtoMessage()               {}
func (*Timestamp) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

func (m *Timestamp) GetPhysical() int64 {
	if m != nil && m.Physical != nil {
		return *m.Physical
	}
	return 0
}

func (m *Timestamp) GetLogical() int64 {
	if m != nil && m.Logical != nil {
		return *m.Logical
	}
	return 0
}

type TsoResponse struct {
	Timestamps       []*Timestamp `protobuf:"bytes,1,rep,name=timestamps" json:"timestamps,omitempty"`
	XXX_unrecognized []byte       `json:"-"`
}

func (m *TsoResponse) Reset()                    { *m = TsoResponse{} }
func (m *TsoResponse) String() string            { return proto.CompactTextString(m) }
func (*TsoResponse) ProtoMessage()               {}
func (*TsoResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{3} }

func (m *TsoResponse) GetTimestamps() []*Timestamp {
	if m != nil {
		return m.Timestamps
	}
	return nil
}

type BootstrapRequest struct {
	Store            *metapb.Store  `protobuf:"bytes,1,opt,name=store" json:"store,omitempty"`
	Region           *metapb.Region `protobuf:"bytes,2,opt,name=region" json:"region,omitempty"`
	XXX_unrecognized []byte         `json:"-"`
}

func (m *BootstrapRequest) Reset()                    { *m = BootstrapRequest{} }
func (m *BootstrapRequest) String() string            { return proto.CompactTextString(m) }
func (*BootstrapRequest) ProtoMessage()               {}
func (*BootstrapRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{4} }

func (m *BootstrapRequest) GetStore() *metapb.Store {
	if m != nil {
		return m.Store
	}
	return nil
}

func (m *BootstrapRequest) GetRegion() *metapb.Region {
	if m != nil {
		return m.Region
	}
	return nil
}

type BootstrapResponse struct {
	XXX_unrecognized []byte `json:"-"`
}

func (m *BootstrapResponse) Reset()                    { *m = BootstrapResponse{} }
func (m *BootstrapResponse) String() string            { return proto.CompactTextString(m) }
func (*BootstrapResponse) ProtoMessage()               {}
func (*BootstrapResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{5} }

type IsBootstrappedRequest struct {
	XXX_unrecognized []byte `json:"-"`
}

func (m *IsBootstrappedRequest) Reset()                    { *m = IsBootstrappedRequest{} }
func (m *IsBootstrappedRequest) String() string            { return proto.CompactTextString(m) }
func (*IsBootstrappedRequest) ProtoMessage()               {}
func (*IsBootstrappedRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{6} }

type IsBootstrappedResponse struct {
	Bootstrapped     *bool  `protobuf:"varint,1,opt,name=bootstrapped" json:"bootstrapped,omitempty"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *IsBootstrappedResponse) Reset()                    { *m = IsBootstrappedResponse{} }
func (m *IsBootstrappedResponse) String() string            { return proto.CompactTextString(m) }
func (*IsBootstrappedResponse) ProtoMessage()               {}
func (*IsBootstrappedResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{7} }

func (m *IsBootstrappedResponse) GetBootstrapped() bool {
	if m != nil && m.Bootstrapped != nil {
		return *m.Bootstrapped
	}
	return false
}

type AllocIdRequest struct {
	XXX_unrecognized []byte `json:"-"`
}

func (m *AllocIdRequest) Reset()                    { *m = AllocIdRequest{} }
func (m *AllocIdRequest) String() string            { return proto.CompactTextString(m) }
func (*AllocIdRequest) ProtoMessage()               {}
func (*AllocIdRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{8} }

type AllocIdResponse struct {
	Id               *uint64 `protobuf:"varint,1,opt,name=id" json:"id,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *AllocIdResponse) Reset()                    { *m = AllocIdResponse{} }
func (m *AllocIdResponse) String() string            { return proto.CompactTextString(m) }
func (*AllocIdResponse) ProtoMessage()               {}
func (*AllocIdResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{9} }

func (m *AllocIdResponse) GetId() uint64 {
	if m != nil && m.Id != nil {
		return *m.Id
	}
	return 0
}

type GetMetaRequest struct {
	MetaType         *MetaType `protobuf:"varint,1,opt,name=meta_type,enum=pdpb.MetaType" json:"meta_type,omitempty"`
	StoreId          *uint64   `protobuf:"varint,2,opt,name=store_id" json:"store_id,omitempty"`
	RegionKey        []byte    `protobuf:"bytes,3,opt,name=region_key" json:"region_key,omitempty"`
	ClusterId        *uint64   `protobuf:"varint,4,opt,name=cluster_id" json:"cluster_id,omitempty"`
	XXX_unrecognized []byte    `json:"-"`
}

func (m *GetMetaRequest) Reset()                    { *m = GetMetaRequest{} }
func (m *GetMetaRequest) String() string            { return proto.CompactTextString(m) }
func (*GetMetaRequest) ProtoMessage()               {}
func (*GetMetaRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{10} }

func (m *GetMetaRequest) GetMetaType() MetaType {
	if m != nil && m.MetaType != nil {
		return *m.MetaType
	}
	return MetaType_InvalidMeta
}

func (m *GetMetaRequest) GetStoreId() uint64 {
	if m != nil && m.StoreId != nil {
		return *m.StoreId
	}
	return 0
}

func (m *GetMetaRequest) GetRegionKey() []byte {
	if m != nil {
		return m.RegionKey
	}
	return nil
}

func (m *GetMetaRequest) GetClusterId() uint64 {
	if m != nil && m.ClusterId != nil {
		return *m.ClusterId
	}
	return 0
}

type GetMetaResponse struct {
	MetaType         *MetaType       `protobuf:"varint,1,opt,name=meta_type,enum=pdpb.MetaType" json:"meta_type,omitempty"`
	Store            *metapb.Store   `protobuf:"bytes,2,opt,name=store" json:"store,omitempty"`
	Region           *metapb.Region  `protobuf:"bytes,3,opt,name=region" json:"region,omitempty"`
	Cluster          *metapb.Cluster `protobuf:"bytes,4,opt,name=cluster" json:"cluster,omitempty"`
	XXX_unrecognized []byte          `json:"-"`
}

func (m *GetMetaResponse) Reset()                    { *m = GetMetaResponse{} }
func (m *GetMetaResponse) String() string            { return proto.CompactTextString(m) }
func (*GetMetaResponse) ProtoMessage()               {}
func (*GetMetaResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{11} }

func (m *GetMetaResponse) GetMetaType() MetaType {
	if m != nil && m.MetaType != nil {
		return *m.MetaType
	}
	return MetaType_InvalidMeta
}

func (m *GetMetaResponse) GetStore() *metapb.Store {
	if m != nil {
		return m.Store
	}
	return nil
}

func (m *GetMetaResponse) GetRegion() *metapb.Region {
	if m != nil {
		return m.Region
	}
	return nil
}

func (m *GetMetaResponse) GetCluster() *metapb.Cluster {
	if m != nil {
		return m.Cluster
	}
	return nil
}

type PutMetaRequest struct {
	MetaType         *MetaType       `protobuf:"varint,1,opt,name=meta_type,enum=pdpb.MetaType" json:"meta_type,omitempty"`
	Store            *metapb.Store   `protobuf:"bytes,2,opt,name=store" json:"store,omitempty"`
	Cluster          *metapb.Cluster `protobuf:"bytes,3,opt,name=cluster" json:"cluster,omitempty"`
	XXX_unrecognized []byte          `json:"-"`
}

func (m *PutMetaRequest) Reset()                    { *m = PutMetaRequest{} }
func (m *PutMetaRequest) String() string            { return proto.CompactTextString(m) }
func (*PutMetaRequest) ProtoMessage()               {}
func (*PutMetaRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{12} }

func (m *PutMetaRequest) GetMetaType() MetaType {
	if m != nil && m.MetaType != nil {
		return *m.MetaType
	}
	return MetaType_InvalidMeta
}

func (m *PutMetaRequest) GetStore() *metapb.Store {
	if m != nil {
		return m.Store
	}
	return nil
}

func (m *PutMetaRequest) GetCluster() *metapb.Cluster {
	if m != nil {
		return m.Cluster
	}
	return nil
}

type PutMetaResponse struct {
	MetaType         *MetaType `protobuf:"varint,1,opt,name=meta_type,enum=pdpb.MetaType" json:"meta_type,omitempty"`
	XXX_unrecognized []byte    `json:"-"`
}

func (m *PutMetaResponse) Reset()                    { *m = PutMetaResponse{} }
func (m *PutMetaResponse) String() string            { return proto.CompactTextString(m) }
func (*PutMetaResponse) ProtoMessage()               {}
func (*PutMetaResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{13} }

func (m *PutMetaResponse) GetMetaType() MetaType {
	if m != nil && m.MetaType != nil {
		return *m.MetaType
	}
	return MetaType_InvalidMeta
}

type AskChangePeerRequest struct {
	Region *metapb.Region `protobuf:"bytes,1,opt,name=region" json:"region,omitempty"`
	// The current leader store id of the region.
	// Pd can first try to send command to this store,
	// if the peer is not leader now, pd will try to
	// find the new leader of the region and then send
	// command again.
	LeaderStoreId    *uint64 `protobuf:"varint,2,opt,name=leader_store_id" json:"leader_store_id,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *AskChangePeerRequest) Reset()                    { *m = AskChangePeerRequest{} }
func (m *AskChangePeerRequest) String() string            { return proto.CompactTextString(m) }
func (*AskChangePeerRequest) ProtoMessage()               {}
func (*AskChangePeerRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{14} }

func (m *AskChangePeerRequest) GetRegion() *metapb.Region {
	if m != nil {
		return m.Region
	}
	return nil
}

func (m *AskChangePeerRequest) GetLeaderStoreId() uint64 {
	if m != nil && m.LeaderStoreId != nil {
		return *m.LeaderStoreId
	}
	return 0
}

type AskChangePeerResponse struct {
	XXX_unrecognized []byte `json:"-"`
}

func (m *AskChangePeerResponse) Reset()                    { *m = AskChangePeerResponse{} }
func (m *AskChangePeerResponse) String() string            { return proto.CompactTextString(m) }
func (*AskChangePeerResponse) ProtoMessage()               {}
func (*AskChangePeerResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{15} }

type AskSplitRequest struct {
	Region           *metapb.Region `protobuf:"bytes,1,opt,name=region" json:"region,omitempty"`
	SplitKey         []byte         `protobuf:"bytes,2,opt,name=split_key" json:"split_key,omitempty"`
	LeaderStoreId    *uint64        `protobuf:"varint,3,opt,name=leader_store_id" json:"leader_store_id,omitempty"`
	XXX_unrecognized []byte         `json:"-"`
}

func (m *AskSplitRequest) Reset()                    { *m = AskSplitRequest{} }
func (m *AskSplitRequest) String() string            { return proto.CompactTextString(m) }
func (*AskSplitRequest) ProtoMessage()               {}
func (*AskSplitRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{16} }

func (m *AskSplitRequest) GetRegion() *metapb.Region {
	if m != nil {
		return m.Region
	}
	return nil
}

func (m *AskSplitRequest) GetSplitKey() []byte {
	if m != nil {
		return m.SplitKey
	}
	return nil
}

func (m *AskSplitRequest) GetLeaderStoreId() uint64 {
	if m != nil && m.LeaderStoreId != nil {
		return *m.LeaderStoreId
	}
	return 0
}

type AskSplitResponse struct {
	XXX_unrecognized []byte `json:"-"`
}

func (m *AskSplitResponse) Reset()                    { *m = AskSplitResponse{} }
func (m *AskSplitResponse) String() string            { return proto.CompactTextString(m) }
func (*AskSplitResponse) ProtoMessage()               {}
func (*AskSplitResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{17} }

type RequestHeader struct {
	// 16 bytes, to distinguish request.
	Uuid             []byte  `protobuf:"bytes,1,opt,name=uuid" json:"uuid,omitempty"`
	ClusterId        *uint64 `protobuf:"varint,2,opt,name=cluster_id" json:"cluster_id,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *RequestHeader) Reset()                    { *m = RequestHeader{} }
func (m *RequestHeader) String() string            { return proto.CompactTextString(m) }
func (*RequestHeader) ProtoMessage()               {}
func (*RequestHeader) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{18} }

func (m *RequestHeader) GetUuid() []byte {
	if m != nil {
		return m.Uuid
	}
	return nil
}

func (m *RequestHeader) GetClusterId() uint64 {
	if m != nil && m.ClusterId != nil {
		return *m.ClusterId
	}
	return 0
}

type ResponseHeader struct {
	// 16 bytes, to distinguish request.
	Uuid             []byte  `protobuf:"bytes,1,opt,name=uuid" json:"uuid,omitempty"`
	ClusterId        *uint64 `protobuf:"varint,2,opt,name=cluster_id" json:"cluster_id,omitempty"`
	Error            *Error  `protobuf:"bytes,3,opt,name=error" json:"error,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *ResponseHeader) Reset()                    { *m = ResponseHeader{} }
func (m *ResponseHeader) String() string            { return proto.CompactTextString(m) }
func (*ResponseHeader) ProtoMessage()               {}
func (*ResponseHeader) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{19} }

func (m *ResponseHeader) GetUuid() []byte {
	if m != nil {
		return m.Uuid
	}
	return nil
}

func (m *ResponseHeader) GetClusterId() uint64 {
	if m != nil && m.ClusterId != nil {
		return *m.ClusterId
	}
	return 0
}

func (m *ResponseHeader) GetError() *Error {
	if m != nil {
		return m.Error
	}
	return nil
}

type Request struct {
	Header           *RequestHeader         `protobuf:"bytes,1,opt,name=header" json:"header,omitempty"`
	CmdType          *CommandType           `protobuf:"varint,2,opt,name=cmd_type,enum=pdpb.CommandType" json:"cmd_type,omitempty"`
	Tso              *TsoRequest            `protobuf:"bytes,3,opt,name=tso" json:"tso,omitempty"`
	Bootstrap        *BootstrapRequest      `protobuf:"bytes,4,opt,name=bootstrap" json:"bootstrap,omitempty"`
	IsBootstrapped   *IsBootstrappedRequest `protobuf:"bytes,5,opt,name=is_bootstrapped" json:"is_bootstrapped,omitempty"`
	AllocId          *AllocIdRequest        `protobuf:"bytes,6,opt,name=alloc_id" json:"alloc_id,omitempty"`
	GetMeta          *GetMetaRequest        `protobuf:"bytes,7,opt,name=get_meta" json:"get_meta,omitempty"`
	PutMeta          *PutMetaRequest        `protobuf:"bytes,8,opt,name=put_meta" json:"put_meta,omitempty"`
	AskChangePeer    *AskChangePeerRequest  `protobuf:"bytes,9,opt,name=ask_change_peer" json:"ask_change_peer,omitempty"`
	AskSplit         *AskSplitRequest       `protobuf:"bytes,10,opt,name=ask_split" json:"ask_split,omitempty"`
	XXX_unrecognized []byte                 `json:"-"`
}

func (m *Request) Reset()                    { *m = Request{} }
func (m *Request) String() string            { return proto.CompactTextString(m) }
func (*Request) ProtoMessage()               {}
func (*Request) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{20} }

func (m *Request) GetHeader() *RequestHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *Request) GetCmdType() CommandType {
	if m != nil && m.CmdType != nil {
		return *m.CmdType
	}
	return CommandType_Invalid
}

func (m *Request) GetTso() *TsoRequest {
	if m != nil {
		return m.Tso
	}
	return nil
}

func (m *Request) GetBootstrap() *BootstrapRequest {
	if m != nil {
		return m.Bootstrap
	}
	return nil
}

func (m *Request) GetIsBootstrapped() *IsBootstrappedRequest {
	if m != nil {
		return m.IsBootstrapped
	}
	return nil
}

func (m *Request) GetAllocId() *AllocIdRequest {
	if m != nil {
		return m.AllocId
	}
	return nil
}

func (m *Request) GetGetMeta() *GetMetaRequest {
	if m != nil {
		return m.GetMeta
	}
	return nil
}

func (m *Request) GetPutMeta() *PutMetaRequest {
	if m != nil {
		return m.PutMeta
	}
	return nil
}

func (m *Request) GetAskChangePeer() *AskChangePeerRequest {
	if m != nil {
		return m.AskChangePeer
	}
	return nil
}

func (m *Request) GetAskSplit() *AskSplitRequest {
	if m != nil {
		return m.AskSplit
	}
	return nil
}

type Response struct {
	Header           *ResponseHeader         `protobuf:"bytes,1,opt,name=header" json:"header,omitempty"`
	CmdType          *CommandType            `protobuf:"varint,2,opt,name=cmd_type,enum=pdpb.CommandType" json:"cmd_type,omitempty"`
	Tso              *TsoResponse            `protobuf:"bytes,3,opt,name=tso" json:"tso,omitempty"`
	Bootstrap        *BootstrapResponse      `protobuf:"bytes,4,opt,name=bootstrap" json:"bootstrap,omitempty"`
	IsBootstrapped   *IsBootstrappedResponse `protobuf:"bytes,5,opt,name=is_bootstrapped" json:"is_bootstrapped,omitempty"`
	AllocId          *AllocIdResponse        `protobuf:"bytes,6,opt,name=alloc_id" json:"alloc_id,omitempty"`
	GetMeta          *GetMetaResponse        `protobuf:"bytes,7,opt,name=get_meta" json:"get_meta,omitempty"`
	PutMeta          *PutMetaResponse        `protobuf:"bytes,8,opt,name=put_meta" json:"put_meta,omitempty"`
	AskChangePeer    *AskChangePeerResponse  `protobuf:"bytes,9,opt,name=ask_change_peer" json:"ask_change_peer,omitempty"`
	AskSplit         *AskSplitResponse       `protobuf:"bytes,10,opt,name=ask_split" json:"ask_split,omitempty"`
	XXX_unrecognized []byte                  `json:"-"`
}

func (m *Response) Reset()                    { *m = Response{} }
func (m *Response) String() string            { return proto.CompactTextString(m) }
func (*Response) ProtoMessage()               {}
func (*Response) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{21} }

func (m *Response) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *Response) GetCmdType() CommandType {
	if m != nil && m.CmdType != nil {
		return *m.CmdType
	}
	return CommandType_Invalid
}

func (m *Response) GetTso() *TsoResponse {
	if m != nil {
		return m.Tso
	}
	return nil
}

func (m *Response) GetBootstrap() *BootstrapResponse {
	if m != nil {
		return m.Bootstrap
	}
	return nil
}

func (m *Response) GetIsBootstrapped() *IsBootstrappedResponse {
	if m != nil {
		return m.IsBootstrapped
	}
	return nil
}

func (m *Response) GetAllocId() *AllocIdResponse {
	if m != nil {
		return m.AllocId
	}
	return nil
}

func (m *Response) GetGetMeta() *GetMetaResponse {
	if m != nil {
		return m.GetMeta
	}
	return nil
}

func (m *Response) GetPutMeta() *PutMetaResponse {
	if m != nil {
		return m.PutMeta
	}
	return nil
}

func (m *Response) GetAskChangePeer() *AskChangePeerResponse {
	if m != nil {
		return m.AskChangePeer
	}
	return nil
}

func (m *Response) GetAskSplit() *AskSplitResponse {
	if m != nil {
		return m.AskSplit
	}
	return nil
}

type BootstrappedError struct {
	XXX_unrecognized []byte `json:"-"`
}

func (m *BootstrappedError) Reset()                    { *m = BootstrappedError{} }
func (m *BootstrappedError) String() string            { return proto.CompactTextString(m) }
func (*BootstrappedError) ProtoMessage()               {}
func (*BootstrappedError) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{22} }

type Error struct {
	Message          *string            `protobuf:"bytes,1,opt,name=message" json:"message,omitempty"`
	Bootstrapped     *BootstrappedError `protobuf:"bytes,2,opt,name=bootstrapped" json:"bootstrapped,omitempty"`
	XXX_unrecognized []byte             `json:"-"`
}

func (m *Error) Reset()                    { *m = Error{} }
func (m *Error) String() string            { return proto.CompactTextString(m) }
func (*Error) ProtoMessage()               {}
func (*Error) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{23} }

func (m *Error) GetMessage() string {
	if m != nil && m.Message != nil {
		return *m.Message
	}
	return ""
}

func (m *Error) GetBootstrapped() *BootstrappedError {
	if m != nil {
		return m.Bootstrapped
	}
	return nil
}

func init() {
	proto.RegisterType((*Leader)(nil), "pdpb.Leader")
	proto.RegisterType((*TsoRequest)(nil), "pdpb.TsoRequest")
	proto.RegisterType((*Timestamp)(nil), "pdpb.Timestamp")
	proto.RegisterType((*TsoResponse)(nil), "pdpb.TsoResponse")
	proto.RegisterType((*BootstrapRequest)(nil), "pdpb.BootstrapRequest")
	proto.RegisterType((*BootstrapResponse)(nil), "pdpb.BootstrapResponse")
	proto.RegisterType((*IsBootstrappedRequest)(nil), "pdpb.IsBootstrappedRequest")
	proto.RegisterType((*IsBootstrappedResponse)(nil), "pdpb.IsBootstrappedResponse")
	proto.RegisterType((*AllocIdRequest)(nil), "pdpb.AllocIdRequest")
	proto.RegisterType((*AllocIdResponse)(nil), "pdpb.AllocIdResponse")
	proto.RegisterType((*GetMetaRequest)(nil), "pdpb.GetMetaRequest")
	proto.RegisterType((*GetMetaResponse)(nil), "pdpb.GetMetaResponse")
	proto.RegisterType((*PutMetaRequest)(nil), "pdpb.PutMetaRequest")
	proto.RegisterType((*PutMetaResponse)(nil), "pdpb.PutMetaResponse")
	proto.RegisterType((*AskChangePeerRequest)(nil), "pdpb.AskChangePeerRequest")
	proto.RegisterType((*AskChangePeerResponse)(nil), "pdpb.AskChangePeerResponse")
	proto.RegisterType((*AskSplitRequest)(nil), "pdpb.AskSplitRequest")
	proto.RegisterType((*AskSplitResponse)(nil), "pdpb.AskSplitResponse")
	proto.RegisterType((*RequestHeader)(nil), "pdpb.RequestHeader")
	proto.RegisterType((*ResponseHeader)(nil), "pdpb.ResponseHeader")
	proto.RegisterType((*Request)(nil), "pdpb.Request")
	proto.RegisterType((*Response)(nil), "pdpb.Response")
	proto.RegisterType((*BootstrappedError)(nil), "pdpb.BootstrappedError")
	proto.RegisterType((*Error)(nil), "pdpb.Error")
	proto.RegisterEnum("pdpb.CommandType", CommandType_name, CommandType_value)
	proto.RegisterEnum("pdpb.MetaType", MetaType_name, MetaType_value)
}

var fileDescriptor0 = []byte{
	// 896 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x94, 0x56, 0xdd, 0x6e, 0xab, 0x46,
	0x10, 0x3e, 0x06, 0xff, 0xc0, 0x60, 0x03, 0xe6, 0xd8, 0x89, 0x95, 0x93, 0x73, 0x94, 0x92, 0xaa,
	0x4d, 0x22, 0xd5, 0x52, 0xdd, 0xf4, 0x01, 0xd2, 0xb4, 0x4a, 0x23, 0xf5, 0x27, 0x4a, 0x72, 0xd3,
	0x8b, 0x0a, 0x11, 0xb3, 0x72, 0x90, 0xc1, 0x50, 0x16, 0x57, 0xca, 0x7b, 0xf4, 0xba, 0x0f, 0xd0,
	0xeb, 0x3e, 0x60, 0x77, 0x67, 0x17, 0x8c, 0x31, 0x89, 0xdc, 0x3b, 0x76, 0xf6, 0x9b, 0x9f, 0xfd,
	0xbe, 0xd9, 0x59, 0x00, 0xd2, 0x20, 0x7d, 0x9a, 0xa6, 0x59, 0x92, 0x27, 0x4e, 0x9b, 0x7f, 0x1f,
	0xf5, 0x63, 0x92, 0xfb, 0x85, 0xcd, 0x3d, 0x85, 0xee, 0x4f, 0xc4, 0x0f, 0x48, 0xe6, 0xf4, 0xa1,
	0xed, 0x07, 0x41, 0x36, 0x69, 0x9d, 0xb4, 0xce, 0x74, 0xc7, 0x00, 0x35, 0x0d, 0x83, 0x89, 0xc2,
	0x16, 0xaa, 0x7b, 0x0c, 0xf0, 0x48, 0x93, 0x7b, 0xf2, 0xc7, 0x9a, 0xd0, 0xdc, 0x31, 0xa1, 0xbb,
	0x5a, 0xc7, 0x4f, 0x44, 0x40, 0x07, 0xee, 0x14, 0xf4, 0xc7, 0x30, 0x66, 0x3b, 0x7e, 0x9c, 0x3a,
	0x36, 0x68, 0xe9, 0xf3, 0x0b, 0x0d, 0xe7, 0x7e, 0x84, 0xdb, 0xaa, 0x63, 0x41, 0x2f, 0x4a, 0x16,
	0x68, 0x10, 0xd1, 0x66, 0x60, 0x60, 0x34, 0x9a, 0x26, 0x2b, 0x4a, 0x9c, 0x53, 0x80, 0xbc, 0x70,
	0xa7, 0xcc, 0x47, 0x3d, 0x33, 0x66, 0xd6, 0x14, 0xcb, 0x2e, 0xc3, 0xba, 0x77, 0x60, 0x7f, 0x97,
	0x24, 0x39, 0xcd, 0x33, 0x3f, 0x2d, 0xea, 0x38, 0x86, 0x0e, 0xcd, 0x93, 0x8c, 0x60, 0x1e, 0x63,
	0x36, 0x98, 0xca, 0x83, 0x3d, 0x70, 0xa3, 0xf3, 0x09, 0xba, 0x19, 0x59, 0x84, 0xc9, 0x0a, 0xb3,
	0x1a, 0x33, 0xb3, 0xd8, 0xbe, 0x47, 0xab, 0xfb, 0x1e, 0x86, 0x95, 0x88, 0xa2, 0x16, 0xf7, 0x10,
	0xc6, 0xb7, 0xb4, 0x34, 0xa7, 0x24, 0x90, 0xb9, 0xd8, 0x19, 0x0f, 0xea, 0x1b, 0xb2, 0xfc, 0x11,
	0xf4, 0x9f, 0x2a, 0x76, 0x2c, 0x46, 0x73, 0x6d, 0x30, 0xaf, 0xa2, 0x28, 0x99, 0xdf, 0x96, 0x11,
	0x3e, 0x82, 0x55, 0x5a, 0xa4, 0x2b, 0x80, 0x12, 0x0a, 0x87, 0xb6, 0x1b, 0x83, 0x79, 0x43, 0xf2,
	0x9f, 0x59, 0x89, 0xc5, 0xf1, 0x3e, 0x03, 0x9d, 0x57, 0xec, 0xe5, 0x2f, 0xa9, 0x38, 0xa2, 0xc9,
	0xce, 0x80, 0xb4, 0x70, 0xd4, 0x23, 0xb3, 0x72, 0xb2, 0x91, 0x01, 0x4f, 0x2a, 0xd5, 0x76, 0x58,
	0x4c, 0x71, 0x6a, 0x6f, 0x49, 0x5e, 0x26, 0x2a, 0xb3, 0xf5, 0xb9, 0x6d, 0x1e, 0xad, 0x69, 0x4e,
	0x32, 0x8e, 0x6b, 0x63, 0xba, 0xbf, 0x5a, 0x60, 0x95, 0xf9, 0x64, 0x39, 0x7b, 0x24, 0x2c, 0x29,
	0x57, 0xde, 0xa6, 0x5c, 0x6d, 0xa2, 0xdc, 0x39, 0x81, 0x9e, 0x2c, 0x04, 0xab, 0xe0, 0x32, 0x4b,
	0xc0, 0xb5, 0x30, 0xbb, 0x14, 0xcc, 0xbb, 0xf5, 0xff, 0x65, 0xe1, 0xed, 0xa2, 0x2a, 0x49, 0xd5,
	0xe6, 0xa4, 0x97, 0x60, 0x95, 0x49, 0xf7, 0xa6, 0xc2, 0xfd, 0x15, 0x46, 0x57, 0x74, 0x79, 0xfd,
	0xec, 0xaf, 0x16, 0xe4, 0x8e, 0x90, 0xac, 0x28, 0x78, 0x43, 0x42, 0xab, 0x91, 0x84, 0x43, 0xb0,
	0x22, 0xbc, 0x70, 0xde, 0xb6, 0x74, 0xbc, 0xf7, 0x6a, 0x01, 0x65, 0x53, 0xfe, 0xce, 0x3a, 0x87,
	0x2e, 0x1f, 0xd2, 0x28, 0xcc, 0xf7, 0x4d, 0x32, 0x04, 0x9d, 0x72, 0x3c, 0x76, 0x81, 0x82, 0x5d,
	0xd0, 0x90, 0x57, 0xc5, 0xbc, 0x0e, 0xd8, 0x9b, 0xf0, 0x32, 0xe5, 0xd7, 0x30, 0x90, 0xa9, 0x7e,
	0x2c, 0x87, 0xc3, 0x7a, 0x2d, 0x9b, 0xb5, 0xde, 0x51, 0xa2, 0xfc, 0x5f, 0xc0, 0x2c, 0xdc, 0xf7,
	0xf5, 0x71, 0x8e, 0xa0, 0x43, 0xb2, 0x2c, 0x29, 0x94, 0x31, 0x04, 0xc5, 0x3f, 0x70, 0x93, 0xfb,
	0x8f, 0x0a, 0xbd, 0xe2, 0xb8, 0x6c, 0x48, 0x3d, 0x63, 0x4c, 0x79, 0xdc, 0xf7, 0x02, 0xb8, 0x5d,
	0xe2, 0x29, 0x68, 0xf3, 0x38, 0x10, 0x92, 0x29, 0x28, 0xd9, 0x50, 0xc0, 0xae, 0x93, 0x38, 0xf6,
	0x57, 0x01, 0xf6, 0xca, 0x47, 0x50, 0x73, 0x9a, 0xc8, 0x7c, 0xb6, 0x9c, 0x32, 0x9b, 0xd1, 0x76,
	0x0e, 0x7a, 0x79, 0x99, 0x65, 0x8f, 0x1e, 0x08, 0xd0, 0xce, 0xf4, 0x61, 0x5d, 0x13, 0x52, 0x6f,
	0xeb, 0xea, 0x77, 0xd0, 0xe1, 0x83, 0x70, 0x68, 0x9c, 0x23, 0xce, 0x17, 0xa0, 0xf9, 0x7c, 0x0a,
	0x70, 0x0e, 0xba, 0x08, 0x1f, 0x09, 0xf8, 0xf6, 0xb4, 0xe0, 0xb8, 0x05, 0xc9, 0x3d, 0xae, 0xea,
	0xa4, 0x57, 0xc5, 0xd5, 0x86, 0x04, 0xc3, 0xa5, 0x6b, 0x89, 0xd3, 0xaa, 0xb8, 0xda, 0x35, 0xfa,
	0x06, 0x2c, 0x9f, 0x2e, 0xbd, 0x39, 0x76, 0x97, 0x97, 0xb2, 0xf6, 0x9a, 0xe8, 0x08, 0x3f, 0x92,
	0xe9, 0x9b, 0x5a, 0xf9, 0x0c, 0x74, 0xee, 0x84, 0x9d, 0x34, 0x01, 0x84, 0x8f, 0x4b, 0x78, 0xb5,
	0x1f, 0xdd, 0x7f, 0x55, 0xd0, 0xca, 0xcb, 0xf3, 0x79, 0x4d, 0xad, 0x51, 0xa1, 0xd6, 0x56, 0x77,
	0xec, 0x25, 0xd7, 0xa7, 0xaa, 0x5c, 0xc3, 0x8a, 0x5c, 0x32, 0xd5, 0xc5, 0xae, 0x5e, 0x87, 0x3b,
	0x7a, 0x49, 0xec, 0xb7, 0xaf, 0x09, 0x76, 0xdc, 0x2c, 0x98, 0x74, 0xfb, 0x72, 0x47, 0xb1, 0x71,
	0x4d, 0xb1, 0x0d, 0xb0, 0x26, 0xd9, 0xb8, 0x26, 0xd9, 0x06, 0x58, 0xd3, 0x6c, 0x5c, 0xd3, 0x4c,
	0x02, 0x2f, 0x5f, 0x13, 0xed, 0x43, 0xa3, 0x68, 0xd2, 0xeb, 0x7c, 0x57, 0xb5, 0x83, 0xba, 0x6a,
	0xf2, 0x9a, 0x57, 0xdf, 0x40, 0x76, 0x66, 0x71, 0xf1, 0x6e, 0xa0, 0x83, 0x1f, 0xfc, 0xe1, 0x66,
	0xcf, 0x2f, 0xf5, 0x17, 0x44, 0xfe, 0x13, 0x7c, 0x55, 0x7b, 0xea, 0x94, 0x46, 0xc2, 0x8b, 0x40,
	0x17, 0x7f, 0xb7, 0xc0, 0xa8, 0x8a, 0x69, 0x40, 0xef, 0x76, 0xf5, 0xa7, 0x1f, 0x85, 0x81, 0xfd,
	0xce, 0xe9, 0x81, 0xca, 0x84, 0xb4, 0x5b, 0xce, 0x00, 0xf4, 0xd2, 0xd5, 0x56, 0xd8, 0x98, 0x30,
	0xb7, 0x85, 0xb0, 0x55, 0xee, 0x28, 0xc9, 0xb6, 0xdb, 0x7c, 0x21, 0x09, 0xb5, 0x3b, 0x7c, 0x21,
	0x49, 0xb3, 0xbb, 0xec, 0xbf, 0x04, 0xbe, 0x27, 0x11, 0xc9, 0x09, 0xae, 0x7b, 0x6c, 0x08, 0x0e,
	0xb6, 0x18, 0xb2, 0x35, 0x36, 0x92, 0xb4, 0x82, 0x04, 0x5b, 0xbf, 0xf8, 0x0d, 0xb4, 0xf2, 0x11,
	0xb1, 0xc0, 0x90, 0xc5, 0xa1, 0xf7, 0x3b, 0x5e, 0x17, 0x3e, 0x20, 0x7c, 0x97, 0x95, 0xc9, 0x82,
	0x8b, 0xd9, 0x8a, 0x6b, 0x85, 0x47, 0xe2, 0x31, 0x71, 0xc5, 0xff, 0x71, 0x0c, 0xf9, 0x9a, 0xa0,
	0xa1, 0xfd, 0x5f, 0x00, 0x00, 0x00, 0xff, 0xff, 0xed, 0x44, 0xea, 0xa7, 0x77, 0x09, 0x00, 0x00,
}
