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

package pd

import (
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Client is a PD (Placement Driver) client.
// It should not be used after calling Close().
type Client interface {
	// GetClusterID gets the cluster ID from PD.
	GetClusterID(ctx context.Context) uint64
	// GetTS gets a timestamp from PD.
	GetTS(ctx context.Context) (int64, int64, error)
	// GetRegion gets a region and its leader Peer from PD by key.
	// The region may expire after split. Caller is responsible for caching and
	// taking care of region change.
	// Also it may return nil if PD finds no Region for the key temporarily,
	// client should retry later.
	GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error)
	// GetRegionByID gets a region and its leader Peer from PD by id.
	GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error)
	// GetStore gets a store from PD by store id.
	// The store may expire later. Caller is responsible for caching and taking care
	// of store change.
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	// Close closes the client.
	Close()
	// GetTSAsync gets a timestamp from PD, it's a nonblock function.
	GetTSAsync(ctx context.Context) *TSOResponse
}

type tsoRequest struct {
	done     chan error
	physical int64
	logical  int64
}

const (
	pdTimeout             = 3 * time.Second
	maxMergeTSORequests   = 10000
	maxInitClusterRetries = 100
)

var (
	// errFailInitClusterID is returned when failed to load clusterID from all supplied PD addresses.
	errFailInitClusterID = errors.New("[pd] failed to get cluster id")
	// errClosing is returned when request is canceled when client is closing.
	errClosing = errors.New("[pd] closing")
	// errTSOLength is returned when the number of response timestamps is inconsistent with request.
	errTSOLength = errors.New("[pd] tso length in rpc response is incorrect")
)

type client struct {
	urls        []string
	clusterID   uint64
	tsoRequests chan *tsoRequest

	connMu struct {
		sync.RWMutex
		clientConns map[string]*grpc.ClientConn
		leader      string
	}

	tsDeadlineCh  chan deadline
	checkLeaderCh chan struct{}

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	asyncTSOMu struct {
		sync.RWMutex
		cli *asyncTSOClient
	}
}

// NewClient creates a PD client.
func NewClient(pdAddrs []string) (Client, error) {
	log.Infof("[pd] create pd client with endpoints %v", pdAddrs)
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		urls:          addrsToUrls(pdAddrs),
		tsoRequests:   make(chan *tsoRequest, maxMergeTSORequests),
		tsDeadlineCh:  make(chan deadline, 1),
		checkLeaderCh: make(chan struct{}, 1),
		ctx:           ctx,
		cancel:        cancel,
	}
	c.connMu.clientConns = make(map[string]*grpc.ClientConn)

	if err := c.initClusterID(); err != nil {
		return nil, errors.Trace(err)
	}
	if err := c.updateLeader(); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("[pd] init cluster id %v", c.clusterID)

	c.wg.Add(3)
	go c.tsLoop()
	go c.tsCancelLoop()
	go c.leaderLoop()

	c.asyncTSOMu.cli = newAsyncTSOClient(c.pdTSOClient(), c.requestHeader())

	// TODO: Update addrs from server continuously by using GetMember.

	return c, nil
}

func (c *client) initClusterID() error {
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	for i := 0; i < maxInitClusterRetries; i++ {
		for _, u := range c.urls {
			members, err := c.getMembers(ctx, u)
			if err != nil || members.GetHeader() == nil {
				log.Errorf("[pd] failed to get cluster id: %v", err)
				continue
			}
			c.clusterID = members.GetHeader().GetClusterId()
			return nil
		}

		time.Sleep(time.Second)
	}

	return errors.Trace(errFailInitClusterID)
}

func (c *client) updateLeader() error {
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	for _, u := range c.urls {
		members, err := c.getMembers(ctx, u)
		if err != nil || members.GetLeader() == nil || len(members.GetLeader().GetClientUrls()) == 0 {
			continue
		}
		if err = c.switchLeader(members.GetLeader().GetClientUrls()); err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	return errors.Errorf("failed to get leader from %v", c.urls)
}

func (c *client) getMembers(ctx context.Context, url string) (*pdpb.GetMembersResponse, error) {
	cc, err := c.getOrCreateGRPCConn(url)
	if err != nil {
		return nil, errors.Trace(err)
	}
	members, err := pdpb.NewPDClient(cc).GetMembers(ctx, &pdpb.GetMembersRequest{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return members, nil
}

func (c *client) switchLeader(addrs []string) error {
	// FIXME: How to safely compare leader urls? For now, only allows one client url.
	addr := addrs[0]

	c.connMu.RLock()
	oldLeader := c.connMu.leader
	c.connMu.RUnlock()

	if addr == oldLeader {
		return nil
	}

	log.Infof("[pd] leader switches to: %v, previous: %v", addr, oldLeader)
	if _, err := c.getOrCreateGRPCConn(addr); err != nil {
		return errors.Trace(err)
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()
	c.connMu.leader = addr
	return nil
}

func (c *client) getOrCreateGRPCConn(addr string) (*grpc.ClientConn, error) {
	c.connMu.RLock()
	conn, ok := c.connMu.clientConns[addr]
	c.connMu.RUnlock()
	if ok {
		return conn, nil
	}

	cc, err := grpc.Dial(addr, grpc.WithDialer(func(addr string, d time.Duration) (net.Conn, error) {
		u, err := url.Parse(addr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// For tests.
		if u.Scheme == "unix" || u.Scheme == "unixs" {
			return net.DialTimeout(u.Scheme, u.Host, d)
		}
		return net.DialTimeout("tcp", u.Host, d)
	}), grpc.WithInsecure()) // TODO: Support HTTPS.
	if err != nil {
		return nil, errors.Trace(err)
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()
	if old, ok := c.connMu.clientConns[addr]; ok {
		cc.Close()
		return old, nil
	}

	c.connMu.clientConns[addr] = cc
	return cc, nil
}

func (c *client) leaderLoop() {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	for {
		select {
		case <-c.checkLeaderCh:
		case <-time.After(time.Minute):
		case <-ctx.Done():
			return
		}

		if err := c.updateLeader(); err != nil {
			log.Errorf("[pd] failed updateLeader: %v", err)
		}
	}
}

type deadline struct {
	timer  <-chan time.Time
	done   chan struct{}
	cancel context.CancelFunc
}

func (c *client) tsCancelLoop() {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	for {
		select {
		case d := <-c.tsDeadlineCh:
			select {
			case <-d.timer:
				log.Error("tso request is canceled due to timeout")
				d.cancel()
			case <-d.done:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *client) tsLoop() {
	defer c.wg.Done()

	loopCtx, loopCancel := context.WithCancel(c.ctx)
	defer loopCancel()

	var requests []*tsoRequest
	var stream pdpb.PD_TsoClient
	var cancel context.CancelFunc

	for {
		var err error

		if stream == nil {
			var ctx context.Context
			ctx, cancel = context.WithCancel(c.ctx)
			stream, err = c.leaderClient().Tso(ctx)
			if err != nil {
				log.Errorf("[pd] create tso stream error: %v", err)
				cancel()
				c.revokeTSORequest(err)
				select {
				case <-time.After(time.Second):
				case <-loopCtx.Done():
					return
				}
				continue
			}
		}

		select {
		case first := <-c.tsoRequests:
			requests = append(requests, first)
			pending := len(c.tsoRequests)
			for i := 0; i < pending; i++ {
				requests = append(requests, <-c.tsoRequests)
			}
			done := make(chan struct{})
			dl := deadline{
				timer:  time.After(pdTimeout),
				done:   done,
				cancel: cancel,
			}
			select {
			case c.tsDeadlineCh <- dl:
			case <-loopCtx.Done():
				return
			}
			err = c.processTSORequests(stream, requests)
			close(done)
			requests = requests[:0]
		case <-loopCtx.Done():
			return
		}

		if err != nil {
			log.Errorf("[pd] getTS error: %v", err)
			cancel()
			stream, cancel = nil, nil
		}
	}
}

func (c *client) processTSORequests(stream pdpb.PD_TsoClient, requests []*tsoRequest) error {
	start := time.Now()
	//	ctx, cancel := context.WithTimeout(c.ctx, pdTimeout)
	req := &pdpb.TsoRequest{
		Header: c.requestHeader(),
		Count:  uint32(len(requests)),
	}
	if err := stream.Send(req); err != nil {
		c.finishTSORequest(requests, 0, 0, err)
		c.scheduleCheckLeader()
		return errors.Trace(err)
	}
	resp, err := stream.Recv()
	if err != nil {
		c.finishTSORequest(requests, 0, 0, errors.Trace(err))
		c.scheduleCheckLeader()
		return errors.Trace(err)
	}
	requestDuration.WithLabelValues("tso").Observe(time.Since(start).Seconds())
	if err == nil && resp.GetCount() != uint32(len(requests)) {
		err = errTSOLength
	}
	if err != nil {
		c.finishTSORequest(requests, 0, 0, errors.Trace(err))
		return errors.Trace(err)
	}

	physical, logical := resp.GetTimestamp().GetPhysical(), resp.GetTimestamp().GetLogical()
	// Server returns the highest ts.
	logical -= int64(resp.GetCount() - 1)
	c.finishTSORequest(requests, physical, logical, nil)
	return nil
}

func (c *client) finishTSORequest(requests []*tsoRequest, physical, firstLogical int64, err error) {
	for i := 0; i < len(requests); i++ {
		requests[i].physical, requests[i].logical = physical, firstLogical+int64(i)
		requests[i].done <- err
	}
}

func (c *client) revokeTSORequest(err error) {
	n := len(c.tsoRequests)
	for i := 0; i < n; i++ {
		req := <-c.tsoRequests
		req.done <- errors.Trace(err)
	}
}

func (c *client) Close() {
	c.cancel()
	c.wg.Wait()

	c.revokeTSORequest(errClosing)

	c.asyncTSOMu.Lock()
	c.asyncTSOMu.cli.close()
	c.asyncTSOMu.cli = nil
	c.asyncTSOMu.Unlock()

	c.connMu.Lock()
	defer c.connMu.Unlock()
	for _, cc := range c.connMu.clientConns {
		if err := cc.Close(); err != nil {
			log.Errorf("[pd] failed close grpc clientConn: %v", err)
		}
	}
}

func (c *client) leaderClient() pdpb.PDClient {
	c.connMu.RLock()
	defer c.connMu.RUnlock()

	return pdpb.NewPDClient(c.connMu.clientConns[c.connMu.leader])
}

func (c *client) scheduleCheckLeader() {
	select {
	case c.checkLeaderCh <- struct{}{}:
	default:
	}
}

func (c *client) GetClusterID(context.Context) uint64 {
	return c.clusterID
}

var tsoReqPool = sync.Pool{
	New: func() interface{} {
		return &tsoRequest{
			done: make(chan error, 1),
		}
	},
}

func (c *client) GetTS(ctx context.Context) (int64, int64, error) {
	start := time.Now()
	defer func() { cmdDuration.WithLabelValues("tso").Observe(time.Since(start).Seconds()) }()

	req := tsoReqPool.Get().(*tsoRequest)
	c.tsoRequests <- req

	select {
	case err := <-req.done:
		if err != nil {
			cmdFailedDuration.WithLabelValues("tso").Observe(time.Since(start).Seconds())
			return 0, 0, errors.Trace(err)
		}
		physical, logical := req.physical, req.logical
		tsoReqPool.Put(req)
		return physical, logical, err
	case <-ctx.Done():
		return 0, 0, errors.Trace(ctx.Err())
	}
}

func (c *client) GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	start := time.Now()
	defer func() { cmdDuration.WithLabelValues("get_region").Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().GetRegion(ctx, &pdpb.GetRegionRequest{
		Header:    c.requestHeader(),
		RegionKey: key,
	})
	requestDuration.WithLabelValues("get_region").Observe(time.Since(start).Seconds())
	cancel()

	if err != nil {
		cmdFailedDuration.WithLabelValues("get_region").Observe(time.Since(start).Seconds())
		c.scheduleCheckLeader()
		return nil, nil, errors.Trace(err)
	}
	return resp.GetRegion(), resp.GetLeader(), nil
}

func (c *client) GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error) {
	start := time.Now()
	defer func() { cmdDuration.WithLabelValues("get_region_byid").Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().GetRegionByID(ctx, &pdpb.GetRegionByIDRequest{
		Header:   c.requestHeader(),
		RegionId: regionID,
	})
	requestDuration.WithLabelValues("get_region_byid").Observe(time.Since(start).Seconds())
	cancel()

	if err != nil {
		cmdFailedDuration.WithLabelValues("get_region_byid").Observe(time.Since(start).Seconds())
		c.scheduleCheckLeader()
		return nil, nil, errors.Trace(err)
	}
	return resp.GetRegion(), resp.GetLeader(), nil
}

func (c *client) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	start := time.Now()
	defer func() { cmdDuration.WithLabelValues("get_store").Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().GetStore(ctx, &pdpb.GetStoreRequest{
		Header:  c.requestHeader(),
		StoreId: storeID,
	})
	requestDuration.WithLabelValues("get_store").Observe(time.Since(start).Seconds())
	cancel()

	if err != nil {
		cmdFailedDuration.WithLabelValues("get_store").Observe(time.Since(start).Seconds())
		c.scheduleCheckLeader()
		return nil, errors.Trace(err)
	}
	store := resp.GetStore()
	if store == nil {
		return nil, errors.New("[pd] store field in rpc response not set")
	}
	if store.GetState() == metapb.StoreState_Tombstone {
		return nil, nil
	}
	return store, nil
}

func (c *client) requestHeader() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: c.clusterID,
	}
}

func addrsToUrls(addrs []string) []string {
	// Add default schema "http://" to addrs.
	urls := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if strings.Contains(addr, "://") {
			urls = append(urls, addr)
		} else {
			urls = append(urls, "http://"+addr)
		}
	}
	return urls
}

func (c *client) pdTSOClient() pdpb.PD_TsoClient {
	for {
		stream, err := c.leaderClient().Tso(c.ctx)
		if err == nil {
			return stream
		}

		log.Errorf("[pd] create tso stream error: %v", err)
		select {
		case <-time.After(time.Second):
		case <-c.ctx.Done():
			return nil
		}
	}
}

// TSOResponse is the result of GetTSAsync.
type TSOResponse struct {
	ch       chan struct{}
	ctx      context.Context
	err      error
	physical int64
	logical  int64
}

// Wait gets the result data, it may block the caller if data not available yet.
func (r *TSOResponse) Wait() (int64, int64, error) {
	select {
	case <-r.ch:
	case <-r.ctx.Done():
		return 0, 0, errors.Trace(r.ctx.Err())
	}
	return r.physical, r.logical, errors.Trace(r.err)
}

type asyncTSOClient struct {
	cli    pdpb.PD_TsoClient
	header *pdpb.RequestHeader

	input  chan *TSOResponse
	workCh chan []*TSOResponse

	mu struct {
		sync.RWMutex
		unhealth bool
	}
}

func newAsyncTSOClient(cli pdpb.PD_TsoClient, header *pdpb.RequestHeader) *asyncTSOClient {
	const chanSize = 64 // 64 is a tuned value.
	ret := &asyncTSOClient{
		cli:    cli,
		header: header,

		input:  make(chan *TSOResponse, chanSize),
		workCh: make(chan []*TSOResponse, chanSize),
	}
	go ret.backgroundRecvWorker()
	go ret.backgroundSendWorker()
	return ret
}

func (async *asyncTSOClient) close() {
	close(async.input)
	for unconsumed := range async.workCh {
		for i := 0; i < len(unconsumed); i++ {
			unconsumed[i].err = errors.New("tso client closed")
			close(unconsumed[i].ch)
		}
	}
}

func (async *asyncTSOClient) backgroundSendWorker() {
	// Close workCh will notify the receive goroutine to exit.
	defer close(async.workCh)

	for {
		batch, closed := getBatch(async.input)
		if closed {
			return
		}

		req := &pdpb.TsoRequest{
			Header: async.header,
			Count:  uint32(len(batch)),
		}
		err := async.cli.Send(req)
		if err != nil {
			finishBatch(batch, 0, 0, err)
			async.mu.Lock()
			async.mu.unhealth = true
			async.mu.Unlock()
			return
		}

		async.workCh <- batch
	}
}

func (async *asyncTSOClient) backgroundRecvWorker() {
	for batch := range async.workCh {
		resp, err := async.cli.Recv()
		if err != nil {
			async.mu.Lock()
			async.mu.unhealth = true
			async.mu.Unlock()
		}
		physical, logical := resp.GetTimestamp().GetPhysical(), resp.GetTimestamp().GetLogical()
		finishBatch(batch, physical, logical, errors.Trace(err))
	}
}

// getBatch gets a batch of data from the channel, returns the batch
// and whether the channel is closed.
// NOTE: this function assumes it's the only consumer of the channel.
func getBatch(ch <-chan *TSOResponse) ([]*TSOResponse, bool) {
	first, ok := <-ch
	if !ok {
		return nil, true
	}
	remain := len(ch)
	batch := make([]*TSOResponse, 0, remain+1)
	batch = append(batch, first)
	for i := 0; i < remain; i++ {
		tmp := <-ch
		batch = append(batch, tmp)
	}
	return batch, false
}

func finishBatch(batch []*TSOResponse, physical, logical int64, err error) {
	for i := 0; i < len(batch); i++ {
		val := batch[i]
		val.physical = physical
		val.logical = logical + int64(i)
		val.err = err
		close(val.ch)
	}
}

func (async *asyncTSOClient) call(ctx context.Context) (*TSOResponse, error) {
	async.mu.RLock()
	if async.mu.unhealth {
		return nil, errors.New("This asyncTSOClient maybe stale")
	}
	async.mu.RUnlock()

	ret := &TSOResponse{
		ch:  make(chan struct{}),
		ctx: ctx,
	}
	async.input <- ret
	return ret, nil
}

func (c *client) GetTSAsync(ctx context.Context) *TSOResponse {
	for {
		c.asyncTSOMu.RLock()
		cli := c.asyncTSOMu.cli
		c.asyncTSOMu.RUnlock()

		ret, err := cli.call(ctx)
		if err == nil {
			return ret
		}

		c.scheduleCheckLeader()
		newCli := newAsyncTSOClient(c.pdTSOClient(), c.requestHeader())

		c.asyncTSOMu.Lock()
		if c.asyncTSOMu.cli == cli {
			cli.close()
			c.asyncTSOMu.cli = newCli
		} else {
			newCli.close()
		}
		c.asyncTSOMu.Unlock()
	}
}
