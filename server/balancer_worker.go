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
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

const (
	defaultBalanceInterval = 60 * time.Second
	defaultBalanceCount    = 3
)

type balanceWorker struct {
	sync.RWMutex

	wg       sync.WaitGroup
	interval time.Duration
	cluster  *raftCluster

	balanceOperators map[uint64]*BalanceOperator
	balanceCount     int
	balancers        []Balancer

	quit chan struct{}
}

func newBalanceWorker(cluster *raftCluster, balancers ...Balancer) *balanceWorker {
	bw := &balanceWorker{
		interval:         defaultBalanceInterval,
		cluster:          cluster,
		balanceOperators: make(map[uint64]*BalanceOperator),
		balanceCount:     defaultBalanceCount,
		balancers:        balancers,
		quit:             make(chan struct{}),
	}

	bw.wg.Add(1)
	go bw.run()
	return bw
}

func (bw *balanceWorker) run() error {
	defer bw.wg.Done()

	ticker := time.NewTicker(bw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-bw.quit:
			return nil
		case <-ticker.C:
			err := bw.doBalance()
			if err != nil {
				log.Warnf("do balance failed - %v", errors.ErrorStack(err))
			}
		}
	}
}

func (bw *balanceWorker) stop() {
	close(bw.quit)
	bw.wg.Wait()

	bw.Lock()
	defer bw.Unlock()

	bw.balanceOperators = map[uint64]*BalanceOperator{}
}

func (bw *balanceWorker) addBalanceOperator(regionID uint64, op *BalanceOperator) bool {
	bw.Lock()
	defer bw.Unlock()

	_, ok := bw.balanceOperators[regionID]
	if ok {
		return false
	}

	bw.balanceOperators[regionID] = op
	return true
}

func (bw *balanceWorker) removeBalanceOperator(regionID uint64) {
	bw.Lock()
	defer bw.Unlock()

	delete(bw.balanceOperators, regionID)
}

func (bw *balanceWorker) getBalanceOperator(regionID uint64) *BalanceOperator {
	bw.RLock()
	defer bw.RUnlock()

	return bw.balanceOperators[regionID]
}

func (bw *balanceWorker) doBalance() error {
	for _, balancer := range bw.balancers {
		bw.RLock()
		balanceCount := len(bw.balanceOperators)
		bw.RUnlock()

		if balanceCount >= bw.balanceCount {
			return nil
		}

		// TODO: support select balance count in balancer.
		balanceOperator, err := balancer.Balance(bw.cluster)
		if err != nil {
			return errors.Trace(err)
		}

		bw.addBalanceOperator(balanceOperator.region.GetId(), balanceOperator)
	}

	return nil
}
