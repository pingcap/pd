// Copyright 2017 PingCAP, Inc.
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

package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pingcap/pd/pkg/faketikv"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
	"github.com/pingcap/pd/server/schedule"
	log "github.com/sirupsen/logrus"
	// Register schedulers.
	_ "github.com/pingcap/pd/server/schedulers"
	// Register namespace classifiers.
	_ "github.com/pingcap/pd/table"
)

var confName = flag.String("conf", "", "config name")

func main() {
	flag.Parse()

	schedule.Simulating = true
	_, local, clean := NewSingleServer()
	err := local.Run()
	if err != nil {
		log.Fatal("run server error:", err)
	}
	driver := faketikv.NewDriver(local.GetAddr(), *confName)
	err = driver.Prepare()
	if err != nil {
		log.Fatal("simulator prepare error:", err)
	}
	tick := time.NewTicker(100 * time.Millisecond)
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	simResult := "FAIL"

EXIT:
	for {
		select {
		case <-tick.C:
			driver.Tick()
			if driver.Check() {
				simResult = "OK"
				break EXIT
			}
		case <-sc:
			break EXIT
		}
	}

	driver.Stop()
	clean()

	log.Infof("Simulation finish. Conf: %s, TotalTick: %d, Result: %s", *confName, driver.TickCount(), simResult)
}

// NewSingleServer creates a pd server for simulator.
func NewSingleServer() (*server.Config, *server.Server, server.CleanupFunc) {
	cfg := server.NewTestSingleConfig()
	s, err := server.CreateServer(cfg, api.NewHandler)
	if err != nil {
		panic("create server failed")
	}

	cleanup := func() {
		s.Close()
		cleanServer(cfg)
	}
	return cfg, s, cleanup
}

func cleanServer(cfg *server.Config) {
	// Clean data directory
	os.RemoveAll(cfg.DataDir)
}
