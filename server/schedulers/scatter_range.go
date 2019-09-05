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

package schedulers

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"github.com/pingcap/pd/pkg/apiutil"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pkg/errors"
	"github.com/unrolled/render"
)

func init() {
	schedule.RegisterArgsToMapper("scatter-range", func(args []string) (schedule.ConfigMapper, error) {
		if len(args) != 3 {
			return nil, errors.New("should specify the range and the name")
		}
		mapper := make(schedule.ConfigMapper)
		mapper["start-key"] = args[0]
		mapper["end-key"] = args[1]
		mapper["range-name"] = args[2]
		return mapper, nil
	})

	schedule.RegisterScheduler("scatter-range", func(opController *schedule.OperatorController, storage *core.Storage, mapper schedule.ConfigMapper) (schedule.Scheduler, error) {
		if len(mapper) != 3 {
			return nil, errors.New("should specify the range and the name")
		}
		rangeName := mapper["range-name"].(string)
		if len(rangeName) == 0 {
			return nil, errors.New("the range name is invalid")
		}
		fmt.Println("mapper", mapper)
		config := &ScatterRangeSchedulerConf{
			mu:        &sync.RWMutex{},
			storage:   storage,
			StartKey:  [](byte)(mapper["start-key"].(string)),
			EndKey:    [](byte)(mapper["end-key"].(string)),
			RangeName: rangeName,
		}

		// persist config firstly
		err := storage.SaveScheduleConfig(config.getScheduleName(), config)
		if err != nil {
			return nil, err
		}
		return newScatterRangeScheduler(opController, storage, config), nil
	})
}

const scatterRangeScheduleType = "scatter-range"

type ScatterRangeSchedulerConf struct {
	mu        *sync.RWMutex
	storage   *core.Storage
	RangeName string `json:"range-name"`
	StartKey  []byte `json:"start-key"`
	EndKey    []byte `json:"end-key"`
}

func (conf *ScatterRangeSchedulerConf) BuildWithArgs(args []string) error {
	if len(args) != 3 {
		return errors.New("scatter range need 3 arguments to setup config")
	}
	conf.mu.Lock()
	defer conf.mu.Unlock()

	conf.RangeName = args[0]
	conf.StartKey = []byte(args[1])
	conf.EndKey = []byte(args[2])
	return nil
}

func (conf *ScatterRangeSchedulerConf) Clone() *ScatterRangeSchedulerConf {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	cpStartkey := make([]byte, len(conf.StartKey))
	copy(cpStartkey, conf.StartKey)
	cpEndKey := make([]byte, len(conf.EndKey))
	copy(cpEndKey, conf.EndKey)
	return &ScatterRangeSchedulerConf{
		mu:        &sync.RWMutex{},
		StartKey:  cpStartkey,
		EndKey:    cpEndKey,
		RangeName: conf.RangeName,
	}
}

func (conf *ScatterRangeSchedulerConf) Persist() {
	name := conf.getScheduleName()
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	conf.storage.SaveScheduleConfig(name, conf.Clone())
}

func (conf *ScatterRangeSchedulerConf) Reload() {

}

func (conf *ScatterRangeSchedulerConf) GetRangeName() string {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return conf.RangeName
}

func (conf *ScatterRangeSchedulerConf) GetStartKey() []byte {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return conf.StartKey
}

func (conf *ScatterRangeSchedulerConf) GetEndKey() []byte {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return conf.EndKey
}

func (conf *ScatterRangeSchedulerConf) getScheduleName() string {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return fmt.Sprintf("scatter-range-%s", conf.RangeName)
}

type scatterRangeScheduler struct {
	*baseScheduler
	name          string
	config        *ScatterRangeSchedulerConf
	balanceLeader schedule.Scheduler
	balanceRegion schedule.Scheduler
	handler       http.Handler
}

// newScatterRangeScheduler creates a scheduler that balances the distribution of leaders and regions that in the specified key range.
func newScatterRangeScheduler(opController *schedule.OperatorController, storage *core.Storage, config *ScatterRangeSchedulerConf) schedule.Scheduler {
	base := newBaseScheduler(opController)

	name := config.getScheduleName()
	handler := newScatterRangeHandler(config)
	scheduler := &scatterRangeScheduler{
		baseScheduler: base,
		config:        config,
		handler:       handler,
		name:          name,
		balanceLeader: newBalanceLeaderScheduler(
			opController,
			WithBalanceLeaderName("scatter-range-leader"),
			WithBalanceLeaderCounter(scatterRangeLeaderCounter),
		),
		balanceRegion: newBalanceRegionScheduler(
			opController,
			WithBalanceRegionName("scatter-range-region"),
			WithBalanceRegionCounter(scatterRangeRegionCounter),
		),
	}
	return scheduler
}

func (l *scatterRangeScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.handler.ServeHTTP(w, r)
}

func (l *scatterRangeScheduler) GetName() string {
	return l.name
}

func (l *scatterRangeScheduler) GetType() string {
	return scatterRangeScheduleType
}

func (l *scatterRangeScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return l.opController.OperatorCount(operator.OpRange) < cluster.GetRegionScheduleLimit()
}

func (l *scatterRangeScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()
	// isolate a new cluster according to the key range
	c := schedule.GenRangeCluster(cluster, l.config.StartKey, l.config.EndKey)
	c.SetTolerantSizeRatio(2)
	ops := l.balanceLeader.Schedule(c)
	if len(ops) > 0 {
		ops[0].SetDesc(fmt.Sprintf("scatter-range-leader-%s", l.config.RangeName))
		ops[0].AttachKind(operator.OpRange)
		schedulerCounter.WithLabelValues(l.GetName(), "new-leader-operator").Inc()
		return ops
	}
	ops = l.balanceRegion.Schedule(c)
	if len(ops) > 0 {
		ops[0].SetDesc(fmt.Sprintf("scatter-range-region-%s", l.config.RangeName))
		ops[0].AttachKind(operator.OpRange)
		schedulerCounter.WithLabelValues(l.GetName(), "new-region-operator").Inc()
		return ops
	}
	schedulerCounter.WithLabelValues(l.GetName(), "no-need").Inc()
	return nil
}

type scatterRangeHandler struct {
	scheduleName string
	storage      *core.Storage
	rd           *render.Render
	config       *ScatterRangeSchedulerConf
}

func (handler *scatterRangeHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}
	var args []string
	name, ok := input["range-name"].(string)
	if ok {
		if name != handler.config.GetRangeName() {
			handler.rd.JSON(w, http.StatusInternalServerError, errors.New("Cannot change the range name, please delete this schedule"))
			return
		}
		args = append(args, name)
	} else {
		args = append(args, handler.config.GetRangeName())
	}

	startKey, ok := input["start-key"].(string)
	if ok {
		args = append(args, startKey)
	} else {
		args = append(args, string(handler.config.GetStartKey()))
	}

	endKey, ok := input["end-key"].(string)
	if ok {
		args = append(args, endKey)
	} else {
		args = append(args, string(handler.config.GetEndKey()))
	}
	handler.config.BuildWithArgs(args)
	handler.config.Persist()

	handler.rd.JSON(w, http.StatusOK, nil)
}

func (handler *scatterRangeHandler) ListConfig(w http.ResponseWriter, r *http.Request) {
	conf := handler.config.Clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

func newScatterRangeHandler(config *ScatterRangeSchedulerConf) http.Handler {
	h := &scatterRangeHandler{
		config: config,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.UpdateConfig).Methods("POST")
	router.HandleFunc("/list", h.ListConfig).Methods("GET")
	return router
}
