//
// Copyright 2016 Gregory Trubetskoy. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package receiver

import (
	"log"
	"sync"
	"time"

	"github.com/tgres/tgres/aggregator"
)

type wrkCtl struct {
	wg, startWg *sync.WaitGroup
	id          string
}

func (w *wrkCtl) ident() string { return w.id }
func (w *wrkCtl) onEnter()      { w.wg.Add(1) }
func (w *wrkCtl) onExit()       { w.wg.Done() }
func (w *wrkCtl) onStarted()    { w.startWg.Done() }

type wController interface {
	ident() string
	onEnter()
	onExit()
	onStarted()
}

var startAllWorkers = func(r *Receiver, startWg *sync.WaitGroup) {
	startFlushers(r, startWg)
	startAggWorker(r, startWg)
	startPacedMetricWorker(r, startWg)
}

var doStart = func(r *Receiver) {
	log.Printf("Receiver: Caching data sources...")
	start := time.Now()
	if err := r.dsc.preLoad(); err != nil {
		log.Printf("Receiver: error caching data sources: %v", err)
	}
	dur := time.Now().Sub(start)
	log.Printf("Receiver: Cached %d data sources in %v.", len(r.dsc.byIdent), dur)

	log.Printf("Receiver: starting...")

	var startWg sync.WaitGroup
	startAllWorkers(r, &startWg)

	// Wait for workers/flushers to start correctly
	startWg.Wait()
	log.Printf("Receiver: All workers running, starting director.")

	startWg.Add(1)
	go director(&wrkCtl{wg: &r.directorWg, startWg: &startWg, id: "director"}, r.dpCh, r.NWorkers, r.cluster, r, r.dsc, r.flusher)
	startWg.Wait()

	log.Printf("Receiver: Starting runtime cpu/mem reporter.")
	go reportRuntime(r)

	log.Printf("Receiver: Ready.")
}

var stopDirector = func(r *Receiver) {
	log.Printf("Closing director channel...")
	r.dpCh <- nil // signal to close
	r.directorWg.Wait()
	log.Printf("Director finished.")
}

var doStop = func(r *Receiver, clstr clusterer) {
	// Order matters here
	stopPacedMetricWorker(r.pacedMetricCh, &r.pacedMetricWg)
	stopAggWorker(r.aggCh, &r.aggWg)
	stopDirector(r)
	stopFlushers(r.flusher, &r.flusherWg)
	log.Printf("Leaving cluster...")
	clstr.Leave(1 * time.Second)
	clstr.Shutdown()
	log.Printf("Left cluster.")
}

var stopWorkers = func(workerChs []chan *cachedDs, workerWg *sync.WaitGroup) {
	log.Printf("stopWorkers(): closing all worker channels...")
	for _, ch := range workerChs {
		close(ch)
	}
	log.Printf("stopWorkers(): waiting for workers to finish...")
	workerWg.Wait()
	log.Printf("stopWorkers(): all workers finished.")
}

var stopFlushers = func(flusher dsFlusherBlocking, flusherWg *sync.WaitGroup) {
	log.Printf("stopFlushers(): stopping flusher(s)...")
	flusher.stop()
	log.Printf("stopFlushers(): waiting for flushers to finish...")
	flusherWg.Wait()
	log.Printf("stopFlushers(): all flushers finished.")
}

var stopAggWorker = func(aggCh chan *aggregator.Command, aggWg *sync.WaitGroup) {
	log.Printf("stopAggWorker(): closing agg channel...")
	close(aggCh)
	log.Printf("stopAggWorker(): waiting for agg worker to finish...")
	aggWg.Wait()
	log.Printf("stopAggWorker(): agg worker finished.")
}

var stopPacedMetricWorker = func(pacedMetricCh chan *pacedMetric, pacedMetricWg *sync.WaitGroup) {
	log.Printf("stopPacedMetricWorker(): closing paced metric channel...")
	close(pacedMetricCh)
	log.Printf("stopPacedMetricWorker(): waiting for paced metric worker to finish...")
	pacedMetricWg.Wait()
	log.Printf("stopPacedMetricWorker(): paced metric worker finished.")
}

var startFlushers = func(r *Receiver, startWg *sync.WaitGroup) {
	if r.flusher.flusher() == nil { // This serde doesn't support flushing
		return
	}

	log.Printf("Starting flusher(s)...")
	r.flusher.start(&r.flusherWg, startWg, r.MaxFlushRatePerSecond, r.MinStep, r.NWorkers)
}

var startAggWorker = func(r *Receiver, startWg *sync.WaitGroup) {
	log.Printf("Starting aggWorker...")
	startWg.Add(1)
	go aggWorker(&wrkCtl{wg: &r.aggWg, startWg: startWg, id: "aggWorker"}, r.aggCh, r.cluster, r.StatFlushDuration, r.StatsNamePrefix, r, r)
}

var startPacedMetricWorker = func(r *Receiver, startWg *sync.WaitGroup) {
	log.Printf("Starting pacedMetricWorker...")
	startWg.Add(1)
	go pacedMetricWorker(&wrkCtl{wg: &r.pacedMetricWg, startWg: startWg, id: "pacedMetricWorker"}, r.pacedMetricCh, r, r, time.Second, r)
}
