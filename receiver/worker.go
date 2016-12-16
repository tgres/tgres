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
	"fmt"
	"log"
	"time"
)

var workerPeriodicFlush = func(ident string, dsf dsFlusherBlocking, recent map[int64]*cachedDs, minCacheDur, maxCacheDur time.Duration, maxPoints, maxFlushes int) map[int64]*cachedDs {
	leftover := make(map[int64]*cachedDs)
	n := 0
	for id, cds := range recent {
		if cds.shouldBeFlushed(maxPoints, minCacheDur, minCacheDur) {
			if debug {
				log.Printf("%s: Requesting (periodic) flush of ds id: %d", ident, id)
			}
			if !dsf.flushDs(cds.DbDataSourcer, false) {
				leftover[id] = cds
			}
			cds.lastFlushRT = time.Now()
			delete(recent, id)
		}
		n++
		if n > maxFlushes {
			break
		}
	}
	return leftover
}

func reportWorkerChannelFillPercent(workerCh chan *incomingDpWithDs, sr statReporter, ident string, nap time.Duration) {
	fillStatName := fmt.Sprintf("receiver.workers.%s.channel.fill_percent", ident)
	lenStatName := fmt.Sprintf("receiver.workers.%s.channel.len", ident)
	cp := float64(cap(workerCh))
	for {
		time.Sleep(nap)
		ln := float64(len(workerCh))
		if cp > 0 {
			fillPct := (ln / cp) * 100
			sr.reportStatGauge(fillStatName, fillPct)
		}
		sr.reportStatGauge(lenStatName, ln)
	}
}

var worker = func(wc wController, dsf dsFlusherBlocking, workerCh chan *incomingDpWithDs,
	minCacheDur, maxCacheDur time.Duration, maxPoints int, flushInt time.Duration, sr statReporter) {
	wc.onEnter()
	defer wc.onExit()

	var recent = make(map[int64]*cachedDs)
	var leftover map[int64]*cachedDs

	periodicFlushTicker := time.NewTicker(flushInt)

	go reportWorkerChannelFillPercent(workerCh, sr, wc.ident(), time.Second)

	log.Printf("  - %s started.", wc.ident())
	wc.onStarted()

	maxFlushes := cap(workerCh) / 2
	for {
		select {
		case <-periodicFlushTicker.C:
			if len(leftover) > 0 {
				leftover = workerPeriodicFlush(wc.ident(), dsf, leftover, minCacheDur, maxCacheDur, maxPoints, maxFlushes)
			} else {
				leftover = workerPeriodicFlush(wc.ident(), dsf, recent, minCacheDur, maxCacheDur, maxPoints, maxFlushes)
			}
		case dpds, ok := <-workerCh:
			if !ok {
				return
			}
			cds := dpds.cds
			if err := cds.ProcessDataPoint(dpds.dp.Value, dpds.dp.TimeStamp); err == nil {
				recent[cds.Id()] = cds
			} else {
				log.Printf("%s: dp.process(%s) error: %v", wc.ident(), cds.Name(), err)
			}
		}

	}
}
