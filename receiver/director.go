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
	"math"
	"sync"
	"time"

	"github.com/tgres/tgres/cluster"
)

var directorincomingDPMessages = func(rcv chan *cluster.Msg, dpCh chan interface{}) {
	defer func() { recover() }() // if we're writing to a closed channel below

	for {
		m, ok := <-rcv
		if !ok {
			return
		}

		// To get an event back:
		var dp incomingDP
		if err := m.Decode(&dp); err != nil {
			log.Printf("director: msg <- rcv data point decoding FAILED, ignoring this data point.")
			continue
		}

		maxHops := 2
		if dp.Hops > maxHops {
			log.Printf("director: dropping data point, max hops (%d) reached", maxHops)
			continue
		}

		dpCh <- &dp // See recover above
	}
}

var directorForwardDPToNode = func(dp *incomingDP, node *cluster.Node, snd chan *cluster.Msg) error {
	if dp.Hops == 0 { // we do not forward more than once
		if node.Ready() {
			dp.Hops++
			msg, _ := cluster.NewMsg(node, dp) // can't possibly error
			snd <- msg
		} else {
			return fmt.Errorf("directorForwardDPToNode: Node is not ready")
		}
	}
	return nil
}

var directorProcessDataPoint = func(cds *cachedDs, dsf dsFlusherBlocking) int {

	cnt, err := cds.processIncoming()

	if err != nil {
		log.Printf("directorProcessDataPoint [%v] error: %v", cds.Ident(), err)
	}

	if cds.PointCount() > 0 && cds.lastFlush.Before(time.Now().Add(-cds.Step())) {
		dsf.flushDs(cds.DbDataSourcer, false)
		cds.lastFlush = time.Now()
	}
	return cnt
}

var directorProcessOrForward = func(dsc *dsCache, cds *cachedDs, workerCh chan *cachedDs, clstr clusterer, snd chan *cluster.Msg, stats *dpStats) {
	if clstr == nil {
		workerCh <- cds
		return
	}

	for _, node := range clstr.NodesForDistDatum(&distDs{DbDataSourcer: cds.DbDataSourcer, dsc: dsc}) {
		if node.Name() == clstr.LocalNode().Name() {
			workerCh <- cds
		} else {
			for _, dp := range cds.incoming {
				if err := directorForwardDPToNode(dp, node, snd); err != nil {
					log.Printf("director: Error forwarding a data point: %v", err)
					// TODO For not ready error - sleep and return the dp to the channel?
					continue
				}
				stats.forwarded++
				stats.forwarded_to[node.SanitizedAddr()]++
			}
			cds.incoming = nil
			// Always clear RRAs to prevent it from being saved
			if pc := cds.PointCount(); pc > 0 {
				log.Printf("director: WARNING: Clearing DS with PointCount > 0: %v", pc)
			}
			cds.ClearRRAs(true)
		}
	}
	return
}

var directorProcessIncomingDP = func(dp *incomingDP, dsc *dsCache, loaderCh chan interface{}, workerCh chan *cachedDs, clstr clusterer, snd chan *cluster.Msg, stats *dpStats) {

	if math.IsNaN(dp.value) {
		// NaN is meaningless, e.g. "the thermometer is
		// registering a NaN". Or it means that "for certain it is
		// offline", but that is not part of our scope. You can
		// only get a NaN by exceeding HB. Silently ignore it.
		return
	}

	cds := dsc.getByIdentOrCreateEmpty(dp.cachedIdent)
	if cds == nil {
		stats.unknown++
		if debug {
			log.Printf("director: No spec matched ident: %#v, ignoring data point", dp.cachedIdent.String())
		}
		return
	}

	cds.appendIncoming(dp)

	if cds.Id() == 0 { // this DS needs to be loaded.
		if !cds.sentToLoader {
			cds.sentToLoader = true
			loaderCh <- cds
		}
	} else {
		directorProcessOrForward(dsc, cds, workerCh, clstr, snd, stats)
	}
}

func reportOverrunQueueSize(queue *fifoQueue, sr statReporter, nap time.Duration) {
	for {
		time.Sleep(nap) // TODO this should be a ticker really
		sr.reportStatGauge("receiver.queue_len", float64(queue.size()))
	}
}

var loader = func(loaderCh, dpCh chan interface{}, dsc *dsCache, sr statReporter) {

	// NOTE: Loader does not use an elastic channel to provide "back
	// pressure" when there are too many db operations. When this
	// happens, channels fill up and ultimately the receiver queue
	// should start growing. If there is a MaxReceiverQueueSize, then
	// we should start dropping data points, otherwise we'll just keep
	// on eating memory. Either strategy is better than an elastic
	// loader channel because unlike incoming data points / receiver
	// queue, load requests cannot be discarded.
	
	go func() {
		for {
			time.Sleep(time.Second)
			sr.reportStatGauge("receiver.load_queue_len", float64(len(loaderCh)))
		}
	}()

	for {
		x, ok := <-loaderCh
		if !ok {
			log.Printf("loader: channel closed, closing director channel and exiting...")
			close(dpCh)
			log.Printf("loader: exiting.")
			return
		}

		cds := x.(*cachedDs)

		if cds.spec != nil { // nil spec means it's been loaded already
			if err := dsc.fetchOrCreateByIdent(cds); err != nil {
				log.Printf("loader: database error: %v", err)
				continue
			}
		}

		if cds.Created() {
			sr.reportStatCount("receiver.created", 1)
		}

		dpCh <- cds
	}
}

type dpStats struct {
	total, forwarded, unknown, dropped int
	forwarded_to                       map[string]int
	last                               time.Time
}

var director = func(wc wController, dpCh chan interface{}, nWorkers int, clstr clusterer, sr statReporter, dsc *dsCache, dsf dsFlusherBlocking, maxQLen int) {
	wc.onEnter()
	defer wc.onExit()

	var (
		clusterChgCh chan bool
		snd, rcv     chan *cluster.Msg
		queue        = &fifoQueue{}
	)

	if clstr != nil {
		clusterChgCh = clstr.NotifyClusterChanges() // Monitor Cluster changes
		snd, rcv = clstr.RegisterMsgType()          // Channel for event forwards to other nodes and us
		go directorincomingDPMessages(rcv, dpCh)
		log.Printf("director: marking cluster node as Ready.")
		clstr.Ready(true)
	}

	go reportOverrunQueueSize(queue, sr, time.Second)

	dpOutCh := make(chan interface{}, 128)
	go elasticCh(dpCh, dpOutCh, queue)

	// Experimentation shows that the length of loader channel doesn't
	// matter much - making it 64K doesn't provide better performance
	// than 4K.
	loaderCh := make(chan interface{}, 4096)
	go loader(loaderCh, dpCh, dsc, sr)

	var workerWg sync.WaitGroup
	workerCh := make(chan *cachedDs, 128)
	log.Printf("director: starting %d workers.", nWorkers)
	for i := 0; i < nWorkers; i++ {
		workerWg.Add(1)
		go worker(&workerWg, workerCh, dsf, sr, i)
	}

	wc.onStarted()

	stats := dpStats{forwarded_to: make(map[string]int), last: time.Now()}

	for {
		var (
			x   interface{}
			dp  *incomingDP
			cds *cachedDs
			ok  bool
		)
		select {
		case _, ok = <-clusterChgCh:
			if ok {
				if err := clstr.Transition(45 * time.Second); err != nil {
					log.Printf("director: Transition error: %v", err)
				}
			}
			continue
		case x, ok = <-dpOutCh:
			switch x := x.(type) {
			case *incomingDP:
				dp = x
			case *cachedDs:
				cds = x
			case nil:
				// close signal
			default:
				log.Printf("director(): unknown type: %T", x)
			}
		}

		if !ok {
			log.Printf("director: exiting the director goroutine.")
			return
		}

		if dp != nil {
			if maxQLen > 0 && queue.size() > maxQLen {
				stats.dropped++
				continue // /dev/null
			}

			// if the dp ident is not found, it will be submitted to
			// the loader, which will return it to us through the dpCh
			// as a cachedDs.
			directorProcessIncomingDP(dp, dsc, loaderCh, workerCh, clstr, snd, &stats)
			stats.total++
		} else if cds != nil {
			// this came from the loader, we do not need to look it up
			directorProcessOrForward(dsc, cds, workerCh, clstr, snd, &stats)
		} else {
			// wait for worker and loader channels to empty
			log.Printf("director: channel closed, waiting for loader and workers to empty...")
			for {
				w, l := len(workerCh), len(loaderCh)
				if w == 0 && l == 0 {
					break
					log.Printf("  -  worker: %d loader: %d", w, l)
					time.Sleep(1 * time.Millisecond)
					w, l = len(workerCh), len(loaderCh)
				}
			}

			// signal to exit
			log.Printf("director: closing worker channels, waiting for workers to finish....")
			close(workerCh)
			workerWg.Wait()
			log.Printf("director: closing worker channels Done.")

			log.Printf("director: closing loader channel.")
			close(loaderCh)
		}

		if stats.last.Before(time.Now().Add(-time.Second)) {
			sr.reportStatCount("receiver.datapoints.total", float64(stats.total))
			sr.reportStatCount("receiver.datapoints.dropped", float64(stats.dropped)) // this too might be dropped...
			sr.reportStatCount("receiver.datapoints.unknown", float64(stats.unknown))
			sr.reportStatCount("receiver.datapoints.forwarded", float64(stats.forwarded))
			for dest, cnt := range stats.forwarded_to {
				sr.reportStatCount(fmt.Sprintf("receiver.forwarded_to.%s", dest), float64(cnt))
			}
			sr.reportStatCount("receiver.created", 0)
			stats = dpStats{forwarded_to: make(map[string]int), last: time.Now()}
			dsCount, rraCount := dsc.stats()
			sr.reportStatGauge("receiver.cache.ds_count", float64(dsCount))
			sr.reportStatGauge("receiver.cache.rra_count", float64(rraCount))
		}
	}
}

var worker = func(wg *sync.WaitGroup, workerCh chan *cachedDs, dsf dsFlusherBlocking, sr statReporter, n int) {
	log.Printf("worker %d: starting.", n)
	defer wg.Done()
	lastStat := time.Now()
	accepted := 0
	for {
		cds, ok := <-workerCh
		if !ok {
			log.Printf("worker %d: exiting.", n)
			return
		}
		accepted += directorProcessDataPoint(cds, dsf)

		if lastStat.Before(time.Now().Add(-time.Second)) {
			sr.reportStatCount("receiver.datapoints.accepted", float64(accepted))
			lastStat = time.Now()
			accepted = 0
		}
	}
}
