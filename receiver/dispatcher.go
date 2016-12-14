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
	"time"

	"github.com/tgres/tgres/cluster"
)

var dispatcherIncomingDPMessages = func(rcv chan *cluster.Msg, dpCh chan *IncomingDP) {
	defer func() { recover() }() // if we're writing to a closed channel below

	for {
		m, ok := <-rcv
		if !ok {
			return
		}

		// To get an event back:
		var dp IncomingDP
		if err := m.Decode(&dp); err != nil {
			log.Printf("dispatcher: msg <- rcv data point decoding FAILED, ignoring this data point.")
			continue
		}

		maxHops := 2
		if dp.Hops > maxHops {
			log.Printf("dispatcher: dropping data point, max hops (%d) reached", maxHops)
			continue
		}

		dpCh <- &dp // See recover above
	}
}

var dispatcherForwardDPToNode = func(dp *IncomingDP, node *cluster.Node, snd chan *cluster.Msg) error {
	if dp.Hops == 0 { // we do not forward more than once
		if node.Ready() {
			dp.Hops++
			msg, _ := cluster.NewMsg(node, dp) // can't possibly error
			snd <- msg
		} else {
			return fmt.Errorf("dispatcherForwardDPToNode: Node is not ready")
		}
	}
	return nil
}

var dispatcherProcessOrForward = func(dsc *dsCache, cds *cachedDs, clstr clusterer, workerChs workerChannels, dp *IncomingDP, snd chan *cluster.Msg) (forwarded int) {

	for _, node := range clstr.NodesForDistDatum(&distDs{MetaDataSource: cds.MetaDataSource, dsc: dsc}) {
		if node.Name() == clstr.LocalNode().Name() {
			workerChs.queue(dp, cds)
		} else {
			if err := dispatcherForwardDPToNode(dp, node, snd); err != nil {
				log.Printf("dispatcher: Error forwarding a data point: %v", err)
				// TODO For not ready error - sleep and return the dp to the channel?
				continue
			}
			forwarded++
			// Always clear RRAs to prevent it from being saved
			if pc := cds.PointCount(); pc > 0 {
				log.Printf("dispatcher: WARNING: Clearing DS with PointCount > 0: %v", pc)
			}
			cds.ClearRRAs(true)
		}
	}
	return
}

var dispatcherProcessIncomingDP = func(dp *IncomingDP, sr statReporter, dsc *dsCache, workerChs workerChannels, clstr clusterer, snd chan *cluster.Msg) {

	sr.reportStatCount("receiver.dispatcher.datapoints.total", 1)

	if math.IsNaN(dp.Value) {
		// NaN is meaningless, e.g. "the thermometer is
		// registering a NaN". Or it means that "for certain it is
		// offline", but that is not part of our scope. You can
		// only get a NaN by exceeding HB. Silently ignore it.
		return
	}

	cds, err := dsc.fetchDataSourceByName(dp.Name)
	if err != nil {
		log.Printf("dispatcher: dsCache error: %v", err)
		return
	}
	if cds == nil {
		log.Printf("dispatcher: No spec matched name: %q, ignoring data point", dp.Name)
		return
	}

	if cds != nil {
		forwarded := dispatcherProcessOrForward(dsc, cds, clstr, workerChs, dp, snd)
		sr.reportStatCount("receiver.dispatcher.datapoints.forwarded", float64(forwarded))
	}
}

func reportDispatcherChannelFillPercent(dpCh chan *IncomingDP, queue *dpQueue, sr statReporter, nap time.Duration) {
	cp := float64(cap(dpCh))
	for {
		time.Sleep(nap) // TODO this should be a ticker really
		ln := float64(len(dpCh))
		if cp > 0 {
			fillPct := (ln / cp) * 100
			sr.reportStatGauge("receiver.dispatcher.channel.fill_percent", fillPct)
			if fillPct > 75 {
				log.Printf("WARNING: dispatcher channel %v percent full!", fillPct)
			}
		}
		sr.reportStatGauge("receiver.dispatcher.channel.len", ln)

		// Overrun queue
		sr.reportStatGauge("receiver.dispatcher.overrun_queue.len", float64(queue.size()))
		pctOfDisp := (float64(queue.size()) / cp) * 100
		sr.reportStatGauge("receiver.dispatcher.overrun_queue.pct", pctOfDisp)
	}
}

var dispatcher = func(wc wController, dpCh chan *IncomingDP, clstr clusterer, sr statReporter, dss *dsCache, workerChs workerChannels) {
	wc.onEnter()
	defer wc.onExit()

	// Monitor Cluster changes
	clusterChgCh := clstr.NotifyClusterChanges()

	// Channel for event forwards to other nodes and us
	snd, rcv := clstr.RegisterMsgType()
	go dispatcherIncomingDPMessages(rcv, dpCh)

	queue := &dpQueue{}

	// Monitor channel fill
	go reportDispatcherChannelFillPercent(dpCh, queue, sr, time.Second)

	log.Printf("dispatcher: marking cluster node as Ready.")
	clstr.Ready(true)

	wc.onStarted()

	for {

		var dp *IncomingDP
		var ok bool
		select {
		case _, ok = <-clusterChgCh:
			if ok {
				if err := clstr.Transition(45 * time.Second); err != nil {
					log.Printf("dispatcher: Transition error: %v", err)
				}
			}
			continue
		case dp, ok = <-dpCh:
		}
		if !ok {
			log.Printf("dispatcher: channel closed, shutting down")
			break
		}

		queueOnly := float32(len(dpCh))/float32(cap(dpCh)) > 0.5
		dp = checkSetAside(dp, queue, queueOnly)

		if dp != nil {
			dispatcherProcessIncomingDP(dp, sr, dss, workerChs, clstr, snd)
		}
	}
}

type dpQueue []*IncomingDP

func (q *dpQueue) push(dp *IncomingDP) {
	*q = append(*q, dp)
}

func (q *dpQueue) pop() (dp *IncomingDP) {
	dp, *q = (*q)[0], (*q)[1:]
	return dp
}

func (q *dpQueue) size() int {
	return len(*q)
}

// If skip is true, just append to the queue and return
// nothing. Otherwise, if there is something in the queue, return
// it. Otherwise, just pass it right through.
func checkSetAside(dp *IncomingDP, queue *dpQueue, skip bool) *IncomingDP {

	if skip {
		queue.push(dp)
		return nil
	}

	if queue.size() > 0 {
		return queue.pop()
	}

	return dp
}
