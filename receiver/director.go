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

var directorincomingDPMessages = func(rcv chan *cluster.Msg, dpCh chan *incomingDP) {
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

var directorProcessOrForward = func(dsc *dsCache, cds *cachedDs, clstr clusterer, workerChs workerChannels, dp *incomingDP, snd chan *cluster.Msg) (forwarded int) {

	for _, node := range clstr.NodesForDistDatum(&distDs{DbDataSourcer: cds.DbDataSourcer, dsc: dsc}) {
		if node.Name() == clstr.LocalNode().Name() {
			workerChs.queue(dp, cds)
		} else {
			if err := directorForwardDPToNode(dp, node, snd); err != nil {
				log.Printf("director: Error forwarding a data point: %v", err)
				// TODO For not ready error - sleep and return the dp to the channel?
				continue
			}
			forwarded++
			// Always clear RRAs to prevent it from being saved
			if pc := cds.PointCount(); pc > 0 {
				log.Printf("director: WARNING: Clearing DS with PointCount > 0: %v", pc)
			}
			cds.ClearRRAs(true)
		}
	}
	return
}

var directorProcessincomingDP = func(dp *incomingDP, sr statReporter, dsc *dsCache, workerChs workerChannels, clstr clusterer, snd chan *cluster.Msg) {

	sr.reportStatCount("receiver.datapoints.total", 1)

	if math.IsNaN(dp.Value) {
		// NaN is meaningless, e.g. "the thermometer is
		// registering a NaN". Or it means that "for certain it is
		// offline", but that is not part of our scope. You can
		// only get a NaN by exceeding HB. Silently ignore it.
		return
	}

	cds, err := dsc.fetchOrCreateByName(dp.Ident)
	if err != nil {
		log.Printf("director: dsCache error: %v", err)
		return
	}
	if cds == nil {
		log.Printf("director: No spec matched ident: %#v, ignoring data point", dp.Ident)
		return
	}

	if cds != nil {
		if clstr == nil {
			workerChs.queue(dp, cds)
		} else {
			forwarded := directorProcessOrForward(dsc, cds, clstr, workerChs, dp, snd)
			sr.reportStatCount("receiver.datapoints.forwarded", float64(forwarded))
		}
	}
}

func reportDirectorChannelFillPercent(dpCh chan *incomingDP, queue *dpQueue, sr statReporter, nap time.Duration) {
	cp := float64(cap(dpCh))
	for {
		time.Sleep(nap) // TODO this should be a ticker really
		ln := float64(len(dpCh))
		if cp > 0 {
			fillPct := (ln / cp) * 100
			sr.reportStatGauge("receiver.channel.fill_percent", fillPct)
			if fillPct > 75 {
				log.Printf("WARNING: receiver channel %v percent full!", fillPct)
			}
		}
		sr.reportStatGauge("receiver.channel.len", ln)

		// Overrun queue
		qsz := queue.size()
		pct := (float64(qsz) / cp) * 100
		sr.reportStatGauge("receiver.overrun_queue.len", float64(qsz))
		sr.reportStatGauge("receiver.overrun_queue.pct", pct)
	}
}

var director = func(wc wController, dpCh chan *incomingDP, clstr clusterer, sr statReporter, dss *dsCache, workerChs workerChannels) {
	wc.onEnter()
	defer wc.onExit()

	var (
		clusterChgCh chan bool
		snd, rcv     chan *cluster.Msg
		queue        = &dpQueue{}
	)

	if clstr != nil {
		clusterChgCh = clstr.NotifyClusterChanges() // Monitor Cluster changes
		snd, rcv = clstr.RegisterMsgType()          // Channel for event forwards to other nodes and us
		go directorincomingDPMessages(rcv, dpCh)
		log.Printf("director: marking cluster node as Ready.")
		clstr.Ready(true)
	}

	// Monitor channel fill TODO: this is wrong, there should be better ways
	go reportDirectorChannelFillPercent(dpCh, queue, sr, time.Second)

	go func() {
		defer func() { recover() }()
		for {
			time.Sleep(time.Second)
			dpCh <- nil // This ensures that overrun queue is flushed
		}
	}()

	wc.onStarted()

	for {

		var dp *incomingDP
		var ok bool
		select {
		case _, ok = <-clusterChgCh:
			if ok {
				if err := clstr.Transition(45 * time.Second); err != nil {
					log.Printf("director: Transition error: %v", err)
				}
			}
			continue
		case dp, ok = <-dpCh:
		}
		if !ok {
			log.Printf("director: channel closed, shutting down")
			break
		}

		queueOnly := float32(len(dpCh))/float32(cap(dpCh)) > 0.5
		dp = checkSetAside(dp, queue, queueOnly)

		if dp != nil {
			directorProcessincomingDP(dp, sr, dss, workerChs, clstr, snd)
		}

		// Try to flush the queue if we are idle
		for (len(dpCh) == 0) && (queue.size() > 0) {
			if dp = checkSetAside(nil, queue, false); dp != nil {
				directorProcessincomingDP(dp, sr, dss, workerChs, clstr, snd)
			}
		}
	}
}

type dpQueue []*incomingDP

func (q *dpQueue) push(dp *incomingDP) {
	*q = append(*q, dp)
}

func (q *dpQueue) pop() (dp *incomingDP) {
	dp, *q = (*q)[0], (*q)[1:]
	return dp
}

func (q *dpQueue) size() int {
	return len(*q)
}

// If skip is true, just append to the queue and return
// nothing. Otherwise, if there is something in the queue, return
// it. Otherwise, just pass it right through.
func checkSetAside(dp *incomingDP, queue *dpQueue, skip bool) *incomingDP {

	if skip {
		if dp != nil {
			queue.push(dp)
		}
		return nil
	}

	if queue.size() > 0 {
		if dp != nil {
			queue.push(dp)
		}
		return queue.pop()
	}

	return dp
}
