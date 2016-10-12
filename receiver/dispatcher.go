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

// Package receiver manages the receiving end of the data. All of the
// queueing, caching, perioding flushing and cluster forwarding logic
// is here.
package receiver

import (
	"fmt"
	"github.com/tgres/tgres/cluster"
	"log"
	"math"
	"time"
)

func dispatcherIncomingDPMessages(rcv chan *cluster.Msg, clstr clusterer, dpCh chan *IncomingDP) {
	defer func() { recover() }() // if we're writing to a closed channel below

	for {
		m := <-rcv

		// To get an event back:
		var dp IncomingDP
		if err := m.Decode(&dp); err != nil {
			log.Printf("dispatcher(): msg <- rcv data point decoding FAILED, ignoring this data point.")
			continue
		}

		var maxHops = clstr.NumMembers() * 2 // This is kind of arbitrary
		if dp.Hops > maxHops {
			log.Printf("dispatcher(): dropping data point, max hops (%d) reached", maxHops)
			continue
		}

		dpCh <- &dp // See recover above
	}
}

func dispatcherForwardDpToNode(dp *IncomingDP, node *cluster.Node, snd chan *cluster.Msg) error {
	if dp.Hops == 0 { // we do not forward more than once
		if node.Ready() {
			dp.Hops++
			msg, err := cluster.NewMsg(node, dp)
			if err != nil {
				return err
			}
			snd <- msg
		} else {
			return fmt.Errorf("sendDpToNode(): Node is not ready")
		}
	}
	return nil
}

func dispatcherProcessOrForward(rds *receiverDs, clstr clusterer, workerChs workerChannels, dp *IncomingDP, snd chan *cluster.Msg) (forwarded int) {
	for _, node := range clstr.NodesForDistDatum(rds) {
		if node.Name() == clstr.LocalNode().Name() {
			workerChs.queue(dp, rds)
		} else {
			if err := dispatcherForwardDpToNode(dp, node, snd); err != nil {
				log.Printf("dispatcher(): Error forwarding a data point: %v", err)
				// TODO For not ready error - sleep and return the dp to the channel?
				continue
			}
			forwarded++
			// Always clear RRAs to prevent it from being saved
			if pc := rds.PointCount(); pc > 0 {
				log.Printf("dispatcher(): WARNING: Clearing DS with PointCount > 0: %v", pc)
			}
			rds.ClearRRAs(true)
		}
	}
	return
}

func dispatcherProcessIncomingDP(dp *IncomingDP, scr statCountReporter, dsc *dsCache,
	workerChs workerChannels, clstr clusterer, snd chan *cluster.Msg) {

	scr.reportStatCount("receiver.dispatcher.datapoints.total", 1)

	if math.IsNaN(dp.Value) {
		// NaN is meaningless, e.g. "the thermometer is
		// registering a NaN". Or it means that "for certain it is
		// offline", but that is not part of our scope. You can
		// only get a NaN by exceeding HB. Silently ignore it.
		return
	}

	rds, err := dsc.getByNameOrLoadOrCreate(dp.Name)
	if err != nil {
		log.Printf("dispatcher(): dsCache error: %v", err)
		return
	}
	if rds == nil {
		log.Printf("dispatcher(): No spec matched name: %q, ignoring data point", dp.Name)
		return
	}

	if rds != nil {
		forwarded := dispatcherProcessOrForward(rds, clstr, workerChs, dp, snd)
		scr.reportStatCount("receiver.dispatcher.datapoints.forwarded", float64(forwarded))
	}
}

var dispatcher = func(wc wController, dpCh chan *IncomingDP, clstr clusterer, scr statCountReporter, dss *dsCache, workerChs workerChannels) {
	wc.onEnter()
	defer wc.onExit()

	// Monitor Cluster changes
	clusterChgCh := clstr.NotifyClusterChanges()

	// Channel for event forwards to other nodes and us
	snd, rcv := clstr.RegisterMsgType()
	go dispatcherIncomingDPMessages(rcv, clstr, dpCh)

	log.Printf("dispatcher(): marking cluster node as Ready.")
	clstr.Ready(true)

	wc.onStarted()

	for {

		var dp *IncomingDP
		var ok bool
		select {
		case _, ok = <-clusterChgCh:
			if ok {
				if err := clstr.Transition(45 * time.Second); err != nil {
					log.Printf("dispatcher(): Transition error: %v", err)
				}
			}
			continue
		case dp, ok = <-dpCh:
		}

		if !ok {
			log.Printf("dispatcher(): channel closed, shutting down")
			break
		}

		dispatcherProcessIncomingDP(dp, scr, dss, workerChs, clstr, snd)
	}
}
