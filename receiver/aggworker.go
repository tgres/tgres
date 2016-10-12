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
	"github.com/tgres/tgres/aggregator"
	"github.com/tgres/tgres/cluster"
	"github.com/tgres/tgres/statsd"
	"log"
	"time"
)

func aggWorker(wc wController, aggCh chan *aggregator.Command, clstr clusterer, statFlushDuration time.Duration, statsNamePrefix string, scr statCountReporter, dpq *Receiver) {

	wc.onEnter()
	defer wc.onExit()

	// Channel for event forwards to other nodes and us
	snd, rcv := clstr.RegisterMsgType()
	go func() {
		defer func() { recover() }() // if we're writing to a closed channel below

		for {
			m := <-rcv

			// To get an event back:
			var ac aggregator.Command
			if err := m.Decode(&ac); err != nil {
				log.Printf("%s: msg <- rcv aggreagator.Command decoding FAILED, ignoring this command.", wc.ident())
				continue
			}

			var maxHops = clstr.NumMembers() * 2 // This is kind of arbitrary
			if ac.Hops > maxHops {
				log.Printf("%s: dropping command, max hops (%d) reached", wc.ident(), maxHops)
				continue
			}

			aggCh <- &ac // See recover above
		}
	}()

	var flushCh = make(chan time.Time, 1)
	go func() {
		for {
			// NB: We do not use a time.Ticker here because my simple
			// experiments show that it will not stay aligned on a
			// multiple of duration if the system clock is
			// adjusted. This thing will mostly remain aligned.
			clock := time.Now()
			time.Sleep(clock.Truncate(statFlushDuration).Add(statFlushDuration).Sub(clock))
			if len(flushCh) == 0 {
				flushCh <- time.Now()
			} else {
				log.Printf("%s: dropping aggreagator flush timer on the floor - busy system?", wc.ident())
			}
		}
	}()

	log.Printf("%s: started.", wc.ident())
	wc.onStarted()

	statsd.Prefix = statsNamePrefix

	agg := aggregator.NewAggregator(dpq) // aggregator.dataPointQueuer
	aggDd := &distDatumAggregator{agg}
	clstr.LoadDistData(func() ([]cluster.DistDatum, error) {
		log.Printf("%s: adding the aggregator.Aggregator DistDatum to the cluster", wc.ident())
		return []cluster.DistDatum{aggDd}, nil
	})

	for {
		// It's nice to flush stats at as precise time as
		// possible. This non-blocking select trick guarantees that we
		// always process flushCh even if there is stuff in the stCh.
		select {
		case now := <-flushCh:
			agg.Flush(now)
		default:
		}

		select {
		case now := <-flushCh:
			agg.Flush(now)
		case ac, ok := <-aggCh:
			if !ok {
				log.Printf("%s: channel closed, performing last flush", wc.ident())
				agg.Flush(time.Now())
				return
			}

			for _, node := range clstr.NodesForDistDatum(aggDd) {
				if node.Name() == clstr.LocalNode().Name() {
					agg.ProcessCmd(ac)
				} else if ac.Hops == 0 { // we do not forward more than once
					if node.Ready() {
						ac.Hops++
						if msg, err := cluster.NewMsg(node, ac); err == nil {
							snd <- msg
							scr.reportStatCount("receiver.aggregations_forwarded", 1)
						}
					} else {
						// Drop it
						log.Printf("%s: dropping command on the floor because no node is Ready!", wc.ident())
					}
				}
			}
		}
	}
}

// Implement cluster.DistDatum for stats

type distDatumAggregator struct {
	a *aggregator.Aggregator
}

func (d *distDatumAggregator) Id() int64       { return 1 }
func (d *distDatumAggregator) Type() string    { return "aggregator.Aggregator" }
func (d *distDatumAggregator) GetName() string { return "TheAggregator" }
func (d *distDatumAggregator) Relinquish() error {
	d.a.Flush(time.Now())
	return nil
}
func (d *distDatumAggregator) Acquire() error { return nil }
