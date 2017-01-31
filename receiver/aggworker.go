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

	"github.com/tgres/tgres/aggregator"
	"github.com/tgres/tgres/cluster"
	"github.com/tgres/tgres/statsd"
)

var aggWorkerIncomingAggCmds = func(ident string, rcv chan *cluster.Msg, aggCh chan *aggregator.Command) {
	defer func() { recover() }() // if we're writing to a closed channel below

	for {
		m, ok := <-rcv
		if !ok {
			return
		}

		// To get an event back:
		var ac aggregator.Command
		if err := m.Decode(&ac); err != nil {
			log.Printf("%s: msg <- rcv aggreagator.Command decoding FAILED, ignoring this command.", ident)
			continue
		}

		maxHops := 2
		if ac.Hops > maxHops {
			log.Printf("%s: dropping command, max hops (%d) reached", ident, maxHops)
			continue
		}

		aggCh <- &ac // See recover above
	}
}

var aggWorkerPeriodicFlushSignal = func(ident string, flushCh chan time.Time, dur time.Duration) {
	defer func() { recover() }() // if we're writing to a closed channel below
	for {
		// NB: We do not use a time.Ticker here because my simple
		// experiments show that it will not stay aligned on a
		// multiple of duration if the system clock is
		// adjusted. This thing will mostly remain aligned.
		clock := time.Now()
		time.Sleep(clock.Truncate(dur).Add(dur).Sub(clock))
		if len(flushCh) == 0 {
			flushCh <- time.Now()
		} else {
			log.Printf("%s: dropping aggreagator flush timer on the floor - busy system?", ident)
		}
	}
}

var aggWorkerForwardACToNode = func(ac *aggregator.Command, node *cluster.Node, snd chan *cluster.Msg) error {
	if ac.Hops == 0 { // we do not forward more than once
		if node.Ready() {
			ac.Hops++
			msg, _ := cluster.NewMsg(node, ac) // can't possibly error
			snd <- msg
		} else {
			return fmt.Errorf("aggWorkerForwardAcToNode: Node is not ready")
		}
	}
	return nil
}

var aggWorkerProcessOrForward = func(ac *aggregator.Command, aggDd *distDatumAggregator, clstr clusterer, snd chan *cluster.Msg) (forwarded int) {
	for _, node := range clstr.NodesForDistDatum(aggDd) {
		if node.Name() == clstr.LocalNode().Name() {
			aggDd.ProcessCmd(ac)
		} else {
			if err := aggWorkerForwardACToNode(ac, node, snd); err != nil {
				log.Printf("aggworker: Error forwarding aggregator command: %v", err)
				continue
			}
			forwarded++
		}
	}
	return forwarded
}

func reportAggChannelFillPercent(aggCh chan *aggregator.Command, sr statReporter, nap time.Duration) {
	fillStatName := "receiver.aggworker.channel.fill_percent"
	lenStatName := "receiver.aggworker.channel.len"
	cp := float64(cap(aggCh))
	for {
		time.Sleep(nap)
		ln := float64(len(aggCh))
		if cp > 0 {
			fillPct := (ln / cp) * 100
			sr.reportStatGauge(fillStatName, fillPct)
		}
		sr.reportStatGauge(lenStatName, ln)
	}
}

var aggWorker = func(wc wController, aggCh chan *aggregator.Command, clstr clusterer, statFlushDuration time.Duration, statsNamePrefix string, sr statReporter, dpq *Receiver) {

	wc.onEnter()
	defer wc.onExit()

	var (
		snd, rcv chan *cluster.Msg
	)

	if clstr != nil {
		// Channel for event forwards to other nodes and us
		snd, rcv = clstr.RegisterMsgType()
		go aggWorkerIncomingAggCmds(wc.ident(), rcv, aggCh)
	}

	flushCh := make(chan time.Time, 1)
	go aggWorkerPeriodicFlushSignal(wc.ident(), flushCh, statFlushDuration)

	go reportAggChannelFillPercent(aggCh, sr, time.Second)

	log.Printf("%s: started.", wc.ident())
	wc.onStarted()

	statsd.Prefix = statsNamePrefix

	agg := aggregator.NewAggregator(dpq) // aggregator.dataPointQueuer
	agg.AppendAttr = "name"
	aggDd := &distDatumAggregator{agg}
	if clstr != nil {
		clstr.LoadDistData(func() ([]cluster.DistDatum, error) {
			log.Printf("%s: adding the aggregator.Aggregator DistDatum to the cluster", wc.ident())
			return []cluster.DistDatum{aggDd}, nil
		})
	}

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
				close(flushCh)
				return
			}

			if clstr == nil {
				aggDd.ProcessCmd(ac)
			} else {
				forwarded := aggWorkerProcessOrForward(ac, aggDd, clstr, snd)
				sr.reportStatCount("receiver.aggworker.agg.forwarded", float64(forwarded))
			}
		}
	}
}

// Implement cluster.DistDatum for stats

type distDatumAggregator struct {
	aggregator.Aggregator
}

func (d *distDatumAggregator) Id() int64       { return 1 }
func (d *distDatumAggregator) Type() string    { return "aggregator.Aggregator" }
func (d *distDatumAggregator) GetName() string { return "TheAggregator" }
func (d *distDatumAggregator) Relinquish() error {
	d.Flush(time.Now())
	return nil
}
func (d *distDatumAggregator) Acquire() error { return nil }
