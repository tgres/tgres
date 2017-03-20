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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/tgres/tgres/aggregator"
	"github.com/tgres/tgres/cluster"
	"github.com/tgres/tgres/serde"
)

func Test_aggworkerIncomingAggCmds(t *testing.T) {

	fl := &fakeLogger{}
	log.SetOutput(fl)

	defer func() {
		log.SetOutput(os.Stderr) // restore default output
	}()

	ident := "FOO"
	rcv := make(chan *cluster.Msg)
	aggCh := make(chan *aggregator.Command)

	count := 0
	go func() {
		for {
			if _, ok := <-aggCh; !ok {
				break
			}
			count++
		}
	}()

	go aggWorkerIncomingAggCmds(ident, rcv, aggCh)

	// Sending a bogus message should not cause anything be written to cmdCh
	rcv <- &cluster.Msg{}
	rcv <- &cluster.Msg{}
	if count > 0 {
		t.Errorf("aggworkerIncomingAggCmds: Malformed messages should not cause data points, count: %d", count)
	}
	if !strings.Contains(string(fl.last), "decoding FAILED") {
		t.Errorf("aggworkerIncomingAggCmds: Malformed messages should log 'decoding FAILED'")
	}

	// now we need a real message
	cmd := aggregator.NewCommand(aggregator.CmdAdd, serde.Ident{"name": "foo"}, 123)
	m, _ := cluster.NewMsg(&cluster.Node{}, cmd)
	rcv <- m
	rcv <- m

	if count < 1 {
		t.Errorf("aggworkerIncomingAggCmds: At least 1 data point should have been sent to dpCh")
	}

	cmd.Hops = 1000 // exceed maxhops
	m, _ = cluster.NewMsg(&cluster.Node{}, cmd)
	rcv <- m // "clear" the loop
	count = 0
	rcv <- m
	rcv <- m
	if count > 0 {
		t.Errorf("aggworkerIncomingAggCmds: Hops exceeded should not cause data points, count: %d", count)
	}
	if !strings.Contains(string(fl.last), "max hops") {
		t.Errorf("aggworkerIncomingAggCmds: Hops exceeded messages should log 'max hops'")
	}

	// Closing the dpCh should cause the recover() to happen
	// The test here is that it doesn't panic
	close(aggCh)
	cmd.Hops = 0
	m, _ = cluster.NewMsg(&cluster.Node{}, cmd)
	rcv <- m

	// Closing the channel exists (not sure how to really test for that)
	go aggWorkerIncomingAggCmds(ident, rcv, aggCh)
	close(rcv)
}

func Test_aggworkerPeriodicFlushSignal(t *testing.T) {
	flushCh := make(chan time.Time, 1)
	called := 0

	fl := &fakeLogger{}
	log.SetOutput(fl)

	defer func() {
		log.SetOutput(os.Stderr) // restore default output
	}()

	go aggWorkerPeriodicFlushSignal("IDENT", flushCh, 5*time.Millisecond)

	time.Sleep(15 * time.Millisecond)

	if !strings.Contains(string(fl.last), "dropping") {
		t.Errorf("aggworkerPeriodicFlushSignal: signal channel overrun should trigger 'dropping' log messages")
	}

	go func() {
		for {
			<-flushCh
			called++
		}
	}()

	time.Sleep(5 * time.Millisecond)

	close(flushCh) // triggers the recover

	time.Sleep(5 * time.Millisecond)

	if called < 2 {
		t.Errorf("aggworkerPeriodicFlushSignal: should be at least 1: %d", called)
	}
}

func Test_aggworkerForwardACToNode(t *testing.T) {
	ac := aggregator.NewCommand(aggregator.CmdAdd, serde.Ident{"name": "foo"}, 123)
	md := make([]byte, 20)
	md[0] = 1 // Ready
	node := &cluster.Node{Node: &memberlist.Node{Meta: md}}
	snd := make(chan *cluster.Msg)

	count := 0
	go func() {
		for {
			if _, ok := <-snd; !ok {
				break
			}
			count++
		}
	}()

	// if hops is > 0, nothing happens
	ac.Hops = 1
	aggWorkerForwardACToNode(ac, node, snd)
	aggWorkerForwardACToNode(ac, node, snd)

	if count > 0 {
		t.Errorf("aggWorkerForwardACToNode: Agg command with hops > 0 should not be forwarded")
	}

	// otherwise it should work
	ac.Hops = 0
	aggWorkerForwardACToNode(ac, node, snd)
	ac.Hops = 0 // because it just got incremented
	aggWorkerForwardACToNode(ac, node, snd)

	if count < 1 {
		t.Errorf("aggWorkerForwardACToNode: Agg command not sent to channel?")
	}

	// mark node not Ready
	md[0] = 0
	ac.Hops = 0 // because it just got incremented
	if err := aggWorkerForwardACToNode(ac, node, snd); err == nil {
		t.Errorf("aggWorkerForwardACToNode: not ready node should cause an error")
	}

}

type fakeAggregatorer struct {
	pcCalled, flushCalled int
}

func (f *fakeAggregatorer) ProcessCmd(cmd *aggregator.Command) { f.pcCalled++ }
func (f *fakeAggregatorer) Flush(_ time.Time)                  { f.flushCalled++ }

func Test_aggworkerProcessOrForward(t *testing.T) {

	saveFn := aggWorkerForwardACToNode
	forward, fwErr := 0, error(nil)
	aggWorkerForwardACToNode = func(ac *aggregator.Command, node *cluster.Node, snd chan *cluster.Msg) error {
		forward++
		return fwErr
	}

	ac := aggregator.NewCommand(aggregator.CmdAdd, serde.Ident{"name": "foo"}, 123)
	agg := &fakeAggregatorer{}
	aggDd := &distDatumAggregator{agg}

	// cluster
	clstr := &fakeCluster{}
	md := make([]byte, 20)
	md[0] = 1 // Ready
	node := &cluster.Node{Node: &memberlist.Node{Meta: md, Name: "local"}}
	clstr.nodesForDd = []*cluster.Node{node}
	clstr.ln = node

	// Test if we are LocalNode
	aggWorkerProcessOrForward(ac, aggDd, clstr, nil)
	aggWorkerProcessOrForward(ac, aggDd, clstr, nil)
	if agg.pcCalled < 1 {
		t.Errorf("aggWorkerProcessOrForward: agg.ProcessCmd() not called")
	}

	// Now test we are NOT LN, forward
	remote := &cluster.Node{Node: &memberlist.Node{Meta: md, Name: "remote"}}
	clstr.nodesForDd = []*cluster.Node{remote}

	n := aggWorkerProcessOrForward(ac, aggDd, clstr, nil)
	if forward != 1 {
		t.Errorf("aggWorkerProcessOrForward: aggWorkerForwardDPToNode not called")
	}
	if n != 1 {
		t.Errorf("aggWorkerProcessOrForward: return value != 1")
	}

	fl := &fakeLogger{}
	log.SetOutput(fl)
	defer func() {
		// restore default output
		log.SetOutput(os.Stderr)
	}()

	fwErr = fmt.Errorf("some error")
	n = aggWorkerProcessOrForward(ac, aggDd, clstr, nil)
	if n != 0 {
		t.Errorf("aggWorkerProcessOrForward: return value != 0")
	}
	if !strings.Contains(string(fl.last), "some error") {
		t.Errorf("aggWorkerProcessOrForward: aggWorkerForwardACToNode not logged")
	}
	fwErr = nil

	// restore
	aggWorkerForwardACToNode = saveFn
}

// func Test_aggworker_theAggworker(t *testing.T) {

// 	fl := &fakeLogger{}
// 	log.SetOutput(fl)

// 	defer func() {
// 		log.SetOutput(os.Stderr) // restore default output
// 	}()

// 	ident := "aggident"
// 	wc := &wrkCtl{wg: &sync.WaitGroup{}, startWg: &sync.WaitGroup{}, id: ident}
// 	aggCh := make(chan *aggregator.Command)

// 	scr := &fakeSr{}
// 	r := &Receiver{}

// 	// cluster with a node
// 	clstr := &fakeCluster{}
// 	md := make([]byte, 20)
// 	md[0] = 1 // Ready
// 	node := &cluster.Node{Node: &memberlist.Node{Meta: md, Name: "local"}}
// 	clstr.nodesForDd = []*cluster.Node{node}
// 	clstr.ln = node

// 	saveFn1, saveFn2, saveFn3 := aggWorkerIncomingAggCmds, aggWorkerPeriodicFlushSignal, aggWorkerProcessOrForward

// 	aiacCalled := 0
// 	aggWorkerIncomingAggCmds = func(ident string, rcv chan *cluster.Msg, aggCh chan *aggregator.Command) {
// 		aiacCalled++
// 	}

// 	apfsCalled := 0
// 	aggWorkerPeriodicFlushSignal = func(ident string, flushCh chan time.Time, dur time.Duration) {
// 		apfsCalled++
// 		for {
// 			flushCh <- time.Now()
// 		}
// 	}

// 	awpofCalled := 0
// 	aggWorkerProcessOrForward = func(ac *aggregator.Command, aggDd *distDatumAggregator, clstr clusterer, snd chan *cluster.Msg) (forwarded int) {
// 		awpofCalled++
// 		return 1
// 	}

// 	wc.startWg.Add(1)
// 	go aggWorker(wc, aggCh, clstr, 5*time.Millisecond, "prefix", scr, r)
// 	wc.startWg.Wait()

// 	time.Sleep(5 * time.Millisecond)
// 	if aiacCalled != 1 {
// 		t.Errorf("aggworker: aggworkerIncomingAggCmds not called")
// 	}

// 	if apfsCalled != 1 {
// 		t.Errorf("aggworker: aggworkerPeriodicFlushSignal not called")
// 	}

// 	// send some data
// 	cmd := aggregator.NewCommand(aggregator.CmdAdd, serde.Ident{"name": "foo"}, 123)

// 	aggCh <- cmd
// 	aggCh <- cmd

// 	if awpofCalled == 0 {
// 		t.Errorf("aggworker: aggWorkerProcessOrForward not called")
// 	}

// 	close(aggCh)

// 	wc.wg.Wait()
// 	if !strings.Contains(string(fl.last), "last flush") {
// 		t.Errorf("aggworker: 'last flush' log message messing on channel close")
// 	}

// 	// Now with nil cluster
// 	aggCh = make(chan *aggregator.Command)
// 	awpofCalled = 0

// 	wc.startWg.Add(1)
// 	go aggWorker(wc, aggCh, nil, 5*time.Millisecond, "prefix", scr, r)
// 	wc.startWg.Wait()

// 	// send some data
// 	cmd = aggregator.NewCommand(aggregator.CmdAdd, serde.Ident{"name": "foo"}, 123)

// 	aggCh <- cmd
// 	aggCh <- cmd

// 	if awpofCalled != 0 {
// 		t.Errorf("aggworker: aggWorkerProcessOrForward called but should not be with nil cluster")
// 	}

// 	close(aggCh)

// 	aggWorkerIncomingAggCmds, aggWorkerPeriodicFlushSignal, aggWorkerProcessOrForward = saveFn1, saveFn2, saveFn3
// }

func Test_aggworker_distDatumAggregator(t *testing.T) {
	agg := &fakeAggregatorer{}
	aggDd := &distDatumAggregator{agg}

	if aggDd.Id() != 1 {
		t.Errorf("distDatumAggregator.Id() != 1")
	}
	if aggDd.Type() != "aggregator.Aggregator" {
		t.Errorf("distDatumAggregator.Type() != 'aggregator.Aggregator'")
	}
	if aggDd.GetName() != "TheAggregator" {
		t.Errorf("distDatumAggregator.GetName() != 'TheAggregator'")
	}
	aggDd.Relinquish()
	if agg.flushCalled == 0 {
		t.Errorf("distDatumAggregator: Flush not called on Relinquish()")
	}
	if aggDd.Acquire() != nil {
		t.Errorf("distDatumAggregator.Acquire() != nil")
	}

}
