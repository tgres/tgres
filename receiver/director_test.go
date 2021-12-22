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
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/tgres/tgres/cluster"
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
)

type fakeLogger struct {
	last []byte
}

func (f *fakeLogger) Write(p []byte) (n int, err error) {
	f.last = p
	return len(p), nil
}

func Test_directorIncomingDPMessages(t *testing.T) {
	defer func() {
		// restore default output
		log.SetOutput(os.Stderr)
	}()

	fl := &fakeLogger{}
	log.SetOutput(fl)

	rcv := make(chan *cluster.Msg)
	dpCh := make(chan interface{})

	count := 0
	go func() {
		for {
			if _, ok := <-dpCh; !ok {
				break
			}
			count++
		}
	}()

	go directorIncomingDPMessages(rcv, dpCh)

	// Sending a bogus message should not cause anything be written to dpCh
	rcv <- &cluster.Msg{}
	rcv <- &cluster.Msg{} // second send ensures the loop has gone full circle
	if count > 0 {
		t.Errorf("Malformed messages should not cause data points, count: %d", count)
	}
	if !strings.Contains(string(fl.last), "decoding FAILED") {
		t.Errorf("Malformed messages should log 'decoding FAILED'")
	}

	// now we need a real message
	dp := &incomingDP{cachedIdent: newCachedIdent(serde.Ident{"name": "foo"}), timeStamp: time.Unix(1000, 0), value: 123}
	m, _ := cluster.NewMsg(&cluster.Node{}, dp)
	rcv <- m
	rcv <- m
	rcv <- m

	if count < 1 {
		t.Errorf("At least 1 data point should have been sent to dpCh")
	}

	dp.Hops = 1000 // exceed maxhops (which in fakeCluster is 0?)
	m, _ = cluster.NewMsg(&cluster.Node{}, dp)
	rcv <- m // "clear" the loop
	count = 0
	rcv <- m
	rcv <- m
	if count > 0 {
		t.Errorf("Hops exceeded should not cause data points, count: %d", count)
	}
	if !strings.Contains(string(fl.last), "max hops") {
		t.Errorf("Hops exceeded messages should log 'max hops'")
	}

	// Closing the dpCh should cause the recover() to happen
	// The test here is that it doesn't panic
	close(dpCh)
	dp.Hops = 0
	m, _ = cluster.NewMsg(&cluster.Node{}, dp)
	rcv <- m

	// Closing the channel exists (not sure how to really test for that)
	go directorIncomingDPMessages(rcv, dpCh)
	close(rcv)
}

func Test_directorForwardDPToNode(t *testing.T) {

	dp := &incomingDP{cachedIdent: newCachedIdent(serde.Ident{"name": "foo"}), timeStamp: time.Unix(1000, 0), value: 123}
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
	dp.Hops = 1
	directorForwardDPToNode(dp, node, snd)
	directorForwardDPToNode(dp, node, snd)

	if count > 0 {
		t.Errorf("directorForwardDPToNode: Data points with hops > 0 should not be forwarded")
	}

	// otherwise it should work
	dp.Hops = 0
	directorForwardDPToNode(dp, node, snd)
	dp.Hops = 0 // because it just got incremented
	directorForwardDPToNode(dp, node, snd)

	if count < 1 {
		t.Errorf("Data point not sent to channel?")
	}

	// mark node not Ready
	md[0] = 0
	dp.Hops = 0 // because it just got incremented
	if err := directorForwardDPToNode(dp, node, snd); err == nil {
		t.Errorf("not ready node should cause an error")
	}
}

func Test_directorProcessOrForward(t *testing.T) {

	saveFn := directorForwardDPToNode
	forward, fwErr := 0, error(nil)
	directorForwardDPToNode = func(dp *incomingDP, node *cluster.Node, snd chan *cluster.Msg) error {
		forward++
		return fwErr
	}

	st := &dpStats{forwarded_to: make(map[string]int), last: time.Now()}

	// dsc
	db := &fakeSerde{}
	df := &SimpleDSFinder{DftDSSPec}
	sr := &fakeSr{}
	dsf := &dsFlusher{db: db.Flusher(), sr: sr}
	dsc := newDsCache(db, df, dsf)

	// cds
	foo := serde.Ident{"name": "foo"}
	ds := serde.NewDbDataSource(0, foo, 0, 0, rrd.NewDataSource(*DftDSSPec))
	cds := &cachedDs{DbDataSourcer: ds, mu: &sync.Mutex{}}
	dp := &incomingDP{cachedIdent: newCachedIdent(serde.Ident{"name": "foo"}), timeStamp: time.Unix(1000, 0), value: 123}
	cds.appendIncoming(dp)

	// cluster
	clstr := &fakeCluster{}
	md := make([]byte, 20)
	md[0] = 1 // Ready
	node := &cluster.Node{Node: &memberlist.Node{Meta: md, Name: "local"}}
	clstr.nodesForDd = []*cluster.Node{node}
	clstr.ln = node

	// workerChs
	workerCh := make(chan *cachedDs)
	sent := 0
	go func() {
		for {
			<-workerCh
			sent++
		}
	}()

	// Test if we are LocalNode
	directorProcessOrForward(dsc, cds, workerCh, clstr, nil, st)
	directorProcessOrForward(dsc, cds, workerCh, clstr, nil, st)
	if sent < 1 {
		t.Errorf("directorProcessOrForward: Nothing sent to workerChs")
	}

	// Now test we are NOT LN, forward
	remote := &cluster.Node{Node: &memberlist.Node{Meta: md, Name: "remote"}}
	clstr.nodesForDd = []*cluster.Node{remote}

	directorProcessOrForward(dsc, cds, workerCh, clstr, nil, st)
	if forward != 1 {
		t.Errorf("directorProcessOrForward: directorForwardDPToNode not called")
	}

	fl := &fakeLogger{}
	log.SetOutput(fl)
	defer func() {
		// restore default output
		log.SetOutput(os.Stderr)
	}()

	// make an cds with points
	foo = serde.Ident{"name": "foo"}
	ds = serde.NewDbDataSource(0, foo, 0, 0, rrd.NewDataSource(rrd.DSSpec{
		Step: 10 * time.Second,
		RRAs: []rrd.RRASpec{
			rrd.RRASpec{Function: rrd.WMEAN,
				Step:   10 * time.Second,
				Span:   30 * time.Second,
				Latest: time.Unix(1000, 0),
			},
		},
	}))
	ds.ProcessDataPoint(123, time.Unix(2000, 0))
	ds.ProcessDataPoint(123, time.Unix(3000, 0))
	cds = &cachedDs{DbDataSourcer: ds}

	directorProcessOrForward(dsc, cds, workerCh, clstr, nil, st)
	if !strings.Contains(string(fl.last), "PointCount") {
		t.Errorf("directorProcessOrForward: Missing the PointCount warning log")
	}
	if cds.PointCount() != 0 {
		t.Errorf("directorProcessOrForward: ClearRRAs(true) not called")
	}

	// restore directorForwardDPToNode
	directorForwardDPToNode = saveFn
}

func Test_directorProcessIncomingDP(t *testing.T) {

	saveFn := directorProcessOrForward
	dpofCalled := 0
	directorProcessOrForward = func(dsc *dsCache, cds *cachedDs, workerCh chan *cachedDs, clstr clusterer, snd chan *cluster.Msg, stats *dpStats) {
		dpofCalled++
	}

	fl := &fakeLogger{}
	log.SetOutput(fl)
	defer func() {
		// restore default output
		log.SetOutput(os.Stderr)
	}()

	st := &dpStats{forwarded_to: make(map[string]int), last: time.Now()}

	// dp
	dp := &incomingDP{cachedIdent: newCachedIdent(serde.Ident{"name": "foo"}), timeStamp: time.Unix(1000, 0), value: 123}

	// dsc
	db := &fakeSerde{}
	df := &SimpleDSFinder{DftDSSPec}
	scr := &fakeSr{}
	dsf := &dsFlusher{db: db.Flusher(), sr: scr}
	dsc := newDsCache(db, df, dsf)

	// cluster
	clstr := &fakeCluster{cChange: make(chan bool)}

	// workerCh
	workerCh := make(chan *cachedDs)
	sent := 0
	go func() {
		for {
			<-workerCh
			sent++
		}
	}()

	// loaderCh
	loaderCh := make(chan interface{})
	lsent := 0
	go func() {
		for {
			<-loaderCh
			lsent++
		}
		fmt.Printf("exited!\n")
	}()

	// NaN
	dp.value = math.NaN()
	directorProcessIncomingDP(dp, dsc, nil, nil, nil, nil, st)
	if dpofCalled > 0 {
		t.Errorf("directorProcessIncomingDP: With a NaN, directorProcessOrForward should not be called")
	}

	// A value
	dp.value = 1234
	lsent, dpofCalled = 0, 0
	directorProcessIncomingDP(dp, dsc, loaderCh, workerCh, clstr, nil, st)
	if lsent != 1 {
		t.Errorf("directorProcessIncomingDP: With a value, should send it to loader")
	}
	if dpofCalled != 0 {
		t.Errorf("directorProcessIncomingDP: With a value, directorProcessOrForward should not be called")
	}

	// A blank name should cause a nil rds
	dp.cachedIdent = newCachedIdent(serde.Ident{"name": ""})
	dpofCalled = 0
	directorProcessIncomingDP(dp, dsc, nil, nil, nil, nil, st)
	if dpofCalled > 0 {
		t.Errorf("directorProcessIncomingDP: With a blank name, directorProcessOrForward should not be called")
	}

	// fake a db error
	dp.cachedIdent = newCachedIdent(serde.Ident{"name": "blah"})
	db.fakeErr = true
	dpofCalled = 0
	directorProcessIncomingDP(dp, dsc, loaderCh, nil, nil, nil, st)
	if dpofCalled > 0 {
		t.Errorf("directorProcessIncomingDP: With a db error, directorProcessOrForward should not be called")
	}

	// nil cluster
	dp.value = 1234
	db.fakeErr = false
	directorProcessIncomingDP(dp, dsc, loaderCh, workerCh, nil, nil, st)
	if dpofCalled != 0 {
		t.Errorf("directorProcessIncomingDP: With a value and no cluster, directorProcessOrForward should not be called: %v", dpofCalled)
	}

	directorProcessOrForward = saveFn
}

func Test_the_director(t *testing.T) {

	saveFn1 := directorIncomingDPMessages
	saveFn2 := directorProcessIncomingDP
	dimCalled := 0
	directorIncomingDPMessages = func(rcv chan *cluster.Msg, dpCh chan<- interface{}) { dimCalled++ }
	dpidpCalled := 0
	directorProcessIncomingDP = func(dp *incomingDP, dsc *dsCache, loaderCh chan interface{}, workerCh chan *cachedDs, clstr clusterer, snd chan *cluster.Msg, stats *dpStats) {
		dpidpCalled++
	}

	fl := &fakeLogger{}
	log.SetOutput(fl)
	defer func() {
		// restore default output
		log.SetOutput(os.Stderr)
	}()

	wc := &wrkCtl{wg: &sync.WaitGroup{}, startWg: &sync.WaitGroup{}, id: "FOO"}
	clstr := &fakeCluster{cChange: make(chan bool)}
	dpCh := make(chan interface{})

	// dsc
	db := &fakeSerde{}
	df := &SimpleDSFinder{DftDSSPec}
	sr := &fakeSr{}
	dsf := &dsFlusher{db: db.Flusher(), sr: sr}
	dsc := newDsCache(db, df, dsf)

	wc.startWg.Add(1)
	go director(wc, dpCh, dpCh, 1, clstr, sr, dsc, nil, nil, 0, 0)
	wc.startWg.Wait()

	if clstr.nReady == 0 {
		t.Errorf("director: Ready(true) not called on cluster")
	}

	if clstr.nReg == 0 {
		t.Errorf("director: cluster.RegisterMsgType() not called")
	}

	// This sometimes can fail because we don't wait for that goroutine in this test...
	time.Sleep(5 * time.Millisecond)
	if dimCalled == 0 {
		t.Errorf("director: directorIncomingDPMessages not started")
	}

	dp := &incomingDP{cachedIdent: newCachedIdent(serde.Ident{"name": "foo"}), timeStamp: time.Unix(1000, 0), value: 123}
	dpCh <- dp
	dpCh <- dp

	if dpidpCalled == 0 {
		t.Errorf("director: directorProcessIncomingDP not called")
	}

	// Trigger a transition
	clstr.cChange <- true
	dpCh <- dp

	time.Sleep(100 * time.Millisecond)
	if clstr.nTrans == 0 {
		t.Errorf("director: on cluster change, Transition() not called")
	}

	// // Transition with error
	// clstr.tErr = true
	// clstr.cChange <- true
	// dpCh <- dp

	// if !strings.Contains(string(fl.last), "some error") {
	// 	t.Errorf("director: on transition error, 'some error' missing from logs")
	// }

	dpidpCalled = 0
	close(dpCh)
	time.Sleep(1 * time.Second) // so that nil dp goroutine panics/recovers

	// if dpidpCalled > 0 {
	// 	t.Errorf("director: directorProcessIncomingDP must not be called on channel close")
	// }

	// if !strings.Contains(string(fl.last), "shutting down") {
	// 	t.Errorf("director: on channel close, missing 'shutting down' log entry")
	// }

	// overrun
	dpCh = make(chan interface{}, 5)
	dpCh <- dp
	dpCh <- dp
	dpCh <- dp
	dpCh <- dp

	wc.startWg.Add(1)
	go director(wc, dpCh, dpCh, 1, clstr, sr, dsc, nil, nil, 0, 0)
	wc.startWg.Wait()

	time.Sleep(100 * time.Millisecond)

	close(dpCh)

	directorIncomingDPMessages = saveFn1
	directorProcessIncomingDP = saveFn2
}

func Test_director_fifoQueue(t *testing.T) {
	queue := &fifoQueue{}
	dp := &incomingDP{}
	queue.push(dp)
	if queue.pop() != dp {
		t.Errorf("queue: pop returned wrong dp")
	}
	if queue.size() != 0 {
		t.Errorf("queue: should be empty")
	}
	queue.push(&incomingDP{})
	if queue.size() != 1 {
		t.Errorf("queue: size != 1")
	}
}
