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
	"github.com/hashicorp/memberlist"
	"github.com/tgres/tgres/cluster"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

type fakeLogger struct {
	last []byte
}

func (f *fakeLogger) Write(p []byte) (n int, err error) {
	f.last = p
	return len(p), nil
}

func Test_dispatcherIncomingDPMessages(t *testing.T) {
	defer func() {
		// restore default output
		log.SetOutput(os.Stderr)
	}()

	fl := &fakeLogger{}
	log.SetOutput(fl)

	rcv := make(chan *cluster.Msg)
	clstr := &fakeCluster{}
	dpCh := make(chan *IncomingDP)

	count := 0
	go func() {
		for {
			if _, ok := <-dpCh; !ok {
				break
			}
			count++
		}
	}()

	go dispatcherIncomingDPMessages(rcv, clstr, dpCh)

	// Sending a bogus message should not cause anything be written to dpCh
	rcv <- &cluster.Msg{}
	rcv <- &cluster.Msg{} // second send ensures the loop has gone full circle
	if count > 0 {
		t.Errorf("Malformed messages should not cause data points, count: %d", count)
	}
	if !strings.Contains(string(fl.last), "decoding FAILED") {
		t.Errorf("Malformed messages shold log 'decoding FAILED'")
	}

	// now we need a real message
	dp := &IncomingDP{Name: "foo", TimeStamp: time.Unix(1000, 0), Value: 123}
	m, _ := cluster.NewMsg(&cluster.Node{}, dp)
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
		t.Errorf("Hops exceeded messages shold log 'max hops'")
	}

	// Closing the dpCh should cause the recover() to happen
	// The test here is that it doesn't panic
	close(dpCh)
	dp.Hops = 0
	m, _ = cluster.NewMsg(&cluster.Node{}, dp)
	rcv <- m

	// Closing the channel exists (not sure how to really test for that)
	go dispatcherIncomingDPMessages(rcv, clstr, dpCh)
	close(rcv)
}

func Test_dispatcherForwardDPToNode(t *testing.T) {

	dp := &IncomingDP{Name: "foo", TimeStamp: time.Unix(1000, 0), Value: 123}
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
	dispatcherForwardDPToNode(dp, node, snd)
	dispatcherForwardDPToNode(dp, node, snd)

	if count > 0 {
		t.Errorf("Data points with hops > 0 should not be forwarded")
	}

	// otherwise it should work
	dp.Hops = 0
	dispatcherForwardDPToNode(dp, node, snd)
	dp.Hops = 0 // because it just got incremented
	dispatcherForwardDPToNode(dp, node, snd)

	if count < 1 {
		t.Errorf("Data point not sent to channel?")
	}

	// mark node not Ready
	md[0] = 0
	dp.Hops = 0 // because it just got incremented
	if err := dispatcherForwardDPToNode(dp, node, snd); err == nil {
		t.Errorf("not ready node should cause an error")
	}
}

func Test_dispatcherProcessOrForward(t *testing.T) {

}
