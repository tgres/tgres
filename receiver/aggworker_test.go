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
	"github.com/tgres/tgres/aggregator"
	"github.com/tgres/tgres/cluster"
	"log"
	"os"
	"strings"
	"testing"
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

	go aggworkerIncomingAggCmds(ident, rcv, aggCh)

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
	cmd := aggregator.NewCommand(aggregator.CmdAdd, "foo", 123)
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
	go aggworkerIncomingAggCmds(ident, rcv, aggCh)
	close(rcv)
}
