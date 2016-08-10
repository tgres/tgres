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

package rrd

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"testing"
	"time"
)

// IncomingDP must be gob encodable
func TestIncomingDP_gobEncodable(t *testing.T) {
	now := time.Now()
	dp1 := &IncomingDP{
		Name:      "foo.bar",
		TimeStamp: now,
		Value:     1.2345,
		Hops:      7,
	}

	var bb bytes.Buffer
	enc := gob.NewEncoder(&bb)
	dec := gob.NewDecoder(&bb)

	err := enc.Encode(dp1)
	if err != nil {
		t.Errorf("gob encode error:", err)
	}

	var dp2 *IncomingDP
	dec.Decode(&dp2)

	if !reflect.DeepEqual(dp1, dp2) {
		t.Errorf("dp1 != dp2 after gob encode/decode")
	}
}

// IncomingDP.Process()
func TestIncomingDP_Process(t *testing.T) {
	dp := &IncomingDP{}
	if dp.Process() == nil {
		t.Errorf("dp.Process() == nil (should be error because DS is nil)")
	}
}

// TODO
// Incoming.Process() as an actual test
