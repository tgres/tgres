//
// Copyright 2017 Gregory Trubetskoy. All Rights Reserved.
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

// Package blaster provides some stress testing capabilities.
package blaster

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/tgres/tgres/serde"
	"golang.org/x/time/rate"
)

type Blaster struct {
	nSeries int
	rcvr    dataPointQueuer
	limiter *rate.Limiter
	prefix  string
	span    time.Duration

	mu sync.Mutex
}

type dataPointQueuer interface {
	QueueDataPoint(serde.Ident, time.Time, float64)
}

func New(rcvr dataPointQueuer) *Blaster {
	b := &Blaster{
		rcvr:    rcvr,
		limiter: rate.NewLimiter(rate.Limit(0), 1), // Zero limit allows no events
		span:    600 * time.Second,
		prefix:  "tgres.blaster"}
	go blast(b)
	return b
}

func (b *Blaster) SetRate(perSec int) {
	// No need to lock, limiters arleady have a lock
	b.limiter.SetLimit(rate.Limit(perSec))
	log.Printf("Blaster: rate is now: %v per second, nSeries is: %v.", perSec, b.nSeries)
}

func (b *Blaster) SetNSeries(n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nSeries = n
	log.Printf("Blaster: nSeries is now: %v, rate is: %v per second.", n, b.limiter.Limit())
}

func (b *Blaster) cycle() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.limiter.Limit() == 0 {
		// rate.Limiter has a bug - Limit of zero should allow no events, but it
		// aparently allows infinite events?
		time.Sleep(time.Second)
		return 0
	}

	if b.nSeries == 0 {
		return 0
	}

	// Pick a random number
	n := int64(rand.Int() % b.nSeries)

	// Current time
	now := time.Now()

	// The offset shifts the sinusoid to the right a bit based on
	// its number for fancier overall appearance.
	offset := time.Duration(n*10) * time.Second

	// Get the Y value
	y := sinTime(now.Add(offset), b.span) * 100

	// Generate name (works with up to 10M)
	name := fmt.Sprintf("%s.test.a%02d.b%02d.c%02d.d%02d", b.prefix, (n%10000000)/100000, (n%100000)/1000, (n%1000)/10, n%10)

	// Send the data point
	b.rcvr.QueueDataPoint(serde.Ident{"name": name}, now, y)

	return len(name) + 8 // more or less accurate size in bytes
}

func blast(b *Blaster) {

	ctx := context.TODO()

	cnt, tsz := 0, 0
	lastStat := time.Now()
	statPeriod := 10 * time.Second

	for {

		b.limiter.Wait(ctx)

		if sz := b.cycle(); sz > 0 {
			cnt++
			tsz += sz
		}

		if cnt%1000 == 0 {
			if time.Now().Sub(lastStat) > statPeriod {
				log.Printf("Blaster: %v Count: %d \tper/sec: %v \tBps: %v\n", time.Now(), cnt, float64(cnt)/time.Now().Sub(lastStat).Seconds(), int64(float64(tsz)/time.Now().Sub(lastStat).Seconds()))
				cnt, tsz = 0, 0
				lastStat = time.Now()
			}
		}
	}
}

// Given a time, return a Y value that will draw a sinusoid spanning span
func sinTime(t time.Time, span time.Duration) float64 {
	seconds := span.Nanoseconds() / 1e9
	x := 2 * math.Pi / float64(seconds) * float64(t.Unix()%seconds)
	return math.Sin(x)
}
