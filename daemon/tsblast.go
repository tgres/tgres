package daemon

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/tgres/tgres/receiver"
	"github.com/tgres/tgres/serde"
	"golang.org/x/time/rate"
)

var (
	wg    sync.WaitGroup
	count uint64
	span  int64 = 600 // 86400 // at 1 second per dp, we will go full circle (2 * math.Pi) in this many seconds
)

func tsblast(rcvr *receiver.Receiver) {
	total := 2000
	per_sec := 2000
	start := 1000
	incr := 100
	period := 60 * time.Second

	limiter := rate.NewLimiter(rate.Limit(per_sec), per_sec)

	go func() {
		current := start
		for {
			log.Printf("tsblast setting limit to %v per second\n", current)
			limiter.SetLimit(rate.Limit(current))
			current += incr
			if current > per_sec {
				return
			}
			time.Sleep(period)
		}
	}()

	ctx := context.TODO()

	cnt, sz := 0, 0
	lastStat := time.Now()
	statPeriod := 10 * time.Second

	tstart := time.Now()
	for {

		if total < 40000 {
			if time.Now().Sub(tstart) > 2*time.Minute {
				total += 1000
				tstart = time.Now()
				log.Printf("tsblast total now: %d", total)
			}
		}

		limiter.Wait(ctx)

		n := int64(rand.Int() % total)

		offset := time.Duration(n*10) * time.Second
		t := time.Now()
		y := sinTime(t.Add(offset)) * 100

		name := fmt.Sprintf("tsblast.test.a%02d.b%02d.c%02d", (n%1000000)/10000, (n%10000)/100, n%100)

		sz += (len(name) + 8)

		rcvr.QueueDataPoint(serde.Ident{"name": name}, t, y)

		if count%1000 == 0 {
			if time.Now().Sub(lastStat) > statPeriod {
				fmt.Printf("%v Count: %d \tper/sec: %v \tBps: %v\n", time.Now(), count, float64(cnt)/time.Now().Sub(lastStat).Seconds(), int64(float64(sz)/time.Now().Sub(lastStat).Seconds()))
				cnt, sz = 0, 0
				lastStat = time.Now()
			}
		}

		count++
		cnt++
	}
}

func sinTime(t time.Time) float64 {
	x := 2 * math.Pi / float64(span) * float64(t.Unix()%span)
	return math.Sin(x)
}
