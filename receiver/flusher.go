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

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
)

type dsFlushRequest struct {
	ds   *rrd.MetaDataSource
	resp chan bool
}

type flusherChannels []chan *dsFlushRequest

func (f flusherChannels) queueBlocking(ds *rrd.MetaDataSource, block bool) {
	fr := &dsFlushRequest{ds: ds.Copy()}
	if block {
		fr.resp = make(chan bool, 1)
	}
	f[ds.Id()%int64(len(f))] <- fr
	if block {
		<-fr.resp
	}
}

func reportFlusherChannelFillPercent(flusherCh chan *dsFlushRequest, sr statReporter, ident string, nap time.Duration) {
	fillStatName := fmt.Sprintf("receiver.flushers.%s.channel.fill_percent", ident)
	lenStatName := fmt.Sprintf("receiver.flushers.%s.channel.len", ident)
	cp := float64(cap(flusherCh))
	for {
		time.Sleep(nap)
		ln := float64(len(flusherCh))
		if cp > 0 {
			fillPct := (ln / cp) * 100
			sr.reportStatGauge(fillStatName, fillPct)
		}
		sr.reportStatGauge(lenStatName, ln)
	}
}

var flusher = func(wc wController, db serde.DataSourceFlusher, sr statReporter, flusherCh chan *dsFlushRequest) {
	wc.onEnter()
	defer wc.onExit()

	go reportFlusherChannelFillPercent(flusherCh, sr, wc.ident(), time.Second)

	log.Printf("  - %s started.", wc.ident())
	wc.onStarted()

	for {
		fr, ok := <-flusherCh
		if !ok {
			log.Printf("%s: channel closed, exiting", wc.ident())
			return
		}
		err := db.FlushDataSource(fr.ds)
		if err != nil {
			log.Printf("%s: error flushing data source %v: %v", wc.ident(), fr.ds, err)
		}
		if fr.resp != nil {
			fr.resp <- (err == nil)
		}
		sr.reportStatCount("serde.datapoints_flushed", float64(fr.ds.PointCount()))
		sr.reportStatCount("serde.flushes", 1)
	}
}
