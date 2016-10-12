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
	"github.com/tgres/tgres/serde"
	"log"
)

type flusherChannels []chan *dsFlushRequest

func (f flusherChannels) queueBlocking(rds *receiverDs, block bool) {
	fr := &dsFlushRequest{ds: rds.DataSource.Copy()}
	if block {
		fr.resp = make(chan bool, 1)
	}
	f[rds.Id()%int64(len(f))] <- fr
	if block {
		<-fr.resp
	}
}

func flusher(wc wController, db serde.DataSourceSerDe, scr statCountReporter, flusherCh chan *dsFlushRequest) {
	wc.onEnter()
	defer wc.onExit()

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
		scr.reportStatCount("serde.datapoints_flushed", float64(fr.ds.PointCount()))
	}
}
