//
// Copyright 2015 Gregory Trubetskoy. All Rights Reserved.
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

package timeriver

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type trTransceiver struct {
	serviceMgr     *trServiceManager
	dss            *trDataSources
	dpCh           chan *trDataPoint     // incoming data point
	workerChs      []chan *trDataPoint   // incoming data point with ds
	flusherChs     []chan *trDataSource  // ds to flush
	dsCheckChs     []chan int64          // check ds id if it's time to flush
	dsFlushChs     []chan int64          // flush ds id immediately
	dsCopyChs      []chan *dsCopyRequest // request a copy of a DS
	toFlushCh      chan int64            // should be flushed eventually
	flushedCh      chan int64            // has been flushed
	workerWg       sync.WaitGroup
	flusherWg      sync.WaitGroup
	flushWatcherWg sync.WaitGroup
	dispatcherWg   sync.WaitGroup
	startWg        sync.WaitGroup
}

type dsCopyRequest struct {
	dsId int64
	resp chan *trDataSource
}

func newTransceiver() *trTransceiver {
	dss := &trDataSources{}
	return &trTransceiver{dss: dss,
		dpCh:      make(chan *trDataPoint, 1048576), // so we can survive a graceful restart
		toFlushCh: make(chan int64, 128),
		flushedCh: make(chan int64, 128)}
}

func (t *trTransceiver) start(gracefulProtos string) error {
	t.startWorkers()
	t.startFlushers()
	t.serviceMgr = newServiceManager(t)

	if err := t.serviceMgr.run(gracefulProtos); err != nil {
		return err
	}

	// Wait for workers/flushers to start correctly
	t.startWg.Wait()

	if gracefulProtos != "" {
		parent := syscall.Getppid()
		log.Printf("start(): Killing parent pid: %v", parent)
		syscall.Kill(parent, syscall.SIGTERM)
		log.Printf("start(): Waiting for the parent to signal that flush is complete...")
		ch := make(chan os.Signal)
		signal.Notify(ch, syscall.SIGUSR1)
		s := <-ch
		log.Printf("start(): Received %v, proceeding to load the data", s)
	}

	t.dss.reload() // *finally* load the data (because graceful restart)

	go t.dispatcher() // now start dispatcher

	return nil
}

func (t *trTransceiver) stop() {

	t.serviceMgr.closeListeners()
	log.Printf("Waiting for all TCP connections to finish...")
	tcpWg.Wait()
	log.Printf("TCP connections finished.")

	log.Printf("Closing dispatcher channel...")
	close(t.dpCh)
	t.dispatcherWg.Wait()
	log.Printf("Dispatcher finished.")

}

func (t *trTransceiver) stopFlushWatcher() {
	log.Printf("stopFlushWatcher(): closing channel...")
	close(t.toFlushCh) // triggers the flushWatcher to request every DS flush
	log.Printf("stopFlushWatcher(): waiting on flushWatcher to finish...")
	t.flushWatcherWg.Wait()
	log.Printf("stopFlushWatcher(): flushWatcher finished.")
}

func (t *trTransceiver) stopWorkers() {
	log.Printf("stopWorkers(): waiting for worker channels to empty...")
	empty := false
	for !empty {
		empty = true
		for _, c := range t.workerChs {
			if len(c) > 0 {
				empty = false
				break
			}
		}
		if empty {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.stopFlushWatcher()
	log.Printf("stopWorkers(): closing all worker channels...")
	for _, ch := range t.dsFlushChs { // this is correct
		close(ch)
	}
	log.Printf("stopWorkers(): waiting for workers to finish...")
	t.workerWg.Wait()
	log.Printf("stopWorkers(): all workers finished.")
}

func (t *trTransceiver) stopFlushers() {
	log.Printf("stopFlushers(): closing all flusher channels...")
	for _, ch := range t.flusherChs {
		close(ch)
	}
	log.Printf("stopFlushers(): waiting for flushers to finish...")
	t.flusherWg.Wait()
	log.Printf("stopFlushers(): all flushers finished.")
}

func (t *trTransceiver) dispatcher() {
	t.dispatcherWg.Add(1)
	defer t.dispatcherWg.Done()

	for {
		dp, ok := <-t.dpCh

		if !ok {
			log.Printf("dispatcher(): channel closed, shutting down")
			t.stopWorkers() // stops the flush watcher too
			t.stopFlushers()
			break
		}

		if dp.ds = t.dss.getByName(dp.Name); dp.ds == nil {
			// DS does not exist, can we create it?
			if dsSpec := config.findMatchingDsSpec(dp.Name); dsSpec != nil {
				if ds, err := createDataSource(dp.Name, dsSpec); err == nil {
					t.dss.insert(ds)
					dp.ds = ds
				} else {
					log.Printf("dispatcher(): createDataSource() error: %v", err)
					continue
				}
			}
		}

		if dp.ds != nil {
			t.workerChs[dp.ds.Id%int64(config.Workers)] <- dp
		}
	}
}

func (t *trTransceiver) queueDataPoint(name string, tstamp time.Time, value float64) {
	dp := &trDataPoint{Name: name, TimeStamp: tstamp, Value: value}
	t.dpCh <- dp
}

func (t *trTransceiver) requestDsCopy(id int64) *trDataSource {
	req := &dsCopyRequest{id, make(chan *trDataSource)}
	t.dsCopyChs[id%int64(config.Workers)] <- req
	return <-req.resp
}

func (t *trTransceiver) worker(id int64) {

	t.workerWg.Add(1)
	defer t.workerWg.Done()

	log.Printf("worker(%d) starting.", id)

	var ds *trDataSource

	t.startWg.Done()
	for {

		flush := false
		select {
		case dp := <-t.workerChs[id]:
			ds = dp.ds // at this point dp.ds has to be already set
			if err := dp.process(); err != nil {
				log.Printf("worker(%d): dp.process() error: %v", id, err)
			} else {
				t.toFlushCh <- ds.Id
			}
		case dsId := <-t.dsCheckChs[id]:
			ds = t.dss.getById(dsId)
			if ds == nil {
				log.Printf("worker(%d): WAT? cannot lookup ds id (%d) sent from flushWatcher?", id, dsId)
				continue
			}
		case dsId, ok := <-t.dsFlushChs[id]:
			if ok {
				ds = t.dss.getById(dsId)
				if ds == nil {
					log.Printf("worker(%d): cannot lookup ds id (%d) sent from flushWatcher?", id, dsId)
					continue
				} else {
					flush = true
				}
			} else {
				log.Printf("worker(%d): channel closed, exiting.", id)
				return
			}
		case r := <-t.dsCopyChs[id]:
			ds = t.dss.getById(r.dsId)
			if ds == nil {
				log.Printf("worker(%d): WAT? cannot lookup ds id (%d) sent for copy?", id, r.dsId)
			}
			r.resp <- ds.flushCopy()
			close(r.resp)
			continue
		}

		if !flush {
			pc := ds.pointCount()
			if pc > config.MaxCachedPoints {
				flush = true
			} else if ds.LastUpdateRT.Add(config.MaxCache.Duration).Before(time.Now()) {
				flush = true
			}
		}

		if flush {
			if ds.LastFlushRT.Add(config.MinCache.Duration).Before(time.Now()) {
				t.flusherChs[ds.Id%int64(config.Workers)] <- ds.flushCopy()
				ds.LastFlushRT = time.Now()
				ds.clearRRAs()
				t.flushedCh <- ds.Id
			}
		}
	}
}

func (t *trTransceiver) startWorkers() {

	t.workerChs = make([]chan *trDataPoint, config.Workers)
	t.dsCheckChs = make([]chan int64, config.Workers)
	t.dsFlushChs = make([]chan int64, config.Workers)
	t.dsCopyChs = make([]chan *dsCopyRequest, config.Workers)

	t.startWg.Add(config.Workers)
	for i := 0; i < config.Workers; i++ {
		t.workerChs[i] = make(chan *trDataPoint, 1024)
		t.dsCheckChs[i] = make(chan int64, 1024)
		t.dsFlushChs[i] = make(chan int64, 1024)
		t.dsCopyChs[i] = make(chan *dsCopyRequest, 1024)

		go t.worker(int64(i))
	}
	log.Printf("Started %d workers.", config.Workers)
}

func (t *trTransceiver) flusher(id int64) {
	t.flusherWg.Add(1)
	defer t.flusherWg.Done()

	log.Printf("flusher(%d) starting.", id)

	t.startWg.Done()
	for {
		ds, ok := <-t.flusherChs[id]
		if ok {
			if err := flushDataSource(ds); err != nil {
				log.Printf("flusher(%d): error flushing data source %v: %v", id, ds, err)
			}
		} else {
			log.Printf("flusher(%d): channel closed, exiting", id)
			break
		}
	}

}

func (t *trTransceiver) startFlushers() {

	t.flusherChs = make([]chan *trDataSource, config.Workers)

	t.startWg.Add(config.Workers + 1)
	for i := 0; i < config.Workers; i++ {
		t.flusherChs[i] = make(chan *trDataSource)
		go t.flusher(int64(i))
	}
	go t.flushWatcher()

	log.Printf("Started %d flushers and the flushWatcher", config.Workers)
}

func (t *trTransceiver) flushWatcher() {

	t.flushWatcherWg.Add(1)
	defer t.flushWatcherWg.Done()

	var id int64

	byId := make(map[int64]time.Time)
	lastCheck := time.Now()

	periodicWake := make(chan bool, 1)
	go func() {
		for {
			time.Sleep(time.Duration(5) * time.Second) // TODO make me configurable?
			periodicWake <- true
		}
	}()

	t.startWg.Done()
	for {

		var ok bool = true // flush everything if false
		select {
		case id, ok = <-t.toFlushCh:
			if ok {
				byId[id] = time.Now()
			}
		case id = <-t.flushedCh:
			delete(byId, id)
		case <-periodicWake:
			// nothing to do
		}

		// See what in the map needs to be flushed
		if !ok || lastCheck.Add(config.MaxCache.Duration).Before(time.Now()) {
			for i, tm := range byId {
				if !ok {
					t.dsFlushChs[i%int64(config.Workers)] <- i
				} else if tm.Add(config.MaxCache.Duration).Before(time.Now()) {
					t.dsCheckChs[i%int64(config.Workers)] <- i
				}
			}
			lastCheck = time.Now()
		}

		if !ok {
			log.Printf("flushWatcher(): toFlush channel closed, notified to flush everyting, then exiting.")
			go func() {
				for {
					<-t.flushedCh // /dev/null
				}
			}()
			return
		}
	}
}
