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

package daemon

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	pickle "github.com/hydrogen18/stalecucumber"
	"github.com/tgres/tgres/graceful"
	"github.com/tgres/tgres/receiver"
	"github.com/tgres/tgres/serde"
)

type graphitePickleServiceManager struct {
	rcvr       *receiver.Receiver
	listener   *graceful.Listener
	listenSpec string
	stop       int32
}

func (g *graphitePickleServiceManager) File() *os.File {
	if g.listener != nil {
		return g.listener.File()
	}
	return nil
}

func (g *graphitePickleServiceManager) Stop() {
	if g.stopped() {
		return
	}
	if g.listener != nil {
		log.Printf("Closing listener %s\n", g.listenSpec)
		g.listener.Close()
	}
	atomic.StoreInt32(&(g.stop), 1)
}

func (g *graphitePickleServiceManager) stopped() bool {
	return atomic.LoadInt32(&(g.stop)) != 0
}

func (g *graphitePickleServiceManager) Start(file *os.File) error {
	var (
		gl  net.Listener
		err error
	)

	if g.listenSpec != "" {
		if file != nil {
			gl, err = net.FileListener(file)
		} else {
			gl, err = net.Listen("tcp", processListenSpec(g.listenSpec))
		}
	} else {
		log.Printf("Not starting Graphite Pickle Protocol because graphite-pickle-listen-spec is blank.")
		return nil
	}

	if err != nil {
		return fmt.Errorf("Error starting Graphite Pickle Protocol serviceManager: %v", err)
	}

	g.listener = graceful.NewListener(gl)

	log.Printf("Graphite Pickle protocol Listening on %s\n", processListenSpec(g.listenSpec))

	go g.graphitePickleServer()

	return nil
}

func (g *graphitePickleServiceManager) graphitePickleServer() error {

	var tempDelay time.Duration
	for {
		conn, err := g.listener.Accept()

		// This code comes from the golang http lib, it attempts to
		// retry accepting a connection when too many files are open
		// under heavy load.
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Printf("Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}
		tempDelay = 0

		go g.handleGraphitePickleProtocol(conn, 30)
	}
}

func (g *graphitePickleServiceManager) handleGraphitePickleProtocol(conn net.Conn, timeout int) {

	defer conn.Close() // decrements graceful.TcpWg

	if timeout != 0 {
		conn.SetDeadline(time.Now().Add(time.Duration(timeout) * time.Second))
	}

	// We use the Scanner, becase it has a MaxScanTokenSize of 64K

	var (
		name                 string
		tstamp               int64
		int_value            int64
		value                float64
		err                  error
		item                 interface{}
		items, itemSlice, dp []interface{}
	)

	for {
		var (
			length uint32
			nRead  int
		)

		if g.stopped() {
			return
		}

		if err = binary.Read(conn, binary.BigEndian, &length); err != nil {
			break
		}

		buff := make([]byte, length)
		if nRead, err = conn.Read(buff); err != nil {
			break
		}
		if nRead != int(length) {
			err = fmt.Errorf("Icomplete read. length: %v nRead: %v", length, nRead)
			break
		}

		if items, err = pickle.ListOrTuple(pickle.Unpickle(bytes.NewBuffer(buff))); err != nil {
			break
		}

		for _, item = range items {
			itemSlice, err = pickle.ListOrTuple(item, err)
			if len(itemSlice) == 2 {
				name, err = pickle.String(itemSlice[0], err)
				dp, err = pickle.ListOrTuple(itemSlice[1], err)
				if len(dp) == 2 {
					tstamp, err = pickle.Int(dp[0], err)
					if value, err = pickle.Float(dp[1], err); err != nil {
						if _, ok := err.(pickle.WrongTypeError); ok {
							if int_value, err = pickle.Int(dp[1], nil); err == nil {
								value = float64(int_value)
							}
						}
					}
					g.rcvr.QueueDataPoint(serde.Ident{"name": name}, time.Unix(tstamp, 0), value)
				} else {
					err = fmt.Errorf("dp wrong length: %d", len(dp))
					break
				}
			} else {
				err = fmt.Errorf("item wrong length: %d", len(itemSlice))
				break
			}
		}
		if err != nil {
			break
		}
	}

	if timeout != 0 {
		conn.SetDeadline(time.Now().Add(time.Duration(timeout) * time.Second))
	}

	if err != nil {
		if !strings.Contains(err.Error(), "use of closed") {
			log.Printf("handleGraphitePickleProtocol(): Error reading: %v", err)
		}
	}
}
