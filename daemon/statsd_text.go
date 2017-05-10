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
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tgres/tgres/graceful"
	"github.com/tgres/tgres/receiver"
	"github.com/tgres/tgres/statsd"
)

type statsdTextServiceManager struct {
	rcvr       *receiver.Receiver
	listenSpec string
	udp        bool
	stop       int32

	// TCP
	listener *graceful.Listener
	timeout  time.Duration

	// UDP
	conn net.Conn
}

func (g *statsdTextServiceManager) Stop() {
	if g.stopped() {
		return
	}
	if g.conn != nil {
		log.Printf("Closing UDP listener %s", g.listenSpec)
		g.conn.Close()
	}
	if g.listener != nil {
		log.Printf("Closing TCP listener %s", g.listenSpec)
		g.listener.Close()
	}
	atomic.StoreInt32(&(g.stop), 1)
}

func (g *statsdTextServiceManager) stopped() bool {
	return atomic.LoadInt32(&(g.stop)) != 0
}

func (g *statsdTextServiceManager) File() *os.File {
	if g.conn != nil {
		f, _ := g.conn.(*net.UDPConn).File()
		return f
	}
	if g.listener != nil {
		return g.listener.File()
	}
	return nil
}

func (g *statsdTextServiceManager) Start(file *os.File) error {
	if g.udp {
		return g.startUDP(file)
	} else {
		return g.startTCP(file)
	}
}

func (g *statsdTextServiceManager) startUDP(file *os.File) error {
	var (
		err     error
		udpAddr *net.UDPAddr
	)

	if g.listenSpec != "" {
		if file != nil {
			g.conn, err = net.FileConn(file)
		} else {
			udpAddr, err = net.ResolveUDPAddr("udp", processListenSpec(g.listenSpec))
			if err == nil {
				g.conn, err = net.ListenUDP("udp", udpAddr)
			}
		}
	} else {
		log.Printf("Not starting Statsd UDP protocol because statsd-udp-listen-spec is blank.")
		return nil
	}
	if err != nil {
		return fmt.Errorf("Error starting Statsd UDP Text Protocol serviceManager: %v", err)
	}

	log.Printf("Statsd UDP protocol Listening on %s\n", processListenSpec(g.listenSpec))

	// for UDP timeout must be 0
	go g.handleStatsdTextProtocol(g.conn)

	return nil
}

func (g *statsdTextServiceManager) startTCP(file *os.File) error {
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
		log.Printf("Not starting Statsd TCP protocol because statsd-text-listen-spec is blank")
		return nil
	}

	if err != nil {
		return fmt.Errorf("Error starting Statsd Text Protocol serviceManager: %v", err)
	}

	g.listener = graceful.NewListener(gl)

	fmt.Println("Statsd TCP protocol Listening on " + processListenSpec(g.listenSpec))

	go g.statsdTCPTextServer()

	return nil
}

func (g *statsdTextServiceManager) statsdTCPTextServer() error {

	var tempDelay time.Duration
	for {
		conn, err := g.listener.Accept()

		if err != nil {
			// see http://golang.org/src/net/http/server.go?s=51504:51550#L1729
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Printf("statsdTCPTextServer(): Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}
		tempDelay = 0

		go g.handleStatsdTextProtocol(conn)
	}
}

func (g *statsdTextServiceManager) handleStatsdTextProtocol(conn net.Conn) {
	defer conn.Close() // decrements graceful.TcpWg

	if g.timeout != 0 {
		conn.SetDeadline(time.Now().Add(g.timeout))
	}

	// We use Scanner, becase it has a MaxScanTokenSize of 64K
	connbuf := bufio.NewScanner(conn)

	for connbuf.Scan() {
		if stat, err := statsd.ParseStatsdPacket(connbuf.Text()); err == nil {
			g.rcvr.QueueAggregatorCommand(stat.AggregatorCmd())
		} else {
			log.Printf("parseStatsdPacket(): %v", err)
		}

		if g.timeout != 0 {
			conn.SetDeadline(time.Now().Add(g.timeout))
		}

		if g.stopped() {
			return
		}
	}

	if err := connbuf.Err(); err != nil {
		if !strings.Contains(err.Error(), "use of closed") {
			log.Println("handleStatsdTextProtocol(): Error reading: %v", err)
		}
	}
}
