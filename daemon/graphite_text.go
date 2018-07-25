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
	"github.com/tgres/tgres/misc"
	"github.com/tgres/tgres/receiver"
	"github.com/tgres/tgres/serde"
)

type graphiteTextServiceManager struct {
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

func (g *graphiteTextServiceManager) Stop() {
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

func (g *graphiteTextServiceManager) File() *os.File {
	if g.conn != nil {
		f, _ := g.conn.(*net.UDPConn).File()
		return f
	}
	if g.listener != nil {
		return g.listener.File()
	}
	return nil
}

func (g *graphiteTextServiceManager) Start(file *os.File) error {
	if g.udp {
		return g.startUDP(file)
	} else {
		return g.startTCP(file)
	}
}

func (g *graphiteTextServiceManager) stopped() bool {
	return atomic.LoadInt32(&(g.stop)) != 0
}

func (g *graphiteTextServiceManager) startUDP(file *os.File) error {
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
		log.Printf("Not starting Graphite UDP protocol because graphite-udp-listen-spec is blank.")
		return nil
	}
	if err != nil {
		return fmt.Errorf("Error starting Graphite UDP Text Protocol serviceManager: %v", err)
	}

	fmt.Printf("Graphite UDP protocol Listening on %s\n", processListenSpec(g.listenSpec))

	// UDP only has one connection, unlike TCP
	go g.handleGraphiteTextProtocol(g.conn)

	return nil
}

func (g *graphiteTextServiceManager) startTCP(file *os.File) error {
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
		log.Printf("Not starting Graphite Text protocol because graphite-text-listen-spec is blank")
		return nil
	}

	if err != nil {
		return fmt.Errorf("Error starting Graphite Text Protocol serviceManager: %v", err)
	}

	g.listener = graceful.NewListener(gl)

	fmt.Println("Graphite text protocol Listening on " + processListenSpec(g.listenSpec))

	go g.graphiteTCPTextServer()

	return nil
}

func (g *graphiteTextServiceManager) graphiteTCPTextServer() error {

	var tempDelay time.Duration
	for {
		if g.stopped() {
			return nil
		}
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
				log.Printf("graphiteTCPTextServer(): Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}
		tempDelay = 0

		go g.handleGraphiteTextProtocol(conn)
	}
}

// Handles incoming requests for both TCP and UDP
func (g *graphiteTextServiceManager) handleGraphiteTextProtocol(conn net.Conn) {
	defer conn.Close() // decrements graceful.TcpWg

	if g.timeout != 0 {
		conn.SetDeadline(time.Now().Add(g.timeout))
	}

	// We use Scanner, becase it has a MaxScanTokenSize of 64K
	connbuf := bufio.NewScanner(conn)

	for connbuf.Scan() {
		packetStr := connbuf.Text()

		if name, ts, v, err := parseGraphitePacket(packetStr); err != nil {
			log.Printf("handleGraphiteTextProtocol(): bad packet: %v", packetStr)
		} else {
			g.rcvr.QueueDataPoint(serde.Ident{"name": name}, ts, v)
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
			log.Printf("handleGraphiteTextProtocol(): Error reading: %v", err)
		}
	}
}

func parseGraphitePacket(packetStr string) (string, time.Time, float64, error) {

	var (
		name   string
		tstamp int64
		value  float64
	)

	if n, err := fmt.Sscanf(packetStr, "%s %f %d", &name, &value, &tstamp); n != 3 || err != nil {
		return "", time.Time{}, 0, fmt.Errorf("error %v scanning input: %q", err, packetStr)
	}

	var t time.Time
	if tstamp == -1 { // https://github.com/graphite-project/carbon/issues/54
		t = time.Now()
	} else {
		t = time.Unix(tstamp, 0)
	}
	return misc.SanitizeName(name), t, value, nil
}
