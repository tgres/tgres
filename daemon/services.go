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
	pickle "github.com/hydrogen18/stalecucumber"
	"github.com/tgres/tgres/graceful"
	"github.com/tgres/tgres/misc"
	"github.com/tgres/tgres/receiver"
	"github.com/tgres/tgres/statsd"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

type trService interface {
	File() *os.File
	Start(*os.File) error
	Stop()
}

type serviceMap map[string]trService
type ServiceManager struct {
	rcvr     *receiver.Receiver
	services serviceMap
}

func newServiceManager(rcvr *receiver.Receiver) *ServiceManager {
	return &ServiceManager{rcvr: rcvr,
		services: serviceMap{
			"gt":  &graphiteTextServiceManager{rcvr: rcvr},
			"gu":  &graphiteUdpTextServiceManager{rcvr: rcvr},
			"gp":  &graphitePickleServiceManager{rcvr: rcvr},
			"su":  &statsdUdpTextServiceManager{rcvr: rcvr},
			"www": &wwwServer{rcvr: rcvr},
		},
	}
}

func processListenSpec(listenSpec string) string {
	if os.Getenv("TGRES_BIND") != "" {
		return strings.Replace(listenSpec, "0.0.0.0", os.Getenv("TGRES_BIND"), 1)
	}
	return listenSpec
}

func (r *ServiceManager) run(gracefulProtos string) error {

	// TODO If a listen-spec changes in the config and a graceful
	// restart is issued, the new config will not take effect as the
	// open file is reused.

	if gracefulProtos == "" {
		for _, service := range r.services {
			if err := service.Start(nil); err != nil {
				return err
			}
		}
	} else {

		protos := strings.Split(gracefulProtos, ",")

		for n, p := range protos {
			f := os.NewFile(uintptr(n+3), "")
			if r.services[p] != nil {
				if err := r.services[p].Start(f); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *ServiceManager) listenerFilesAndProtocols() ([]*os.File, string) {

	files := []*os.File{}
	protos := []string{}

	for name, service := range r.services {
		files = append(files, service.File())
		protos = append(protos, name)
	}
	return files, strings.Join(protos, ",")
}

func (r *ServiceManager) closeListeners() {
	for _, service := range r.services {
		service.Stop()
	}
	graceful.TcpWg.Wait()
}

// ---

type wwwServer struct {
	rcvr     *receiver.Receiver
	listener *graceful.Listener
}

func (g *wwwServer) File() *os.File {
	if g.listener != nil {
		return g.listener.File()
	}
	return nil
}

func (g *wwwServer) Stop() {
	if g.listener != nil {
		g.listener.Close()
	}
}

func (g *wwwServer) Start(file *os.File) error {
	var (
		gl  net.Listener
		err error
	)

	if Cfg.HttpListenSpec != "" {
		if file != nil {
			gl, err = net.FileListener(file)
		} else {
			gl, err = net.Listen("tcp", processListenSpec(Cfg.HttpListenSpec))
		}
	} else {
		fmt.Printf("Not starting HTTP server because http-listen-spec is blank.\n")
		log.Printf("Not starting HTTP server because http-listen-spec is blank.")
		return nil
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error starting HTTP protocol: %v\n", err)
		return fmt.Errorf("Error starting HTTP protocol: %v", err)
	}

	g.listener = graceful.NewListener(gl)

	fmt.Printf("HTTP protocol Listening on %s\n", processListenSpec(Cfg.HttpListenSpec))

	go httpServer(Cfg.HttpListenSpec, g.listener, g.rcvr)

	return nil
}

// ---

type graphitePickleServiceManager struct {
	rcvr     *receiver.Receiver
	listener *graceful.Listener
}

func (g *graphitePickleServiceManager) File() *os.File {
	if g.listener != nil {
		return g.listener.File()
	}
	return nil
}

func (g *graphitePickleServiceManager) Stop() {
	if g.listener != nil {
		g.listener.Close()
	}
}

func (g *graphitePickleServiceManager) Start(file *os.File) error {
	var (
		gl  net.Listener
		err error
	)

	if Cfg.GraphitePickleListenSpec != "" {
		if file != nil {
			gl, err = net.FileListener(file)
		} else {
			gl, err = net.Listen("tcp", processListenSpec(Cfg.GraphitePickleListenSpec))
		}
	} else {
		log.Printf("Not starting Graphite Pickle Protocol because graphite-pickle-listen-spec is blank.")
		return nil
	}

	if err != nil {
		return fmt.Errorf("Error starting Graphite Pickle Protocol serviceManager: %v", err)
	}

	g.listener = graceful.NewListener(gl)

	fmt.Printf("Graphite Pickle protocol Listening on %s\n", processListenSpec(Cfg.GraphitePickleListenSpec))

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

		go handleGraphitePickleProtocol(g.rcvr, conn, 10)
	}
}

func handleGraphitePickleProtocol(rcvr *receiver.Receiver, conn net.Conn, timeout int) {

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

	items, err = pickle.ListOrTuple(pickle.Unpickle(conn))
	if err == nil {
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
					rcvr.QueueDataPoint(name, time.Unix(tstamp, 0), value)
				} else {
					err = fmt.Errorf("dp wrong length: %d", len(dp))
					break
				}
			} else {
				err = fmt.Errorf("item wrong length: %d", len(itemSlice))
				break
			}
		}
	}

	if timeout != 0 {
		conn.SetDeadline(time.Now().Add(time.Duration(timeout) * time.Second))
	}

	if err != nil {
		log.Println("handleGraphitePickleProtocol(): Error reading:", err.Error())
	}
}

// --

type graphiteUdpTextServiceManager struct {
	rcvr *receiver.Receiver
	conn net.Conn
}

func (g *graphiteUdpTextServiceManager) Stop() {
	if g.conn != nil {
		g.conn.Close()
	}
}

func (g *graphiteUdpTextServiceManager) File() *os.File {
	if g.conn != nil {
		f, _ := g.conn.(*net.UDPConn).File()
		return f
	}
	return nil
}

func (g *graphiteUdpTextServiceManager) Start(file *os.File) error {
	var (
		err     error
		udpAddr *net.UDPAddr
	)

	if Cfg.GraphiteUdpListenSpec != "" {
		if file != nil {
			g.conn, err = net.FileConn(file)
		} else {
			udpAddr, err = net.ResolveUDPAddr("udp", processListenSpec(Cfg.GraphiteUdpListenSpec))
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

	fmt.Printf("Graphite UDP protocol Listening on %s\n", processListenSpec(Cfg.GraphiteTextListenSpec))

	// for UDP timeout must be 0
	go handleGraphiteTextProtocol(g.rcvr, g.conn, 0)

	return nil
}

// ---

type graphiteTextServiceManager struct {
	rcvr     *receiver.Receiver
	listener *graceful.Listener
}

func (g *graphiteTextServiceManager) File() *os.File {
	if g.listener != nil {
		return g.listener.File()
	}
	return nil
}

func (g *graphiteTextServiceManager) Stop() {
	if g.listener != nil {
		g.listener.Close()
	}
}

func (g *graphiteTextServiceManager) Start(file *os.File) error {
	var (
		gl  net.Listener
		err error
	)

	if Cfg.GraphiteTextListenSpec != "" {
		if file != nil {
			gl, err = net.FileListener(file)
		} else {
			gl, err = net.Listen("tcp", processListenSpec(Cfg.GraphiteTextListenSpec))
		}
	} else {
		log.Printf("Not starting Graphite Text protocol because graphite-test-listen-spec is blank")
		return nil
	}

	if err != nil {
		return fmt.Errorf("Error starting Graphite Text Protocol serviceManager: %v", err)
	}

	g.listener = graceful.NewListener(gl)

	fmt.Println("Graphite text protocol Listening on " + processListenSpec(Cfg.GraphiteTextListenSpec))

	go g.graphiteTextServer()

	return nil
}

func (g *graphiteTextServiceManager) graphiteTextServer() error {

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
				log.Printf("graphiteTextServer(): Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}
		tempDelay = 0

		go handleGraphiteTextProtocol(g.rcvr, conn, 10)
	}
}

// Handles incoming requests for both TCP and UDP
func handleGraphiteTextProtocol(rcvr *receiver.Receiver, conn net.Conn, timeout int) {

	defer conn.Close() // decrements graceful.TcpWg

	if timeout != 0 {
		conn.SetDeadline(time.Now().Add(time.Duration(timeout) * time.Second))
	}

	// We use the Scanner, becase it has a MaxScanTokenSize of 64K

	connbuf := bufio.NewScanner(conn)

	for connbuf.Scan() {
		packetStr := connbuf.Text()

		if name, ts, v, err := parseGraphitePacket(packetStr); err != nil {
			log.Printf("handleGraphiteTextProtocol(): bad backet: %v")
		} else {
			rcvr.QueueDataPoint(name, ts, v)
		}

		if timeout != 0 {
			conn.SetDeadline(time.Now().Add(time.Duration(timeout) * time.Second))
		}
	}

	if err := connbuf.Err(); err != nil {
		log.Println("handleGraphiteTextProtocol(): Error reading: %v", err)
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

	return misc.SanitizeName(name), time.Unix(tstamp, 0), value, nil
}

// TODO isn't this identical to handleGraphiteTextProtocol?
func handleStatsdTextProtocol(rcvr *receiver.Receiver, conn net.Conn, timeout int) {
	defer conn.Close() // decrements graceful.TcpWg

	if timeout != 0 {
		conn.SetDeadline(time.Now().Add(time.Duration(timeout) * time.Second))
	}

	// We use the Scanner, becase it has a MaxScanTokenSize of 64K

	connbuf := bufio.NewScanner(conn)

	for connbuf.Scan() {
		if stat, err := statsd.ParseStatsdPacket(connbuf.Text()); err == nil {
			rcvr.QueueAggregatorCommand(stat.AggregatorCmd())
		} else {
			log.Printf("parseStatsdPacket(): %v", err)
		}

		if timeout != 0 {
			conn.SetDeadline(time.Now().Add(time.Duration(timeout) * time.Second))
		}
	}

	if err := connbuf.Err(); err != nil {
		log.Println("handleStatsdTextProtocol(): Error reading: %v", err)
	}
}

// --

type statsdUdpTextServiceManager struct {
	rcvr *receiver.Receiver
	conn net.Conn
}

func (g *statsdUdpTextServiceManager) Stop() {
	if g.conn != nil {
		g.conn.Close()
	}
}

func (g *statsdUdpTextServiceManager) File() *os.File {
	if g.conn != nil {
		f, _ := g.conn.(*net.UDPConn).File()
		return f
	}
	return nil
}

func (g *statsdUdpTextServiceManager) Start(file *os.File) error {
	var (
		err     error
		udpAddr *net.UDPAddr
	)

	if Cfg.StatsdUdpListenSpec != "" {
		if file != nil {
			g.conn, err = net.FileConn(file)
		} else {
			udpAddr, err = net.ResolveUDPAddr("udp", processListenSpec(Cfg.StatsdUdpListenSpec))
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

	fmt.Printf("Statsd UDP protocol Listening on %s\n", processListenSpec(Cfg.StatsdUdpListenSpec))

	// for UDP timeout must be 0
	go handleStatsdTextProtocol(g.rcvr, g.conn, 0)

	return nil
}
