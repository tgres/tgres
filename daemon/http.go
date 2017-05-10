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

package daemon

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/tgres/tgres/blaster"
	"github.com/tgres/tgres/dsl"
	"github.com/tgres/tgres/graceful"
	h "github.com/tgres/tgres/http"
	"github.com/tgres/tgres/receiver"
)

func httpServer(addr string, l net.Listener, rcvr *receiver.Receiver, rcache dsl.NamedDSFetcher) {

	http.HandleFunc("/metrics/find", h.GraphiteMetricsFindHandler(rcache))
	http.HandleFunc("/metrics/find/", h.GraphiteMetricsFindHandler(rcache))
	http.HandleFunc("/render", h.GraphiteRenderHandler(rcache))
	http.HandleFunc("/render/", h.GraphiteRenderHandler(rcache))

	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintf(w, "OK\n") })

	http.HandleFunc("/pixel", h.PixelHandler(rcvr))
	http.HandleFunc("/pixel/add", h.PixelAddHandler(rcvr))
	http.HandleFunc("/pixel/addgauge", h.PixelAddGaugeHandler(rcvr))
	http.HandleFunc("/pixel/setgauge", h.PixelSetGaugeHandler(rcvr))
	http.HandleFunc("/pixel/append", h.PixelAppendHandler(rcvr))

	if rcvr.Blaster != nil {
		http.HandleFunc("/blaster/set", h.BlasterSetHandler(rcvr.Blaster))
	}

	server := &http.Server{
		Addr:           addr,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 16}
	server.Serve(l)
}

type wwwServer struct {
	rcvr       *receiver.Receiver
	rcache     dsl.NamedDSFetcher
	blstr      *blaster.Blaster
	listener   *graceful.Listener
	listenSpec string
	stop       int32
}

func (g *wwwServer) File() *os.File {
	if g.listener != nil {
		return g.listener.File()
	}
	return nil
}

func (g *wwwServer) Stop() {
	if g.stopped() {
		return
	}
	if g.listener != nil {
		log.Printf("Closing listener %s\n", g.listenSpec)
		g.listener.Close()
	}
	atomic.StoreInt32(&(g.stop), 1)
}

func (g *wwwServer) stopped() bool {
	return atomic.LoadInt32(&(g.stop)) != 0
}

func (g *wwwServer) Start(file *os.File) error {
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
		log.Printf("Not starting HTTP server because http-listen-spec is blank.")
		return nil
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error starting HTTP protocol: %v\n", err)
		return fmt.Errorf("Error starting HTTP protocol: %v", err)
	}

	g.listener = graceful.NewListener(gl)

	log.Printf("HTTP protocol Listening on %s\n", processListenSpec(g.listenSpec))

	go httpServer(g.listenSpec, g.listener, g.rcvr, g.rcache)

	return nil
}
