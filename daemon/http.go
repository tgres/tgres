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
	"net"
	"net/http"
	"time"

	"github.com/tgres/tgres/dsl"
	h "github.com/tgres/tgres/http"
	"github.com/tgres/tgres/receiver"
)

func httpServer(addr string, l net.Listener, rcvr *receiver.Receiver, rcache dsl.NamedDSFetcher) {

	http.HandleFunc("/metrics/find", h.GraphiteMetricsFindHandler(rcache))
	http.HandleFunc("/render", h.GraphiteRenderHandler(rcache))

	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintf(w, "OK\n") })

	http.HandleFunc("/pixel", h.PixelHandler(rcvr))
	http.HandleFunc("/pixel/add", h.PixelAddHandler(rcvr))
	http.HandleFunc("/pixel/addgauge", h.PixelAddGaugeHandler(rcvr))
	http.HandleFunc("/pixel/setgauge", h.PixelSetGaugeHandler(rcvr))
	http.HandleFunc("/pixel/append", h.PixelAppendHandler(rcvr))

	server := &http.Server{
		Addr:           addr,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 16}
	server.Serve(l)
}
