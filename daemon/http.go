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
	h "github.com/tgres/tgres/http"
	x "github.com/tgres/tgres/transceiver"
	"net"
	"net/http"
	"time"
)

func httpServer(addr string, l net.Listener, t *x.Transceiver) {

	http.HandleFunc("/metrics/find", h.GraphiteMetricsFindHandler(t))
	http.HandleFunc("/render", h.GraphiteRenderHandler(t))
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintf(w, "OK\n") })
	http.HandleFunc("/pixel", h.PixelHandler(t))
	http.HandleFunc("/pixel/add", h.PixelAddHandler(t))
	http.HandleFunc("/pixel/addgauge", h.PixelAddGaugeHandler(t))
	http.HandleFunc("/pixel/setgauge", h.PixelSetGaugeHandler(t))
	http.HandleFunc("/pixel/append", h.PixelAppendHandler(t))

	server := &http.Server{
		Addr:           addr,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 16}
	server.Serve(l)
}
