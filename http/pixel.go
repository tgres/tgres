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

package http

import (
	"fmt"
	"github.com/tgres/tgres/aggregator"
	"github.com/tgres/tgres/misc"
	"github.com/tgres/tgres/receiver"
	"log"
	"net/http"
	"time"
)

func sendPixel(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "Tue, 03 Mar 1998 01:34:48 GMT") // 888888888
	w.Header().Set("Last-Modified", "Tue, 03 Mar 1998 01:34:48 GMT")
	w.Header().Set("X-Content-Type-Options", "nosniff") // IE: https://msdn.microsoft.com/en-us/library/gg622941(v=vs.85).aspx
	w.Header().Set("Content-Type", "image/gif")
	w.Header().Set("Cache-Control", "private, no-cache, no-cache=Set-Cookie, proxy-revalidate")
	// Date set by net/http

	w.Write([]byte("GIF89a\x01\x00\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x00;"))
}

func PixelHandler(rcvr *receiver.Receiver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rc := recover(); rc != nil {
				log.Printf("PixelHandler: Recovered (this request is dropped): %v", rc)
			}
		}()

		sendPixel(w)

		err := r.ParseForm()
		if err != nil {
			log.Printf("PixelHandler: error from ParseForm(): %v", err)
			return
		}

		for name, vals := range r.Form {

			// foo.bar.baz=12.345@1425959940

			for _, valStr := range vals {

				var val, ut float64
				n, _ := fmt.Sscanf(valStr, "%f@%f", &val, &ut)
				if n < 1 {
					log.Printf("PixelHandler: error parsing %q", valStr)
					return
				}

				var ts time.Time
				if ut == 0 {
					ts = time.Now()
				} else {
					nsec := int64(ut*1000000000) % 1000000000
					ts = time.Unix(int64(ut), nsec)
				}

				rcvr.QueueDataPoint(misc.SanitizeName(name), ts, val)
			}
		}

	}
}

func PixelAddHandler(rcvr *receiver.Receiver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pixelAggHandler(r, w, rcvr, aggregator.CmdAdd)
	}
}

func PixelAddGaugeHandler(rcvr *receiver.Receiver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pixelAggHandler(r, w, rcvr, aggregator.CmdAddGauge)
	}
}

func PixelSetGaugeHandler(rcvr *receiver.Receiver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pixelAggHandler(r, w, rcvr, aggregator.CmdSetGauge)
	}
}

func PixelAppendHandler(rcvr *receiver.Receiver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pixelAggHandler(r, w, rcvr, aggregator.CmdAppend)
	}
}

func pixelAggHandler(r *http.Request, w http.ResponseWriter, rcvr *receiver.Receiver, cmd aggregator.AggCmd) {
	defer func() {
		if rc := recover(); rc != nil {
			log.Printf("pixelAggHandler: Recovered (this request is dropped): %v", rc)
		}
	}()

	sendPixel(w)

	err := r.ParseForm()
	if err != nil {
		log.Printf("pixelAggHandler: error from ParseForm(): %v", err)
		return
	}

	for name, vals := range r.Form {

		// foo.bar.baz=12.345

		for _, valStr := range vals {

			var val float64
			n, _ := fmt.Sscanf(valStr, "%f", &val)
			if n < 1 {
				log.Printf("PixelAddHandler: error parsing %q", valStr)
				return
			}

			rcvr.QueueAggregatorCommand(aggregator.NewCommand(cmd, misc.SanitizeName(name), val))
		}
	}

}
