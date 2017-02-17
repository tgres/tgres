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
	"log"
	"net/http"

	"github.com/tgres/tgres/blaster"
)

func BlasterSetHandler(blstr *blaster.Blaster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rc := recover(); rc != nil {
				log.Printf("BlasterSetHandler: Recovered (this request is dropped): %v", rc)
			}
		}()

		err := r.ParseForm()
		if err == nil {
			for name, vals := range r.Form {
				if name == "rate" {
					for _, valStr := range vals {
						var rate int
						n, _ := fmt.Sscanf(valStr, "%d", &rate)
						if n < 1 {
							log.Printf("BlasterSetHandler: error parsing %q", valStr)
							w.WriteHeader(http.StatusInternalServerError)
							fmt.Fprintf(w, "Error\n")
							return
						}
						blstr.SetRate(rate)
						fmt.Fprintf(w, "New rate: %v\n", rate)
					}
				} else if name == "n" {
					for _, valStr := range vals {
						var ns int
						n, _ := fmt.Sscanf(valStr, "%d", &ns)
						if n < 1 {
							log.Printf("BlasterSetHandler: error parsing %q", valStr)
							w.WriteHeader(http.StatusInternalServerError)
							fmt.Fprintf(w, "Error\n")
							return
						}
						blstr.SetNSeries(ns)
						fmt.Fprintf(w, "New nSeries: %v\n", ns)
					}
				}
			}
		}
	}
}
