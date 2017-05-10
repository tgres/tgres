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
	"log"
	"os"
	"strings"
	"time"

	"github.com/tgres/tgres/dsl"
	"github.com/tgres/tgres/graceful"
	"github.com/tgres/tgres/receiver"
)

type trService interface {
	File() *os.File
	Start(*os.File) error
	Stop()
}

type serviceMap map[string]trService
type serviceManager struct {
	rcvr     *receiver.Receiver
	services serviceMap
}

func newServiceManager(rcvr *receiver.Receiver, rcache dsl.NamedDSFetcher, cfg *Config) *serviceManager {
	return &serviceManager{rcvr: rcvr,
		services: serviceMap{
			"gt":  &graphiteTextServiceManager{rcvr: rcvr, listenSpec: cfg.GraphiteTextListenSpec, timeout: 30 * time.Second},
			"gu":  &graphiteTextServiceManager{rcvr: rcvr, listenSpec: cfg.GraphiteUdpListenSpec, udp: true},
			"gp":  &graphitePickleServiceManager{rcvr: rcvr, listenSpec: cfg.GraphitePickleListenSpec},
			"st":  &statsdTextServiceManager{rcvr: rcvr, listenSpec: cfg.StatsdTextListenSpec, timeout: 30 * time.Second},
			"su":  &statsdTextServiceManager{rcvr: rcvr, listenSpec: cfg.StatsdUdpListenSpec, udp: true},
			"www": &wwwServer{rcvr: rcvr, rcache: rcache, listenSpec: cfg.HttpListenSpec},
		},
	}
}

func processListenSpec(listenSpec string) string {
	if os.Getenv("TGRES_BIND") != "" {
		return strings.Replace(listenSpec, "0.0.0.0", os.Getenv("TGRES_BIND"), 1)
	}
	return listenSpec
}

func (r *serviceManager) run(gracefulProtos string) error {
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
		log.Printf("Reusing file descriptors for graceful protocols: %v", protos)

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

func (r *serviceManager) listenerFilesAndProtocols() ([]*os.File, string) {

	files := []*os.File{}
	protos := []string{}

	for name, service := range r.services {
		files = append(files, service.File())
		protos = append(protos, name)
	}
	return files, strings.Join(protos, ",")
}

func (r *serviceManager) closeListeners(wait bool) {
	for _, service := range r.services {
		service.Stop()
	}
	if wait {
		log.Printf("Waiting for graceful.TcpWg...")
		graceful.TcpWg.Wait()
	}
}
