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
	"testing"
	"time"

	"github.com/tgres/tgres/cluster"
	"github.com/tgres/tgres/receiver"
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
	"github.com/tgres/tgres/series"
)

func Test_Init(t *testing.T) {

	// Stub out all the function Init calls

	// readConfig
	save_readConfig := readConfig
	readConfig = func(cfgPath string) (*Config, error) {
		return &Config{}, nil
	}

	// getCwd
	save_getCwd := getCwd
	getCwd = func() string { return "cwd" }

	// processConfig
	save_processConfig := processConfig
	processConfig = func(c configer, wd string) error { return nil }

	// savePid
	save_savePid := savePid
	savePid = func(pidPath string) error { return nil }

	// initDb
	save_initDb := initDb
	initDb = func(connectString string) (serde.DbSerDe, error) { return &fakeSerde{}, nil }

	// determineClusterBindAddress
	save_determineClusterBindAddress := determineClusterBindAddress
	determineClusterBindAddress = func(db serde.DbAddresser) (bindAddr, advAddr string, err error) {
		return "", "", nil
	}

	// determineClusterJoinAddress
	save_determineClusterJoinAddress := determineClusterJoinAddress
	determineClusterJoinAddress = func(join string, db serde.DbAddresser) (ips []string, err error) {
		return ips, err
	}

	// initCluster
	save_initCluster := initCluster
	initCluster = func(bindAddr, advAddr string, joinIps []string) (c *cluster.Cluster, err error) {
		return nil, nil
	}

	// createReceiver
	save_createReceiver := createReceiver
	createReceiver = func(cfg *Config, c *cluster.Cluster, db serde.SerDe) *receiver.Receiver {
		return receiver.New(db, nil)
	}

	save_startReceiver := startReceiver
	startReceiver = func(r *receiver.Receiver) {}

	// waitForSignal
	save_waitForSignal := waitForSignal
	waitForSignal = func(r *receiver.Receiver, sm *serviceManager, cfgPath, join string) {}

	Init("", "", "")

	// restore
	readConfig = save_readConfig
	getCwd = save_getCwd
	processConfig = save_processConfig
	savePid = save_savePid
	initDb = save_initDb
	determineClusterBindAddress = save_determineClusterBindAddress
	determineClusterJoinAddress = save_determineClusterJoinAddress
	initCluster = save_initCluster
	createReceiver = save_createReceiver
	startReceiver = save_startReceiver
	waitForSignal = save_waitForSignal
}

type fakeSerde struct {
	flushCalled, createCalled, fetchCalled int
	fakeErr                                bool
	returnDss                              []rrd.DataSourcer
	nondb                                  bool
}

func (m *fakeSerde) Fetcher() serde.Fetcher                                     { return m }
func (m *fakeSerde) Flusher() serde.Flusher                                     { return nil } // Flushing not supported
func (m *fakeSerde) EventListener() serde.EventListener                         { return nil } // not supported
func (m *fakeSerde) DbAddresser() serde.DbAddresser                             { return m }
func (f *fakeSerde) FetchDataSourceById(id int64) (rrd.DataSourcer, error)      { return nil, nil }
func (m *fakeSerde) Search(query serde.SearchQuery) (serde.SearchResult, error) { return nil, nil }
func (f *fakeSerde) FetchSeries(ds rrd.DataSourcer, from, to time.Time, maxPoints int64) (series.Series, error) {
	return nil, nil
}
func (*fakeSerde) ListDbClientIps() ([]string, error) { return nil, nil }
func (*fakeSerde) MyDbAddr() (*string, error)         { return nil, nil }

func (f *fakeSerde) FetchDataSources(_ time.Duration) ([]rrd.DataSourcer, error) {
	f.fetchCalled++
	if f.fakeErr {
		return nil, fmt.Errorf("some error")
	}
	if len(f.returnDss) == 0 {
		return make([]rrd.DataSourcer, 0), nil
	}
	return f.returnDss, nil
}

func (f *fakeSerde) FlushDataSource(ds rrd.DataSourcer) error {
	f.flushCalled++
	return nil
}

func (f *fakeSerde) FetchOrCreateDataSource(ident serde.Ident, dsSpec *rrd.DSSpec) (rrd.DataSourcer, error) {
	f.createCalled++
	if f.fakeErr {
		return nil, fmt.Errorf("some error")
	}
	if f.nondb {
		return rrd.NewDataSource(*receiver.DftDSSPec), nil
	} else {
		return serde.NewDbDataSource(0, serde.Ident{"name": "foo"}, 0, 0, rrd.NewDataSource(*receiver.DftDSSPec)), nil
	}
}
