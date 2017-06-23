//
// Copyright 2017 Gregory Trubetskoy. All Rights Reserved.
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

package receiver

import (
	"fmt"
	"sync"
	"time"

	"github.com/tgres/tgres/rrd"
)

type syncDS struct {
	rrd.DataSourcer
	*sync.RWMutex
}

func newSyncDS(ds rrd.DataSourcer) *syncDS {
	return &syncDS{
		DataSourcer: ds,
		RWMutex:     &sync.RWMutex{},
	}
}

func (ds *syncDS) ProcessDataPoint(value float64, ts time.Time) error {
	ds.Lock()
	rras := ds.RRAs()
	for i := 0; i < len(rras); i++ {
		if srra, ok := rras[i].(*syncRRA); ok {
			fmt.Printf("ZZZ locking rra %d\n", i)
			srra.Lock()
		}
	}
	result := ds.DataSourcer.ProcessDataPoint(value, ts)
	for i := len(rras) - 1; i >= 0; i-- {
		if srra, ok := rras[i].(*syncRRA); ok {
			fmt.Printf("ZZZ unlocking rra %d\n", i)
			srra.Unlock()
		}
	}
	ds.Unlock()
	return result
}
