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

// Package receiver manages the receiving end of the data. All of the
// queueing, caching, perioding flushing and cluster forwarding logic
// is here.
package receiver

import (
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
	"time"
)

type dftDSFinder struct{}

type MatchingDSSpecFinder interface {
	FindMatchingDSSpec(name string) *serde.DSSpec
}

func (_ *dftDSFinder) FindMatchingDSSpec(name string) *serde.DSSpec {
	return &serde.DSSpec{
		Step:      10 * time.Second,
		Heartbeat: 2 * time.Hour,
		RRAs: []*serde.RRASpec{
			&serde.RRASpec{Function: rrd.WMEAN,
				Step: 10 * time.Second,
				Size: 6 * time.Hour,
			},
			&serde.RRASpec{Function: rrd.WMEAN,
				Step: 1 * time.Minute,
				Size: 24 * time.Hour,
			},
			&serde.RRASpec{Function: rrd.WMEAN,
				Step: 10 * time.Minute,
				Size: 93 * 24 * time.Hour,
			},
			&serde.RRASpec{Function: rrd.WMEAN,
				Step: 24 * time.Hour,
				Size: 1825 * 24 * time.Hour,
			},
		},
	}
}
