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

package receiver

import (
	"time"

	"github.com/tgres/tgres/rrd"
)

// A DSSpec Finder can find a DSSpec for a name. For previously
// unknown DS names that need to be created on-the-fly this interface
// provides a mechanism for specifying DS/RRA configurations based on
// the name.
type MatchingDSSpecFinder interface {
	FindMatchingDSSpec(name string) *rrd.DSSpec
}

// A default "reasonable" spec for those who do not want to think about it.
var DftDSSPec = &rrd.DSSpec{
	Step:      10 * time.Second,
	Heartbeat: 2 * time.Hour,
	RRAs: []rrd.RRASpec{
		rrd.RRASpec{Function: rrd.WMEAN,
			Step: 10 * time.Second,
			Span: 6 * time.Hour,
		},
		rrd.RRASpec{Function: rrd.WMEAN,
			Step: 1 * time.Minute,
			Span: 24 * time.Hour,
		},
		rrd.RRASpec{Function: rrd.WMEAN,
			Step: 10 * time.Minute,
			Span: 93 * 24 * time.Hour,
		},
		rrd.RRASpec{Function: rrd.WMEAN,
			Step: 24 * time.Hour,
			Span: 1825 * 24 * time.Hour,
		},
	},
}

// A simple DS finder always returns itself as the only DSSpec it
// knows.
type SimpleDSFinder struct {
	*rrd.DSSpec
}

func (s *SimpleDSFinder) FindMatchingDSSpec(name string) *rrd.DSSpec {
	if name == "" {
		return nil
	}
	return s.DSSpec
}
