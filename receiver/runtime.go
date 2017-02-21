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
	"runtime"
	"time"

	"github.com/shirou/gopsutil/cpu"
)

// Some rudimentary runtime stats collected here, perhaps this should
// be a separate package.

func runtimeMemory() uint64 {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return mem.Alloc
}

func runtimeCpuPercent() float64 {
	ps, _ := cpu.Percent(0, false)
	if len(ps) > 0 {
		return ps[0]
	}
	return 0
}

func reportRuntime(sr statReporter) {
	for {
		time.Sleep(5 * time.Second)
		sr.reportStatGauge("runtime.cpu.percent", float64(runtimeCpuPercent()))
		sr.reportStatGauge("runtime.mem.alloc", float64(runtimeMemory()))
	}
}
