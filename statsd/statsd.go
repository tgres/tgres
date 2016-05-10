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

package statsd

import (
	"fmt"
	"github.com/tgres/tgres/misc"
	"strings"
)

type Stat struct {
	Name   string
	Value  float64
	Metric string
	Sample float64
}

func ParseStatsdPacket(packet string) (*Stat, error) {

	var (
		result = &Stat{}
		parts  []string
	)

	parts = strings.Split(packet, ":")
	if len(parts) < 1 {
		return nil, fmt.Errorf("invalid packet: %q", packet)
	}

	result.Name = misc.SanitizeName(parts[0])
	if len(parts) == 1 {
		result.Value, result.Metric = 1, "c"
		return result, nil
	}

	parts = strings.Split(parts[1], "|")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid packet: %q", packet)
	}

	if n, err := fmt.Sscanf(parts[0], "%f", &result.Value); n != 1 || err != nil {
		return nil, fmt.Errorf("error %v scanning input (cannot parse value|metric): %q", err, packet)
	}
	result.Metric = parts[1]

	if len(parts) > 2 {
		if n, err := fmt.Sscanf(parts[2], "@%f", &result.Sample); n != 1 || err != nil {
			return nil, fmt.Errorf("error %v scanning input (bad @sample?): %q", err, packet)
		}
	}

	return result, nil
}
