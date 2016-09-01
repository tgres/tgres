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

// Package statsd provides the statsd-like functionality.
package statsd

import (
	"fmt"
	"github.com/tgres/tgres/aggregator"
	"github.com/tgres/tgres/misc"
	"strings"
)

var (
	Prefix string = "stats"
)

func (st *Stat) AggregatorCmd() *aggregator.Command {
	if st.Metric == "c" {
		return aggregator.NewCommand(
			aggregator.CmdAdd,
			Prefix+"."+st.Name,
			st.Value*(1/st.Sample))
	} else if st.Metric == "g" {
		if st.Delta {
			return aggregator.NewCommand(
				aggregator.CmdAddGauge,
				Prefix+".gauges."+st.Name,
				st.Value)
		} else {
			return aggregator.NewCommand(
				aggregator.CmdSetGauge,
				Prefix+".gauges."+st.Name,
				st.Value)
		}
	} else if st.Metric == "ms" {
		return aggregator.NewCommand(
			aggregator.CmdAppend,
			Prefix+".timers."+st.Name,
			st.Value)
	}
	return nil
}

type Stat struct {
	Name   string
	Value  float64
	Metric string
	Sample float64
	Delta  bool
}

// ParseStatsdPacket parses a statsd packet e.g: gorets:1|c|@0.1. See
// https://github.com/etsy/statsd/blob/master/docs/metric_types.md
// There is no need to support multi-metric packets here, since it
// uses newline as separator, the text handler in daemon/services.go
// would take care of it.
func ParseStatsdPacket(packet string) (*Stat, error) {

	var (
		result = &Stat{Sample: 1}
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

	// NB: This cannot be done with a single Sscanf for "%f|%s|@%f"
	// because the %s is hungry and will eat the rest of the string.
	parts = strings.Split(parts[1], "|")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid packet: %q", packet)
	}

	if n, err := fmt.Sscanf(parts[0], "%f", &result.Value); n != 1 || err != nil {
		return nil, fmt.Errorf("error %v scanning input (cannot parse value|metric): %q", err, packet)
	}
	if parts[0][0] == '+' || parts[1][0] == '-' { // safe because "" would cause an error above
		result.Delta = true
	}
	if parts[1] != "c" && parts[1] != "g" && parts[1] != "ms" {
		return nil, fmt.Errorf("invalid metric type: %q", parts[1])
	}
	result.Metric = parts[1]

	if len(parts) > 2 {
		if n, err := fmt.Sscanf(parts[2], "@%f", &result.Sample); n != 1 || err != nil {
			return nil, fmt.Errorf("error %v scanning input (bad @sample?): %q", err, packet)
		}
		if result.Sample < 0 || result.Sample > 1 {
			return nil, fmt.Errorf("invalid sample: %q (must be between 0 and 1.0)", parts[2])
		}
	}

	return result, nil
}
