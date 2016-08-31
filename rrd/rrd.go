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

// Package rrd contains the logic for updating in-memory partial
// Round-Robin Archives of data points. In other words, this is the
// logic governing how incoming data modifies RRAs only, there is no
// code here to load an RRA from db and do something with it.
//
// Throughout documentation and code the following terms are used
// (sometimes as abbreviations, listed in parenthesis):
//
// Round-Robin Database (RRD): Collectively all the logic in this
// package and an instance of the data it maintains is referred to as
// an RRD.
//
// Data Point (DP): There actually isn't a data structure representing
// a data point (except for an incoming data point IncomingDP). A
// datapoint is just a float64.
//
// Data Sourse (DS): Data Source is all there is to know about a time
// series, its name, resolution and other parameters, as well as the
// data. A DS has at least one, but usually several RRAs.
//
// DS Step: Step is the smallest unit of time for the DS in
// milliseconds. RRA resolutions and sizes must be multiples of the DS
// step.
//
// DS Heartbeat (HB): Duration of time that can pass without data. A
// gap in data which exceeds HB is filled with NaNs.
//
// Round-Robin Archive (RRA): An array of data points at a specific
// resolutoin and going back a pre-defined duration of time.
//
// Primary Data Point (PDP): A conceptual data point which represents
// a time slot. Many actual data points can come in and fall into the
// current (not-yet-complete) PDP. There is one PDP per DS and one per
// each RRA. When the DS PDP is complete its content is saved into one
// or more RRA PDPs.
package rrd
