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
// Round-Robin Archives from incoming and usually unevenly-spaced data
// points by converting the input to a rate, consolidating and
// aggregating the data across a list of evenly spaced series of
// pre-defined resolution and time span.
//
// Throughout this documentation and code the following terms are used
// (sometimes as abbreviations, listed in parenthesis):
//
// Round-Robin Database (RRD): Collectively all the logic in this
// package and an instance of the data it maintains is referred to as
// an RRD.
//
// Data Point (DP): There actually isn't a data structure representing
// a data point. A datapoint is just a float64.
//
// Round-Robin Archive (RRA): An array of data points at a specific
// resolutoin and going back a pre-defined duration of time.
//
// Primary Data Point (PDP): A conceptual value which represents a
// step-sized time slot in a series. Many (or none) actual data points
// can come in and fall into a PDP. Each DS and each RRA maintain a
// current (not yet complete) PDP, whose value upon completion is used
// to update PDPs of lower resolution (i.e. larger Step/PDP) RRAs.
//
// Data Sourse (DS): Data Source loosely represents a "time series":
// the smalles resolution (PDP size) and other parameters, as well as
// the data. A DS should have at least one, but usually several RRAs.
//
// Step: Step is the smallest unit of time of a DS and/or RRA's in
// milliseconds. RRA resolutions and sizes must be multiples of the DS
// step they belong to. In this implementation a step cannot be
// smaller than a millisecond.
//
// DS Heartbeat (HB): Duration of time that can pass without data. A
// gap in data which exceeds HB is filled with NaNs.
//
// Note that this package does not concern itself with loading a series
// from storage for analysis.
package rrd
