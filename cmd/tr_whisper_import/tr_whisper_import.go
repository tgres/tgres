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

package main

import (
	"flag"
	"fmt"
	"github.com/kisielk/whisper-go/whisper"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
)

// This was heavily inspired by https://github.com/vimeo/whisper-to-influxdb

var (
	whisperDir  string
	sendTo      string
	workers     int
	from, until uint

	sendWorkersWg, whisperWorkersWg sync.WaitGroup
	whisperFiles                    chan string
	series                          chan *abstractSerie

	prefix           string
	include, exclude string

	skipWhisperErrors bool
)

func sendWorker() {
	for abstractSerie := range series {

		basename := strings.TrimSuffix(abstractSerie.Path[len(whisperDir)+1:], ".wsp")
		name := strings.Replace(basename, "/", ".", -1)
		if prefix != "" {
			name = prefix + "." + name
		}

		keys := []int{}
		for k, _ := range abstractSerie.Points {
			keys = append(keys, int(k))
		}

		sort.Ints(keys)

		var conn io.Writer
		var err error
		if sendTo == "stdout" {
			conn, err = os.Stdout, nil
		} else {
			conn, err = net.Dial("tcp", sendTo)
		}
		if err != nil {
			fmt.Printf("Error connecting, skipping series: %v", name)
			continue
		}

		var lastTimestamp uint32 = 0
		var lastValue float64 // because graphite marks beginning of the DP, not end
		for _, k := range keys {

			abstractPoint := abstractSerie.Points[uint32(k)]

			if from != 0 && abstractPoint.Timestamp < uint32(from) {
				continue
			}

			if until != 0 && abstractPoint.Timestamp > uint32(until) {
				continue
			}

			if lastTimestamp != 0 {
				// make it a rate per second
				tmpValue := abstractPoint.Value
				abstractPoint.Value = lastValue / float64(abstractPoint.Timestamp-lastTimestamp)
				datapoint := fmt.Sprintf("%s %v %v", name, abstractPoint.Value, abstractPoint.Timestamp)

				fmt.Fprintf(conn, "%s\r\n", datapoint)

				lastValue = tmpValue
			}
			lastTimestamp = abstractPoint.Timestamp
		}
		conn.(io.Closer).Close()
	}
	sendWorkersWg.Done()
}

type abstractSerie struct {
	Path   string // used to keep track of ordering of processing
	Points map[uint32]whisper.Point
}

func whisperWorker() {
	for path := range whisperFiles {
		fd, err := os.Open(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: Failed to open whisper file '%s': %s\n", path, err.Error())
			if !skipWhisperErrors {
				return
			}
		}
		w, err := whisper.OpenWhisper(fd)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: Failed to open whisper file '%s': %s\n", path, err.Error())
			if !skipWhisperErrors {
				return
			}
		}

		var earliestArchiveTimestamp uint32

		numTotalPoints := uint32(0)
		for i := range w.Header.Archives {
			numTotalPoints += w.Header.Archives[i].Points
		}
		points := make(map[uint32]whisper.Point)

		// iterate over archives from high-res to low-res, remembering
		// the earliest timestamp and filtering out those rows from
		// subsequent archives so as to never have duplicates.

		for i, archive := range w.Header.Archives {
			allPoints, err := w.DumpArchive(i)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: Failed to read archive %d in '%s', skipping: %s\n", i, path, err.Error())
				if !skipWhisperErrors {
					return
				}
			}

			var earliestTimestamp uint32 = 0
			var latestTimestamp uint32 = 0
			n := 0
			for _, point := range allPoints {
				// we have to filter out the "None" records (where we didn't fill in data) explicitly here!
				if point.Timestamp != 0 {
					if earliestArchiveTimestamp == 0 || point.Timestamp < earliestArchiveTimestamp {

						point.Value = point.Value
						points[point.Timestamp] = point
						n += 1
						if earliestTimestamp > point.Timestamp || earliestTimestamp == 0 {
							earliestTimestamp = point.Timestamp
						}
						if latestTimestamp < point.Timestamp || latestTimestamp == 0 {
							latestTimestamp = point.Timestamp
						}
					}
				}
			}

			// And now we need to remove all points that are latest - retention
			if earliestTimestamp < latestTimestamp-archive.SecondsPerPoint*archive.Points {
				earliestArchiveTimestamp = latestTimestamp - archive.SecondsPerPoint*archive.Points
			} else {
				earliestArchiveTimestamp = earliestTimestamp
			}

			n = 0
			for ts, _ := range points {
				if ts < earliestArchiveTimestamp {
					delete(points, ts)
					n += 1
				}
			}
		}

		w.Close()

		serie := &abstractSerie{path, points}
		series <- serie
	}
	whisperWorkersWg.Done()
}

func process(path string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if !strings.HasSuffix(path, ".wsp") {
		return nil
	}
	if exclude != "" && strings.Contains(path, exclude) {
		return nil
	}
	if !strings.Contains(path, include) {
		return nil
	}
	whisperFiles <- path
	return nil
}

func init() {
	whisperFiles = make(chan string)
	series = make(chan *abstractSerie)
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.StringVar(&whisperDir, "whisperDir", "/opt/graphite/storage/whisper/", "location where all whisper files are stored")
	flag.StringVar(&sendTo, "sendTo", "127.0.0.1:2003", "host:port of where to send the data")
	flag.IntVar(&workers, "workers", 1, "number of workersworkers (default: 1)")
	flag.UintVar(&from, "from", 0, "Unix epoch time of the beginning of the requested interval. (0: beginning of series, default: 0).")
	flag.UintVar(&until, "until", 0, "Unix epoch time of the end of the requested interval. (0: until end of series, default: 0).")
	flag.StringVar(&prefix, "prefix", "", "prefix this string to all imported data")
	flag.StringVar(&include, "include", "", "only process whisper files whose filename contains this string (\"\" is a no-op, and matches everything")
	flag.StringVar(&exclude, "exclude", "", "don't process whisper files whose filename contains this string (\"\" disables the filter, and matches nothing")
	flag.BoolVar(&skipWhisperErrors, "skipWhisperErrors", false, "when a whisper read fails, skip to the next one instead of failing")

	flag.Parse()

	if strings.HasSuffix(whisperDir, "/") {
		whisperDir = whisperDir[:len(whisperDir)-1]
	}

	var err error

	for i := 1; i <= workers; i++ {
		sendWorkersWg.Add(1)
		go sendWorker()
	}
	for i := 1; i <= workers; i++ {
		whisperWorkersWg.Add(1)
		go whisperWorker()
	}

	err = filepath.Walk(whisperDir, process)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
	}
	close(whisperFiles)
	whisperWorkersWg.Wait()
	close(series)
	sendWorkersWg.Wait()
}
