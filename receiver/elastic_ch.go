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

type fifoQueue []interface{}

func (q *fifoQueue) push(dp interface{}) {
	*q = append(*q, dp)
}

func (q *fifoQueue) pop() (dp interface{}) {
	if len(*q) == 0 {
		return nil
	}
	dp, *q = (*q)[0], (*q)[1:]
	if len(*q) == 0 {
		*q = make([]interface{}, 0, 256) // replace the queue to free memory
	}
	return dp
}

func (q *fifoQueue) size() int {
	return len(*q)
}

// Inspired by https://github.com/npat-efault/musings/wiki/Elastic-channels
//
// TL;DR This clever structure provides never-blocking channel-like
// behavior.  inLoop and outLoop are optimizations to read or send as
// much as we can at a time for performance.
func elasticCh(cin <-chan interface{}, cout chan<- interface{}, queue *fifoQueue, maxQueue int) {

	const maxReceive = 1024
	var (
		in     <-chan interface{}
		out    chan<- interface{}
		vi, vo interface{}
		ok     bool
	)

	in, out = cin, nil
	for {
		select {
		case vi, ok = <-in:
		inLoop:
			for i := 0; i < maxReceive; i++ {
				if !ok {
					if out == nil {
						close(cout)
						return
					}
					in = nil
					break
				}
				if out == nil {
					vo = vi
					out = cout
				} else {
					if maxQueue > 0 && queue.size() < maxQueue {
						queue.push(vi)
					} // else /dev/null
				}
				select {
				case vi, ok = <-in:
				default:
					break inLoop
				}
			}
		case out <- vo:
		outLoop:
			for {
				if queue.size() > 0 {
					vo = queue.pop()
				} else {
					if in == nil {
						close(cout)
						return
					}
					out = nil
				}
				select {
				case out <- vo:
				default:
					break outLoop
				}
			}
		}
	}
}
