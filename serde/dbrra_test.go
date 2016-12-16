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

package serde

// // SlotRow()
// var slot int64
// rra.width, slot = 10, 20
// if rra.SlotRow(slot) != slot/rra.width {
// 	t.Errorf("SlotRow: width %v slot %v != %v (but %v)", rra.width, slot, rra.width/slot, rra.SlotRow(slot))
// }
// rra.width, slot = 15, 20
// if rra.SlotRow(slot) != slot/rra.width+1 {
// 	t.Errorf("SlotRow: width %v slot %v != %v (but %v)", rra.width, slot, rra.width/slot+1, rra.SlotRow(slot))
// }

// // DpsAsPGString
// expect := "{123.45,0}"
// if rra.DpsAsPGString(1, 2) != expect {
// 	t.Errorf("DpsAsPGString() didn't return %q", expect)
// }
