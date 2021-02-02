// Copyright 2015 The etcd Authors
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

// Package pbutil defines interfaces for handling Protocol Buffer objects.
package pbutil

import (
	"fmt"

	"github.com/golang/protobuf/proto" // nolint"
)

func MustMarshal(m proto.Message) []byte {
	d, err := proto.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("marshal should never fail (%v)", err))
	}
	return d
}

func MustUnmarshal(m proto.Message, data []byte) {
	if err := proto.Unmarshal(data, m); err != nil {
		panic(fmt.Sprintf("unmarshal should never fail (%v)", err))
	}
}

func MaybeUnmarshal(m proto.Message, data []byte) bool {
	if err := proto.Unmarshal(data, m); err != nil {
		return false
	}
	return true
}

func GetBool(v *bool) (vv bool, set bool) {
	if v == nil {
		return false, false
	}
	return *v, true
}

func Boolp(b bool) *bool { return &b }
