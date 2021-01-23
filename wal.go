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

package wal

import (
	"os"
	"sync"

	"go.uber.org/zap"

	"github.com/amazingchow/photon-dance-wal/fileutil"
	"github.com/amazingchow/photon-dance-wal/walpb"
)

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
type WAL struct {
	lg *zap.Logger

	dir string // the living directory of the underlay files

	// dirFile is a fd for the wal directory for syncing on Rename
	dirFile *os.File

	metadata []byte // metadata recorded at the head of each WAL

	start walpb.Snapshot // snapshot to start reading
	// decoder   *decoder       // decoder to decode records
	readClose func() error // closer for decode reader

	unsafeNoSync bool // if set, do not fsync

	mu   sync.Mutex
	enti uint64 // index of the last entry saved to the wal
	// encoder *encoder // encoder to encode records

	locks []*fileutil.LockedFile // the locked files the WAL holds (the name is increasing)
	// fp    *filePipeline
}
