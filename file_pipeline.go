// Copyright 2016 The etcd Authors
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
	"fmt"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"

	"github.com/amazingchow/photon-dance-wal/fileutil"
)

// FilePipeline pipelines allocating disk space
type FilePipeline struct {
	dir   string
	size  int64
	count int

	filec chan *fileutil.LockedFile
	errc  chan error
	donec chan struct{}
}

func NewFilePipeline(dir string, fileSize int64) *FilePipeline {
	fp := &FilePipeline{
		dir:   dir,
		size:  fileSize,
		count: 0,
		filec: make(chan *fileutil.LockedFile),
		errc:  make(chan error, 1),
		donec: make(chan struct{}),
	}
	go fp.run()
	return fp
}

// Open returns a fresh file for writing. Rename the file before calling
// Open again or there will be file collisions.
func (fp *FilePipeline) Open() (f *fileutil.LockedFile, err error) {
	select {
	case f = <-fp.filec:
	case err = <-fp.errc:
	}
	return f, err
}

func (fp *FilePipeline) Close() error {
	close(fp.donec)
	return <-fp.errc
}

func (fp *FilePipeline) alloc() (f *fileutil.LockedFile, err error) {
	// count % 2 so this file isn't the same as the one last published
	fpath := filepath.Join(fp.dir, fmt.Sprintf("%d.tmp", fp.count%2))
	if f, err = fileutil.LockFile(fpath, os.O_CREATE|os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return nil, err
	}
	if err = fileutil.Preallocate(f.File, fp.size, true); err != nil {
		log.Error().Err(err).Int64("size", fp.size).Msg("failed to preallocate disk space when creating a new WAL file")
		f.Close() // nolint
		return nil, err
	}
	fp.count++
	return f, nil
}

func (fp *FilePipeline) run() {
	defer close(fp.errc)
	for {
		f, err := fp.alloc()
		if err != nil {
			fp.errc <- err
			return
		}
		select {
		case fp.filec <- f:
		case <-fp.donec:
			os.Remove(f.Name()) // nolint
			f.Close()           // nolint
			return
		}
	}
}
