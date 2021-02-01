package wal

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/golang/protobuf/proto" // nolint
	"github.com/stretchr/testify/assert"

	"github.com/amazingchow/photon-dance-wal/fileutil"
	"github.com/amazingchow/photon-dance-wal/walpb"
)

func TestNew(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.Empty(t, err)
	defer os.RemoveAll(p)

	w, err := Create(p, []byte("some metadata"))
	assert.Empty(t, err)
	g := filepath.Base(w.tail().Name())
	assert.Equal(t, walName(0, 0), g)
	defer w.Close()

	// file is preallocated to segment size; only read data written by wal
	off, err := w.tail().Seek(0, io.SeekCurrent)
	assert.Empty(t, err)
	gd := make([]byte, off)
	f, err := os.Open(filepath.Join(p, filepath.Base(w.tail().Name())))
	assert.Empty(t, err)
	defer f.Close()
	n, err := io.ReadFull(f, gd)
	assert.Empty(t, err)
	assert.Equal(t, 64, n)

	var wb bytes.Buffer
	enc := newEncoder(&wb, 0, 0)
	err = enc.encode(&walpb.Record{Type: walpb.RecordType_CrcType, Crc: 0})
	assert.Empty(t, err)
	err = enc.encode(&walpb.Record{Type: walpb.RecordType_MetadataType, Data: []byte("some metadata")})
	assert.Empty(t, err)
	data, err := proto.Marshal(&walpb.Snapshot{})
	assert.Empty(t, err)
	err = enc.encode(&walpb.Record{Type: walpb.RecordType_SnapshotType, Data: data})
	assert.Empty(t, err)
	err = enc.flush()
	assert.Empty(t, err)
	assert.Equal(t, wb.Bytes(), gd)
}

func TestCreateFailFromPollutedDir(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.Empty(t, err)
	defer os.RemoveAll(p)

	err = ioutil.WriteFile(filepath.Join(p, "test.wal"), []byte("data"), os.ModeTemporary)
	assert.Empty(t, err)

	_, err = Create(p, []byte("data"))
	assert.Equal(t, os.ErrExist, err)
}

func TestWalCleanup(t *testing.T) {
	testRoot, err := ioutil.TempDir(os.TempDir(), "waltestroot")
	assert.Empty(t, err)
	p, err := ioutil.TempDir(testRoot, "waltest")
	assert.Empty(t, err)
	defer os.RemoveAll(testRoot)

	w, err := Create(p, []byte(""))
	assert.Empty(t, err)
	w.cleanupWAL()
	fnames, err := fileutil.ReadDir(testRoot)
	assert.Empty(t, err)
	assert.Equal(t, 1, len(fnames))
	pattern := fmt.Sprintf(`%s.broken\.[\d]{8}\.[\d]{6}\.[\d]{1,6}?`, filepath.Base(p))
	match, _ := regexp.MatchString(pattern, fnames[0])
	assert.Equal(t, true, match)
}

func TestCreateFailFromNoSpaceLeft(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.Empty(t, err)
	defer os.RemoveAll(p)

	oldSegmentSizeBytes := SegmentSizeBytes
	defer func() {
		SegmentSizeBytes = oldSegmentSizeBytes
	}()
	SegmentSizeBytes = math.MaxInt64

	_, err = Create(p, []byte("data"))
	assert.NotEmpty(t, err)
}

func TestNewForInitedDir(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.Empty(t, err)
	defer os.RemoveAll(p)

	_, err = os.Create(filepath.Join(p, walName(0, 0)))
	assert.Empty(t, err)
	_, err = Create(p, nil)
	assert.Equal(t, os.ErrExist, err)
}

func TestOpenAtIndex(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.Empty(t, err)
	defer os.RemoveAll(dir)

	f, err := os.Create(filepath.Join(dir, walName(0, 0)))
	assert.Empty(t, err)
	f.Close()

	w, err := Open(dir, &walpb.Snapshot{})
	assert.Empty(t, err)
	g := filepath.Base(w.tail().Name())
	assert.Equal(t, walName(0, 0), g)
	assert.Equal(t, uint64(0), w.seq())
	err = w.Close()
	assert.Empty(t, err)

	wname := walName(2, 10)
	f, err = os.Create(filepath.Join(dir, wname))
	assert.Empty(t, err)
	f.Close()

	w, err = Open(dir, &walpb.Snapshot{Index: 5})
	assert.Empty(t, err)
	g = filepath.Base(w.tail().Name())
	assert.Equal(t, wname, g)
	assert.Equal(t, uint64(2), w.seq())
	w.Close()

	emptydir, err := ioutil.TempDir(os.TempDir(), "waltestempty")
	assert.Empty(t, err)
	defer os.RemoveAll(emptydir)
	_, err = Open(emptydir, &walpb.Snapshot{})
	assert.Equal(t, ErrFileNotFound, err)
}
