package wal

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto" // nolint
	"github.com/stretchr/testify/assert"

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
