package wal

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
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

// TestVerify tests that Verify throws a non-nil error when the WAL is corrupted.
// The test creates a WAL directory and cuts out multiple WAL files. Then
// it corrupts one of the files by completely truncating it.
func TestVerify(t *testing.T) {
	walDir, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.Empty(t, err)
	defer os.RemoveAll(walDir)

	// create WAL
	w, err := Create(walDir, []byte("data"))
	assert.Empty(t, err)
	defer w.Close()

	// make 5 separate files
	for i := 0; i < 5; i++ {
		ents := []walpb.Entry{{Type: walpb.RecordType_EntryType, Index: uint64(i), Data: []byte(fmt.Sprintf("waldata%d", i+1))}}
		err = w.Save(ents)
		assert.Empty(t, err)
		err = w.cut()
		assert.Empty(t, err)
	}

	// to verify the WAL is not corrupted at this point
	err = Verify(walDir, &walpb.Snapshot{})
	assert.Empty(t, err)

	walFiles, err := ioutil.ReadDir(walDir)
	assert.Empty(t, err)

	// corrupt the WAL by truncating one of the WAL files completely
	err = os.Truncate(path.Join(walDir, walFiles[2].Name()), 0)
	assert.Empty(t, err)

	err = Verify(walDir, &walpb.Snapshot{})
	assert.NotEmpty(t, err)
}

// TODO: split it into smaller tests for better readability
func TestCut(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.Empty(t, err)
	defer os.RemoveAll(p)

	w, err := Create(p, []byte("data"))
	assert.Empty(t, err)
	defer w.Close()

	err = w.cut()
	assert.Empty(t, err)
	wname := walName(1, 1)
	g := filepath.Base(w.tail().Name())
	assert.Equal(t, wname, g)

	ents := []walpb.Entry{{Type: walpb.RecordType_EntryType, Index: 1, Data: []byte{1}}}
	err = w.Save(ents)
	assert.Empty(t, err)
	err = w.cut()
	assert.Empty(t, err)
	snap := walpb.Snapshot{Index: 2, Term: 1}
	err = w.SaveSnapshot(&snap)
	assert.Empty(t, err)
	wname = walName(2, 2)
	g = filepath.Base(w.tail().Name())
	assert.Equal(t, wname, g)

	// check the state in the last WAL
	// We do check before closing the WAL to ensure that Cut syncs the data
	// into the disk.
	f, err := os.Open(filepath.Join(p, wname))
	assert.Empty(t, err)
	defer f.Close()
	nw := &WAL{
		decoder: newDecoder(f),
		start:   &snap,
	}
	_, _, _, err = nw.ReadAll()
	assert.Empty(t, err)
}

func TestSaveWithCut(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.Empty(t, err)
	defer os.RemoveAll(p)

	w, err := Create(p, []byte("metadata"))
	assert.Empty(t, err)

	bigData := make([]byte, 500)
	strdata := "Hello World!!!"
	copy(bigData, strdata)
	// set a lower value for SegmentSizeBytes, else the test takes too long to complete
	restoreLater := SegmentSizeBytes
	const EntrySize int = 500
	SegmentSizeBytes = 2 * 1024
	defer func() { SegmentSizeBytes = restoreLater }()
	index := uint64(0)
	for totalSize := 0; totalSize < int(SegmentSizeBytes); totalSize += EntrySize {
		ents := []walpb.Entry{{Type: walpb.RecordType_EntryType, Index: index, Data: bigData}}
		err = w.Save(ents)
		assert.Empty(t, err)
		index++
	}

	w.Close()

	neww, err := Open(p, &walpb.Snapshot{})
	assert.Empty(t, err)
	defer neww.Close()
	wname := walName(1, index)
	g := filepath.Base(neww.tail().Name())
	assert.Equal(t, wname, g)

	_, _, ents, err := neww.ReadAll()
	assert.Empty(t, err)

	assert.Equal(t, int(SegmentSizeBytes/int64(EntrySize)), len(ents))
	for _, oneent := range ents {
		assert.Equal(t, bigData, oneent.GetData())
	}
}

func TestRecover(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.Empty(t, err)
	defer os.RemoveAll(p)

	w, err := Create(p, []byte("metadata"))
	assert.Empty(t, err)
	err = w.SaveSnapshot(&walpb.Snapshot{})
	assert.Empty(t, err)
	ents := []walpb.Entry{
		{Type: walpb.RecordType_EntryType, Index: 1, Data: []byte{1}},
		{Type: walpb.RecordType_EntryType, Index: 2, Data: []byte{2}},
	}
	err = w.Save(ents)
	assert.Empty(t, err)
	w.Close()

	w, err = Open(p, &walpb.Snapshot{})
	assert.Empty(t, err)
	metadata, entries, recoveredEnts, err := w.ReadAll()
	assert.Empty(t, err)

	assert.Equal(t, []byte("metadata"), metadata)
	assert.Equal(t, uint64(2), entries)
	for i := 0; i < 2; i++ {
		assert.Equal(t, ents[i].GetIndex(), recoveredEnts[i].GetIndex())
		assert.Equal(t, ents[i].GetData(), recoveredEnts[i].GetData())
	}
	w.Close()
}

func TestSearchIndex(t *testing.T) {
	tests := []struct {
		names []string
		index uint64
		widx  int
		wok   bool
	}{
		{
			[]string{
				"0000000000000000-0000000000000000.wal",
				"0000000000000001-0000000000001000.wal",
				"0000000000000002-0000000000002000.wal",
			},
			0x1000, 1, true,
		},
		{
			[]string{
				"0000000000000001-0000000000004000.wal",
				"0000000000000002-0000000000003000.wal",
				"0000000000000003-0000000000005000.wal",
			},
			0x4000, 1, true,
		},
		{
			[]string{
				"0000000000000001-0000000000002000.wal",
				"0000000000000002-0000000000003000.wal",
				"0000000000000003-0000000000005000.wal",
			},
			0x1000, -1, false,
		},
	}
	for _, tt := range tests {
		idx, ok := searchIndex(tt.names, tt.index)
		assert.Equal(t, tt.widx, idx)
		assert.Equal(t, tt.wok, ok)
	}
}

func TestScanWalName(t *testing.T) {
	tests := []struct {
		str          string
		wseq, windex uint64
		wok          bool
	}{
		{"0000000000000000-0000000000000000.wal", 0, 0, true},
		{"0000000000000003-0000000000000008.wal", 3, 8, true},
		{"0000000000000000.wal", 0, 0, false},
		{"0000000000000000-0000000000000000.snap", 0, 0, false},
	}
	for _, tt := range tests {
		seq, index, err := parseWALName(tt.str)
		assert.Equal(t, tt.wok, err == nil)
		assert.Equal(t, tt.wseq, seq)
		assert.Equal(t, tt.windex, index)
	}
}

func TestRecoverAfterCut(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.Empty(t, err)
	defer os.RemoveAll(p)

	md, err := Create(p, []byte("metadata"))
	assert.Empty(t, err)
	for i := 0; i < 10; i++ {
		err = md.SaveSnapshot(&walpb.Snapshot{Index: uint64(i)})
		assert.Empty(t, err)
		ents := []walpb.Entry{{Type: walpb.RecordType_EntryType, Index: uint64(i)}}
		err = md.Save(ents)
		assert.Empty(t, err)
		err = md.cut()
		assert.Empty(t, err)
	}
	md.Close()

	err = os.Remove(filepath.Join(p, walName(4, 4)))
	assert.Empty(t, err)

	for i := 0; i < 10; i++ {
		w, err := Open(p, &walpb.Snapshot{Index: uint64(i)})
		if err != nil {
			if i <= 4 {
				assert.Equal(t, ErrFileNotFound, err)
				continue
			}
			assert.Empty(t, err)
		}
		metadata, _, ents, err := w.ReadAll()
		assert.Empty(t, err)
		assert.Equal(t, []byte("metadata"), metadata)
		for j, e := range ents {
			assert.Equal(t, uint64(j+i+1), e.Index)
		}
		w.Close()
	}
}

func TestOpenAtUncommittedIndex(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.Empty(t, err)
	defer os.RemoveAll(p)

	w, err := Create(p, []byte("metadata"))
	assert.Empty(t, err)
	err = w.SaveSnapshot(&walpb.Snapshot{})
	assert.Empty(t, err)
	err = w.Save([]walpb.Entry{{Type: walpb.RecordType_EntryType, Index: 0}})
	assert.Empty(t, err)
	w.Close()

	w, err = Open(p, &walpb.Snapshot{})
	assert.Empty(t, err)
	// commit up to index 0, try to read index 1
	_, _, _, err = w.ReadAll()
	assert.Empty(t, err)
	w.Close()
}

// TestOpenForRead tests that OpenForRead can load all files.
// The tests creates WAL directory, and cut out multiple WAL files. Then
// it releases the lock of part of data, and excepts that OpenForRead
// can read out all files even if some are locked for write.
func TestOpenForRead(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.Empty(t, err)
	defer os.RemoveAll(p)

	w, err := Create(p, []byte("metadata"))
	assert.Empty(t, err)
	defer w.Close()

	for i := 0; i < 10; i++ {
		ents := []walpb.Entry{{Type: walpb.RecordType_EntryType, Index: uint64(i)}}
		err = w.Save(ents)
		assert.Empty(t, err)
		err = w.cut()
		assert.Empty(t, err)
	}

	unlockIndex := uint64(5)
	err = w.ReleaseLockTo(unlockIndex)
	assert.Empty(t, err)

	w2, err := OpenForRead(p, &walpb.Snapshot{})
	assert.Empty(t, err)
	defer w2.Close()
	_, _, ents, err := w2.ReadAll()
	assert.Empty(t, err)
	g := ents[len(ents)-1].Index
	assert.Equal(t, uint64(9), g)
}
