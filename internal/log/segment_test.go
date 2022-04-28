package log

import (
	api "github.com/erfanz/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestSegmentAppendRead(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	var recordsCnt uint64 = 3

	// create segment
	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * recordsCnt
	segment, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), segment.nextOffset)

	for i := uint64(0); i < recordsCnt; i++ {
		// append
		rec := &api.Record{}
		rec.Value = []byte("Hello World")
		rec.Offset = 20 + i
		off, err := segment.Append(rec)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		// read
		readRec, err := segment.Read(off)
		require.NoError(t, err)
		require.Equal(t, rec.Value, readRec.Value)
		require.Equal(t, rec.Offset, readRec.Offset)
	}

	// index must be filled up by now
	require.True(t, segment.IsMaxed())

	// appending beyond the index limit should result in an error
	rec := &api.Record{}
	_, err = segment.Append(rec)
	require.Equal(t, io.EOF, err)
}
