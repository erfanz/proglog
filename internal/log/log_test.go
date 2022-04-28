package log

import (
	api "github.com/erfanz/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		// "append and read a record succeeds": testAppendRead,
		// "offset out of range":               testOutOfRangeErr,
		// "init with existing segments":       testInitExisting,
		// "truncate":                          testTruncate,
		"remove": testRemove,
	} {
		t.Run(scenario, func(t *testing.T) {
			// this is like Setup() for all tests
			dir, _ := ioutil.TempDir("", "segment-test")
			defer os.RemoveAll(dir)
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			l, err := NewLog(dir, c)
			require.NoError(t, err)
			fn(t, l)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	for i := uint64(0); i < 3; i++ {
		rec := &api.Record{}
		rec.Value = []byte("Hello World")
		rec.Offset = 20 + i
		off, err := log.Append(rec)
		require.NoError(t, err)
		require.Equal(t, i, off)

		readRec, err := log.Read(off)
		require.NoError(t, err)
		require.Equal(t, rec.Value, readRec.Value)
		require.Equal(t, rec.Offset, readRec.Offset)
	}
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(5)
	require.Nil(t, read)
	require.Error(t, err)
}

func testInitExisting(t *testing.T, log *Log) {
	append_n_records(t, log, 3)
	lowest, highest := log.GetOffsetRange()
	require.Equal(t, uint64(0), lowest)
	require.Equal(t, uint64(3), highest)
	err := log.Close()
	require.NoError(t, err)

	n, err := NewLog(log.dir, log.config)
	require.NoError(t, err)
	lowest, highest = n.GetOffsetRange()
	require.Equal(t, uint64(0), lowest)
	require.Equal(t, uint64(3), highest)
}

func testTruncate(t *testing.T, log *Log) {
	append_n_records(t, log, 3)
	err := log.Truncate(1)
	require.NoError(t, err)
	_, err = log.Read(0)
	require.Error(t, err)
}

func testRemove(t *testing.T, log *Log) {
	append_n_records(t, log, 3)
	err := log.Remove()
	require.NoError(t, err)
	_, err = NewLog(log.dir, log.config)
	require.Error(t, err)
}

func append_n_records(t *testing.T, log *Log, n uint32) {
	t.Helper()
	for i := uint32(0); i < n; i++ {
		append := &api.Record{
			Value:  []byte("hello world"),
			Offset: uint64(i + 20),
		}
		_, err := log.Append(append)
		require.NoError(t, err)
	}
}
