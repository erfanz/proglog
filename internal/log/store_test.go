package log

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

var (
	record   = []byte("Hello world!")
	recWidth = uint64(len(record)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(record)
		require.NoError(t, err)
		require.Equal(t, pos+n, recWidth*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, record, read)
		pos += recWidth
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	width := make([]byte, lenWidth)
	for i, off := uint64(1), int64(0); i < 4; i++ {
		n, err := s.ReadAt(width, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		off += int64(n)

		size := enc.Uint64(width)
		cont := make([]byte, size)
		n, err = s.ReadAt(cont, off)
		require.NoError(t, err)
		require.Equal(t, int(size), n)
		require.Equal(t, record, cont)
		off += int64(n)
	}
}
