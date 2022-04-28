package log

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestIndexWriteRead(t *testing.T) {
	f, err := ioutil.TempFile("", "index_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)

	_, _, err = idx.ReadLast()
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())
	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	// test reads after writes
	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		require.NoError(t, err)
		outOff, pos, err := idx.Read(want.Off)
		require.NoError(t, err)
		require.Equal(t, want.Off, outOff)
		require.Equal(t, want.Pos, pos)
	}

	// index should return error when reading past existing entries
	_, _, err = idx.Read(uint32(len(entries)))
	require.Error(t, err)

	// close
	err = idx.Close()
	require.NoError(t, err)

	// index should rebuild its state from the existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)
	off, pos, err := idx.ReadLast()
	require.NoError(t, err)
	last := len(entries) - 1
	require.Equal(t, entries[last].Off, off)
	require.Equal(t, entries[last].Pos, pos)
}
