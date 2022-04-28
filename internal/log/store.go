package log

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File // store embeds os.File
	mu       sync.Mutex
	size     uint64

	// we write to buffered io instead of writing directly to file to reduce the number of system calls
	buf *bufio.Writer
}

// Creates a new store that stores logs in file 'f'. If 'f' is not empty, it will append to it.
func newStore(f *os.File) (*store, error) {
	// check if file is good, and also get its size, in case we are recreating the store from an existing file
	fi, err := os.Stat(f.Name())
	if err != nil {
		log.Println("Error in openning the named file when creating the store")
		return nil, err
	}

	size := uint64(fi.Size())
	defer log.Println("Creating a store using file:", f.Name())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Appends the input data to the log. It returns the number of bytes written, and the record position in the log.
func (s *store) Append(data []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size

	// write the length of the record using the set encoding
	if err := binary.Write(s.buf, enc, uint64(len(data))); err != nil {
		log.Println("Error in writing the record length in the store")
		return 0, 0, err
	}

	// write the content of the record.
	w, err := s.buf.Write(data)
	if err != nil {
		log.Println("Error in writing the content of record in the store")
		return 0, 0, err
	}

	w += lenWidth       // because we also wrote the length of the record
	s.size += uint64(w) // update store's size
	log.Println("Succesfully appended", w, "bytes to the store")
	return uint64(w), pos, nil
}

// Reads the record at the specified position, and returns its content.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// first, flush the writer buffer
	if err := s.buf.Flush(); err != nil {
		log.Println("Error in flushing the store file")
		return nil, err
	}

	// read the record size
	sizeSection := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(sizeSection, int64(pos)); err != nil {
		log.Println("Error in reading the record length from the store")
		return nil, err
	}

	// read the content of the record
	content := make([]byte, enc.Uint64(sizeSection))

	n, err := s.File.ReadAt(content, int64(pos+lenWidth))
	if err != nil {
		log.Println("Error in reading the content of record from the store")
		return nil, err
	}
	log.Println("Successfully read", n+lenWidth, "bytes from the store")
	return content, nil
}

// Implements io.ReaderAt interface. It reads len(p) bytes into p beginning at the off offset in the store's file.
func (s *store) ReadAt(p []byte, off int64) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// first, flush the writer buffer
	if err := s.buf.Flush(); err != nil {
		log.Println("Error in flushing the store file")
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// first, flush the writer buffer
	if err := s.buf.Flush(); err != nil {
		log.Println("Error in flushing the store file")
		return err
	}
	return s.File.Close()
}
