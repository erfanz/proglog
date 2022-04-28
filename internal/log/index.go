package log

import (
	"github.com/tysonmote/gommap"
	"io"
	"log"
	"os"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	// the persisted file
	file *os.File

	// memory-mapped file
	mmap gommap.MMap

	// tells us where the next entry should be written in the index
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	// check if file is good, and also gets its size, in case we are recreating from an existing file
	fi, err := os.Stat(f.Name())
	if err != nil {
		log.Println("Error in openning the named file when creating the index")
		return nil, err
	}

	// grow the size of file to max (if we don't do it now, we won't be able to grow its size after memory-mapping it)
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	idx := &index{
		file: f,
	}
	idx.size = uint64(fi.Size())

	idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	)
	if err != nil {
		log.Println("Error in creating memory-mapped file")
		return nil, err
	}
	log.Println("Creating an index using file:", f.Name())
	return idx, nil
}

func (i *index) Close() error {
	// sync memory-mapped file to persisted file
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	// flush the content of persisted file to disk
	if err := i.file.Sync(); err != nil {
		return err
	}
	// truncate back to the index's actual size (otherwise there will be unknown amount of space between last entry and the end of file)
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Read(n uint32) (off uint32, pos uint64, err error) {
	indPos := uint64(n) * entWidth
	if i.size < indPos+entWidth {
		return 0, 0, io.EOF
	}

	off = enc.Uint32(i.mmap[indPos : indPos+offWidth])
	pos = enc.Uint64(i.mmap[indPos+offWidth : indPos+entWidth])
	return off, pos, nil
}

func (i *index) ReadLast() (off uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	n := uint32((i.size / entWidth) - 1)
	return i.Read(n)
}

func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += entWidth
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
