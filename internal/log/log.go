package log

import (
	"fmt"
	api "github.com/erfanz/proglog/api/v1"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Log struct {
	mu            sync.RWMutex
	dir           string
	config        Config
	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	// some fields in config may be uninitialized. Initialize them!
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	l := &Log{
		dir:    dir,
		config: c,
	}

	err := l.bootstrap()
	return l, err
}

// Sets up the log by any possible existing segments
func (l *Log) bootstrap() error {
	files, err := ioutil.ReadDir(l.dir)
	if err != nil {
		return err
	}

	// sort by their baseOffsets (cause we want to keep our segments in chronological order)
	var baseOffsets []uint64
	for _, file := range files {
		// extract offset from the filename
		// to avoid having duplicates, we only consider the store files, and not index files
		if strings.Contains(file.Name(), ".store") {
			offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
			off, _ := strconv.ParseUint(offStr, 10, 0)
			baseOffsets = append(baseOffsets, off)
		}
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for _, off := range baseOffsets {
		if err = l.registerNewSegment(off); err != nil {
			return err
		}
	}

	// if there is no existing segments, then create the first one
	if l.segments == nil {
		if err = l.registerNewSegment(l.config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) registerNewSegment(offset uint64) error {
	s, err := newSegment(l.dir, offset, l.config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		err = l.registerNewSegment(off + 1)
	}
	return off, err
}

func (l *Log) Read(offset uint64) (*api.Record, error) {
	// find the segment which covers the offset
	l.mu.RLock()
	defer l.mu.RUnlock()

	var i int64 = int64(len(l.segments)) - 1
	for ; i >= 0; i-- {
		if offset >= l.segments[i].baseOffset && offset < l.segments[i].nextOffset {
			return l.segments[i].Read(offset)
		}
	}
	return nil, fmt.Errorf("offset out of range: %d", offset)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.segments {
		if err := s.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.bootstrap()
}

func (l *Log) GetOffsetRange() (lowest uint64, highest uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	lowest = l.segments[0].baseOffset
	highest = l.activeSegment.nextOffset
	return lowest, highest
}

func (l *Log) Truncate(offset uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var newSegments []*segment

	for _, s := range l.segments {
		if s.nextOffset <= offset+1 {
			if err := s.Remove(); err != nil {
				return err
			}
		} else {
			newSegments = append(newSegments, s)
		}
	}

	if len(newSegments) > 0 {
		l.segments = newSegments
		return nil
	} else {
		return fmt.Errorf("truncate removed all segments")
	}
}
