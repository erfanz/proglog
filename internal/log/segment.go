package log

import (
	"fmt"
	api "github.com/erfanz/proglog/api/v1"
	"google.golang.org/protobuf/proto"
	"os"
	"path"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	if s.index, err = newIndex(indexFile, s.config); err != nil {
		return nil, err
	}

	if off, _, err := s.index.ReadLast(); err != nil {
		// we couldn't read the last entry, so it must have been empty
		s.nextOffset = baseOffset
	} else {
		// the index is not empty, so continue where it left off
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	data, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(data)
	if err != nil {
		return 0, err
	}
	// index offsets are relative to base offset
	relOffset := uint32(s.nextOffset - s.baseOffset)
	if err = s.index.Write(relOffset, pos); err != nil {
		return 0, err
	}
	cur := s.nextOffset
	s.nextOffset++
	return cur, nil
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	relOffset := uint32(off - s.baseOffset)
	_, pos, err := s.index.Read(relOffset)
	if err != nil {
		return nil, err
	}
	content, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(content, record)
	return record, err
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}
