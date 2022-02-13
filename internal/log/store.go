package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	encoding = binary.BigEndian
)

const (
	width = 8
)

type store struct {
	*os.File
	mutex  sync.Mutex
	buffer *bufio.Writer
	size   uint64
}

/*
	A store is a wrapper around a file for us to use as a log.
	A store is also the file that stores the current active record - each time we append a new record to this file,
	we make the newly appended record the active one. So on subsequent appends, we always append to the last record.
*/
func newStore(f *os.File) (*store, error) {
	file, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(file.Size())
	return &store{
		File:   f,
		size:   size,
		buffer: bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(data []byte) (n uint64, pos uint64, err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	pos = s.size
	if err := binary.Write(s.buffer, encoding, uint64(len(data))); err != nil {
		return 0, 0, err
	}

	bytesWritten, err := s.buffer.Write(data)
	if err != nil {
		return 0, 0, err
	}
	bytesWritten += width
	s.size += uint64(bytesWritten)
	return uint64(bytesWritten), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.buffer.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, width)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	readBytes := make([]byte, encoding.Uint64(size))
	if _, err := s.File.ReadAt(readBytes, int64(pos+width)); err != nil {
		return nil, err
	}
	return readBytes, nil
}

func (s *store) ReadAt(data []byte, offset int64) (int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.buffer.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(data, offset)
}

func (s *store) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	err := s.buffer.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
