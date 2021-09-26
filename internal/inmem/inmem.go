package inmem

import (
	"errors"
	"sync"
)

type Storage struct {
	mx     sync.RWMutex
	values map[string][]byte
}

func NewStorage(dbLocation string) *Storage {
	return &Storage{
		mx:     sync.RWMutex{},
		values: make(map[string][]byte),
	}
}

var (
	ErrNotFound = errors.New("not found")
)

func (s *Storage) Get(key string) ([]byte, error) {
	s.mx.RLock()
	defer s.mx.RUnlock()
	res, ok := s.values[key]
	if !ok {
		return []byte{}, ErrNotFound

	}
	return res, nil
}
func (s *Storage) Set(key string, value []byte) error {
	s.mx.Lock()
	defer s.mx.Unlock()
	s.values[key] = value
	return nil
}
