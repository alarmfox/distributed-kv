package inmem

import (
	"sync"

	"gitlab.com/alarmfox/distributed-kv/internal/shard/storage"
)

type Storage struct {
	mx     sync.RWMutex
	values map[string][]byte
}

func NewStorage() *Storage {
	return &Storage{
		mx:     sync.RWMutex{},
		values: make(map[string][]byte),
	}
}

func (s *Storage) Get(key string) ([]byte, error) {
	s.mx.RLock()
	defer s.mx.RUnlock()
	res, ok := s.values[key]
	if !ok {
		return []byte{}, storage.ErrNotFound

	}
	return res, nil
}
func (s *Storage) Set(key string, value []byte) error {
	s.mx.Lock()
	defer s.mx.Unlock()
	s.values[key] = value
	return nil
}
