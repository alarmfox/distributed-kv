package domain

import (
	"sync"
	"sync/atomic"
)

type ShardMap struct {
	shards map[uint64]string
	count  uint64
	sync.RWMutex
}

func NewShardMap() *ShardMap {
	return &ShardMap{
		shards: make(map[uint64]string),
	}
}

func (s *ShardMap) Get(id uint64) string {
	s.RLock()
	defer s.RUnlock()
	return s.shards[id]
}

func (s *ShardMap) Set(id uint64, address string) {
	s.Lock()
	defer s.Unlock()
	s.shards[id] = address
	atomic.StoreUint64(&s.count, uint64(len(s.shards)))
}

func (s *ShardMap) Count() uint64 {
	return atomic.LoadUint64(&s.count)
}

func (s *ShardMap) Remove(id uint64) {
	s.Lock()
	defer s.Unlock()
	delete(s.shards, id)
	atomic.StoreUint64(&s.count, uint64(len(s.shards)))
}
