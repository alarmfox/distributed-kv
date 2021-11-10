package storage

import "sync"

type ShardMap struct {
	shards map[uint64]string
	count  uint64
	sync.RWMutex
}

func NewShardMap(selfID uint64, selfAddress string) *ShardMap {
	shards := make(map[uint64]string)
	shards[selfID] = selfAddress
	return &ShardMap{
		shards: shards,
		count:  0,
	}
}

func (s *ShardMap) Set(id uint64, address string) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.shards[id] = address
	s.count++
}

func (s *ShardMap) Get(id uint64) string {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.shards[id]
}

func (s *ShardMap) Remove(id uint64) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.count--
	delete(s.shards, id)
}

func (s *ShardMap) Count() uint64 {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.count
}
