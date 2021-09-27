package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"log"
)

type Service struct {
	shards     map[int]string
	hashEngine hash.Hash64
}

func NewService(shards map[int]string) *Service {
	return &Service{
		shards:     shards,
		hashEngine: fnv.New64(),
	}
}

var (
	ErrInvalidHash = errors.New("hashing error")
	ErrNotFound    = errors.New("not found")
)

func (s *Service) GetShardAddress(key string) (string, error) {
	defer s.hashEngine.Reset()
	if _, err := s.hashEngine.Write([]byte(key)); err != nil {
		return "", ErrInvalidHash
	}
	shardID := int(s.hashEngine.Sum64()%uint64(len(s.shards))) + 1
	address, ok := s.shards[shardID]

	log.Printf("key %q from shard %d on %q\n", key, shardID, address)

	if !ok {
		return "", ErrNotFound
	}

	return address, nil
}

type Shard struct {
	Address string
	Id      int
	Name    string
}

type Configuration struct {
	Shards []Shard
}

func MakeConfig(r io.Reader) (*Configuration, error) {
	var c *Configuration
	if err := json.NewDecoder(r).Decode(&c); err != nil {
		return &Configuration{}, fmt.Errorf("json.Decode %w", err)
	}
	return c, nil
}
