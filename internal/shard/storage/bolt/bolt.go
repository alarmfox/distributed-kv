package bolt

import (
	"fmt"
	"os"
	"time"

	"gitlab.com/alarmfox/distributed-kv/internal/shard/storage"
	bolt "go.etcd.io/bbolt"
)

type Storage struct {
	db *bolt.DB
}

func NewStorage(dbLocation string) (*Storage, func() error, error) {
	db, err := bolt.Open(dbLocation, os.ModePerm, &bolt.Options{Timeout: time.Second * 1})
	if err != nil {
		return &Storage{}, func() error { return nil }, fmt.Errorf("could not open %q: %w", dbLocation, err)
	}
	if err := createDefaultBucket(db); err != nil {
		db.Close()
		return &Storage{}, func() error { return nil }, fmt.Errorf("could not init %q: %w", dbLocation, err)
	}
	return &Storage{db: db}, func() error { return db.Close() }, nil
}

var (
	defaultBucket = []byte("default")
)

func createDefaultBucket(db *bolt.DB) error {
	return db.Update(func(t *bolt.Tx) error {
		_, err := t.CreateBucketIfNotExists(defaultBucket)
		return err
	})
}

func (s *Storage) Get(key string) ([]byte, error) {
	var res []byte
	err := s.db.View(func(t *bolt.Tx) error {
		res = t.Bucket(defaultBucket).Get([]byte(key))
		if res == nil {
			return storage.ErrNotFound
		}
		return nil
	})

	return res, err
}

func (s *Storage) Set(key string, value []byte) error {
	return s.db.Update(func(t *bolt.Tx) error {
		return t.Bucket(defaultBucket).Put([]byte(key), value)
	})
}
