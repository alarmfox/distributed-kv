package storage

import (
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

type Storage struct {
	db *bbolt.DB
}

func New(dbLocation string) (*Storage, func(), error) {
	db, err := bbolt.Open(dbLocation, 0664, &bbolt.Options{Timeout: time.Second * 1})
	if err != nil {
		return &Storage{}, func() {}, fmt.Errorf("could not open %q: %w", dbLocation, err)
	}
	if err := createDefaultBucket(db); err != nil {
		db.Close()
		return &Storage{}, func() {}, fmt.Errorf("could not init %q: %w", dbLocation, err)
	}
	return &Storage{db: db}, func() { db.Close() }, nil
}

var (
	defaultBucket = []byte("default")
)

func createDefaultBucket(db *bbolt.DB) error {
	return db.Update(func(t *bbolt.Tx) error {
		_, err := t.CreateBucketIfNotExists(defaultBucket)
		return err
	})
}

func (s *Storage) Get(key string) []byte {
	var res []byte
	s.db.View(func(t *bbolt.Tx) error {
		res = t.Bucket(defaultBucket).Get([]byte(key))
		return nil
	})

	return res
}

type Item struct {
	Key   string
	Value []byte
}

func (s *Storage) GetExtraKeys(isExtra func(key string) bool) []Item {
	var items []Item
	s.db.View(func(t *bbolt.Tx) error {
		bucket := t.Bucket(defaultBucket)
		bucket.ForEach(func(k, v []byte) error {
			ks := string(k)
			if isExtra(ks) {
				items = append(items, Item{Key: ks, Value: v})
			}
			return nil
		})
		return nil
	})
	return items
}

func (s *Storage) Set(key string, value []byte) error {
	return s.db.Update(func(t *bbolt.Tx) error {
		return t.Bucket(defaultBucket).Put([]byte(key), value)
	})
}

func (s *Storage) DeleteKeys(items []Item) error {
	return s.db.Update(func(t *bbolt.Tx) error {
		bucket := t.Bucket(defaultBucket)
		for _, item := range items {
			if err := bucket.Delete([]byte(item.Key)); err != nil {
				return fmt.Errorf("could not delete key: %v", err)
			}
		}
		return nil
	})

}
