package storage

import (
	"fmt"
	"io/fs"
	"time"

	"go.etcd.io/bbolt"
)

type Storage struct {
	db *bbolt.DB
}

func New(dbLocation string) (*Storage, func(), error) {
	db, err := bbolt.Open(dbLocation, fs.FileMode(0664), &bbolt.Options{Timeout: time.Second * 1})
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

func (s *Storage) Set(key string, value []byte) error {
	return s.db.Update(func(t *bbolt.Tx) error {
		return t.Bucket(defaultBucket).Put([]byte(key), value)
	})
}
