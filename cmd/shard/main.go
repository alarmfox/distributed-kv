package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io/fs"
	"log"
	"net/http"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

func main() {
	var (
		dbLocation    = flag.String("db-location", "", "Path where to create db file.")
		listenAddress = flag.String("listen-addr", "127.0.0.1:9999", "Listen address for the shard.")
		name          = flag.String("name", "", "Shard name.")
		configFile    = flag.String("config-file", "config.json", "Configuration file for shards.")
	)
	flag.Parse()

	db, dispose, err := New(*dbLocation)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	defer dispose()

	conf, err := parseConfig(*configFile)
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	_ = conf

	shardMap := make(map[uint]string)
	currShard := -1
	for _, shard := range conf.Shards {
		shardMap[shard.ID] = shard.Address
		if shard.Name == *name {
			currShard = int(shard.ID)
		}
	}
	fmt.Printf("%+v\n", shardMap)
	if currShard < 0 {
		log.Fatalf("current shard (%q) not found in config %q", *name, *configFile)
	}
	controller := newController(shardMap, db, currShard)
	log.Printf("Shard %s listening on %s\n", *name, *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, makeHTTPHandler(controller)))
}

type storage struct {
	db *bolt.DB
}

func New(dbLocation string) (*storage, func(), error) {
	db, err := bolt.Open(dbLocation, fs.FileMode(0664), &bolt.Options{Timeout: time.Second * 1})
	if err != nil {
		return &storage{}, func() {}, fmt.Errorf("could not open %q: %w", dbLocation, err)
	}
	if err := createDefaultBucket(db); err != nil {
		db.Close()
		return &storage{}, func() {}, fmt.Errorf("could not init %q: %w", dbLocation, err)
	}
	return &storage{db: db}, func() { db.Close() }, nil
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

func (s *storage) Get(key string) []byte {
	var res []byte
	s.db.View(func(t *bolt.Tx) error {
		res = t.Bucket(defaultBucket).Get([]byte(key))
		return nil
	})

	return res
}

func (s *storage) Set(key string, value []byte) error {
	return s.db.Update(func(t *bolt.Tx) error {
		return t.Bucket(defaultBucket).Put([]byte(key), value)
	})
}

func makeHTTPHandler(c *controller) *http.ServeMux {
	r := http.NewServeMux()
	r.HandleFunc("/get", getHandler(c))
	// r.HandleFunc("/set", set(c))
	return r
}

func getHandler(c *controller) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(rw, fmt.Sprintf("invalid query params: %v", err), http.StatusBadRequest)
			return
		}
		key := r.Form.Get("key")

		if key == "" {
			http.Error(rw, "invalid query params", http.StatusBadRequest)
			return
		}

		res := c.get(key)

		// if errors.Is(err, errNotFound) {
		// 	http.Error(rw, fmt.Sprintf("key %q not found", key), http.StatusNotFound)
		// 	return
		// } else if err != nil {
		// 	http.Error(rw, "unknown error", http.StatusInternalServerError)
		// 	return
		// }

		fmt.Fprintf(rw, "key: %s; val: %s", key, string(res))
	}
}

// func set(c *controller) http.HandlerFunc {
// 	return func(rw http.ResponseWriter, r *http.Request) {
// 		if err := r.ParseForm(); err != nil {
// 			http.Error(rw, fmt.Sprintf("invalid query params: %v", err), http.StatusBadRequest)
// 			return
// 		}
// 		key := r.Form.Get("key")
// 		val := r.Form.Get("value")

// 		if key == "" || val == "" {
// 			http.Error(rw, "invalid query params", http.StatusBadRequest)
// 			return
// 		}

// 		if err := storage.Set(key, []byte(val)); err != nil {
// 			http.Error(rw, fmt.Sprintf("cannot store: %v", err), http.StatusBadRequest)
// 			return
// 		}

// 		fmt.Fprintf(rw, "key: %s; val: %s", key, val)
// 	}
// }

type config struct {
	Shards []struct {
		ID      uint   `json:"id"`
		Name    string `json:"name"`
		Address string `json:"address"`
	} `json:"shards"`
}

func parseConfig(configPath string) (config, error) {
	f, err := os.ReadFile(configPath)
	if err != nil {
		return config{}, err
	}

	var c config
	if err := json.Unmarshal(f, &c); err != nil {
		return config{}, err
	}
	return c, nil
}

type controller struct {
	nShard      int
	shards      map[uint]string
	currStorage *storage
	currShardID int
}

func newController(shards map[uint]string, currStorage *storage, currShardID int) *controller {
	return &controller{
		nShard:      len(shards),
		shards:      shards,
		currStorage: currStorage,
		currShardID: currShardID,
	}
}

func (c *controller) getShardID(key string) uint64 {
	return binary.BigEndian.Uint64(fnv.New128().Sum([]byte(key))) % uint64(c.nShard)
}

func (c *controller) get(key string) string {
	shard := c.getShardID(key)
	fmt.Printf("Shard ID: %d\n", shard)
	return ""
}
