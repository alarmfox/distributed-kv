package main

import (
	"flag"
	"log"
	"net/http"

	"gitlab.com/alarmfox/distributed-kv/internal/shard/storage/bolt"
	"gitlab.com/alarmfox/distributed-kv/internal/shard/storage/inmem"
	"gitlab.com/alarmfox/distributed-kv/internal/shard/transport"
)

func main() {
	var (
		dbLocation    = flag.String("db-location", "", "Path where to create db file. Use :memory: to get a lightweight non persistent in memory storage")
		listenAddress = flag.String("listen-addr", "127.0.0.1:9999", "Listen address for the shard.")
		name          = flag.String("name", "", "Shard name.")
	)
	flag.Parse()

	var db transport.Storage
	var closeFunc func() error = func() error { return nil }
	if *dbLocation == ":memory:" {
		db = inmem.NewStorage()
	} else {
		boltDB, dispose, err := bolt.NewStorage(*dbLocation)
		if err != nil {
			log.Fatalf("error: %v", err)
		}
		closeFunc = dispose
		db = boltDB
	}

	defer closeFunc()

	log.Printf("Shard %s listening on %s\n", *name, *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, transport.MakeHTTPHandler(db)))
}
