package main

import (
	"flag"
	"log"
	"net/http"

	"gitlab.com/alarmfox/distributed-kv/internal/bolt"
	transport "gitlab.com/alarmfox/distributed-kv/internal/http"
)

var (
	dbLocation    = flag.String("db-location", "", "Path where to create db file.")
	listenAddress = flag.String("listen-address", "127.0.0.1:9999", "Listen address for the shard.")
)

func main() {
	flag.Parse()

	db, dispose, err := bolt.NewStorage(*dbLocation)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	defer dispose()

	handler := transport.MakeHTTPHandler(db)
	http.Handle("/api", handler)

	log.Fatal(http.ListenAndServe(*listenAddress, handler))
}
