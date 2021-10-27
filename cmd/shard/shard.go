package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/alarmfox/distributed-kv/storage/disk"
	"github.com/alarmfox/distributed-kv/transport"
)

func main() {
	var (
		dbLocation    = flag.String("db-location", "", "Path where to create db file.")
		listenAddress = flag.String("listen-addr", "127.0.0.1:9999", "Listen address for the shard.")
		name          = flag.String("name", "", "Shard name.")
	)
	flag.Parse()

	db, dispose, err := disk.New(*dbLocation)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	defer dispose()

	log.Printf("Shard %s listening on %s\n", *name, *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, transport.MakeHTTPHandler(db)))
}
