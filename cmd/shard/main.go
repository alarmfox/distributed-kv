package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/alarmfox/distributed-kv/controller"
	"github.com/alarmfox/distributed-kv/storage"
	"github.com/alarmfox/distributed-kv/transport"
)

func main() {
	var (
		dbLocation    = flag.String("db-location", "", "Path where to create db file.")
		listenAddress = flag.String("listen-addr", "127.0.0.1:9999", "Listen address for the shard.")
		name          = flag.String("name", "", "Shard name.")
		configFile    = flag.String("config-file", "config.json", "Configuration file for shards.")
	)
	flag.Parse()

	shardMap, currShard, err := parseConfig(*configFile, *name)
	if err != nil {
		log.Fatalf("[%s] config error: %v", *name, err)
	}
	log.SetPrefix(fmt.Sprintf("[%s-%d]", *name, currShard))

	db, dispose, err := storage.New(*dbLocation)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	defer dispose()
	log.Printf("Listening on %s", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, transport.MakeHTTPHandler(controller.New(shardMap, db, currShard))))
}

type config struct {
	Shards []struct {
		ID      uint   `json:"id"`
		Name    string `json:"name"`
		Address string `json:"address"`
	} `json:"shards"`
}

func parseConfig(configPath, currShardName string) (map[uint64]string, uint64, error) {
	f, err := os.ReadFile(configPath)

	if err != nil {
		return nil, 0, err
	}

	var c config
	if err := json.Unmarshal(f, &c); err != nil {
		return nil, 0, err
	}

	shardMap := make(map[uint64]string)
	currShard := -1
	for _, shard := range c.Shards {
		shardMap[uint64(shard.ID)] = shard.Address
		if shard.Name == currShardName {
			currShard = int(shard.ID)
		}
	}
	if currShard < 0 {
		return nil, 0, fmt.Errorf("current shard (%q) not found in config %q", currShardName, configPath)
	}
	return shardMap, uint64(currShard), nil
}
