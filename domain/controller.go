package domain

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/alarmfox/distributed-kv/storage"
)

type Controller struct {
	shards      map[uint64]string
	currStorage *storage.Storage
	currShardID uint64
	client      *http.Client
}

type PeerMessage struct {
	ShardID uint64
	Address string
}

func NewController(currStorage *storage.Storage, currShardID uint64) *Controller {
	return &Controller{
		shards:      make(map[uint64]string),
		currStorage: currStorage,
		currShardID: currShardID,
		client: &http.Client{
			Timeout: time.Second,
		},
	}
}

func (c *Controller) getShardID(key string) uint64 {
	return (binary.BigEndian.Uint64(fnv.New128().Sum([]byte(key))) % uint64(len(c.shards))) + 1
}

func (c *Controller) Get(key string) ([]byte, error) {
	shardID := c.getShardID(key)
	if shardID != c.currShardID {
		log.Printf("key: %s; curShard: %d; targetShard: %d", key, c.currShardID, shardID)
		return c.getRemoteKey(c.shards[shardID], key)
	}

	return c.currStorage.Get(key), nil
}

func (c *Controller) Set(key string, value []byte) error {
	shardID := c.getShardID(key)
	if shardID != c.currShardID {
		log.Printf("key: %s; curShard: %d; targetShard: %d", key, c.currShardID, shardID)
		return c.setRemoteKey(c.shards[shardID], key, value)
	}

	return c.currStorage.Set(key, value)
}

func (c *Controller) Reshard() error {
	itemsToReshard := c.currStorage.GetExtraKeys(func(key string) bool { return c.currShardID != c.getShardID(key) })

	for _, item := range itemsToReshard {
		shardID := c.getShardID(item.Key)
		if err := c.setRemoteKey(c.shards[shardID], item.Key, item.Value); err != nil {
			log.Printf("cannot set key %s: %v", item.Key, err)
		}
	}
	return c.currStorage.DeleteKeys(itemsToReshard)

}

func (c *Controller) setRemoteKey(address, key string, value []byte) error {
	params := url.Values{}
	params.Add("key", key)
	params.Add("value", string(value))

	resp, err := c.client.Get(fmt.Sprintf("http://%s/set?%s", address, params.Encode()))

	if err != nil {
		return fmt.Errorf("get(%q) error: %v", address, err)
	} else if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("get(%q) status %s", address, resp.Status)
	}

	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return nil
}

func (c *Controller) getRemoteKey(address, key string) ([]byte, error) {
	params := url.Values{}
	params.Add("key", key)

	shardAddress := fmt.Sprintf("http://%s/get?%s", address, params.Encode())
	resp, err := c.client.Get(shardAddress)

	if err != nil {
		return nil, fmt.Errorf("get(%q) error: %v", shardAddress, err)
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get(%q) status %s", shardAddress, resp.Status)
	}

	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (c *Controller) UpdateShardMap(pm PeerMessage) {
	_, ok := c.shards[pm.ShardID]

	if !ok {
		c.shards[pm.ShardID] = pm.Address
		if err := c.Reshard(); err != nil {
			log.Printf("reshard: %v", err)
		}
	}
}
