package controller

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/alarmfox/distributed-kv/storage"
)

type Controller struct {
	nShard      uint64
	shards      map[uint64]string
	currStorage *storage.Storage
	currShardID uint64
	client      *http.Client
}

func New(shards map[uint64]string, currStorage *storage.Storage, currShardID uint64) *Controller {
	return &Controller{
		nShard:      uint64(len(shards)),
		shards:      shards,
		currStorage: currStorage,
		currShardID: currShardID,
		client: &http.Client{
			Timeout: time.Second,
		},
	}
}

func (c *Controller) getShardID(key string) uint64 {
	return (binary.BigEndian.Uint64(fnv.New128().Sum([]byte(key))) % uint64(c.nShard)) + 1
}

func (c *Controller) Get(key string) ([]byte, error) {
	shardID := c.getShardID(key)
	if shardID == c.currShardID {
		return c.currStorage.Get(key), nil
	}

	params := url.Values{}
	params.Add("key", key)

	shardAddress := fmt.Sprintf("http://%s/get?%s", c.shards[shardID], params.Encode())
	resp, err := c.client.Get(shardAddress)

	if err != nil {
		return nil, fmt.Errorf("get(%q) error: %v", shardAddress, err)
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get(%q) status %s", shardAddress, resp.Status)
	}

	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (c *Controller) Set(key string, value []byte) error {
	shardID := c.getShardID(key)
	if shardID == c.currShardID {
		return c.currStorage.Set(key, value)
	}

	params := url.Values{}
	params.Add("key", key)
	params.Add("value", string(value))

	shardAddress := fmt.Sprintf("http://%s/set?%s", c.shards[shardID], params.Encode())
	resp, err := c.client.Get(shardAddress)

	if err != nil {
		return fmt.Errorf("get(%q) error: %v", shardAddress, err)
	} else if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("get(%q) status %s", shardAddress, resp.Status)
	}

	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return nil
}
