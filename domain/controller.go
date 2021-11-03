package domain

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/alarmfox/distributed-kv/storage"
)

type Controller struct {
	shards      map[uint64]string
	currShardID uint64
	currStorage *storage.Storage
	client      *http.Client
}

const (
	bufferSize       = 8192
	socketBufferSize = 1024 * 1024
	pingInterval     = time.Second
)

func NewController(currStorage *storage.Storage, currShardID uint64, shardAddress string) *Controller {
	return &Controller{
		shards:      map[uint64]string{currShardID: shardAddress},
		currStorage: currStorage,
		currShardID: currShardID,
		client: &http.Client{
			Timeout: time.Second,
		},
	}
}

func (c *Controller) getShardID(key string) uint64 {
	return (binary.BigEndian.Uint64(fnv.New128().Sum([]byte(key))) % uint64(len(c.shards)))
}

func (c *Controller) Get(key string) ([]byte, error) {
	shardID := c.getShardID(key)
	if shardID != c.currShardID {
		log.Printf("Key: %s; curShard: %d; redirecting to Shard-%d", key, c.currShardID, shardID)
		return c.getRemoteKey(c.shards[shardID], key)
	}

	return c.currStorage.Get(key), nil
}

func (c *Controller) Set(key string, value []byte) error {
	shardID := c.getShardID(key)
	if shardID != c.currShardID {
		log.Printf("Key: %s; curShard: %d; redirecting to Shard-%d", key, c.currShardID, shardID)
		return c.setRemoteKey(c.shards[shardID], key, value)
	}

	return c.currStorage.Set(key, value)
}

func (c *Controller) Reshard() error {
	itemsToReshard := c.currStorage.GetExtraKeys(func(key string) bool { return c.currShardID != c.getShardID(key) })

	for _, item := range itemsToReshard {
		shardID := c.getShardID(item.Key)
		if err := c.setRemoteKey(c.shards[shardID], item.Key, item.Value); err != nil {
			return fmt.Errorf("cannot set key %s: %v", item.Key, err)
		}
	}
	return c.currStorage.DeleteKeys(itemsToReshard)

}

func (c *Controller) setRemoteKey(address, key string, value []byte) error {
	params := url.Values{}
	params.Add("key", key)
	params.Add("value", string(value))

	reqUrl := fmt.Sprintf("http://%s/set?%s", address, params.Encode())
	resp, err := c.client.Get(reqUrl)

	if err != nil {
		return fmt.Errorf("Set(%q) error: %v", address, err)
	} else if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("Set(%q) status %s", address, resp.Status)
	}

	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return nil
}

func (c *Controller) getRemoteKey(address, key string) ([]byte, error) {
	params := url.Values{}
	params.Add("key", key)

	reqUrl := fmt.Sprintf("http://%s/get?%s", address, params.Encode())
	resp, err := c.client.Get(reqUrl)

	if err != nil {
		return nil, fmt.Errorf("Get(%q) error: %v", reqUrl, err)
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Get(%q) status %s", reqUrl, resp.Status)
	}

	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

const (
	pingEvent = iota
	byeEvent
)

type peerMessage struct {
	ActionID uint8
	ShardID  uint64
	Address  string
}

func (c *Controller) processPeerMessage(pm peerMessage) {
	switch pm.ActionID {
	case pingEvent:
		c.processPing(pm.ShardID, pm.Address)
	case byeEvent:
		c.processBye(pm.ShardID)
	default:
		log.Printf("unknown action: %d", pm.ActionID)
	}
}

func (c *Controller) JoinCluster(ctx context.Context, peerAddress string) error {
	go selfPublish(ctx, peerAddress, c.currShardID, c.shards[c.currShardID])
	return listenForPeers(ctx, peerAddress, c.processPeerMessage)
}

func (c *Controller) processPing(shardID uint64, shardAddress string) {
	address, ok := c.shards[shardID]

	if !ok {
		c.shards[shardID] = shardAddress
		log.Printf("Got new peer %s", shardAddress)
		if err := c.Reshard(); err != nil {
			log.Printf("Reshard error: %v", err)
		}
	} else if c.currShardID == shardID && shardAddress != c.shards[c.currShardID] {
		log.Printf("Conflict: detected bad shard with id %d on %s", c.currShardID, address)
	}
}

func (c *Controller) processBye(shardID uint64) {
	address, ok := c.shards[shardID]
	if ok && shardID != c.currShardID {
		delete(c.shards, shardID)
		log.Printf("Deleting peer %s", address)
		if err := c.Reshard(); err != nil {
			log.Printf("Reshard error: %v", err)
		}
	}
}

func listenForPeers(ctx context.Context, address string, onMessageFn func(peerMessage)) error {
	laddr, err := net.ResolveUDPAddr("udp4", address)
	if err != nil {
		return fmt.Errorf("resolve(%q): %v", address, err)
	}

	conn, err := net.ListenMulticastUDP("udp4", nil, laddr)
	if err != nil {
		return fmt.Errorf("listen(%q): %v", address, err)
	}
	defer conn.Close()

	conn.SetReadBuffer(socketBufferSize)

	log.Printf("listening for peers on: %s", address)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			buffer := make([]byte, bufferSize)
			bytesRead, from, err := conn.ReadFromUDP(buffer)
			if err != nil {
				log.Printf("receive error: %v", err)
				continue
			}
			var msg peerMessage
			if err := json.Unmarshal(buffer[:bytesRead], &msg); err != nil {
				log.Printf("decode error from %s: %v", from.IP, err)
			}
			onMessageFn(msg)
		}
	}
}

func selfPublish(ctx context.Context, peerAddress string, shardID uint64, listenAddress string) {
	raddr, err := net.ResolveUDPAddr("udp4", peerAddress)
	if err != nil {
		log.Printf("resolve(%q): %v", peerAddress, err)
		return
	}

	pingBody, _ := json.Marshal(peerMessage{ActionID: pingEvent, ShardID: shardID, Address: listenAddress})

	conn, err := net.DialUDP("udp4", nil, raddr)
	if err != nil {
		log.Printf("dial error: %v", err)
		return
	}

	defer func() {
		byeMessage, _ := json.Marshal(peerMessage{ActionID: byeEvent, ShardID: shardID, Address: listenAddress})
		conn.Write(byeMessage)
		conn.Close()
	}()
	for {
		select {
		case <-time.After(pingInterval):
			if _, err := conn.Write(pingBody); err != nil {
				log.Printf("publish error: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}
