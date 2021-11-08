package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"
)

const (
	pingEvent = iota
	ConflictEvent
	clusterLocked
	clusterUnlocked
)

type PeerEvent struct {
	Event   uint8
	ShardID uint64
	Address string
}

type Client struct {
	clusterLocked  int32
	clusterHealthy int32
	broadcastQueue chan PeerEvent
}

func (c *Client) processPeerMessage(pm PeerEvent) {
	switch pm.Event {
	case pingEvent:
		log.Printf("Ping from %s", pm.Address)
	case ConflictEvent:
		if atomic.CompareAndSwapInt32(&c.clusterHealthy, 1, 0) {
			log.Printf("Conflict for shard %d; cluster is unhealthy", pm.ShardID)
		}
	case clusterLocked:
		atomic.StoreInt32(&c.clusterLocked, 1)
	case clusterUnlocked:
		atomic.StoreInt32(&c.clusterLocked, 0)
	default:
		log.Printf("Unknown action: %d", pm.Event)
	}
}

func (c *Client) JoinCluster(ctx context.Context, selfID uint64, peerAddress, selfAddress string) error {
	go c.manageCluster(ctx, selfID, peerAddress, selfAddress)
	return c.listenForPeers(ctx, peerAddress)

}

const (
	bufferSize       = 8192
	socketBufferSize = 1024 * 1024
)

func (c *Client) listenForPeers(ctx context.Context, address string) error {
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

	log.Printf("Listening for peers on: %s", address)
LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		default:
			buffer := make([]byte, bufferSize)
			bytesRead, from, err := conn.ReadFromUDP(buffer)
			if err != nil {
				log.Printf("Receive error: %v", err)
				continue
			}
			var msg PeerEvent
			if err := json.Unmarshal(buffer[:bytesRead], &msg); err != nil {
				log.Printf("Decode error from %s: %v", from.IP, err)
			}
			c.processPeerMessage(msg)
		}
	}
	return nil
}

const (
	pingInterval  = time.Second
	conflictRetry = 5 * time.Second
)

func (c *Client) manageCluster(ctx context.Context, selfID uint64, peerAddress, selfAddress string) {
	raddr, err := net.ResolveUDPAddr("udp4", peerAddress)
	if err != nil {
		log.Printf("Resolve(%q): %v", peerAddress, err)
		return
	}

	conn, err := net.DialUDP("udp4", nil, raddr)
	if err != nil {
		log.Printf("Dial error: %v", err)
		return
	}

	defer conn.Close()

	pingMessage, _ := json.Marshal(PeerEvent{Event: pingEvent, ShardID: selfID, Address: selfAddress})
	pingTicks := time.NewTicker(pingInterval)
	recoverConflictTicks := time.NewTicker(conflictRetry)

	defer func() {
		pingTicks.Stop()
		recoverConflictTicks.Stop()
	}()

LOOP:
	for {
		select {
		case <-recoverConflictTicks.C:
			if atomic.CompareAndSwapInt32(&c.clusterHealthy, 0, 1) {
				log.Printf("trying to recover cluster")
			}
		case <-pingTicks.C:
			broadcast(conn, pingMessage)
		case m := <-c.broadcastQueue:
			body, _ := json.Marshal(m)
			broadcast(conn, body)
		case <-ctx.Done():
			break LOOP
		}
	}
}

func (c *Client) LockCluster() {
	atomic.StoreInt32(&c.clusterLocked, 1)
	c.broadcastQueue <- PeerEvent{Event: clusterLocked}
	log.Printf("cluster locked")

}

func (c *Client) UnlockCluster() {
	atomic.StoreInt32(&c.clusterLocked, 0)
	c.broadcastQueue <- PeerEvent{Event: clusterUnlocked}
	log.Printf("cluster unlocked")
}

func (c *Client) IsClusterReady() bool {
	return atomic.LoadInt32(&c.clusterHealthy) == 1 && atomic.LoadInt32(&c.clusterLocked) == 0
}

func (c *Client) Broadcast(message PeerEvent) {
	c.broadcastQueue <- message
}

func broadcast(w io.Writer, body []byte) {
	if _, err := w.Write(body); err != nil {
		log.Printf("Write: %v", err)
	}
}
