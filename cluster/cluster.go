package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/alarmfox/distributed-kv/domain"
)

type Client struct {
	OnPeerMessage func(domain.PeerMessage)
	Self          domain.PeerMessage
}

func (c *Client) Join(ctx context.Context, peerAddress string) error {
	c.OnPeerMessage(c.Self)
	go c.selfPublish(ctx, peerAddress)
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

	log.Printf("listening on: %s", address)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			buffer := make([]byte, bufferSize)
			nBytes, from, err := conn.ReadFromUDP(buffer)
			if err != nil {
				log.Printf("receive error: %v", err)
				continue
			}
			var msg domain.PeerMessage
			if err := json.Unmarshal(buffer[:nBytes], &msg); err != nil {
				log.Printf("unmarshal from %s: %v", err, from.IP)
			}
			c.OnPeerMessage(msg)
		}
	}
}

const pingInterval = time.Second

func (c *Client) selfPublish(ctx context.Context, peerAddress string) {
	raddr, err := net.ResolveUDPAddr("udp4", peerAddress)
	if err != nil {
		log.Printf("resolve(%q): %v", peerAddress, err)
		return

	}
	body, _ := json.Marshal(&c.Self)

	conn, err := net.DialUDP("udp4", nil, raddr)
	if err != nil {
		log.Printf("dial error: %v", err)
		return
	}

	defer conn.Close()
	for {
		select {
		case <-time.After(pingInterval):
			conn.Write(body)
		case <-ctx.Done():
			return
		}
	}
}
