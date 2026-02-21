// Cluster PeerClient：用于“透明转发”时与其它节点通信。
// 关键点：连接池复用 TCP 连接、串行读写一问一答、读取单个 RESP reply 并返回给上层 Router。
// 说明：这里只做静态节点列表下的转发，不实现 Redis Cluster 的 MOVED/ASK 协议。
package cluster

import (
	"errors"
	"myredis/resp"
	"net"
	"sync"
	"time"
)

// 本文件实现对等节点（peer）的客户端：
// - 复用 TCP 连接（简单连接池），降低转发开销
// - 采用 RESP request/reply：发送 MultiBulk 命令，读取一个 Reply 返回

type peerConn struct {
	conn   net.Conn
	parser *resp.StreamParser
}

// PeerClient 为某个 peer 地址维护一个小型连接池。
type PeerClient struct {
	addr        string
	dialTimeout time.Duration
	rwTimeout   time.Duration

	pool      chan *peerConn
	closing   chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
}

func NewPeerClient(addr string, poolSize int) *PeerClient {
	if poolSize <= 0 {
		poolSize = 4
	}
	return &PeerClient{
		addr:        addr,
		dialTimeout: 2 * time.Second,
		rwTimeout:   5 * time.Second,
		pool:        make(chan *peerConn, poolSize),
		closing:     make(chan struct{}),
	}
}

func (c *PeerClient) Close() {
	c.closeOnce.Do(func() {
		close(c.closing)
	})
	// 关闭所有空闲连接（正在使用的连接会在 release 时被关闭）
	for {
		select {
		case pc := <-c.pool:
			if pc != nil {
				_ = pc.conn.Close()
			}
		default:
			return
		}
	}
}

func (c *PeerClient) Do(cmd [][]byte) (resp.Reply, error) {
	select {
	case <-c.closing:
		return nil, errors.New("peer client closed")
	default:
	}

	pc, err := c.acquire()
	if err != nil {
		return nil, err
	}

	// 超时保护：避免 peer 卡住导致当前连接 goroutine 无限制阻塞
	_ = pc.conn.SetDeadline(time.Now().Add(c.rwTimeout))

	// 发送请求
	_, err = pc.conn.Write(resp.MakeMultiBulkReply(cmd).ToBytes())
	if err != nil {
		_ = pc.conn.Close()
		return nil, err
	}

	// 读取单个 RESP reply
	reply, err := pc.parser.ReadReply()
	if err != nil {
		_ = pc.conn.Close()
		return nil, err
	}

	// 清理 deadline，归还连接
	_ = pc.conn.SetDeadline(time.Time{})
	c.release(pc)
	return reply, nil
}

func (c *PeerClient) acquire() (*peerConn, error) {
	select {
	case <-c.closing:
		return nil, errors.New("peer client closed")
	default:
	}

	select {
	case pc := <-c.pool:
		return pc, nil
	default:
		conn, err := net.DialTimeout("tcp", c.addr, c.dialTimeout)
		if err != nil {
			return nil, err
		}
		return &peerConn{
			conn:   conn,
			parser: resp.NewStreamParser(conn),
		}, nil
	}
}

func (c *PeerClient) release(pc *peerConn) {
	select {
	case <-c.closing:
		_ = pc.conn.Close()
		return
	default:
	}

	select {
	case c.pool <- pc:
	default:
		_ = pc.conn.Close()
	}
}
