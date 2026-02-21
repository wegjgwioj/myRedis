// server 包：TCP 服务端实现（RESP 解析、命令执行、回包）。
// 关键点：连接级 goroutine + DB Actor 串行执行；支持 SHUTDOWN/信号触发的优雅关闭。
// 说明：优雅关闭需要先停 listener 再关连接，最后关闭 DB，确保 AOF drain+fsync 完成。
package server

import (
	"context"
	"io"
	"log"
	"myredis/db"
	"myredis/resp"
	"net"
	"strings"
	"sync"
	"time"
)

// 本文件实现 TCP Server：
// - 基于 RESP 协议解析请求
// - 每个连接一个 goroutine 负责读/写
// - 命令执行交给 db.DB（Actor 串行执行）
//
// 同时提供优雅关闭：
// - 关闭 listener，停止 accept 新连接
// - 主动关闭现有连接，等待所有连接 goroutine 退出
// - 最后关闭 DB（保证 AOF drain + fsync）

type Server struct {
	Addr string
	Db   db.DB

	listener net.Listener

	closing   chan struct{}
	closeOnce sync.Once

	wg      sync.WaitGroup
	conns   map[net.Conn]struct{}
	connsMu sync.Mutex
}

func NewServer(addr string, db db.DB) *Server {
	return &Server{
		Addr:    addr,
		Db:      db,
		closing: make(chan struct{}),
		conns:   make(map[net.Conn]struct{}),
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	s.listener = listener
	log.Printf("MyRedis listening on %s", s.Addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.closing:
				return nil // 正常关闭
			default:
			}
			log.Printf("Accept error: %v", err)
			continue
		}
		s.trackConn(conn)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(conn)
		}()
	}
}

// Shutdown 优雅关闭服务器：停止 accept、关闭连接、等待 goroutine 退出、最后关闭 DB。
func (s *Server) Shutdown(ctx context.Context) error {
	s.closeOnce.Do(func() {
		close(s.closing)
		if s.listener != nil {
			_ = s.listener.Close()
		}

		// 关闭所有活动连接，促使 handleConnection 退出
		s.connsMu.Lock()
		for c := range s.conns {
			_ = c.Close()
		}
		s.connsMu.Unlock()
	})

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		// 即使超时，也继续关闭 DB，尽最大努力落盘
	}

	if s.Db != nil {
		s.Db.Close()
	}
	return ctx.Err()
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	defer s.untrackConn(conn)

	// Parse requests from connection
	payloads := resp.ParseStream(conn)

	for payload := range payloads {
		if payload == nil {
			continue // Should not happen, but safe guard
		}

		if payload.Err != nil {
			if payload.Err != io.EOF {
				log.Printf("Connection error: %v", payload.Err)
				conn.Write(resp.MakeErrReply(payload.Err.Error()).ToBytes())
			}
			return
		}

		if payload.Data == nil {
			continue
		}

		// Expecting MultiBulkReply (Array of Bulk Strings)
		multiBulk, ok := payload.Data.(*resp.MultiBulkReply)
		if !ok {
			log.Printf("Protocol error: expected MultiBulkReply, got %T", payload.Data)
			conn.Write(resp.MakeErrReply("protocol error: expected array").ToBytes())
			continue
		}

		// SHUTDOWN：用于评估流程/优雅退出（返回 +OK 后触发 Shutdown）
		if len(multiBulk.Args) > 0 && strings.EqualFold(string(multiBulk.Args[0]), "shutdown") {
			_, _ = conn.Write(resp.OkReply.ToBytes())
			go func() {
				// 给一个默认超时，避免卡死
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = s.Shutdown(ctx)
			}()
			return
		}

		// Execute command
		reply := s.Db.Exec(multiBulk.Args)
		if reply != nil {
			conn.Write(reply.ToBytes())
		} else {
			conn.Write(resp.MakeErrReply("unknown error").ToBytes())
		}
	}
}

func (s *Server) trackConn(conn net.Conn) {
	s.connsMu.Lock()
	s.conns[conn] = struct{}{}
	s.connsMu.Unlock()
}

func (s *Server) untrackConn(conn net.Conn) {
	s.connsMu.Lock()
	delete(s.conns, conn)
	s.connsMu.Unlock()
}
