package server

import (
	"io"
	"log"
	"myredis/db"
	"myredis/resp"
	"net"
)

type Server struct {
	Addr string
	Db   db.DB
}

func NewServer(addr string, db db.DB) *Server {
	return &Server{
		Addr: addr,
		Db:   db,
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	log.Printf("MyRedis listening on %s", s.Addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

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

		// Execute command
		reply := s.Db.Exec(multiBulk.Args)
		if reply != nil {
			conn.Write(reply.ToBytes())
		} else {
			conn.Write(resp.MakeErrReply("unknown error").ToBytes())
		}
	}
}
