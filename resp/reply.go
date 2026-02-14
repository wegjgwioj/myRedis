package resp

import (
	"bytes"
	"strconv"
)

// ===================================
//  RESP 协议响应封装 (Redis Serialization Protocol)
// ===================================

const (
	CRLF = "\r\n"
)

// Reply 所有 RESP 响应的接口
type Reply interface {
	ToBytes() []byte
}

// -----------------------------------
// Simple String: +OK\r\n
// -----------------------------------

type StatusReply struct {
	Status string
}

func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{Status: status}
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

// -----------------------------------
// Error: -ERR unknown command\r\n
// -----------------------------------

type ErrorReply struct {
	Status string
}

func MakeErrReply(status string) *ErrorReply {
	return &ErrorReply{Status: status}
}

func (r *ErrorReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

// -----------------------------------
// Integer: :1000\r\n
// -----------------------------------

type IntReply struct {
	Code int64
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{Code: code}
}

func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

// -----------------------------------
// Bulk String: $6\r\nfoobar\r\n
//              $-1\r\n (NULL Bulk String)
// -----------------------------------

type BulkReply struct {
	Arg []byte
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{Arg: arg}
}

func (r *BulkReply) ToBytes() []byte {
	if r.Arg == nil {
		return []byte("$-1" + CRLF)
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

// -----------------------------------
// Array: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
//        *-1\r\n (NULL Array)
// -----------------------------------

type MultiBulkReply struct {
	Args [][]byte
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{Args: args}
}

func (r *MultiBulkReply) ToBytes() []byte {
	var buf bytes.Buffer
	argLen := len(r.Args)
	if argLen == 0 && r.Args == nil {
		return []byte("*-1" + CRLF)
	}

	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

var (
	OkReply       = MakeStatusReply("OK")
	PongReply     = MakeStatusReply("PONG")
	NullBulkReply = MakeBulkReply(nil)
)
