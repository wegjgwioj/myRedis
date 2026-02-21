// RESP 解析器：实现 Redis Serialization Protocol 的解析（支持粘包/拆包与 Pipeline）。
// 关键点：基于 bufio.Reader 读取行与定长 bulk，按前缀分派解析，持续产出 Payload。
// 输入/输出：输入为 io.Reader（TCP 连接），输出为 Payload channel（逐条命令/回复）。
package resp

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

// 本文件实现 RESP 协议解析器（Redis Serialization Protocol）：
// - 使用状态机/分支解析不同前缀：*（数组）、$（Bulk）、+（状态）、-（错误）、:（整数）
// - 依赖 bufio.Reader 的 ReadBytes/ReadFull 来天然处理 TCP 粘包/拆包
// - ParseStream 支持 Pipeline：一个连接连续发送多条命令，会逐条产出 Payload

// Pipeline/Payload
type Payload struct {
	Data Reply
	Err  error
}

// ParseStream continuously reads from reader and sends Payloads to channel
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

func parse0(reader io.Reader, ch chan<- *Payload) {
	defer close(ch)
	bufReader := bufio.NewReader(reader)

	for {
		// Read first line
		line, err := readLine(bufReader)
		if err != nil {
			if err == io.EOF {
				return
			}
			ch <- &Payload{Err: err}
			return
		}

		// Parse based on prefix
		payload := &Payload{}
		payload.Data, payload.Err = parseLine(line, bufReader)

		ch <- payload
		if payload.Err != nil {
			return
		}
	}
}

func parseLine(line []byte, reader *bufio.Reader) (Reply, error) {
	if len(line) == 0 {
		return nil, errors.New("empty line")
	}

	switch line[0] {
	case '*': // Array: *3\r\n
		return parseArray(line, reader)
	case '$': // Bulk String: $4\r\n
		return parseBulk(line, reader)
	case '+': // Simpe String: +OK\r\n
		return MakeStatusReply(string(line[1:])), nil
	case '-': // Error: -ERR\r\n
		return MakeErrReply(string(line[1:])), nil
	case ':': // Integer: :1000\r\n
		val, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, err
		}
		return MakeIntReply(val), nil
	default:
		// Text protocol (inline commands, e.g. "PING\r\n") support?
		// For strict RESP, this is an error, but telnet sends inline commands.
		// Let's implement strict RESP first.
		return nil, errors.New("protocol error: " + string(line))
	}
}

func parseArray(header []byte, reader *bufio.Reader) (*MultiBulkReply, error) {
	// *3\r\n -> 3
	n, err := strconv.Atoi(string(header[1:]))
	if err != nil {
		return nil, err
	}
	if n == -1 {
		return nil, nil // Null array
	}

	lines := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		line, err := readLine(reader)
		if err != nil {
			return nil, err
		}

		// Typically elements are BulkStrings ($...)
		// but formally can be any type. Let's handle BulkString optimization
		if line[0] == '$' {
			bulk, err := parseBulk(line, reader)
			if err != nil {
				return nil, err
			}
			lines = append(lines, bulk.Arg)
		} else {
			// Recursive logic for mixed arrays is complex, skip for now.
			// Redis clients strictly send arrays of bulk strings for commands.
			return nil, errors.New("protocol error: expected bulk string in array")
		}
	}
	return MakeMultiBulkReply(lines), nil
}

func parseBulk(header []byte, reader *bufio.Reader) (*BulkReply, error) {
	// $4\r\n -> 4
	n, err := strconv.Atoi(string(header[1:]))
	if err != nil {
		return nil, err
	}
	if n == -1 {
		return MakeBulkReply(nil), nil // Null Bulk String
	}

	// Read N bytes + \r\n
	body := make([]byte, n+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, err
	}

	// Verify CRLF
	if body[n] != '\r' || body[n+1] != '\n' {
		return nil, errors.New("protocol error: bad bulk string format")
	}

	return MakeBulkReply(body[:n]), nil
}

func readLine(bufReader *bufio.Reader) ([]byte, error) {
	// Read until \n
	line, err := bufReader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	// Trim \r\n
	if len(line) > 1 && line[len(line)-2] == '\r' {
		return line[:len(line)-2], nil
	}
	return nil, errors.New("protocol error: no CRLF")
}
