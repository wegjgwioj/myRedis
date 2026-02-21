// StreamParser：同步读取单个 RESP reply 的解析器。
// 用途：Cluster 转发场景中需要“一问一答”读取单条返回，而不是持续流式消费。
// 说明：内部复用 RESP 解析逻辑，但以阻塞方式读取单条 Reply，便于实现 request/response 语义。
package resp

import (
	"bufio"
	"io"
)

// 本文件提供“同步读取单个 RESP Reply”的能力。
// 目的：在分布式转发（cluster peer client）中复用 TCP 连接时，需要 request/reply 的同步解析，
// 而不是 ParseStream 那种“持续解析直到 EOF”的异步模型。

// StreamParser 是一个面向流的 RESP 解析器（同步读取）。
type StreamParser struct {
	reader *bufio.Reader
}

func NewStreamParser(r io.Reader) *StreamParser {
	return &StreamParser{reader: bufio.NewReader(r)}
}

// ReadReply 从流中读取一个完整的 RESP Reply。
func (p *StreamParser) ReadReply() (Reply, error) {
	line, err := readLine(p.reader)
	if err != nil {
		return nil, err
	}
	return parseLine(line, p.reader)
}
