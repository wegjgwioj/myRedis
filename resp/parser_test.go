// RESP 解析测试：覆盖 Pipeline（一次写入多条命令）与拆包（分块读取）两类关键场景。
// 目标：确保解析器在真实 TCP 场景下稳定工作。
// 覆盖：多条命令连续解析、分片输入仍能正确拼包解析。
package resp

import (
	"bytes"
	"io"
	"testing"
)

// 本文件验证 RESP 解析器的两个关键能力：
// 1) TCP 粘包：多个命令连续写入（Pipeline）能逐条解析
// 2) TCP 拆包：命令被拆成很小的片段输入，仍能正确解析

type chunkReader struct {
	data      []byte
	chunkSize int
	pos       int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := r.chunkSize
	if n <= 0 {
		n = 1
	}
	if n > len(p) {
		n = len(p)
	}
	if r.pos+n > len(r.data) {
		n = len(r.data) - r.pos
	}
	copy(p, r.data[r.pos:r.pos+n])
	r.pos += n
	return n, nil
}

func TestParseStream_Pipeline(t *testing.T) {
	// Pipeline：一次 write 连续拼接多条命令，解析器应逐条产出 payload。
	// 量化：本用例使用 N=1000，覆盖“高频 pipeline”场景，确保不会只解析出第一条。
	const N = 1000

	var data []byte
	for i := 0; i < N; i++ {
		cmd := MakeMultiBulkReply([][]byte{[]byte("PING")}).ToBytes()
		data = append(data, cmd...)
	}

	got := 0
	for p := range ParseStream(bytes.NewReader(data)) {
		if p.Err != nil {
			t.Fatalf("parse error: %v", p.Err)
		}
		mb, ok := p.Data.(*MultiBulkReply)
		if !ok {
			t.Fatalf("expected MultiBulkReply, got %T", p.Data)
		}
		if len(mb.Args) != 1 || string(mb.Args[0]) != "PING" {
			t.Fatalf("unexpected args: %q", mb.Args)
		}
		got++
	}

	if got != N {
		t.Fatalf("expected %d commands, got %d", N, got)
	}
}

func TestParseStream_FragmentedInput(t *testing.T) {
	cmd := MakeMultiBulkReply([][]byte{[]byte("SET"), []byte("k"), []byte("v")}).ToBytes()
	r := &chunkReader{data: cmd, chunkSize: 1}

	payloads := ParseStream(r)
	p, ok := <-payloads
	if !ok {
		t.Fatalf("expected 1 payload")
	}
	if p.Err != nil {
		t.Fatalf("parse error: %v", p.Err)
	}
	mb, ok := p.Data.(*MultiBulkReply)
	if !ok {
		t.Fatalf("expected MultiBulkReply, got %T", p.Data)
	}
	if len(mb.Args) != 3 || string(mb.Args[0]) != "SET" || string(mb.Args[1]) != "k" || string(mb.Args[2]) != "v" {
		t.Fatalf("unexpected args: %q", mb.Args)
	}

	// channel 应在 EOF 后关闭
	if p2, ok := <-payloads; ok && p2 != nil {
		t.Fatalf("expected no more payloads, got %+v", p2)
	}
}
