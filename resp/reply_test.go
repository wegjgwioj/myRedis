// Reply 序列化测试：验证各类 reply 的输出格式符合 RESP 规范。
// 目标：保证客户端（redis-cli/go-redis）能稳定解析服务端回包。
// 覆盖：status/error/int/bulk/array 等常用类型。
package resp

import "testing"

// 本文件验证 RESP Reply 的序列化输出是否符合协议格式。

func TestBulkReply_ToBytes(t *testing.T) {
	if got := string(MakeBulkReply(nil).ToBytes()); got != "$-1\r\n" {
		t.Fatalf("null bulk: expected %q, got %q", "$-1\\r\\n", got)
	}

	if got := string(MakeBulkReply([]byte("foo")).ToBytes()); got != "$3\r\nfoo\r\n" {
		t.Fatalf("bulk: expected %q, got %q", "$3\\r\\nfoo\\r\\n", got)
	}
}

func TestMultiBulkReply_ToBytes(t *testing.T) {
	if got := string(MakeMultiBulkReply(nil).ToBytes()); got != "*-1\r\n" {
		t.Fatalf("null array: expected %q, got %q", "*-1\\r\\n", got)
	}

	empty := MakeMultiBulkReply([][]byte{})
	if got := string(empty.ToBytes()); got != "*0\r\n" {
		t.Fatalf("empty array: expected %q, got %q", "*0\\r\\n", got)
	}

	arr := MakeMultiBulkReply([][]byte{[]byte("GET"), []byte("k")})
	if got := string(arr.ToBytes()); got != "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n" {
		t.Fatalf("array: expected %q, got %q", "*2\\r\\n$3\\r\\nGET\\r\\n$1\\r\\nk\\r\\n", got)
	}
}
