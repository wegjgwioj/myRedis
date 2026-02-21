// 淘汰与 AOF 一致性测试：验证“容量淘汰导致的删除”会写入 AOF。
// 目标：避免重启后被淘汰的 key 复活（这是常见一致性缺陷，必须测试覆盖）。
// 覆盖：写入触发淘汰 -> Close -> Load -> 不应恢复被淘汰 key。
package db

import (
	"bytes"
	"myredis/resp"
	"os"
	"path/filepath"
	"testing"
)

// 本文件验证“淘汰写入 AOF”：
// - 运行时因为 max-bytes 触发的淘汰，会在 AOF 中追加 DEL
// - 即使重放时 max-bytes 更大（不再触发淘汰），被淘汰的数据也不会复活

func TestAOF_EvictionShouldNotResurrectAfterRestart(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "node.aof")

	// 第一次运行：用很小的 maxBytes 触发淘汰
	db1 := NewStandaloneDBWithConfig(StandaloneDBConfig{
		AofFilename: filename,
		MaxBytes:    20,
		Eviction:    "lru",
	})

	// k1(2)+v1(10)=12, k2(2)+v2(10)=12 => 总 24 > 20，触发淘汰 k1
	db1.Exec([][]byte{[]byte("SET"), []byte("k1"), []byte("0123456789")})
	db1.Exec([][]byte{[]byte("SET"), []byte("k2"), []byte("abcdefghij")})

	if db1.aofHandler == nil {
		t.Fatalf("expected aof handler")
	}
	if err := db1.aofHandler.Flush(); err != nil {
		t.Fatalf("flush error: %v", err)
	}
	db1.Close()

	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("read aof error: %v", err)
	}
	if !bytes.Contains(data, []byte("\r\n$3\r\nDEL\r\n")) {
		t.Fatalf("expected aof contains DEL for eviction, got %q", string(data))
	}

	// 第二次运行：用很大的 maxBytes（不再触发淘汰），验证 k1 不会复活
	db2 := NewStandaloneDBWithConfig(StandaloneDBConfig{
		AofFilename: filename,
		MaxBytes:    DefaultMaxBytes,
		Eviction:    "lru",
	})
	db2.Load()
	defer db2.Close()

	r1 := db2.Exec([][]byte{[]byte("GET"), []byte("k1")})
	if br, ok := r1.(*resp.BulkReply); !ok || br.Arg != nil {
		t.Fatalf("expected k1 not exist after replay, got %T %+v", r1, r1)
	}

	r2 := db2.Exec([][]byte{[]byte("GET"), []byte("k2")})
	if br, ok := r2.(*resp.BulkReply); !ok || string(br.Arg) != "abcdefghij" {
		t.Fatalf("expected k2 exist after replay, got %T %+v", r2, r2)
	}
}
