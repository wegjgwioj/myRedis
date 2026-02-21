// AOF 回放相关测试：重点覆盖 TTL 绝对过期时间（PEXPIREAT）语义。
// 目标：保证 “重启不续命”，并且 AOF 内容可被稳定回放复原数据状态。
// 覆盖：EXPIRE 写入 AOF（转换为 PEXPIREAT）+ 重启回放。
package db

import (
	"bytes"
	"myredis/resp"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 本文件验证 AOF 重放的关键一致性：
// 1) EXPIRE 写入 AOF 时应转为 PEXPIREAT（绝对时间），避免重启后 TTL 续命
// 2) 重启后 TTL 应明显变小（不会回到初始 seconds）

func TestAOF_ExpireUsesPexpireat(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "node.aof")

	db1 := NewStandaloneDBWithConfig(StandaloneDBConfig{
		AofFilename: filename,
		MaxBytes:    DefaultMaxBytes,
		Eviction:    "lru",
	})

	// SET k v
	if r := db1.Exec([][]byte{[]byte("SET"), []byte("k"), []byte("v")}); r == nil {
		t.Fatalf("SET reply nil")
	}
	// EXPIRE k 5
	if r := db1.Exec([][]byte{[]byte("EXPIRE"), []byte("k"), []byte("5")}); r == nil {
		t.Fatalf("EXPIRE reply nil")
	}

	if db1.aofHandler == nil {
		t.Fatalf("expected aof handler")
	}
	if err := db1.aofHandler.Flush(); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("read aof error: %v", err)
	}
	if !bytes.Contains(data, []byte("PEXPIREAT")) {
		t.Fatalf("expected aof contains PEXPIREAT, got %q", string(data))
	}

	// 等待一段时间，让 TTL 真实减少
	time.Sleep(2 * time.Second)
	db1.Close()

	db2 := NewStandaloneDBWithConfig(StandaloneDBConfig{
		AofFilename: filename,
		MaxBytes:    DefaultMaxBytes,
		Eviction:    "lru",
	})
	db2.Load()
	defer db2.Close()

	// GET k -> v
	rGet := db2.Exec([][]byte{[]byte("GET"), []byte("k")})
	br, ok := rGet.(*resp.BulkReply)
	if !ok || string(br.Arg) != "v" {
		t.Fatalf("expected GET k == v, got %T %+v", rGet, rGet)
	}

	// TTL k：不应回到 5（重启不续命）；2 秒过去后应 <= 3（允许为 0）
	rTTL := db2.Exec([][]byte{[]byte("TTL"), []byte("k")})
	ir, ok := rTTL.(*resp.IntReply)
	if !ok {
		t.Fatalf("expected IntReply, got %T", rTTL)
	}
	if ir.Code < 0 {
		t.Fatalf("expected TTL >= 0, got %d", ir.Code)
	}
	if ir.Code > 3 {
		t.Fatalf("expected TTL <= 3 after restart, got %d", ir.Code)
	}
}
