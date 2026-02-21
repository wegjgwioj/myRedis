// AOF rewrite 测试：验证 REWRITEAOF/BGREWRITEAOF 能生成可回放的新 AOF。
// 重点：重写期间的新写入不会丢失或乱序（通过最终 Load 结果验收）。
// 说明：该测试不追求比较文件体积的绝对大小，只验证“语义正确 + 可恢复”。
package db

import (
	"myredis/resp"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestAOF_RewriteAOF_Sync(t *testing.T) {
	dir := t.TempDir()
	aofFile := filepath.Join(dir, "appendonly.aof")

	db1 := NewStandaloneDBWithConfig(StandaloneDBConfig{
		AofFilename: aofFile,
		RdbFilename: "",
		MaxBytes:    DefaultMaxBytes,
		Eviction:    "lru",
	})
	defer db1.Close()

	mustStatus := func(r resp.Reply) {
		if _, ok := r.(*resp.StatusReply); !ok {
			t.Fatalf("expected status, got %T", r)
		}
	}
	mustBulkStr := func(r resp.Reply) string {
		br, ok := r.(*resp.BulkReply)
		if !ok {
			t.Fatalf("expected bulk, got %T", r)
		}
		if br.Arg == nil {
			return ""
		}
		return string(br.Arg)
	}

	// 制造一些“可压缩”的历史：同一个 key 多次覆盖
	mustStatus(db1.Exec([][]byte{[]byte("SET"), []byte("k1"), []byte("v1")}))
	mustStatus(db1.Exec([][]byte{[]byte("SET"), []byte("k1"), []byte("v2")}))
	mustStatus(db1.Exec([][]byte{[]byte("SET"), []byte("k1"), []byte("v3")}))

	// 其它结构
	_ = db1.Exec([][]byte{[]byte("LPUSH"), []byte("l1"), []byte("a"), []byte("b")})
	_ = db1.Exec([][]byte{[]byte("HSET"), []byte("h1"), []byte("f1"), []byte("v1")})
	_ = db1.Exec([][]byte{[]byte("SADD"), []byte("s1"), []byte("m1"), []byte("m2")})

	// TTL：写入绝对时间（PEXPIREAT）应保留语义
	nowMs := time.Now().UnixMilli()
	expireAt := strconv.FormatInt(nowMs+4000, 10)
	_ = db1.Exec([][]byte{[]byte("PEXPIREAT"), []byte("k1"), []byte(expireAt)})

	// 确保 AOF 落盘后再重写
	if db1.aofHandler == nil {
		t.Fatalf("aof handler nil")
	}
	if err := db1.aofHandler.Flush(); err != nil {
		t.Fatalf("flush error: %v", err)
	}
	before, _ := os.Stat(aofFile)

	// 同步重写
	mustStatus(db1.Exec([][]byte{[]byte("REWRITEAOF")}))
	if err := db1.aofHandler.Flush(); err != nil {
		t.Fatalf("flush after rewrite: %v", err)
	}
	after, _ := os.Stat(aofFile)
	if after == nil || after.Size() == 0 {
		t.Fatalf("aof file invalid after rewrite")
	}
	_ = before // size 不做硬断言（数据量小可能差异不明显）

	// 重写后追加新写入，验证 handler 仍可用
	mustStatus(db1.Exec([][]byte{[]byte("SET"), []byte("after"), []byte("1")}))
	if err := db1.aofHandler.Flush(); err != nil {
		t.Fatalf("flush after set: %v", err)
	}

	db1.Close()

	// 新实例仅通过 AOF 回放恢复
	db2 := NewStandaloneDBWithConfig(StandaloneDBConfig{
		AofFilename: aofFile,
		RdbFilename: "",
		MaxBytes:    DefaultMaxBytes,
		Eviction:    "lru",
	})
	defer db2.Close()
	db2.Load()

	if got := mustBulkStr(db2.Exec([][]byte{[]byte("GET"), []byte("k1")})); got != "v3" {
		t.Fatalf("GET k1 = %q", got)
	}
	if got := mustBulkStr(db2.Exec([][]byte{[]byte("GET"), []byte("after")})); got != "1" {
		t.Fatalf("GET after = %q", got)
	}

	// TTL 不应被“重写/重启”重置为 4 秒
	ttlReply := db2.Exec([][]byte{[]byte("TTL"), []byte("k1")})
	ir, ok := ttlReply.(*resp.IntReply)
	if !ok {
		t.Fatalf("expected TTL int, got %T", ttlReply)
	}
	if ir.Code < 0 || ir.Code > 3 {
		t.Fatalf("TTL k1 = %d (expected 0..3)", ir.Code)
	}
}

func TestAOF_BGRewriteAOF_NoLoss(t *testing.T) {
	dir := t.TempDir()
	aofFile := filepath.Join(dir, "appendonly.aof")

	db1 := NewStandaloneDBWithConfig(StandaloneDBConfig{
		AofFilename: aofFile,
		RdbFilename: "",
		MaxBytes:    DefaultMaxBytes,
		Eviction:    "lru",
	})
	defer db1.Close()

	if err := db1.aofHandler.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// 写入一些初始数据
	_ = db1.Exec([][]byte{[]byte("SET"), []byte("k1"), []byte("v1")})
	_ = db1.Exec([][]byte{[]byte("SET"), []byte("k1"), []byte("v2")})
	_ = db1.Exec([][]byte{[]byte("SET"), []byte("k2"), []byte("v1")})
	if err := db1.aofHandler.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// 后台重写开始
	r := db1.Exec([][]byte{[]byte("BGREWRITEAOF")})
	if _, ok := r.(*resp.StatusReply); !ok {
		t.Fatalf("expected status, got %T", r)
	}

	// 重写期间继续写入（这些命令必须出现在最终 AOF 中）
	_ = db1.Exec([][]byte{[]byte("SET"), []byte("during"), []byte("1")})
	_ = db1.Exec([][]byte{[]byte("SET"), []byte("k1"), []byte("v3")})
	if err := db1.aofHandler.Flush(); err != nil {
		t.Fatalf("flush during: %v", err)
	}

	// 等待后台重写完成（通过 Actor 内部状态轮询，避免数据竞争）
	deadline := time.Now().Add(10 * time.Second)
	for {
		req := &commandRequest{
			fn: func() resp.Reply {
				if db1.aofRewriting {
					return resp.MakeIntReply(1)
				}
				return resp.MakeIntReply(0)
			},
			result: make(chan resp.Reply, 1),
			noAof:  true,
		}
		db1.ops <- req
		ir := (<-req.result).(*resp.IntReply)
		if ir.Code == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for BGREWRITEAOF finish")
		}
		time.Sleep(50 * time.Millisecond)
	}

	if err := db1.aofHandler.Flush(); err != nil {
		t.Fatalf("flush after: %v", err)
	}
	db1.Close()

	// Load 验证：during 与最终 k1=v3 必须存在
	db2 := NewStandaloneDBWithConfig(StandaloneDBConfig{
		AofFilename: aofFile,
		RdbFilename: "",
		MaxBytes:    DefaultMaxBytes,
		Eviction:    "lru",
	})
	defer db2.Close()
	db2.Load()

	gr, ok := db2.Exec([][]byte{[]byte("GET"), []byte("during")}).(*resp.BulkReply)
	if !ok || gr.Arg == nil || string(gr.Arg) != "1" {
		t.Fatalf("GET during mismatch: %#v", gr)
	}
	k1, ok := db2.Exec([][]byte{[]byte("GET"), []byte("k1")}).(*resp.BulkReply)
	if !ok || k1.Arg == nil || string(k1.Arg) != "v3" {
		t.Fatalf("GET k1 mismatch: %#v", k1)
	}
}
