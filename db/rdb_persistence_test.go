// RDB 持久化测试：验证 SAVE 写出快照、Load 启动加载。
// 重点：TTL 使用绝对时间（UnixMilli）写入快照，保证“重启不续命”。
// 说明：本项目 RDB 为自定义格式，仅覆盖当前支持的数据类型。
package db

import (
	"myredis/resp"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestRDB_SaveAndLoad_WithAbsoluteTTL(t *testing.T) {
	dir := t.TempDir()
	rdbFile := filepath.Join(dir, "node.rdb")

	db1 := NewStandaloneDBWithConfig(StandaloneDBConfig{
		AofFilename: "",
		RdbFilename: rdbFile,
		MaxBytes:    DefaultMaxBytes,
		Eviction:    "lru",
	})
	defer db1.Close()

	mustStatus := func(r resp.Reply) {
		if _, ok := r.(*resp.StatusReply); !ok {
			t.Fatalf("expected status, got %T", r)
		}
	}
	mustInt := func(r resp.Reply) int64 {
		ir, ok := r.(*resp.IntReply)
		if !ok {
			t.Fatalf("expected int, got %T", r)
		}
		return ir.Code
	}
	mustBulk := func(r resp.Reply) []byte {
		br, ok := r.(*resp.BulkReply)
		if !ok {
			t.Fatalf("expected bulk, got %T", r)
		}
		if br.Arg == nil {
			t.Fatalf("expected non-nil bulk")
		}
		return br.Arg
	}

	// 基础数据
	mustStatus(db1.Exec([][]byte{[]byte("SET"), []byte("k1"), []byte("v1")}))                                         // string
	mustInt(db1.Exec([][]byte{[]byte("LPUSH"), []byte("l1"), []byte("a"), []byte("b"), []byte("c")}))                 // list
	mustInt(db1.Exec([][]byte{[]byte("HSET"), []byte("h1"), []byte("f1"), []byte("v1"), []byte("f2"), []byte("v2")})) // hash
	mustInt(db1.Exec([][]byte{[]byte("SADD"), []byte("s1"), []byte("m1"), []byte("m2"), []byte("m3")}))               // set

	// TTL：一个会在重启前过期，一个重启后仍存活但 TTL 变小（不续命）
	mustStatus(db1.Exec([][]byte{[]byte("SET"), []byte("expireKey"), []byte("v")})) // will expire
	mustStatus(db1.Exec([][]byte{[]byte("SET"), []byte("liveKey"), []byte("v")}))   // should survive

	nowMs := time.Now().UnixMilli()
	expireAtExpired := strconv.FormatInt(nowMs+500, 10)
	expireAtLive := strconv.FormatInt(nowMs+3000, 10)
	mustInt(db1.Exec([][]byte{[]byte("PEXPIREAT"), []byte("expireKey"), []byte(expireAtExpired)}))
	mustInt(db1.Exec([][]byte{[]byte("PEXPIREAT"), []byte("liveKey"), []byte(expireAtLive)}))

	// 同步 SAVE 写入 RDB
	mustStatus(db1.Exec([][]byte{[]byte("SAVE")}))

	db1.Close()

	// 等待 expireKey 过期，同时让 liveKey 的剩余 TTL 下降
	time.Sleep(1200 * time.Millisecond)

	// 新实例加载 RDB
	db2 := NewStandaloneDBWithConfig(StandaloneDBConfig{
		AofFilename: "",
		RdbFilename: rdbFile,
		MaxBytes:    DefaultMaxBytes,
		Eviction:    "lru",
	})
	defer db2.Close()
	db2.Load()

	// string
	if got := string(mustBulk(db2.Exec([][]byte{[]byte("GET"), []byte("k1")}))); got != "v1" {
		t.Fatalf("GET k1 = %q", got)
	}

	// list（LPUSH a b c => list = [c b a]）
	lr := db2.Exec([][]byte{[]byte("LRANGE"), []byte("l1"), []byte("0"), []byte("-1")})
	mb, ok := lr.(*resp.MultiBulkReply)
	if !ok {
		t.Fatalf("expected array, got %T", lr)
	}
	gotList := make([]string, 0, len(mb.Args))
	for _, a := range mb.Args {
		gotList = append(gotList, string(a))
	}
	wantList := []string{"c", "b", "a"}
	if len(gotList) != len(wantList) {
		t.Fatalf("LRANGE len = %d", len(gotList))
	}
	for i := range wantList {
		if gotList[i] != wantList[i] {
			t.Fatalf("LRANGE[%d] = %q", i, gotList[i])
		}
	}

	// hash：HGETALL 转 map 比较（顺序不保证）
	ha := db2.Exec([][]byte{[]byte("HGETALL"), []byte("h1")})
	hm, ok := ha.(*resp.MultiBulkReply)
	if !ok {
		t.Fatalf("expected array, got %T", ha)
	}
	gotHash := make(map[string]string)
	for i := 0; i+1 < len(hm.Args); i += 2 {
		gotHash[string(hm.Args[i])] = string(hm.Args[i+1])
	}
	if gotHash["f1"] != "v1" || gotHash["f2"] != "v2" {
		t.Fatalf("HGETALL mismatch: %#v", gotHash)
	}

	// set：SMEMBERS 结果无序，排序后比较
	sa := db2.Exec([][]byte{[]byte("SMEMBERS"), []byte("s1")})
	sm, ok := sa.(*resp.MultiBulkReply)
	if !ok {
		t.Fatalf("expected array, got %T", sa)
	}
	gotSet := make([]string, 0, len(sm.Args))
	for _, a := range sm.Args {
		gotSet = append(gotSet, string(a))
	}
	sort.Strings(gotSet)
	wantSet := []string{"m1", "m2", "m3"}
	for i := range wantSet {
		if gotSet[i] != wantSet[i] {
			t.Fatalf("SMEMBERS mismatch: %v", gotSet)
		}
	}

	// expireKey：应当已过期（不应被加载/续命）
	er := db2.Exec([][]byte{[]byte("GET"), []byte("expireKey")})
	if br, ok := er.(*resp.BulkReply); !ok || br.Arg != nil {
		t.Fatalf("expireKey expected nil bulk, got %T %#v", er, er)
	}

	// liveKey：仍存在，但 TTL 必须显著变小（<=2），证明不是“重启重新给 3 秒”
	if got := string(mustBulk(db2.Exec([][]byte{[]byte("GET"), []byte("liveKey")}))); got != "v" {
		t.Fatalf("GET liveKey = %q", got)
	}
	ttl := mustInt(db2.Exec([][]byte{[]byte("TTL"), []byte("liveKey")}))
	if ttl < 0 || ttl > 2 {
		t.Fatalf("TTL liveKey = %d (expected 0..2)", ttl)
	}

	// rdb 文件应存在且非空
	if st, err := os.Stat(rdbFile); err != nil || st.Size() == 0 {
		t.Fatalf("rdb file invalid: %v size=%d", err, st.Size())
	}
}
