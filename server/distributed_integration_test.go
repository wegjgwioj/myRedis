// 分布式集成测试：启动 3 节点并验证“按 key 分片 + 透明转发 + DEL 聚合”。
// 目标：保证对齐图片描述中的分布式 KV 最小闭环能力。
// 说明：这里用 freeAddr 动态端口避免端口冲突，确保 go test 可重复执行。
package server

import (
	"context"
	"fmt"
	"myredis/cluster"
	"myredis/db"
	"myredis/resp"
	"net"
	"testing"
	"time"
)

// 本文件验证“分布式 KV（多节点分片 + 透明转发）”的最小可用能力：
// - 启动 3 个节点（本机不同端口）
// - 连接任意一个节点即可对所有 key 做 SET/GET（自动转发）
// - DEL 多 key 能跨节点聚合返回值

func TestDistributed_3Nodes_TransparentForward(t *testing.T) {
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	ring := cluster.NewRing(addrs, 160)

	servers := make([]*Server, 0, 3)
	for _, addr := range addrs {
		localDB := db.NewStandaloneDBWithConfig(db.StandaloneDBConfig{
			AofFilename: "",
			MaxBytes:    db.DefaultMaxBytes,
			Eviction:    "lru",
		})
		router := cluster.NewRouter(addr, localDB, addrs, 160)
		srv := NewServer(addr, router)
		servers = append(servers, srv)

		go func(s *Server) {
			_ = s.Start()
		}(srv)
	}

	for _, addr := range addrs {
		if err := waitForListen(addr, 2*time.Second); err != nil {
			t.Fatalf("server not ready %s: %v", addr, err)
		}
	}

	t.Cleanup(func() {
		for _, s := range servers {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = s.Shutdown(ctx)
			cancel()
		}
	})

	// 找到分别落在三个节点上的 key
	keysByNode := pickKeysByNode(t, ring, addrs)

	// 连接第一个节点作为入口
	conn, err := net.Dial("tcp", addrs[0])
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.Close()
	parser := resp.NewStreamParser(conn)

	// helper：发送命令并读取一个 reply
	do := func(args ...string) resp.Reply {
		cmd := make([][]byte, 0, len(args))
		for _, a := range args {
			cmd = append(cmd, []byte(a))
		}
		_, _ = conn.Write(resp.MakeMultiBulkReply(cmd).ToBytes())
		r, e := parser.ReadReply()
		if e != nil {
			t.Fatalf("read reply error: %v", e)
		}
		return r
	}

	// 逐个节点的 key 做 SET/GET（其中 2 个必然触发转发）
	for node, key := range keysByNode {
		val := "v-" + node
		if r := do("SET", key, val); r == nil {
			t.Fatalf("SET reply nil")
		}
		r := do("GET", key)
		br, ok := r.(*resp.BulkReply)
		if !ok || string(br.Arg) != val {
			t.Fatalf("GET %s expected %q, got %T %+v", key, val, r, r)
		}
	}

	// DEL 多 key 跨节点聚合
	k1 := keysByNode[addrs[0]]
	k2 := keysByNode[addrs[1]]
	k3 := keysByNode[addrs[2]]
	rDel := do("DEL", k1, k2, k3)
	ir, ok := rDel.(*resp.IntReply)
	if !ok || ir.Code != 3 {
		t.Fatalf("DEL expected 3, got %T %+v", rDel, rDel)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen :0 error: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func waitForListen(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timeout")
}

func pickKeysByNode(t *testing.T, ring *cluster.Ring, addrs []string) map[string]string {
	t.Helper()
	out := make(map[string]string, len(addrs))
	for i := 0; i < 100000 && len(out) < len(addrs); i++ {
		k := fmt.Sprintf("key-%d", i)
		node := ring.NodeForKey(k)
		if node == "" {
			continue
		}
		if _, ok := out[node]; !ok {
			out[node] = k
		}
	}
	if len(out) != len(addrs) {
		t.Fatalf("failed to pick keys for all nodes, got %v", out)
	}
	return out
}
