// myredis-server 入口：解析 CLI 参数并启动 TCP Server。
// 支持：单机模式 / 3 节点静态分片+透明转发 / AOF everysec / LRU|LFU 淘汰 / 优雅关闭。
// 说明：为控范围与对齐描述，--appendfsync 目前只支持 everysec。
package main

import (
	"context"
	"flag"
	"log"
	"myredis/cluster"
	"myredis/db"
	"myredis/server"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	// 对齐图片描述的可配置入口：
	// - 支持分布式 nodes（透明转发）
	// - 支持 LRU/LFU 淘汰策略切换
	// - 支持 AOF EverySec（可关闭）
	addr := flag.String("addr", ":6399", "listen address, e.g. 127.0.0.1:6399")
	nodes := flag.String("nodes", "", "cluster nodes, comma-separated, e.g. 127.0.0.1:6399,127.0.0.1:6400,127.0.0.1:6401")
	aofFile := flag.String("aof", "", "aof filename (empty to disable), e.g. artifacts/aof/node-6399.aof")
	rdbFile := flag.String("rdb", "", "rdb snapshot filename (empty to disable), e.g. artifacts/rdb/node-6399.rdb")
	appendfsync := flag.String("appendfsync", "everysec", "AOF fsync policy (only everysec is supported)")
	eviction := flag.String("eviction", "lru", "eviction policy: lru|lfu")
	maxBytes := flag.Int64("max-bytes", db.DefaultMaxBytes, "max memory in bytes for eviction")
	vnodes := flag.Int("vnodes", 160, "virtual nodes for consistent hashing")
	flag.Parse()

	if strings.ToLower(strings.TrimSpace(*appendfsync)) != "everysec" {
		log.Fatal("only --appendfsync=everysec is supported")
	}

	localDB := db.NewStandaloneDBWithConfig(db.StandaloneDBConfig{
		AofFilename: *aofFile,
		RdbFilename: *rdbFile,
		MaxBytes:    *maxBytes,
		Eviction:    *eviction,
	})

	var database db.DB = localDB
	nodeList := parseNodes(*nodes)
	if len(nodeList) > 0 {
		if !containsNode(nodeList, *addr) {
			log.Fatal("--addr must be included in --nodes when cluster mode enabled")
		}
		database = cluster.NewRouter(*addr, localDB, nodeList, *vnodes)
	}

	// Load AOF (Persistence)
	database.Load()

	// Initialize Server
	s := server.NewServer(*addr, database)

	// Ctrl+C / SIGTERM 优雅关闭
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(shutdownCtx)
	}()

	// Start
	if err := s.Start(); err != nil {
		log.Fatal(err)
	}
}

func parseNodes(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func containsNode(nodes []string, addr string) bool {
	for _, n := range nodes {
		if n == addr {
			return true
		}
	}
	return false
}
