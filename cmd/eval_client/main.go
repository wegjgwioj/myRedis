// eval_client：评估/验收用的内置客户端（不依赖 redis-cli）。
// 用途：被 scripts/eval.ps1 调用，执行单条命令或预置验收场景并输出 JSON 结果。
// 输出：stdout 为结构化 JSON，便于脚本落盘与生成报告。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"myredis/cluster"
	"myredis/resp"
	"net"
	"os"
	"strings"
	"time"
)

// 本工具用于 scripts/eval.ps1 的自动化评估（不依赖 redis-cli）：
// 1) 单条命令模式：发送一个 RESP 命令并输出 JSON reply
//    - 示例：myredis-eval-client.exe --addr 127.0.0.1:6399 SET k v
// 2) 场景模式：执行一组验收步骤并输出 JSON 报告
//    - 示例：myredis-eval-client.exe --addr 127.0.0.1:6399 --nodes ... --scenario distributed

type replyJSON struct {
	Type   string        `json:"type"`
	Value  interface{}   `json:"value,omitempty"`
	Null   bool          `json:"null,omitempty"`
	Values []interface{} `json:"values,omitempty"`
}

type scenarioReport struct {
	Scenario string `json:"scenario"`
	Ok       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:6399", "server address, e.g. 127.0.0.1:6399")
	nodes := flag.String("nodes", "", "cluster nodes, comma-separated")
	vnodes := flag.Int("vnodes", 160, "virtual nodes for consistent hashing")
	scenario := flag.String("scenario", "", "scenario name: distributed")
	flag.Parse()

	if *scenario != "" {
		switch strings.ToLower(strings.TrimSpace(*scenario)) {
		case "distributed":
			runDistributedScenario(*addr, *nodes, *vnodes)
		default:
			writeJSON(scenarioReport{Scenario: *scenario, Ok: false, Error: "unknown scenario"})
			os.Exit(2)
		}
		return
	}

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: myredis-eval-client --addr 127.0.0.1:6399 <CMD> [ARGS...]")
		os.Exit(2)
	}

	r, err := doOnce(*addr, args)
	if err != nil {
		writeJSON(replyJSON{Type: "client_error", Value: err.Error()})
		os.Exit(2)
	}

	out := encodeReply(r)
	writeJSON(out)
	if out.Type == "error" {
		os.Exit(1)
	}
}

func doOnce(addr string, args []string) (resp.Reply, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	cmd := make([][]byte, 0, len(args))
	for _, a := range args {
		cmd = append(cmd, []byte(a))
	}
	_, err = conn.Write(resp.MakeMultiBulkReply(cmd).ToBytes())
	if err != nil {
		return nil, err
	}

	parser := resp.NewStreamParser(conn)
	return parser.ReadReply()
}

func encodeReply(r resp.Reply) replyJSON {
	switch v := r.(type) {
	case *resp.StatusReply:
		return replyJSON{Type: "status", Value: v.Status}
	case *resp.ErrorReply:
		return replyJSON{Type: "error", Value: v.Status}
	case *resp.IntReply:
		return replyJSON{Type: "int", Value: v.Code}
	case *resp.BulkReply:
		if v.Arg == nil {
			return replyJSON{Type: "bulk", Null: true}
		}
		return replyJSON{Type: "bulk", Value: string(v.Arg)}
	case *resp.MultiBulkReply:
		values := make([]interface{}, 0, len(v.Args))
		for _, a := range v.Args {
			if a == nil {
				values = append(values, nil)
			} else {
				values = append(values, string(a))
			}
		}
		return replyJSON{Type: "array", Values: values}
	default:
		return replyJSON{Type: "unknown", Value: fmt.Sprintf("%T", r)}
	}
}

func writeJSON(v interface{}) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func runDistributedScenario(entryAddr string, nodesStr string, vnodes int) {
	nodes := parseNodes(nodesStr)
	if len(nodes) == 0 {
		writeJSON(scenarioReport{Scenario: "distributed", Ok: false, Error: "--nodes is required"})
		os.Exit(2)
	}

	ring := cluster.NewRing(nodes, vnodes)
	keysByNode, ok := pickKeysByNode(ring, nodes)
	if !ok {
		writeJSON(scenarioReport{Scenario: "distributed", Ok: false, Error: "failed to pick keys for all nodes"})
		os.Exit(2)
	}

	c, err := newClient(entryAddr)
	if err != nil {
		writeJSON(scenarioReport{Scenario: "distributed", Ok: false, Error: err.Error()})
		os.Exit(2)
	}
	defer c.Close()

	// SET/GET：透明转发验证
	for _, node := range nodes {
		key := keysByNode[node]
		val := "v-" + node
		if !expectStatus(c, "SET", key, val) {
			return
		}
		if !expectBulk(c, "GET", key, val) {
			return
		}
	}

	// DEL：跨节点聚合
	k1 := keysByNode[nodes[0]]
	k2 := keysByNode[nodes[1]]
	k3 := keysByNode[nodes[2]]
	if !expectInt(c, int64(len(nodes)), "DEL", k1, k2, k3) {
		return
	}

	// TTL：EXPIRE + 过期后 GET 应为 nil
	testKey := "ttl-key"
	if !expectStatus(c, "SET", testKey, "v") {
		return
	}
	if !expectInt(c, 1, "EXPIRE", testKey, "2") {
		return
	}
	// TTL 可能返回 1 或 0（剩余 < 1s）
	if !expectIntGE(c, 0, "TTL", testKey) {
		return
	}
	time.Sleep(2100 * time.Millisecond)
	if !expectNullBulk(c, "GET", testKey) {
		return
	}

	writeJSON(scenarioReport{Scenario: "distributed", Ok: true})
}

type client struct {
	conn   net.Conn
	parser *resp.StreamParser
}

func newClient(addr string) (*client, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	return &client{conn: conn, parser: resp.NewStreamParser(conn)}, nil
}

func (c *client) Close() { _ = c.conn.Close() }

func (c *client) Do(args ...string) (resp.Reply, error) {
	cmd := make([][]byte, 0, len(args))
	for _, a := range args {
		cmd = append(cmd, []byte(a))
	}
	_, err := c.conn.Write(resp.MakeMultiBulkReply(cmd).ToBytes())
	if err != nil {
		return nil, err
	}
	return c.parser.ReadReply()
}

func expectStatus(c *client, cmd string, args ...string) bool {
	all := append([]string{cmd}, args...)
	r, err := c.Do(all...)
	if err != nil {
		writeJSON(scenarioReport{Scenario: "distributed", Ok: false, Error: err.Error()})
		os.Exit(2)
	}
	if _, ok := r.(*resp.StatusReply); !ok {
		writeJSON(scenarioReport{Scenario: "distributed", Ok: false, Error: "expected status reply"})
		os.Exit(1)
	}
	return true
}

func expectBulk(c *client, cmd string, key string, want string) bool {
	r, err := c.Do(cmd, key)
	if err != nil {
		writeJSON(scenarioReport{Scenario: "distributed", Ok: false, Error: err.Error()})
		os.Exit(2)
	}
	br, ok := r.(*resp.BulkReply)
	if !ok || string(br.Arg) != want {
		writeJSON(scenarioReport{Scenario: "distributed", Ok: false, Error: "unexpected bulk reply"})
		os.Exit(1)
	}
	return true
}

func expectNullBulk(c *client, cmd string, key string) bool {
	r, err := c.Do(cmd, key)
	if err != nil {
		writeJSON(scenarioReport{Scenario: "distributed", Ok: false, Error: err.Error()})
		os.Exit(2)
	}
	br, ok := r.(*resp.BulkReply)
	if !ok || br.Arg != nil {
		writeJSON(scenarioReport{Scenario: "distributed", Ok: false, Error: "expected null bulk"})
		os.Exit(1)
	}
	return true
}

func expectInt(c *client, want int64, args ...string) bool {
	r, err := c.Do(args...)
	if err != nil {
		writeJSON(scenarioReport{Scenario: "distributed", Ok: false, Error: err.Error()})
		os.Exit(2)
	}
	ir, ok := r.(*resp.IntReply)
	if !ok || ir.Code != want {
		writeJSON(scenarioReport{Scenario: "distributed", Ok: false, Error: "unexpected int reply"})
		os.Exit(1)
	}
	return true
}

func expectIntGE(c *client, min int64, args ...string) bool {
	r, err := c.Do(args...)
	if err != nil {
		writeJSON(scenarioReport{Scenario: "distributed", Ok: false, Error: err.Error()})
		os.Exit(2)
	}
	ir, ok := r.(*resp.IntReply)
	if !ok || ir.Code < min {
		writeJSON(scenarioReport{Scenario: "distributed", Ok: false, Error: "unexpected int reply"})
		os.Exit(1)
	}
	return true
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

func pickKeysByNode(ring *cluster.Ring, nodes []string) (map[string]string, bool) {
	out := make(map[string]string, len(nodes))
	for i := 0; i < 100000 && len(out) < len(nodes); i++ {
		k := fmt.Sprintf("key-%d", i)
		n := ring.NodeForKey(k)
		if n == "" {
			continue
		}
		if _, ok := out[n]; !ok {
			out[n] = k
		}
	}
	return out, len(out) == len(nodes)
}
