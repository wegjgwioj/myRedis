// Cluster Router：分布式路由器（对外实现 db.DB）。
// 关键点：按 key 分片路由，本地执行或转发到目标节点；对 DEL 做跨节点分组与结果聚合。
// 限制：当前不支持动态拓扑变更，也不支持 Redis Cluster 协议（MOVED/ASK 等）。
package cluster

import (
	"myredis/db"
	"myredis/resp"
	"strings"
	"sync"
)

// 本文件实现分布式路由器（Router）：
// - 对外表现为一个 db.DB（Exec/Load/Close）
// - 内部根据 key 的一致性哈希结果选择：本地执行 or 转发到目标节点
//
// 当前支持的路由规则：
// - 单 key 命令：默认 key 在 args[1]
// - 多 key 命令：DEL，会按 key 分组并聚合返回值

type Router struct {
	localAddr string
	localDB   db.DB
	ring      *Ring

	peersMu sync.RWMutex
	peers   map[string]*PeerClient // addr -> client
}

func NewRouter(localAddr string, localDB db.DB, nodes []string, vnodes int) *Router {
	r := &Router{
		localAddr: localAddr,
		localDB:   localDB,
		ring:      NewRing(nodes, vnodes),
		peers:     make(map[string]*PeerClient),
	}
	for _, n := range nodes {
		if n == "" || n == localAddr {
			continue
		}
		r.peers[n] = NewPeerClient(n, 4)
	}
	return r
}

func (r *Router) Load() {
	// 每个节点都有自己的 AOF；Router 只负责本地加载
	r.localDB.Load()
}

func (r *Router) Close() {
	r.peersMu.Lock()
	for _, c := range r.peers {
		c.Close()
	}
	r.peers = make(map[string]*PeerClient)
	r.peersMu.Unlock()
	r.localDB.Close()
}

func (r *Router) Exec(cmd [][]byte) resp.Reply {
	if len(cmd) == 0 {
		return resp.MakeErrReply("ERR empty command")
	}
	name := strings.ToLower(string(cmd[0]))

	// 无 key 的命令直接本地执行
	if name == "ping" {
		return r.localDB.Exec(cmd)
	}

	// 多 key：DEL 需要分组到各节点并聚合删除数量
	if name == "del" {
		return r.execDel(cmd)
	}

	// 单 key 默认在 args[1]
	if len(cmd) < 2 {
		return r.localDB.Exec(cmd)
	}
	key := string(cmd[1])
	target := r.ring.NodeForKey(key)
	if target == "" || target == r.localAddr {
		return r.localDB.Exec(cmd)
	}

	reply, err := r.peerDo(target, cmd)
	if err != nil {
		return resp.MakeErrReply("ERR cluster forward failed: " + err.Error())
	}
	return reply
}

func (r *Router) execDel(cmd [][]byte) resp.Reply {
	if len(cmd) < 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'del' command")
	}

	// node -> keys
	groups := make(map[string][][]byte)
	for i := 1; i < len(cmd); i++ {
		kb := cmd[i]
		node := r.ring.NodeForKey(string(kb))
		if node == "" {
			node = r.localAddr
		}
		groups[node] = append(groups[node], kb)
	}

	type delResult struct {
		count int64
		err   resp.Reply
	}

	var wg sync.WaitGroup
	results := make(chan delResult, len(groups))

	for node, keys := range groups {
		node := node
		keys := keys
		wg.Add(1)
		go func() {
			defer wg.Done()
			subCmd := make([][]byte, 0, len(keys)+1)
			subCmd = append(subCmd, []byte("DEL"))
			subCmd = append(subCmd, keys...)

			var reply resp.Reply
			if node == r.localAddr {
				reply = r.localDB.Exec(subCmd)
			} else {
				rep, err := r.peerDo(node, subCmd)
				if err != nil {
					results <- delResult{err: resp.MakeErrReply("ERR cluster forward failed: " + err.Error())}
					return
				}
				reply = rep
			}

			if er, ok := reply.(*resp.ErrorReply); ok {
				results <- delResult{err: er}
				return
			}
			intReply, ok := reply.(*resp.IntReply)
			if !ok {
				results <- delResult{err: resp.MakeErrReply("ERR cluster: DEL unexpected reply")}
				return
			}
			results <- delResult{count: intReply.Code}
		}()
	}

	wg.Wait()
	close(results)

	var total int64
	for res := range results {
		if res.err != nil {
			return res.err
		}
		total += res.count
	}
	return resp.MakeIntReply(total)
}

func (r *Router) peerDo(addr string, cmd [][]byte) (resp.Reply, error) {
	r.peersMu.RLock()
	c := r.peers[addr]
	r.peersMu.RUnlock()
	if c == nil {
		r.peersMu.Lock()
		// double check
		c = r.peers[addr]
		if c == nil {
			c = NewPeerClient(addr, 4)
			r.peers[addr] = c
		}
		r.peersMu.Unlock()
	}
	return c.Do(cmd)
}
