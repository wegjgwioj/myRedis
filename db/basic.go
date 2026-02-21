// DB 基础命令实现：包含 PING/GET/SET/DEL 等字符串与键空间常用命令。
// 说明：所有命令由 db.DB 的 Actor 协程串行执行，避免并发读写 map 的锁竞争与数据竞争。
// 关键点：写命令需要追加 AOF；删除/覆盖需要同步维护 TTL 与淘汰统计。
package db

import (
	"myredis/resp"
	"time"
)

// 本文件实现 String 相关命令：SET / GET / DEL
// 说明：
// - 数据存储在可插拔 cache（LRU/LFU）中
// - TTL 由 db.ttlMap 管理（惰性删除 + 定期删除）

// SET key value
func (db *StandaloneDB) set(args [][]byte) resp.Reply {
	if len(args) != 3 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'set' command")
	}
	key := string(args[1])
	val := args[2]

	// Store as StringData (implements Len())
	db.cache.Add(key, StringData(val), 0)

	// DEL ttl if exists (SET removes expire)
	// Redis behavior: SET key val invokes "Remove expire".
	delete(db.ttlMap, key)

	return resp.OkReply
}

// GET key
func (db *StandaloneDB) get(args [][]byte) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'get' command")
	}
	key := string(args[1])

	// Use helper for Lazy Expiration + Type Check
	// But getEntity returns DataEntity interface.
	// We need to assert type.

	// 1. Get from cache (updates LRU)
	val, ok := db.cache.Get(key)
	if !ok {
		return resp.NullBulkReply
	}

	// 2. Check TTL (Manual check here or via helper? Helper `getEntity` does check)
	// But `getEntity` is in `ttl.go` (package `db`). Accessible.
	// But `getEntity` returns `DataEntity`.
	// `db.cache.Get` returns `lru.Value` (interface{ Len() int }).

	// Check TTL manually to avoid double lookup if helper does Get again?
	// Helper `getEntity` does `db.cache.Get`.

	// Let's use `getEntity` or replicate logic?
	// Replicating logic is safer to avoid interface casting confusion.

	if expireTime, ok := db.ttlMap[key]; ok {
		if time.Now().After(expireTime) {
			db.cache.Remove(key)
			return resp.NullBulkReply
		}
	}

	// 3. Type Assertion
	str, ok := val.(StringData)
	if !ok {
		return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return resp.MakeBulkReply([]byte(str))
}

// DEL k1 k2 ...
func (db *StandaloneDB) del(args [][]byte) resp.Reply {
	if len(args) < 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'del' command")
	}

	deleted := 0
	for i := 1; i < len(args); i++ {
		key := string(args[i])
		// DEL 不需要更新 LRU/LFU 统计，因此使用 Peek
		if _, ok := db.cache.Peek(key); ok {
			// Remove from cache (OnEvicted removes from ttlMap)
			db.cache.Remove(key)
			deleted++
		}
	}

	return resp.MakeIntReply(int64(deleted))
}
