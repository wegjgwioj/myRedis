// TTL/过期管理：实现 EXPIRE/TTL/PERSIST/PEXPIREAT 以及惰性+定期删除。
// 关键点：AOF 使用 PEXPIREAT（绝对过期时间）记录，避免重启“续命”；TTL 查询使用 Peek 不污染淘汰统计。
// 线程模型：ttlMap 由 Actor 串行维护；定期删除在后台按批扫描，避免长时间阻塞。
package db

import (
	"myredis/resp"
	"strconv"
	"time"
)

// 本文件实现 TTL 相关命令：
// - EXPIRE：相对秒（对外命令）
// - TTL：查询剩余秒数（不应影响 LRU/LFU 统计，因此用 cache.Peek）
// - PERSIST：取消过期
// - PEXPIREAT：绝对毫秒时间戳（主要用于 AOF 重放，避免重启续命）

// --- TTL Commands ---

// EXPIRE key seconds
func (db *StandaloneDB) expire(args [][]byte) resp.Reply {
	if len(args) != 3 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'expire' command")
	}
	key := string(args[1])
	seconds, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer or out of range")
	}

	// Check if key exists
	if _, ok := db.cache.Peek(key); !ok {
		return resp.MakeIntReply(0)
	}

	// Redis: seconds <= 0 相当于立即过期（删除 key）
	if seconds <= 0 {
		db.cache.Remove(key)
		return resp.MakeIntReply(1)
	}

	// Set TTL
	db.ttlMap[key] = time.Now().Add(time.Duration(seconds) * time.Second)

	return resp.MakeIntReply(1)
}

// PEXPIREAT key unix_ms
// 说明：对齐 Redis AOF 的绝对过期时间语义，用于重放时保持一致性。
func (db *StandaloneDB) pexpireat(args [][]byte) resp.Reply {
	if len(args) != 3 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'pexpireat' command")
	}
	key := string(args[1])
	ms, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer or out of range")
	}

	// key 不存在则返回 0（与 Redis 行为一致）
	if _, ok := db.cache.Peek(key); !ok {
		return resp.MakeIntReply(0)
	}

	expireAt := time.UnixMilli(ms)
	if !expireAt.After(time.Now()) {
		// 已经过期，直接删除
		db.cache.Remove(key)
		return resp.MakeIntReply(1)
	}

	db.ttlMap[key] = expireAt
	return resp.MakeIntReply(1)
}

// TTL key
func (db *StandaloneDB) ttl(args [][]byte) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'ttl' command")
	}
	key := string(args[1])

	// TTL 不应影响 LRU/LFU 统计，因此使用 Peek 而不是 Get
	if _, ok := db.cache.Peek(key); !ok {
		return resp.MakeIntReply(-2) // Key not exists
	}

	// Check ttlMap
	expireTime, ok := db.ttlMap[key]
	if !ok {
		return resp.MakeIntReply(-1) // No expire
	}

	ttl := time.Until(expireTime)
	if ttl <= 0 {
		// 已过期但尚未被访问触发惰性删除：这里直接删除并返回 -2
		db.cache.Remove(key)
		return resp.MakeIntReply(-2)
	}

	return resp.MakeIntReply(int64(ttl.Seconds()))
}

// PERSIST key
func (db *StandaloneDB) persist(args [][]byte) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'persist' command")
	}
	key := string(args[1])

	// PERSIST 不应影响 LRU/LFU 统计，因此使用 Peek
	if _, ok := db.cache.Peek(key); !ok {
		return resp.MakeIntReply(0)
	}

	if _, ok := db.ttlMap[key]; !ok {
		return resp.MakeIntReply(0)
	}

	delete(db.ttlMap, key)
	return resp.MakeIntReply(1)
}

// --- Helper for Lazy Expiration ---

func (db *StandaloneDB) getEntity(key string) (DataEntity, bool) {
	// 1. Get from cache
	val, ok := db.cache.Get(key)
	if !ok {
		return nil, false
	}

	// 2. Check TTL
	if expireTime, ok := db.ttlMap[key]; ok {
		if time.Now().After(expireTime) {
			db.cache.Remove(key) // Removes from both cache and ttlMap
			// AOF 使用 PEXPIREAT 记录绝对时间，重启时不会“续命”。
			return nil, false
		}
	}

	// 3. Type assertion
	entity, ok := val.(DataEntity)
	if !ok {
		return nil, false // Should happen only if data corruption
	}
	return entity, true
}
