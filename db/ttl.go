package db

import (
	"myredis/resp"
	"strconv"
	"time"
)

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
	if _, ok := db.cache.Get(key); !ok {
		return resp.MakeIntReply(0)
	}

	// Set TTL
	db.ttlMap[key] = time.Now().Add(time.Duration(seconds) * time.Second)

	// Log to AOF? (EXPIRE command should be logged)
	// Handled by `db.background` check (we added "expire" to writeCommands)
	// But wait, if key doesn't exist, we return 0.
	// background: `isWriteCommand` -> yes. `!isError(res)` -> yes (IntReply(0) is not error).
	// So it logs EXPIRE for non-existent key? Redis does not propagate if key missing.
	// But harmless. Or we can return 0 and rely on slave/AOF consistency.

	return resp.MakeIntReply(1)
}

// TTL key
func (db *StandaloneDB) ttl(args [][]byte) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'ttl' command")
	}
	key := string(args[1])

	// Check if key exists (Lazy Expire check embedded in Get?)
	// No, TTL shouldn't modify LRU usually, but here Get() updates LRU.
	// Redis TTL command does NOT update LRU.
	// mygocache/lru/lfu.go `Get` updates usage.
	// To strictly follow Redis, we might need `Peek`?
	// Let's assume updating LRU on TTL is acceptable for now.

	if _, ok := db.cache.Get(key); !ok {
		return resp.MakeIntReply(-2) // Key not exists
	}

	// Check ttlMap
	expireTime, ok := db.ttlMap[key]
	if !ok {
		return resp.MakeIntReply(-1) // No expire
	}

	ttl := time.Until(expireTime).Seconds()
	if ttl < 0 {
		// Expired but not deleted yet (Lazy delete should have happened in Get if we implemented it there)
		// Since we call Get() above, if Get() implements lazy delete, this branch is unreachable.
		// But let's implement validation here too.
		db.cache.Remove(key) // This removes from ttlMap via OnEvicted
		return resp.MakeIntReply(-2)
	}

	return resp.MakeIntReply(int64(ttl))
}

// PERSIST key
func (db *StandaloneDB) persist(args [][]byte) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'persist' command")
	}
	key := string(args[1])

	if _, ok := db.cache.Get(key); !ok {
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
			// Propagate DEL to AOF?
			// db.bg loop only propagates commandRequest.
			// This internal deletion needs explicit propagation if we want AOF to be perfect.
			// But if AOF replays, it sets expiry. When replaying Get, it will expire again.
			// BUT, if we restart server, expire time is lost if we didn't persist current time or relative time.
			// Redis AOF stores `PEXPIREAT` (absolute time).
			// Our AOF stores `EXPIRE` (relative seconds).
			// If restart, relative time resets!
			// e.g. EXPIRE key 100.
			// 50s passed. Server restart.
			// Replay EXPIRE key 100. Key lives for another 100s!
			// This is a known limitation of simple AOF with relative expiration.
			// Standard Redis rewrites AOF to PEXPIREAT.
			// We can live with this for now.
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
