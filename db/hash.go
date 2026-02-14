package db

import (
	"myredis/resp"
	"time"
)

func (db *StandaloneDB) getHash(key string) (HashData, bool) {
	val, ok := db.cache.Get(key)
	if !ok {
		return nil, false
	}

	if expireTime, ok := db.ttlMap[key]; ok {
		if time.Now().After(expireTime) {
			db.cache.Remove(key)
			if db.aofHandler != nil {
				db.aofHandler.AddAof([][]byte{[]byte("del"), []byte(key)})
			}
			return nil, false
		}
	}

	h, ok := val.(HashData)
	return h, ok
}

// HSET key field value
func (db *StandaloneDB) hset(args [][]byte) resp.Reply {
	if len(args) < 4 || len(args)%2 != 0 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'hset' command")
	}
	key := string(args[1])

	h, ok := db.getHash(key)
	if !ok {
		// Create new
		h = make(HashData)
		// Check if WRONGTYPE happened inside getHash?
		// getHash returns (nil, false) if not found OR expired.
		// It doesn't distinguish "not found" vs "wrong type".
		// Wait, cache.Get returns interface{}.

		// Let's refine getHash logic or do it inline.
		// If key exists but wrong type, getHash returns (nil, false)?
		// No, val.(HashData) fails -> return (nil, false).
		// So HSET overwrites if wrong type? No, Redis returns WRONGTYPE.

		// We need to check existence first.
		if val, exists := db.cache.Get(key); exists {
			if _, ok := val.(HashData); !ok {
				// Check expiry here too?
				// If expired, we should treat as new.
				if expireTime, ok := db.ttlMap[key]; ok {
					if time.Now().After(expireTime) {
						db.cache.Remove(key)
						if db.aofHandler != nil {
							db.aofHandler.AddAof([][]byte{[]byte("del"), []byte(key)})
						}
						// Now it's deleted. Treat as new.
						goto CreateNew
					}
				}
				return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
		}
	}

CreateNew:
	if h == nil {
		h = make(HashData)
	}

	count := 0
	for i := 2; i < len(args); i += 2 {
		field := string(args[i])
		val := args[i+1]
		if _, exists := h[field]; !exists {
			count++
		}
		h[field] = val
	}

	db.cache.Add(key, h, 0)
	return resp.MakeIntReply(int64(count))
}

func (db *StandaloneDB) hget(args [][]byte) resp.Reply {
	if len(args) != 3 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'hget' command")
	}
	key := string(args[1])
	field := string(args[2])

	h, ok := db.getHash(key)
	if !ok {
		// Could be wrong type or not found.
		if val, exists := db.cache.Get(key); exists {
			if _, ok := val.(HashData); !ok {
				return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
		}
		return resp.NullBulkReply
	}

	val, exists := h[field]
	if !exists {
		return resp.NullBulkReply
	}
	return resp.MakeBulkReply(val)
}

func (db *StandaloneDB) hgetall(args [][]byte) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'hgetall' command")
	}
	key := string(args[1])

	h, ok := db.getHash(key)
	if !ok {
		if val, exists := db.cache.Get(key); exists {
			if _, ok := val.(HashData); !ok {
				return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
		}
		return resp.MakeMultiBulkReply(nil)
	}

	res := make([][]byte, 0, len(h)*2)
	for k, v := range h {
		res = append(res, []byte(k))
		res = append(res, v)
	}
	return resp.MakeMultiBulkReply(res)
}

func (db *StandaloneDB) hdel(args [][]byte) resp.Reply {
	if len(args) < 3 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'hdel' command")
	}
	key := string(args[1])

	h, ok := db.getHash(key)
	if !ok {
		if val, exists := db.cache.Get(key); exists {
			if _, ok := val.(HashData); !ok {
				return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
		}
		return resp.MakeIntReply(0)
	}

	count := 0
	for i := 2; i < len(args); i++ {
		field := string(args[i])
		if _, exists := h[field]; exists {
			delete(h, field)
			count++
		}
	}

	if len(h) == 0 {
		db.cache.Remove(key)
	} else {
		db.cache.Add(key, h, 0)
	}

	return resp.MakeIntReply(int64(count))
}
