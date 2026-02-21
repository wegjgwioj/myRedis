// Set 命令实现：SADD/SREM/SCARD/SMEMBERS 等。
// 说明：该模块与 basic/list/hash 一样，依赖 Actor 串行执行保证并发安全。
// 关键点：当集合元素删空导致 key 被移除时，要同步清理 TTL 并记录 AOF。
package db

import (
	"myredis/resp"
	"time"
)

// 本文件实现 Set 相关命令：SADD / SREM / SCARD / SMEMBERS
// 说明：
// - SetData 使用 map[string]struct{}
// - TTL 由 db.ttlMap 管理；过期只做内存删除，不额外写 AOF（AOF 用 PEXPIREAT 保证重启一致性）

func (db *StandaloneDB) getSet(key string) (SetData, bool) {
	val, ok := db.cache.Get(key)
	if !ok {
		return nil, false
	}

	if expireTime, ok := db.ttlMap[key]; ok {
		if time.Now().After(expireTime) {
			db.cache.Remove(key)
			return nil, false
		}
	}

	s, ok := val.(SetData)
	return s, ok
}

// SADD key member [member ...]
// Returns: number of elements added
func (db *StandaloneDB) sadd(args [][]byte) resp.Reply {
	if len(args) < 3 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'sadd' command")
	}
	key := string(args[1])
	members := args[2:]

	s, ok := db.getSet(key)
	if !ok {
		// Check WRONGTYPE or expired
		if val, exists := db.cache.Get(key); exists {
			if _, ok := val.(SetData); !ok {
				// Check expiry here too?
				if expireTime, ok := db.ttlMap[key]; ok {
					if time.Now().After(expireTime) {
						db.cache.Remove(key)
						goto CreateNewSet
					}
				}
				return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
		}
	}

CreateNewSet:
	if s == nil {
		s = make(SetData)
	}

	added := 0
	for _, member := range members {
		memberStr := string(member)
		if _, exists := s[memberStr]; !exists {
			s[memberStr] = struct{}{}
			added++
		}
	}

	if added > 0 || len(s) == 0 { // Update cache even if no new added (to refresh LRU)?
		// cache.Get already refreshes LRU.
		// But s modified (size changed), so update size in cache.
		db.cache.Add(key, s, 0)
	}

	return resp.MakeIntReply(int64(added))
}

func (db *StandaloneDB) srem(args [][]byte) resp.Reply {
	if len(args) < 3 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'srem' command")
	}
	key := string(args[1])

	s, ok := db.getSet(key)
	if !ok {
		// Check WRONGTYPE
		if val, exists := db.cache.Get(key); exists {
			if _, ok := val.(SetData); !ok {
				return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
		}
		return resp.MakeIntReply(0)
	}

	removed := 0
	members := args[2:]
	for _, member := range members {
		memberStr := string(member)
		if _, exists := s[memberStr]; exists {
			delete(s, memberStr)
			removed++
		}
	}

	if len(s) == 0 {
		db.cache.Remove(key)
	} else {
		db.cache.Add(key, s, 0)
	}

	return resp.MakeIntReply(int64(removed))
}

func (db *StandaloneDB) scard(args [][]byte) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'scard' command")
	}
	key := string(args[1])

	s, ok := db.getSet(key)
	if !ok {
		// Check WRONGTYPE
		if val, exists := db.cache.Get(key); exists {
			if _, ok := val.(SetData); !ok {
				return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
		}
		return resp.MakeIntReply(0)
	}

	return resp.MakeIntReply(int64(len(s)))
}

func (db *StandaloneDB) smembers(args [][]byte) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'smembers' command")
	}
	key := string(args[1])

	s, ok := db.getSet(key)
	if !ok {
		// Check WRONGTYPE
		if val, exists := db.cache.Get(key); exists {
			if _, ok := val.(SetData); !ok {
				return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
		}
		return resp.MakeMultiBulkReply(nil)
	}

	res := make([][]byte, 0, len(s))
	for k := range s {
		res = append(res, []byte(k))
	}
	return resp.MakeMultiBulkReply(res)
}
