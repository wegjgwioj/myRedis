// List 命令实现：LPUSH/RPUSH/LPOP/RPOP/LRANGE/LLEN 等。
// 说明：在淘汰/删除 key 时需要触发缓存删除回调，确保 TTL 与 AOF 状态一致。
// 关键点：当列表为空导致 key 被移除时，需要当作“显式删除”处理以保持一致性。
package db

import (
	"container/list"
	"myredis/resp"
	"strconv"
	"time"
)

// 本文件实现 List 相关命令：LPUSH / RPUSH / LPOP / RPOP / LLEN / LRANGE
// 说明：
// - List 的具体值存为 ListData{*list.List}
// - TTL 由 db.ttlMap 管理；过期时只做内存删除，不额外写入 AOF（AOF 使用 PEXPIREAT 语义保证一致性）

func (db *StandaloneDB) getListForWrite(key string) (*list.List, resp.Reply) {
	// 1. Get from cache
	val, ok := db.cache.Get(key)
	var l *list.List
	if !ok {
		l = list.New()
		// Don't add to cache yet, caller adds
	} else {
		// 2. Check Expiration
		if expireTime, ok := db.ttlMap[key]; ok {
			if time.Now().After(expireTime) {
				db.cache.Remove(key)
				l = list.New() // New empty list
				// fallthrough to return new list
			}
		}
	}

	// If val exists and not expired
	if l == nil {
		listData, ok := val.(ListData)
		if !ok {
			return nil, resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		l = listData.L
	}
	return l, nil
}

func (db *StandaloneDB) lpush(args [][]byte) resp.Reply {
	if len(args) < 3 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'lpush' command")
	}
	key := string(args[1])
	values := args[2:]

	l, errReply := db.getListForWrite(key)
	if errReply != nil {
		return errReply
	}

	for _, v := range values {
		l.PushFront(v)
	}

	// Update Cache
	db.cache.Add(key, ListData{L: l}, 0)
	// LPUSH does NOT reset TTL in Redis. Only SET does.

	return resp.MakeIntReply(int64(l.Len()))
}

func (db *StandaloneDB) rpush(args [][]byte) resp.Reply {
	if len(args) < 3 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'rpush' command")
	}
	key := string(args[1])
	values := args[2:]

	l, errReply := db.getListForWrite(key)
	if errReply != nil {
		return errReply
	}

	for _, v := range values {
		l.PushBack(v)
	}
	db.cache.Add(key, ListData{L: l}, 0)
	return resp.MakeIntReply(int64(l.Len()))
}

// LPOP key
func (db *StandaloneDB) lpop(args [][]byte) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'lpop' command")
	}
	key := string(args[1])

	val, ok := db.cache.Get(key)
	if !ok {
		return resp.NullBulkReply
	}

	// Check TTL
	if expireTime, ok := db.ttlMap[key]; ok {
		if time.Now().After(expireTime) {
			db.cache.Remove(key)
			return resp.NullBulkReply
		}
	}

	listData, ok := val.(ListData)
	if !ok {
		return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	l := listData.L

	if l.Len() == 0 {
		return resp.NullBulkReply
	}

	front := l.Front()
	l.Remove(front)

	// Update or Remove if empty
	if l.Len() == 0 {
		db.cache.Remove(key) // Removing key also removes TTL
	} else {
		// Technically value changed (size changed), so update LFU weight?
		// cache.Add updates value and recalculates weight.
		db.cache.Add(key, ListData{L: l}, 0)
	}

	return resp.MakeBulkReply(front.Value.([]byte))
}

func (db *StandaloneDB) rpop(args [][]byte) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'rpop' command")
	}
	key := string(args[1])

	val, ok := db.cache.Get(key)
	if !ok {
		return resp.NullBulkReply
	}

	if expireTime, ok := db.ttlMap[key]; ok {
		if time.Now().After(expireTime) {
			db.cache.Remove(key)
			return resp.NullBulkReply
		}
	}

	listData, ok := val.(ListData)
	if !ok {
		return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	l := listData.L

	if l.Len() == 0 {
		return resp.NullBulkReply
	}

	back := l.Back()
	l.Remove(back)

	if l.Len() == 0 {
		db.cache.Remove(key)
	} else {
		db.cache.Add(key, ListData{L: l}, 0)
	}

	return resp.MakeBulkReply(back.Value.([]byte))
}

func (db *StandaloneDB) llen(args [][]byte) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'llen' command")
	}
	key := string(args[1])

	val, ok := db.cache.Get(key)
	if !ok {
		return resp.MakeIntReply(0)
	}

	if expireTime, ok := db.ttlMap[key]; ok {
		if time.Now().After(expireTime) {
			db.cache.Remove(key)
			return resp.MakeIntReply(0)
		}
	}

	listData, ok := val.(ListData)
	if !ok {
		return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return resp.MakeIntReply(int64(listData.L.Len()))
}

func (db *StandaloneDB) lrange(args [][]byte) resp.Reply {
	if len(args) != 4 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'lrange' command")
	}
	key := string(args[1])
	start, err1 := strconv.Atoi(string(args[2]))
	stop, err2 := strconv.Atoi(string(args[3]))
	if err1 != nil || err2 != nil {
		return resp.MakeErrReply("ERR value is not an integer or out of range")
	}

	val, ok := db.cache.Get(key)
	if !ok {
		return resp.MakeMultiBulkReply(nil)
	}

	if expireTime, ok := db.ttlMap[key]; ok {
		if time.Now().After(expireTime) {
			db.cache.Remove(key)
			return resp.MakeMultiBulkReply(nil)
		}
	}

	listData, ok := val.(ListData)
	if !ok {
		return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	l := listData.L
	size := l.Len()

	if start < 0 {
		start = size + start
	}
	if stop < 0 {
		stop = size + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= size {
		stop = size - 1
	}
	if start > stop {
		return resp.MakeMultiBulkReply(nil)
	}

	slice := make([][]byte, 0, stop-start+1)
	i := 0
	for e := l.Front(); e != nil; e = e.Next() {
		if i >= start && i <= stop {
			slice = append(slice, e.Value.([]byte))
		}
		if i > stop {
			break
		}
		i++
	}
	return resp.MakeMultiBulkReply(slice)
}
