package db

import (
	"myredis/aof"
	"myredis/pkg/lru"
	"myredis/resp"
	"strings"
	"time"
)

// DB 数据库接口
type DB interface {
	Exec(cmd [][]byte) resp.Reply
	Close()
	Load()
}

// commandRequest 内部命令请求
type commandRequest struct {
	cmd    [][]byte
	result chan resp.Reply
	noAof  bool
}

// StandaloneDB 单机数据库 (Single-Threaded Actor Model)
type StandaloneDB struct {
	// Replaced map with Cache interface from mygocache/lru
	cache      *lru.Cache
	ttlMap     map[string]time.Time // Expiration map
	ops        chan *commandRequest
	stop       chan struct{}
	aofHandler *aof.AofHandler
}

// maxMemory hardcoded for now, or pass in.
// 100MB = 100 * 1024 * 1024
const DefaultMaxBytes = 100 * 1024 * 1024

func NewStandaloneDB(aofFilename string) *StandaloneDB {
	db := &StandaloneDB{
		ttlMap: make(map[string]time.Time),
		ops:    make(chan *commandRequest, 1000),
		stop:   make(chan struct{}),
	}

	// Initialize LRU Cache (Default strategy)
	// OnEvicted callback to clean up ttlMap
	onEvicted := func(key string, value lru.Value) {
		delete(db.ttlMap, key)
	}
	db.cache = lru.New(DefaultMaxBytes, onEvicted)

	if aofFilename != "" {
		handler, err := aof.NewAofHandler(aofFilename)
		if err == nil {
			db.aofHandler = handler
		}
	}

	go db.background()
	return db
}

// ... Exec, Load, Close methods remain same ...
// I will copy them to preserve file content.

func (db *StandaloneDB) Exec(cmd [][]byte) resp.Reply {
	req := &commandRequest{
		cmd:    cmd,
		result: make(chan resp.Reply, 1),
	}
	// 1. Try to send request
	select {
	case <-db.stop:
		return resp.MakeErrReply("ERR server closed")
	case db.ops <- req:
	}

	// 2. Wait for result
	select {
	case res := <-req.result:
		return res
	case <-db.stop:
		return resp.MakeErrReply("ERR server closed")
	case <-time.After(5 * time.Second): // Safety timeout
		return resp.MakeErrReply("ERR timeout")
	}
}

func (db *StandaloneDB) Load() {
	if db.aofHandler == nil {
		return
	}
	db.aofHandler.LoadAof(func(cmd [][]byte) resp.Reply {
		req := &commandRequest{
			cmd:    cmd,
			result: make(chan resp.Reply, 1),
			noAof:  true,
		}
		db.ops <- req
		return <-req.result
	})
}

func (db *StandaloneDB) Close() {
	if db.aofHandler != nil {
		db.aofHandler.Close()
	}
	select {
	case <-db.stop:
	default:
		close(db.stop)
	}
}

func (db *StandaloneDB) background() {
	// Ticker for active expiration (every 100ms)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case req := <-db.ops:
			res := db.execInternal(req.cmd)
			if !req.noAof && db.aofHandler != nil && !isError(res) && isWriteCommand(req.cmd) {
				db.aofHandler.AddAof(req.cmd)
			}
			req.result <- res
		case <-ticker.C:
			db.activeExpire()
		case <-db.stop:
			return
		}
	}
}

// Active Expiration
func (db *StandaloneDB) activeExpire() {
	// Sample 20 keys, delete expired
	// Since ttlMap is separate, we can iterate it.
	// Go map iteration is random.
	sampleSize := 20
	deleted := 0
	now := time.Now()

	for key, t := range db.ttlMap {
		if now.After(t) {
			db.cache.Remove(key) // OnEvicted will remove from ttlMap safely?
			// Wait, iterating map while deleting? Safe in Go.
			// But OnEvicted deletes from db.ttlMap!
			// If I delete here, I should ensure OnEvicted doesn't cause issue or double delete.
			// Actually lru.Remove calls OnEvicted. default OnEvicted deletes from ttlMap.
			// So calling db.cache.Remove(key) modifies ttlMap.
			// Modifying map during iteration is safe in Go (it just stops returning that element).

			// Also need to log to AOF as DEL?
			if db.aofHandler != nil {
				// Generate DEL command
				cmd := [][]byte{[]byte("DEL"), []byte(key)}
				db.aofHandler.AddAof(cmd)
			}
			deleted++
		}
		sampleSize--
		if sampleSize <= 0 {
			break
		}
	}
}

// ... isError, writeCommands, isWriteCommand ...

func isError(reply resp.Reply) bool {
	if reply == nil {
		return false
	}
	_, ok := reply.(*resp.ErrorReply)
	return ok
}

var writeCommands = map[string]struct{}{
	"set": {}, "del": {},
	"lpush": {}, "rpush": {}, "lpop": {}, "rpop": {},
	"hset": {}, "hdel": {},
	"sadd": {}, "srem": {},
	"expire": {}, "persist": {}, // Add new commands
}

func isWriteCommand(cmd [][]byte) bool {
	if len(cmd) == 0 {
		return false
	}
	name := strings.ToLower(string(cmd[0]))
	_, ok := writeCommands[name]
	return ok
}

func (db *StandaloneDB) execInternal(cmd [][]byte) resp.Reply {
	if len(cmd) == 0 {
		return nil
	}
	commandName := strings.ToLower(string(cmd[0]))

	// Lazy Expiration Check for Read/Write commands
	// Except keys (DEL, EXISTS, TTL etc might need special handling)
	// Redis checks expire on access.
	// Simplify: check usage in get/set logic.
	// Or check here if we can parse key.
	// Most commands [cmd, key, ...]
	if len(cmd) > 1 {
		// We can't blindly check key expiration here because some commands like MSET have multiple keys
		// or keys at different positions.
		// But for our subset, key is usually at index 1.
		// Let individual handlers handle lazy expiration (via Get/Set helpers).
	}

	switch commandName {
	case "ping":
		return resp.MakeStatusReply("PONG")
	case "set":
		return db.set(cmd)
	case "get":
		return db.get(cmd)
	case "del":
		return db.del(cmd)
	case "lpush":
		return db.lpush(cmd)
	case "rpush":
		return db.rpush(cmd)
	case "lpop":
		return db.lpop(cmd)
	case "rpop":
		return db.rpop(cmd)
	case "lrange":
		return db.lrange(cmd)
	case "llen":
		return db.llen(cmd)
	case "hset":
		return db.hset(cmd)
	case "hget":
		return db.hget(cmd)
	case "hgetall":
		return db.hgetall(cmd)
	case "hdel":
		return db.hdel(cmd)
	case "sadd":
		return db.sadd(cmd)
	case "srem":
		return db.srem(cmd)
	case "smembers":
		return db.smembers(cmd)
	case "scard":
		return db.scard(cmd)
	// New Commands
	case "expire":
		return db.expire(cmd)
	case "ttl":
		return db.ttl(cmd)
	case "persist":
		return db.persist(cmd)
	default:
		return resp.MakeErrReply("ERR unknown command '" + commandName + "'")
	}
}
