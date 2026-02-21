// DB 核心：单线程 Actor 模型的命令执行引擎。
// 关键点：串行处理请求、对接 LRU/LFU 淘汰、维护 TTL、追加写 AOF、支持 Close 优雅关闭。
// 线程模型：网络层可并发，但真正读写内存状态的逻辑集中在一个后台 goroutine 中完成。
package db

import (
	"myredis/aof"
	"myredis/pkg/lru"
	"myredis/resp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 本文件实现核心数据库（KV 引擎）：
// - 单线程 Actor 模型：所有命令通过 channel 串行化，避免锁竞争
// - AOF：对写命令进行追加日志（EverySec 刷盘）
// - TTL：惰性删除 + 定期删除（db.ttlMap）
// - 内存淘汰：通过可插拔缓存实现（LRU/LFU）
//
// 注意：db.execInternal 只在 background goroutine 内执行，因此可以安全地操作非并发安全结构。

// DB 数据库接口
type DB interface {
	Exec(cmd [][]byte) resp.Reply
	Close()
	Load()
}

// commandRequest 内部命令请求
type commandRequest struct {
	cmd [][]byte
	// fn 用于内部任务（例如：加载 RDB 快照、执行持久化后台任务的收尾）。
	// 当 fn != nil 时，background 会优先执行 fn，而不是走 execInternal(cmd)。
	fn     func() resp.Reply
	result chan resp.Reply
	noAof  bool
}

// StandaloneDB 单机数据库 (Single-Threaded Actor Model)
type StandaloneDB struct {
	// cache 为可插拔淘汰策略（LRU/LFU）。由 Actor 串行调用，因此不要求并发安全。
	cache  lru.EvictionCache
	ttlMap map[string]time.Time // key -> 绝对过期时间

	ops       chan *commandRequest
	closing   chan struct{}
	closeOnce sync.Once
	bgWg      sync.WaitGroup

	// evictedKeys 用于记录“一次命令执行过程中由于 maxBytes 触发的淘汰”，并在 AOF 中追加 DEL，避免重启后复活。
	// 该 slice 仅在 background goroutine 中读写。
	evictedKeys []string

	aofHandler *aof.AofHandler

	// rdbFilename 为可选快照文件路径（为空表示关闭 RDB）。
	rdbFilename string
	rdbMu       sync.Mutex
	rdbSaving   bool

	// aofRewriteDone 用于 BGREWRITEAOF 后台写入完成后的回调收尾（在 Actor 线程执行 FinishRewrite）。
	aofRewriteDone chan aofRewriteResult
	aofRewriting   bool
}

// maxMemory hardcoded for now, or pass in.
// 100MB = 100 * 1024 * 1024
const DefaultMaxBytes = 100 * 1024 * 1024

// StandaloneDBConfig 用于配置 StandaloneDB 的运行参数（便于 CLI/评估脚本控制）。
type StandaloneDBConfig struct {
	AofFilename string
	RdbFilename string
	MaxBytes    int64  // 内存上限（用于 LRU/LFU 淘汰）；0 表示使用默认值
	Eviction    string // "lru" / "lfu"
}

func NewStandaloneDB(aofFilename string) *StandaloneDB {
	return NewStandaloneDBWithConfig(StandaloneDBConfig{
		AofFilename: aofFilename,
		RdbFilename: "",
		MaxBytes:    DefaultMaxBytes,
		Eviction:    "lru",
	})
}

func NewStandaloneDBWithConfig(cfg StandaloneDBConfig) *StandaloneDB {
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = DefaultMaxBytes
	}
	eviction := strings.ToLower(strings.TrimSpace(cfg.Eviction))

	db := &StandaloneDB{
		ttlMap:  make(map[string]time.Time),
		ops:     make(chan *commandRequest, 1000),
		closing: make(chan struct{}),
		// 这里用一个有缓冲 channel，避免后台重写 goroutine 写入结果时被阻塞（Actor 会尽快消费）。
		aofRewriteDone: make(chan aofRewriteResult, 1),
		rdbFilename:    cfg.RdbFilename,
	}

	// Initialize LRU Cache (Default strategy)
	// OnEvicted callback:
	// 1) 始终清理 ttlMap，避免过期表泄漏
	// 2) 若是容量淘汰（Evicted），记录到 evictedKeys，稍后由 background 统一写入 AOF（DEL key）
	onEvicted := func(key string, value lru.Value, reason lru.RemoveReason) {
		// 任何删除都需要同步清理 ttlMap，避免内存泄漏
		delete(db.ttlMap, key)
		if reason == lru.RemoveReasonEvicted {
			db.evictedKeys = append(db.evictedKeys, key)
		}
	}
	switch eviction {
	case "", "lru":
		db.cache = lru.New(cfg.MaxBytes, onEvicted)
	case "lfu":
		db.cache = lru.NewLFU(cfg.MaxBytes, onEvicted)
	default:
		// 非法值降级为 LRU（并在文档/评估中明确只支持 lru/lfu）
		db.cache = lru.New(cfg.MaxBytes, onEvicted)
	}

	if cfg.AofFilename != "" {
		handler, err := aof.NewAofHandler(cfg.AofFilename)
		if err == nil {
			db.aofHandler = handler
		}
	}

	db.bgWg.Add(1)
	go db.background()
	return db
}

func (db *StandaloneDB) Exec(cmd [][]byte) resp.Reply {
	// 关闭过程中直接返回，避免 goroutine 堆积
	select {
	case <-db.closing:
		return resp.MakeErrReply("ERR server closed")
	default:
	}

	req := &commandRequest{
		cmd:    cmd,
		result: make(chan resp.Reply, 1),
	}
	// 1. Try to send request
	select {
	case <-db.closing:
		return resp.MakeErrReply("ERR server closed")
	case db.ops <- req:
	}

	// 2. Wait for result
	select {
	case res := <-req.result:
		return res
	case <-db.closing:
		return resp.MakeErrReply("ERR server closed")
	case <-time.After(5 * time.Second): // Safety timeout
		return resp.MakeErrReply("ERR timeout")
	}
}

func (db *StandaloneDB) Load() {
	// 优先加载 RDB 快照（若配置），再加载 AOF（若配置），实现“快照 + 增量日志”恢复。
	db.loadRdb()
	if db.aofHandler == nil {
		return
	}
	_ = db.aofHandler.LoadAof(func(cmd [][]byte) resp.Reply {
		req := &commandRequest{
			cmd:    cmd,
			result: make(chan resp.Reply, 1),
			noAof:  true,
		}
		select {
		case <-db.closing:
			return resp.MakeErrReply("ERR server closed")
		case db.ops <- req:
		}
		select {
		case res := <-req.result:
			return res
		case <-db.closing:
			return resp.MakeErrReply("ERR server closed")
		}
	})
}

func (db *StandaloneDB) Close() {
	// Close 需要幂等：评估脚本/测试/多层封装可能会重复调用 Close。
	db.closeOnce.Do(func() {
		close(db.closing)
		db.bgWg.Wait()

		// background 退出后再关闭 AOF，避免 AddAof 向已关闭 channel 写入导致 panic
		if db.aofHandler != nil {
			db.aofHandler.Close()
		}
		if db.cache != nil {
			db.cache.Close()
		}
	})
}

func (db *StandaloneDB) background() {
	defer db.bgWg.Done()

	// Ticker for active expiration (every 100ms)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case req := <-db.ops:
			db.evictedKeys = db.evictedKeys[:0]
			var res resp.Reply
			if req.fn != nil {
				res = req.fn()
			} else {
				res = db.execInternal(req.cmd)
			}

			if !req.noAof && db.aofHandler != nil && !isError(res) {
				db.appendAof(req.cmd, res)
				// 将本次命令触发的“容量淘汰”写入 AOF，避免重启后被淘汰的数据复活
				for _, key := range db.evictedKeys {
					db.aofHandler.AddAof([][]byte{[]byte("DEL"), []byte(key)})
				}
			}

			req.result <- res
		case done := <-db.aofRewriteDone:
			db.handleAofRewriteDone(done)
		case <-ticker.C:
			db.activeExpire()
		case <-db.closing:
			// 优雅关闭：尽可能处理完队列中已进入 ops 的请求，再退出
			for {
				select {
				case req := <-db.ops:
					db.evictedKeys = db.evictedKeys[:0]
					var res resp.Reply
					if req.fn != nil {
						res = req.fn()
					} else {
						res = db.execInternal(req.cmd)
					}
					if !req.noAof && db.aofHandler != nil && !isError(res) {
						db.appendAof(req.cmd, res)
						for _, key := range db.evictedKeys {
							db.aofHandler.AddAof([][]byte{[]byte("DEL"), []byte(key)})
						}
					}
					req.result <- res
				case done := <-db.aofRewriteDone:
					db.handleAofRewriteDone(done)
				default:
					return
				}
			}
		}
	}
}

func (db *StandaloneDB) appendAof(cmd [][]byte, res resp.Reply) {
	if len(cmd) == 0 {
		return
	}
	name := strings.ToLower(string(cmd[0]))

	switch name {
	case "expire":
		// EXPIRE 采用绝对过期时间写入 AOF（PEXPIREAT），避免重启后“续命”
		intReply, ok := res.(*resp.IntReply)
		if !ok || intReply.Code != 1 || len(cmd) < 2 {
			return
		}
		key := string(cmd[1])
		expireAt, ok := db.ttlMap[key]
		if ok {
			db.aofHandler.AddAof([][]byte{
				[]byte("PEXPIREAT"),
				[]byte(key),
				[]byte(strconv.FormatInt(expireAt.UnixMilli(), 10)),
			})
			return
		}

		// seconds <= 0 会直接删除 key，此时 ttlMap 已被清理；AOF 用 DEL 保证重放一致性
		db.aofHandler.AddAof([][]byte{[]byte("DEL"), []byte(key)})
		return
	case "persist":
		// PERSIST 只有成功删除 TTL（返回 1）才写入 AOF
		intReply, ok := res.(*resp.IntReply)
		if !ok || intReply.Code != 1 {
			return
		}
		db.aofHandler.AddAof(cmd)
		return
	default:
		// 其他写命令按原样追加
		if isWriteCommand(cmd) {
			db.aofHandler.AddAof(cmd)
		}
	}
}

// Active Expiration
func (db *StandaloneDB) activeExpire() {
	// Sample 20 keys, delete expired
	// Since ttlMap is separate, we can iterate it.
	// Go map iteration is random.
	sampleSize := 20
	now := time.Now()

	for key, t := range db.ttlMap {
		if now.After(t) {
			// 定期删除只负责清理内存；AOF 由 PEXPIREAT 语义保证重启一致性
			db.cache.Remove(key) // OnEvicted 会同步删除 ttlMap
			// Wait, iterating map while deleting? Safe in Go.
			// But OnEvicted deletes from db.ttlMap!
			// If I delete here, I should ensure OnEvicted doesn't cause issue or double delete.
			// Actually lru.Remove calls OnEvicted. default OnEvicted deletes from ttlMap.
			// So calling db.cache.Remove(key) modifies ttlMap.
			// Modifying map during iteration is safe in Go (it just stops returning that element).

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
	// expire/persist 在 appendAof 中做了“只在成功时记录 + 写 PEXPIREAT”特殊处理
	"pexpireat": {},
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
	case "pexpireat":
		return db.pexpireat(cmd)
	case "ttl":
		return db.ttl(cmd)
	case "persist":
		return db.persist(cmd)
	// Persistence / Admin
	case "save":
		return db.save()
	case "bgsave":
		return db.bgsave()
	case "rewriteaof":
		return db.rewriteaof()
	case "bgrewriteaof":
		return db.bgrewriteaof()
	default:
		return resp.MakeErrReply("ERR unknown command '" + commandName + "'")
	}
}
