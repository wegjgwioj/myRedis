// DB 快照（Snapshot）实现：用于 RDB 保存与 AOF rewrite 的数据来源。
//
// 关键点：
// - 必须“深拷贝”当前内存数据，避免后台持久化过程中数据被后续写命令修改导致不一致。
// - TTL 使用绝对时间（UnixMilli），保证“重启不续命”。
package db

import (
	"container/list"
	"errors"
	"myredis/pkg/lru"
	"myredis/rdb"
	"sort"
	"time"
)

// snapshotEntries 在 Actor 线程内生成一份当前 DB 的深拷贝快照。
func (db *StandaloneDB) snapshotEntries() ([]rdb.Entry, error) {
	now := time.Now()

	// 快照阶段做一次“全量过期清理”，避免把已过期的 key 写入快照/重写文件。
	db.purgeExpiredAll(now)

	entries := make([]rdb.Entry, 0, db.cache.Len())
	var snapErr error

	db.cache.ForEach(func(key string, value lru.Value) bool {
		// 读取 TTL：ttlMap 存的是绝对过期时间
		expireAt := db.ttlMap[key]
		var expireAtMs int64
		if !expireAt.IsZero() {
			if !now.Before(expireAt) {
				// 理论上 purgeExpiredAll 已经清理，这里再做一道防御。
				return true
			}
			expireAtMs = expireAt.UnixMilli()
		}

		switch v := value.(type) {
		case StringData:
			copied := append([]byte(nil), v...)
			entries = append(entries, rdb.Entry{
				Key:            key,
				Type:           rdb.TypeString,
				ExpireAtUnixMs: expireAtMs,
				String:         copied,
			})
		case ListData:
			out := make([][]byte, 0)
			if v.L != nil {
				out = make([][]byte, 0, v.L.Len())
				for e := v.L.Front(); e != nil; e = e.Next() {
					b, ok := e.Value.([]byte)
					if !ok {
						snapErr = errors.New("invalid list element type")
						return false
					}
					out = append(out, append([]byte(nil), b...))
				}
			}
			entries = append(entries, rdb.Entry{
				Key:            key,
				Type:           rdb.TypeList,
				ExpireAtUnixMs: expireAtMs,
				List:           out,
			})
		case HashData:
			h := make(map[string][]byte, len(v))
			for fk, fv := range v {
				h[fk] = append([]byte(nil), fv...)
			}
			entries = append(entries, rdb.Entry{
				Key:            key,
				Type:           rdb.TypeHash,
				ExpireAtUnixMs: expireAtMs,
				Hash:           h,
			})
		case SetData:
			members := make([]string, 0, len(v))
			for m := range v {
				members = append(members, m)
			}
			sort.Strings(members)
			entries = append(entries, rdb.Entry{
				Key:            key,
				Type:           rdb.TypeSet,
				ExpireAtUnixMs: expireAtMs,
				Set:            members,
			})
		default:
			// 未知类型：为了可定位，直接中止快照。
			snapErr = errors.New("unknown value type in snapshot")
			return false
		}

		return true
	})

	if snapErr != nil {
		return nil, snapErr
	}

	// 为了输出更稳定（也便于比较/测试），按 key 排序。
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return entries, nil
}

// applySnapshot 在 Actor 线程内将 entries 恢复到当前 DB（用于启动加载 RDB）。
func (db *StandaloneDB) applySnapshot(entries []rdb.Entry) {
	nowMs := time.Now().UnixMilli()

	// 清空现有数据（启动加载时一般是空的，这里做防御）。
	keys := make([]string, 0, db.cache.Len())
	db.cache.ForEach(func(key string, _ lru.Value) bool {
		keys = append(keys, key)
		return true
	})
	for _, k := range keys {
		db.cache.Remove(k)
	}

	// 重新初始化 ttlMap（避免残留）。
	db.ttlMap = make(map[string]time.Time, len(entries))

	for _, e := range entries {
		// 跳过已过期条目
		if e.ExpireAtUnixMs > 0 && e.ExpireAtUnixMs <= nowMs {
			continue
		}

		switch e.Type {
		case rdb.TypeString:
			db.cache.Add(e.Key, StringData(append([]byte(nil), e.String...)), 0)
		case rdb.TypeList:
			l := list.New()
			for _, b := range e.List {
				l.PushBack(append([]byte(nil), b...))
			}
			db.cache.Add(e.Key, ListData{L: l}, 0)
		case rdb.TypeHash:
			h := make(HashData, len(e.Hash))
			for fk, fv := range e.Hash {
				h[fk] = append([]byte(nil), fv...)
			}
			db.cache.Add(e.Key, h, 0)
		case rdb.TypeSet:
			s := make(SetData, len(e.Set))
			for _, m := range e.Set {
				s[m] = struct{}{}
			}
			db.cache.Add(e.Key, s, 0)
		default:
			// 未知类型跳过（防御），避免启动直接崩溃。
			continue
		}

		if e.ExpireAtUnixMs > 0 {
			db.ttlMap[e.Key] = time.UnixMilli(e.ExpireAtUnixMs)
		}
	}
}

// purgeExpiredAll 扫描 ttlMap 并删除所有已过期 key（用于快照/重写）。
func (db *StandaloneDB) purgeExpiredAll(now time.Time) {
	for key, t := range db.ttlMap {
		if !now.Before(t) {
			db.cache.Remove(key)
		}
	}
}
