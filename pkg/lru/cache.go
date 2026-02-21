// lru 包的公共抽象：定义淘汰缓存接口与删除原因。
// 目的：让上层 DB 可在 LRU/LFU 间切换，并区分“淘汰/过期/显式删除”等原因用于 AOF 一致性。
// 约束：缓存实现不要求并发安全，上层通过 Actor 串行调用保证线程安全。
package lru

// 本文件定义了缓存抽象接口与删除原因枚举。
// 目的：
// 1) 让 DB 可以在 LRU / LFU 之间切换（对齐项目描述中的 “LRU/LFU 淘汰算法”）。
// 2) 让上层能区分 “容量淘汰/过期删除/主动删除”，用于 AOF 一致性与问题排查。

// RemoveReason 表示缓存条目被移除的原因。
type RemoveReason uint8

const (
	// RemoveReasonEvicted 表示因为超过最大内存而触发的淘汰（maxBytes）。
	RemoveReasonEvicted RemoveReason = iota
	// RemoveReasonExpired 表示条目因为 TTL 过期而被移除（缓存内部 TTL 的惰性删除/主动删除）。
	RemoveReasonExpired
	// RemoveReasonDeleted 表示外部显式删除（例如 DEL、RPOP 导致 key 被移除）。
	RemoveReasonDeleted
	// RemoveReasonCleared 表示清空缓存导致的删除。
	RemoveReasonCleared
)

// OnRemoveFunc 删除回调：上层可以用它同步清理辅助结构（如 ttlMap）或记录 AOF。
type OnRemoveFunc func(key string, value Value, reason RemoveReason)

// EvictionCache 是 DB 依赖的最小缓存接口（不要求并发安全，由 Actor 串行调用）。
//
// 说明：
// - Get：会更新 LRU/LFU 统计（影响淘汰策略）
// - Peek：不会更新统计（TTL/INFO 等命令应使用 Peek，避免污染淘汰热度）
type EvictionCache interface {
	Add(key string, value Value, ttl int64)
	Get(key string) (value Value, ok bool)
	Peek(key string) (value Value, ok bool)
	Remove(key string)
	// ForEach 遍历缓存中的所有 key/value（不改变 LRU/LFU 统计）。
	// 返回值：回调返回 false 时中止遍历。
	ForEach(fn func(key string, value Value) bool)
	Len() int
	Close()
}
