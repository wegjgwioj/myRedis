// LFU 淘汰实现：Least Frequently Used（最不经常使用）。
// 关键点：用频率桶（bucket）实现 O(1) 增减；同频率下按 LRU 次序淘汰保证可解释性。
// 适用：当热点 key 的访问频率差异明显时，相比 LRU 更不容易误伤“高频但短暂冷却”的 key。
package lru

import (
	"container/list"
	"time"
)

// 本文件实现 LFU（Least Frequently Used）缓存：
// - 以 O(1) 的方式维护 “访问频次 -> 双向链表” 的桶（bucket）
// - 同频次下再按 LRU（最近最少使用）淘汰，保证行为稳定可解释
//
// 设计取舍：
// - 该实现不追求并发安全，默认由上层 Actor 串行调用
// - ttl 参数提供兼容接口（惰性过期检查）；项目主 DB 的 TTL 由 db.ttlMap 统一管理

type lfuEntry struct {
	key       string
	value     Value
	freq      int
	expiresAt int64 // 秒级时间戳；0 表示永不过期
	element   *list.Element
}

// LFUCache 为 LFU 淘汰策略实现。
type LFUCache struct {
	maxBytes int64
	nbytes   int64

	items   map[string]*lfuEntry
	buckets map[int]*list.List // freq -> *list.List，元素为 *lfuEntry
	minFreq int

	onRemove OnRemoveFunc
}

// NewLFU 创建一个 LFU 缓存实例。
func NewLFU(maxBytes int64, onRemove OnRemoveFunc) *LFUCache {
	return &LFUCache{
		maxBytes: maxBytes,
		items:    make(map[string]*lfuEntry),
		buckets:  make(map[int]*list.List),
		onRemove: onRemove,
	}
}

// Close LFU 无后台协程，Close 为空实现，保留接口一致性。
func (c *LFUCache) Close() {}

func (c *LFUCache) Len() int { return len(c.items) }

// ForEach 遍历缓存中的所有 key/value（不改变 LFU 频次/顺序）。
// 注意：默认由上层 Actor 串行调用，不做并发保护。
func (c *LFUCache) ForEach(fn func(key string, value Value) bool) {
	for key, ent := range c.items {
		if !fn(key, ent.value) {
			return
		}
	}
}

// Add 新增/更新条目。
// 说明：写入也视为一次“访问”，会增加频次（便于更贴近真实热度）。
func (c *LFUCache) Add(key string, value Value, ttl int64) {
	var expiresAt int64
	if ttl > 0 {
		expiresAt = time.Now().Unix() + ttl
	}

	if ent, ok := c.items[key]; ok {
		// 更新 value 大小
		c.nbytes += int64(value.Len()) - int64(ent.value.Len())
		ent.value = value
		ent.expiresAt = expiresAt
		c.increment(ent)
	} else {
		ent := &lfuEntry{
			key:       key,
			value:     value,
			freq:      1,
			expiresAt: expiresAt,
		}
		l := c.getOrCreateBucket(1)
		ent.element = l.PushFront(ent)
		c.items[key] = ent
		c.nbytes += int64(len(key)) + int64(value.Len())
		c.minFreq = 1
	}

	// 超出容量则淘汰，直到满足 maxBytes
	for c.maxBytes != 0 && c.nbytes > c.maxBytes {
		c.evictOne()
	}
}

// Get 获取条目并更新访问统计（freq++，同频按 LRU 调整）。
func (c *LFUCache) Get(key string) (value Value, ok bool) {
	ent, ok := c.items[key]
	if !ok {
		return nil, false
	}
	if c.isExpired(ent) {
		c.removeEntry(ent, RemoveReasonExpired)
		return nil, false
	}
	c.increment(ent)
	return ent.value, true
}

// Peek 获取条目但不更新访问统计（避免 TTL/INFO 等命令污染热度）。
func (c *LFUCache) Peek(key string) (value Value, ok bool) {
	ent, ok := c.items[key]
	if !ok {
		return nil, false
	}
	if c.isExpired(ent) {
		c.removeEntry(ent, RemoveReasonExpired)
		return nil, false
	}
	return ent.value, true
}

func (c *LFUCache) Remove(key string) {
	ent, ok := c.items[key]
	if !ok {
		return
	}
	c.removeEntry(ent, RemoveReasonDeleted)
}

func (c *LFUCache) isExpired(ent *lfuEntry) bool {
	return ent.expiresAt > 0 && ent.expiresAt < time.Now().Unix()
}

func (c *LFUCache) getOrCreateBucket(freq int) *list.List {
	if l, ok := c.buckets[freq]; ok {
		return l
	}
	l := list.New()
	c.buckets[freq] = l
	return l
}

func (c *LFUCache) increment(ent *lfuEntry) {
	oldFreq := ent.freq
	oldBucket := c.buckets[oldFreq]
	if oldBucket != nil && ent.element != nil {
		oldBucket.Remove(ent.element)
		if oldBucket.Len() == 0 {
			delete(c.buckets, oldFreq)
			if c.minFreq == oldFreq {
				c.minFreq = oldFreq + 1
			}
		}
	}

	ent.freq++
	newBucket := c.getOrCreateBucket(ent.freq)
	ent.element = newBucket.PushFront(ent)
}

func (c *LFUCache) evictOne() {
	if len(c.items) == 0 {
		c.minFreq = 0
		return
	}

	b := c.buckets[c.minFreq]
	// 理论上不会为空；若出现则尝试向上找一个非空桶
	for b == nil || b.Len() == 0 {
		delete(c.buckets, c.minFreq)
		c.minFreq++
		b = c.buckets[c.minFreq]
		if c.minFreq > 1_000_000 { // 防御：避免异常情况下死循环
			c.recalculateMinFreq()
			b = c.buckets[c.minFreq]
			break
		}
	}

	if b == nil || b.Len() == 0 {
		// 兜底：无法淘汰
		return
	}

	// 同频按 LRU：淘汰链表尾部（最久未访问）
	back := b.Back()
	ent := back.Value.(*lfuEntry)
	c.removeEntry(ent, RemoveReasonEvicted)
}

func (c *LFUCache) recalculateMinFreq() {
	c.minFreq = 0
	for f, l := range c.buckets {
		if l == nil || l.Len() == 0 {
			continue
		}
		if c.minFreq == 0 || f < c.minFreq {
			c.minFreq = f
		}
	}
}

func (c *LFUCache) removeEntry(ent *lfuEntry, reason RemoveReason) {
	// 从 bucket 移除
	if b := c.buckets[ent.freq]; b != nil && ent.element != nil {
		b.Remove(ent.element)
		if b.Len() == 0 {
			delete(c.buckets, ent.freq)
			if c.minFreq == ent.freq {
				if len(c.items) == 1 { // 即将删到空
					c.minFreq = 0
				} else {
					c.minFreq = ent.freq + 1
					if c.buckets[c.minFreq] == nil {
						c.recalculateMinFreq()
					}
				}
			}
		}
	}

	delete(c.items, ent.key)
	c.nbytes -= int64(len(ent.key)) + int64(ent.value.Len())

	if c.onRemove != nil {
		c.onRemove(ent.key, ent.value, reason)
	}
}
