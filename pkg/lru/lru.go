// LRU 淘汰实现：Least Recently Used（最近最少使用）。
// 关键点：Get 会更新访问顺序，Peek 不更新；RemoveReason 透传给上层用于 AOF/TTL 协同。
// 说明：在内存上限较小、访问模式局部性强时，LRU 通常有较好的命中率与可解释性。
package lru

import (
	"container/list"
	"myredis/pkg/pool"
	"sync"
	"time"
)

// 本文件实现 LRU（最近最少使用）缓存：
// - 通过双向链表维护访问顺序（O(1) MoveToFront）
// - 通过 map 维护 key -> 节点映射（O(1) 定位）
// - 通过 maxBytes 控制内存上限，超限时自动淘汰最旧数据
//
// 重要说明：
// - 该结构默认由上层 Actor 串行调用（不保证并发安全）。
// - 内部 TTL（heap + expirationLoop）目前仅用于可选场景；在本项目 DB 中 TTL 由 db.ttlMap 统一管理。

// Cache 是一个带过期时间支持的 LRU 缓存。它不是并发安全的。
type Cache struct {
	maxBytes int64                    // 缓存的最大字节数
	nbytes   int64                    // 当前缓存的字节数
	ll       *list.List               // 双向链表，用于实现 LRU
	cache    map[string]*list.Element // 键到链表元素的映射
	// 优先级队列（最小堆），用于过期管理
	heap    []*pool.HeapItem // 最小堆数组
	heapMap map[string]int   // 键到堆索引的映射
	// 删除回调：用于上层同步清理辅助结构/记录删除原因（淘汰/过期/显式删除等）
	OnEvicted OnRemoveFunc
	// 过期协程的停止信号
	stopChan chan struct{}
	// 堆操作的互斥锁
	heapMu sync.Mutex
	// 对象池，用于优化内存管理
	entryPool    *pool.EntryPool
	heapItemPool *pool.HeapItemPool
}

// entry 表示缓存中的一个条目
type entry struct {
	key       string // 键
	value     Value  // 值
	expiresAt int64  // 过期时间戳，0 表示永不过期
}

// Value 接口用于计算值占用的字节数
type Value interface {
	Len() int // 返回值占用的字节数
}

// New 创建一个新的缓存实例
// maxBytes 是缓存的最大字节数
// onEvicted 是当条目被删除时执行的回调函数
func New(maxBytes int64, onEvicted OnRemoveFunc) *Cache {
	c := &Cache{
		maxBytes:     maxBytes,
		ll:           list.New(),
		cache:        make(map[string]*list.Element),
		heap:         make([]*pool.HeapItem, 0),
		heapMap:      make(map[string]int),
		OnEvicted:    onEvicted,
		stopChan:     make(chan struct{}),
		entryPool:    pool.NewEntryPool(),
		heapItemPool: pool.NewHeapItemPool(),
	}
	// 启动过期检查协程
	go c.expirationLoop()
	return c
}

// Close 停止过期检查协程
func (c *Cache) Close() {
	close(c.stopChan)
}

// Add 向缓存中添加一个值，带有可选的过期时间
// key 是缓存的键
// value 是缓存的值
// ttl 是生存时间（秒），0 表示永不过期
func (c *Cache) Add(key string, value Value, ttl int64) {
	var expiresAt int64
	if ttl > 0 {
		expiresAt = time.Now().Unix() + ttl
	}

	if ele, ok := c.cache[key]; ok {
		// 更新现有条目
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		c.nbytes += int64(value.Len()) - int64(kv.value.Len())
		kv.value = value

		// 更新过期时间
		oldExpiresAt := kv.expiresAt
		kv.expiresAt = expiresAt

		// 如果过期时间发生变化，更新堆
		if oldExpiresAt > 0 || expiresAt > 0 {
			c.heapMu.Lock()
			if oldExpiresAt > 0 {
				// 从堆中移除
				c.removeFromHeap(key)
			}
			if expiresAt > 0 {
				// 添加到堆中
				c.addToHeap(key, expiresAt)
			}
			c.heapMu.Unlock()
		}
	} else {
		// 添加新条目
		ele := c.ll.PushFront(&entry{key, value, expiresAt})
		c.cache[key] = ele
		c.nbytes += int64(len(key)) + int64(value.Len())

		// 如果有过期时间，添加到堆中
		if expiresAt > 0 {
			c.heapMu.Lock()
			c.addToHeap(key, expiresAt)
			c.heapMu.Unlock()
		}
	}

	// 清理超出容量的项
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

// Get 查找并返回缓存中键对应的值（惰性过期）
// key 是要查找的键
// 返回值和是否找到的标志
func (c *Cache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.cache[key]; ok {
		kv := ele.Value.(*entry)
		// 检查是否过期（惰性过期）
		if kv.expiresAt > 0 && kv.expiresAt < time.Now().Unix() {
			// 过期，删除该项
			c.removeEntry(ele, RemoveReasonExpired)
			return nil, false
		}
		// 未过期，移到队首
		c.ll.MoveToFront(ele)
		return kv.value, true
	}
	return
}

// Peek 查找并返回缓存中键对应的值（不更新 LRU 顺序）
// 适用场景：TTL/INFO 等只想“查看”而不想影响淘汰统计的命令。
func (c *Cache) Peek(key string) (value Value, ok bool) {
	if ele, ok := c.cache[key]; ok {
		kv := ele.Value.(*entry)
		if kv.expiresAt > 0 && kv.expiresAt < time.Now().Unix() {
			c.removeEntry(ele, RemoveReasonExpired)
			return nil, false
		}
		return kv.value, true
	}
	return nil, false
}

// RemoveOldest 删除最旧的条目
func (c *Cache) RemoveOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.removeEntry(ele, RemoveReasonEvicted)
	}
}

// removeEntry 从缓存和堆中删除一个条目
// ele 是要删除的链表元素
func (c *Cache) removeEntry(ele *list.Element, reason RemoveReason) {
	kv := ele.Value.(*entry)
	key := kv.key

	// 从链表中删除
	c.ll.Remove(ele)
	delete(c.cache, key)
	c.nbytes -= int64(len(key)) + int64(kv.value.Len())

	// 如果有过期时间，从堆中删除
	if kv.expiresAt > 0 {
		c.heapMu.Lock()
		c.removeFromHeap(key)
		c.heapMu.Unlock()
	}

	// 调用删除回调
	if c.OnEvicted != nil {
		c.OnEvicted(key, kv.value, reason)
	}
}

// addToHeap 将键添加到过期堆中
// key 是要添加的键
// expiresAt 是过期时间戳
func (c *Cache) addToHeap(key string, expiresAt int64) {
	// 从对象池获取 heapItem
	item := c.heapItemPool.Get()
	item.Key = key
	item.ExpiresAt = expiresAt
	c.heap = append(c.heap, item)
	index := len(c.heap) - 1
	c.heapMap[key] = index
	c.heapifyUp(index)
}

// removeFromHeap 从过期堆中删除键
// key 是要删除的键
func (c *Cache) removeFromHeap(key string) {
	if index, ok := c.heapMap[key]; ok {
		lastIndex := len(c.heap) - 1
		// 检查索引是否有效
		if index < 0 || index >= len(c.heap) {
			delete(c.heapMap, key)
			return
		}
		// 与最后一个元素交换
		c.heap[index] = c.heap[lastIndex]
		c.heapMap[c.heap[index].Key] = index
		// 删除最后一个元素并回收
		item := c.heap[lastIndex]
		c.heap = c.heap[:lastIndex]
		c.heapItemPool.Put(item)
		delete(c.heapMap, key)

		// 堆化
		if index < len(c.heap) {
			c.heapifyDown(index)
			c.heapifyUp(index)
		}
	}
}

// heapifyUp 将元素向上移动以维护堆属性
// index 是要移动的元素的索引
func (c *Cache) heapifyUp(index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if c.heap[index].ExpiresAt >= c.heap[parent].ExpiresAt {
			break
		}
		// 与父元素交换
		c.heap[index], c.heap[parent] = c.heap[parent], c.heap[index]
		c.heapMap[c.heap[index].Key] = index
		c.heapMap[c.heap[parent].Key] = parent
		index = parent
	}
}

// heapifyDown 将元素向下移动以维护堆属性
// index 是要移动的元素的索引
func (c *Cache) heapifyDown(index int) {
	for {
		left := 2*index + 1
		right := 2*index + 2
		smallest := index

		if left < len(c.heap) && c.heap[left].ExpiresAt < c.heap[smallest].ExpiresAt {
			smallest = left
		}
		if right < len(c.heap) && c.heap[right].ExpiresAt < c.heap[smallest].ExpiresAt {
			smallest = right
		}

		if smallest == index {
			break
		}

		// 与最小的子元素交换
		c.heap[index], c.heap[smallest] = c.heap[smallest], c.heap[index]
		c.heapMap[c.heap[index].Key] = index
		c.heapMap[c.heap[smallest].Key] = smallest
		index = smallest
	}
}

// expirationLoop 处理基于堆的主动过期
func (c *Cache) expirationLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.checkExpiration()
		case <-c.stopChan:
			return
		}
	}
}

// checkExpiration 检查并删除过期的项
func (c *Cache) checkExpiration() {
	now := time.Now().Unix()

	c.heapMu.Lock()
	// 处理堆中的过期项
	for len(c.heap) > 0 && c.heap[0].ExpiresAt < now {
		item := c.heap[0]
		key := item.Key

		// 从堆中移除
		c.removeFromHeap(key)
		c.heapMu.Unlock()

		// 如果缓存中还存在，删除它
		if ele, ok := c.cache[key]; ok {
			c.removeEntry(ele, RemoveReasonExpired)
		}

		c.heapMu.Lock()
	}
	c.heapMu.Unlock()
}

// Len 返回缓存中的条目数
func (c *Cache) Len() int {
	return c.ll.Len()
}

// ForEach 遍历缓存中的所有 key/value（不改变 LRU 访问顺序）。
// 注意：该方法不做并发保护，默认由上层 Actor 串行调用。
func (c *Cache) ForEach(fn func(key string, value Value) bool) {
	for key, ele := range c.cache {
		ent := ele.Value.(*entry)
		if !fn(key, ent.value) {
			return
		}
	}
}

// Remove 删除指定键的条目
func (c *Cache) Remove(key string) {
	if ele, ok := c.cache[key]; ok {
		c.removeEntry(ele, RemoveReasonDeleted)
	}
}

// Clear 清空所有条目
func (c *Cache) Clear() {
	// 遍历所有条目并删除
	for key := range c.cache {
		if ele, ok := c.cache[key]; ok {
			c.removeEntry(ele, RemoveReasonCleared)
		}
	}
}
