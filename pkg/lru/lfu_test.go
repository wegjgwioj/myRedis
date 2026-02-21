// LFU 单元测试：验证频率统计、同频 LRU 退化策略、Peek 不影响统计等关键语义。
// 目标：在容量受限时，淘汰结果必须可预测、可解释。
// 覆盖：频次提升、同频淘汰顺序、删除原因回调。
package lru

import "testing"

// 本文件为 LFU 缓存的单元测试：
// - 频次优先淘汰
// - 同频按 LRU 淘汰
// - Peek 不应改变淘汰顺序/频次

func TestLFU_EvictByFrequency(t *testing.T) {
	evicted := make([]string, 0)
	onRemove := func(key string, _ Value, reason RemoveReason) {
		if reason == RemoveReasonEvicted {
			evicted = append(evicted, key)
		}
	}

	// 容量只够放下 2 个 key（粗略按 len(key)+len(value) 计算）
	// k1/v1: 2+2=4, k2/v2: 2+2=4, k3/v3: 2+2=4
	// maxBytes=8 => 第三个会触发淘汰 1 个
	c := NewLFU(8, onRemove)
	c.Add("k1", String("v1"), 0)
	c.Add("k2", String("v2"), 0)

	// 提升 k1 频次
	for i := 0; i < 3; i++ {
		if _, ok := c.Get("k1"); !ok {
			t.Fatalf("expected k1 exist")
		}
	}

	c.Add("k3", String("v3"), 0)

	// k2 应该被淘汰（频次更低）
	if _, ok := c.Peek("k2"); ok {
		t.Fatalf("expected k2 evicted")
	}
	if _, ok := c.Peek("k1"); !ok {
		t.Fatalf("expected k1 kept")
	}
	if _, ok := c.Peek("k3"); !ok {
		t.Fatalf("expected k3 kept")
	}

	if len(evicted) == 0 || evicted[len(evicted)-1] != "k2" {
		t.Fatalf("expected last evicted key to be k2, got %v", evicted)
	}
}

func TestLFU_EvictLRUWithinSameFreq(t *testing.T) {
	c := NewLFU(8, nil)
	c.Add("k1", String("v1"), 0)
	c.Add("k2", String("v2"), 0)

	// 此时 k1 和 k2 都是 freq=1。
	// 最近插入的在 bucket 前面：k2 更“新”，k1 更“旧”。
	c.Add("k3", String("v3"), 0)

	// 触发淘汰时，应淘汰同频的 LRU：k1
	if _, ok := c.Peek("k1"); ok {
		t.Fatalf("expected k1 evicted as LRU within same freq")
	}
	if _, ok := c.Peek("k2"); !ok {
		t.Fatalf("expected k2 kept")
	}
	if _, ok := c.Peek("k3"); !ok {
		t.Fatalf("expected k3 kept")
	}
}

func TestLFU_PeekDoesNotAffectOrderOrFrequency(t *testing.T) {
	c := NewLFU(8, nil)
	c.Add("k1", String("v1"), 0)
	c.Add("k2", String("v2"), 0)

	// Peek 不应改变同频下的 LRU 顺序
	if _, ok := c.Peek("k1"); !ok {
		t.Fatalf("expected k1 exist")
	}

	// 如果 Peek 错误地把 k1 变成“更热/更近”，下面新增时可能淘汰 k2。
	c.Add("k3", String("v3"), 0)

	if _, ok := c.Peek("k1"); ok {
		t.Fatalf("expected k1 still be LRU and evicted")
	}
	if _, ok := c.Peek("k2"); !ok {
		t.Fatalf("expected k2 kept")
	}
}
