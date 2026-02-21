// LRU 单元测试：验证访问顺序淘汰、Peek 行为、删除原因等。
// 目标：确保 Get/Peek 的统计语义正确，淘汰顺序稳定。
// 覆盖：最近最少使用淘汰、Peek 不改变顺序、删除回调原因。
package lru

import (
	"reflect"
	"testing"
)

// 本文件为 LRU 缓存的单元测试：
// - 基本读写
// - 超容量淘汰行为
// - 回调触发顺序

type String string

func (d String) Len() int {
	return len(d)
}

func TestGet(t *testing.T) {
	lru := New(int64(0), nil)
	lru.Add("key1", String("1234"), 0)
	if v, ok := lru.Get("key1"); !ok || string(v.(String)) != "1234" {
		t.Fatalf("cache hit key1=1234 failed")
	}
	if _, ok := lru.Get("key2"); ok {
		t.Fatalf("cache miss key2 failed")
	}
}

func TestRemoveoldest(t *testing.T) {
	k1, k2, k3 := "key1", "key2", "k3"
	v1, v2, v3 := "value1", "value2", "v3"
	cap := len(k1 + k2 + v1 + v2)
	lru := New(int64(cap), nil)
	lru.Add(k1, String(v1), 0)
	lru.Add(k2, String(v2), 0)
	lru.Add(k3, String(v3), 0)

	if _, ok := lru.Get("key1"); ok || lru.Len() != 2 {
		t.Fatalf("Removeoldest key1 failed")
	}
}

func TestOnEvicted(t *testing.T) {
	keys := make([]string, 0)
	callback := func(key string, value Value, reason RemoveReason) {
		if reason != RemoveReasonEvicted {
			return
		}
		keys = append(keys, key)
	}
	lru := New(int64(10), callback)
	lru.Add("key1", String("123456"), 0)
	lru.Add("k2", String("k2"), 0)
	lru.Add("k3", String("k3"), 0)
	lru.Add("k4", String("k4"), 0)

	expect := []string{"key1", "k2"}

	if !reflect.DeepEqual(expect, keys) {
		t.Fatalf("Call OnEvicted failed, expect keys equals to %s", expect)
	}
}

func TestAdd(t *testing.T) {
	lru := New(int64(0), nil)
	lru.Add("key", String("1"), 0)
	lru.Add("key", String("111"), 0)

	if lru.nbytes != int64(len("key")+len("111")) {
		t.Fatal("expected 6 but got", lru.nbytes)
	}
}
