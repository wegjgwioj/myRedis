// DB 类型定义：集中放置 Value/数据结构抽象、配置项等基础类型。
// 说明：上层模块通过这些类型解耦命令实现与缓存/持久化细节。
// 备注：尽量保持该文件轻量，避免引入重依赖导致循环引用或耦合膨胀。
package db

import (
	"container/list"
)

// DataEntity 适配 mygocache/lru.Value 接口
// 用于提供数据大小给 LFU/LRU 算法计算内存占用
type DataEntity interface {
	Len() int
}

// String
type StringData []byte

func (d StringData) Len() int {
	return len(d)
}

// List
type ListData struct {
	L *list.List
}

func (d ListData) Len() int {
	// 粗略估算：节点头大小 × 长度 + 元素内容大小
	// 这里简单假设每个节点头部 16 字节
	if d.L == nil {
		return 0
	}
	size := 0
	for e := d.L.Front(); e != nil; e = e.Next() {
		if b, ok := e.Value.([]byte); ok {
			size += len(b)
		}
		size += 16
	}
	return size
}

// Hash
type HashData map[string][]byte

func (d HashData) Len() int {
	// 粗略估算
	size := 0
	for k, v := range d {
		size += len(k) + len(v) + 16 // 16 overhead
	}
	return size
}

// Set
type SetData map[string]struct{}

func (d SetData) Len() int {
	size := 0
	for k := range d {
		size += len(k) + 16
	}
	return size
}
