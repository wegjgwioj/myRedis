// Cluster Ring：一致性哈希环实现。
// 目的：把 key 稳定映射到节点，实现多节点分片（减少扩容/缩容时的迁移量）。
// 说明：通过 vnodes（虚拟节点）提升负载均衡与映射稳定性。
package cluster

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// 本文件实现一致性哈希环（Consistent Hash Ring）。
// 目的：在多节点模式下，将 key 稳定地映射到某个节点，实现“分布式分片 + 透明转发”。

type Ring struct {
	vnodes int
	nodes  []string

	sortedHashes []uint32
	hashToNode   map[uint32]string
}

func NewRing(nodes []string, vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = 160
	}

	r := &Ring{
		vnodes:       vnodes,
		nodes:        append([]string(nil), nodes...),
		hashToNode:   make(map[uint32]string),
		sortedHashes: make([]uint32, 0, len(nodes)*vnodes),
	}

	for _, node := range nodes {
		for i := 0; i < vnodes; i++ {
			h := crc32.ChecksumIEEE([]byte(node + "#" + strconv.Itoa(i)))
			r.sortedHashes = append(r.sortedHashes, h)
			r.hashToNode[h] = node
		}
	}

	sort.Slice(r.sortedHashes, func(i, j int) bool { return r.sortedHashes[i] < r.sortedHashes[j] })
	return r
}

// NodeForKey 返回 key 应该落在哪个节点上。
func (r *Ring) NodeForKey(key string) string {
	if len(r.sortedHashes) == 0 {
		return ""
	}
	h := crc32.ChecksumIEEE([]byte(key))

	// 找到第一个 >= h 的位置，找不到则环形回绕到 0
	idx := sort.Search(len(r.sortedHashes), func(i int) bool { return r.sortedHashes[i] >= h })
	if idx == len(r.sortedHashes) {
		idx = 0
	}
	return r.hashToNode[r.sortedHashes[idx]]
}
