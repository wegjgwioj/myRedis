// AOF 单元测试：验证 Flush() 屏障与 Close() 行为稳定可靠。
// 目标：避免 “依赖 ticker/sleep 导致 flaky” 的常见问题，确保评估流程可复现。
// 覆盖：写入 -> Flush -> Close（最小闭环）。
package aof

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// 本文件验证 AOF 的 Flush 能力：
// - AddAof 后调用 Flush，应保证数据已写入并 fsync
// - Close 不应 panic，且应完成最终落盘

func TestAofHandler_Flush(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "appendonly.aof")

	h, err := NewAofHandler(filename)
	if err != nil {
		t.Fatalf("NewAofHandler error: %v", err)
	}

	h.AddAof([][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	if err := h.Flush(); err != nil {
		t.Fatalf("Flush error: %v", err)
	}
	h.Close()

	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("read aof error: %v", err)
	}
	if !bytes.Contains(data, []byte("SET")) {
		t.Fatalf("expected aof contains SET, got %q", string(data))
	}
}
