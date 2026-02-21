// test_client：用于手工验证的极简 TCP/RESP 客户端。
// 注意：该工具仅用于快速实验；正式评估请使用 scripts/eval.ps1 + cmd/eval_client。
// 输出：直接打印/断言响应，适合快速调试协议细节。
package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

// 一个极简 TCP/RESP 测试客户端（用于手工验证协议与服务可用性）。
// 注意：评估流程推荐使用 scripts/eval.ps1 + cmd/eval_client（更系统、更稳定）。

func main() {
	conn, err := net.Dial("tcp", "localhost:6399")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// GET
	fmt.Println("Sending GET...")
	fmt.Fprintf(conn, "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n")

	// Read $5\r\n
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)
	if line != "$5" {
		fmt.Printf("FAIL: Expected $5, got %q\n", line)
		return
	}

	// Read myval\r\n
	line, _ = reader.ReadString('\n')
	line = strings.TrimSpace(line)
	if line != "myval" {
		fmt.Printf("FAIL: Expected myval, got %q\n", line)
		return
	}
	fmt.Printf("SUCCESS: GET Response: %s\n", line)
}
