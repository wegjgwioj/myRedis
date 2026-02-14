package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

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
