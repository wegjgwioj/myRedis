// AOF 加载模块：从 AOF 文件读取 RESP 命令并回放到 DB。
// 关键点：逐条解析 MultiBulk 命令，遇到错误要及时终止并输出可定位的信息。
// 说明：加载阶段属于启动关键路径，出错应快速失败，避免带病运行。
package aof

import (
	"io"
	"log"
	"myredis/resp"
	"os"
)

// 本文件负责 AOF 的加载与重放（replay）：
// - 启动时读取 AOF 文件
// - 解析为 RESP MultiBulk（命令数组）
// - 逐条交给上层 executor 执行（通常是 db.Exec 的内部通道版本）

// LoadAof 启动时加载 AOF 文件并重放命令
func (handler *AofHandler) LoadAof(executor func(cmd [][]byte) resp.Reply) error {
	file, err := os.Open(handler.aofFilename)
	if err != nil {
		if os.IsNotExist(err) {
			// File not exists, nothing to load.
			log.Println("AOF file not exists, starting with empty DB")
			return nil
		}
		return err
	}
	defer file.Close()

	log.Println("Loading AOF file...")

	// Replay commands
	// ParseStream creates a channel, we iterate it.
	payloads := resp.ParseStream(file)

	for payload := range payloads {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			log.Printf("AOF parse error: %v", payload.Err)
			return payload.Err
		}

		if payload.Data == nil {
			continue
		}

		// Expect MultiBulkReply (Command Array)
		multiBulk, ok := payload.Data.(*resp.MultiBulkReply)
		if !ok {
			log.Printf("AOF corruption: expected MultiBulkReply")
			continue
		}

		// Exec command using provided callback
		executor(multiBulk.Args)
	}

	log.Println("AOF load finished")
	return nil
}
