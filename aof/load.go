package aof

import (
	"io"
	"log"
	"myredis/resp"
	"os"
)

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
