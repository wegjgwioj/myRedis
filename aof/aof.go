package aof

import (
	"log"
	"myredis/resp"
	"os"
	"sync"
	"time"
)

// AofHandler AOF 持久化处理器
type AofHandler struct {
	aofFile     *os.File
	aofFilename string
	aofChan     chan *resp.MultiBulkReply
	mu          sync.Mutex
	wg          sync.WaitGroup
}

func NewAofHandler(filename string) (*AofHandler, error) {
	handler := &AofHandler{
		aofFilename: filename,
		aofChan:     make(chan *resp.MultiBulkReply, 1000),
	}

	// Open file (append mode)
	file, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = file

	// Start background routine
	handler.wg.Add(1)
	go func() {
		defer handler.wg.Done()
		handler.handleAof()
	}()

	return handler, nil
}

// AddAof 将写命令写入缓冲区
func (handler *AofHandler) AddAof(args [][]byte) {
	payload := resp.MakeMultiBulkReply(args)
	handler.aofChan <- payload
}

// handleAof 后台刷盘协程
func (handler *AofHandler) handleAof() {
	// Ticker for fsync
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case payload, ok := <-handler.aofChan:
			if !ok {
				return // Channel closed, drain done (loop finishes)
			}
			handler.mu.Lock()
			_, err := handler.aofFile.Write(payload.ToBytes())
			if err != nil {
				log.Printf("AOF write error: %v", err)
			}
			handler.mu.Unlock()
		case <-ticker.C:
			handler.mu.Lock()
			handler.aofFile.Sync()
			handler.mu.Unlock()
		}
	}
}

func (handler *AofHandler) Close() {
	close(handler.aofChan) // Close channel
	handler.wg.Wait()      // Wait for background routine to finish draining

	// Final sync and close
	handler.mu.Lock()
	defer handler.mu.Unlock()
	handler.aofFile.Sync()
	handler.aofFile.Close()
}
