// AOF 模块：提供 Append Only File 持久化能力。
// 关键点：异步追加写命令、EverySec 定时 fsync、Flush 测试屏障、Close 时 drain 并最终落盘。
// 说明：为了让测试稳定，不依赖 sleep，这里显式提供 Flush() 等待写入+Sync 完成。
package aof

import (
	"errors"
	"log"
	"myredis/resp"
	"os"
	"sync"
	"time"
)

// 本文件实现 AOF（Append Only File）持久化：
// - AddAof：将写命令追加到内存队列（异步写入）
// - EverySec：后台每秒 fsync，兼顾性能与可靠性
// - Flush：测试/评估用的“强制落盘屏障”，避免依赖 sleep 导致 flaky

type aofTask struct {
	payload   *resp.MultiBulkReply
	flushDone chan struct{}
	// startRewrite / finishRewrite / abortRewrite 都通过同一个 aofChan 串行化到写协程中，避免并发复杂度。
	startRewriteDone chan error
	finishRewrite    *finishRewriteTask
	abortRewriteDone chan struct{}
}

type finishRewriteTask struct {
	tmpFilename string
	done        chan error
}

// AofHandler AOF 持久化处理器
type AofHandler struct {
	aofFile     *os.File
	aofFilename string
	aofChan     chan *aofTask
	mu          sync.Mutex
	chMu        sync.Mutex
	closed      bool
	wg          sync.WaitGroup

	// rewrite 状态只在 handleAof 写协程中读写（通过 task 串行化），无需额外锁。
	rewriting  bool
	rewriteBuf [][]byte
}

func NewAofHandler(filename string) (*AofHandler, error) {
	handler := &AofHandler{
		aofFilename: filename,
		aofChan:     make(chan *aofTask, 1000),
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

// Filename 返回当前 AOF 文件名。
func (handler *AofHandler) Filename() string { return handler.aofFilename }

// AddAof 将写命令写入缓冲区
func (handler *AofHandler) AddAof(args [][]byte) {
	task := &aofTask{payload: resp.MakeMultiBulkReply(args)}

	// Close() 可能与 AddAof 并发发生；用 chMu 保证不会向已关闭 channel 写入导致 panic。
	handler.chMu.Lock()
	defer handler.chMu.Unlock()
	if handler.closed {
		return
	}
	handler.aofChan <- task
}

// StartRewrite 通知 AOF 写协程开始进入 “rewrite 缓冲” 模式。
// 语义：在 StartRewrite 被写协程处理后，后续 AddAof 写入的命令会被额外缓存在 rewriteBuf 中，供 FinishRewrite 追加到新文件。
func (handler *AofHandler) StartRewrite() error {
	done := make(chan error, 1)

	handler.chMu.Lock()
	if handler.closed {
		handler.chMu.Unlock()
		return errors.New("aof handler closed")
	}
	handler.aofChan <- &aofTask{startRewriteDone: done}
	handler.chMu.Unlock()

	return <-done
}

// AbortRewrite 取消 rewrite 模式并清空 rewrite buffer（用于后台重写失败的收尾）。
func (handler *AofHandler) AbortRewrite() error {
	done := make(chan struct{})

	handler.chMu.Lock()
	if handler.closed {
		handler.chMu.Unlock()
		return errors.New("aof handler closed")
	}
	handler.aofChan <- &aofTask{abortRewriteDone: done}
	handler.chMu.Unlock()

	<-done
	return nil
}

// FinishRewrite 将 rewrite buffer 追加到 tmpFilename，并原子替换当前 AOF 文件。
// 注意：必须在 StartRewrite 之后调用。
func (handler *AofHandler) FinishRewrite(tmpFilename string) error {
	done := make(chan error, 1)

	handler.chMu.Lock()
	if handler.closed {
		handler.chMu.Unlock()
		return errors.New("aof handler closed")
	}
	handler.aofChan <- &aofTask{
		finishRewrite: &finishRewriteTask{tmpFilename: tmpFilename, done: done},
	}
	handler.chMu.Unlock()

	return <-done
}

// Flush 强制将当前队列中的内容写入并 fsync（阻塞直到完成）。
// 用于单测/评估流程，避免依赖 ticker + sleep 造成不稳定。
func (handler *AofHandler) Flush() error {
	done := make(chan struct{})

	handler.chMu.Lock()
	if handler.closed {
		handler.chMu.Unlock()
		return errors.New("aof handler closed")
	}
	handler.aofChan <- &aofTask{flushDone: done}
	handler.chMu.Unlock()

	<-done
	return nil
}

// handleAof 后台刷盘协程
func (handler *AofHandler) handleAof() {
	// Ticker for fsync
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case task, ok := <-handler.aofChan:
			if !ok {
				return // Channel closed, drain done (loop finishes)
			}
			if task.payload != nil {
				handler.mu.Lock()
				data := task.payload.ToBytes()
				_, err := handler.aofFile.Write(data)
				if err != nil {
					log.Printf("AOF write error: %v", err)
				}
				// rewrite 模式下，额外记录这条命令（保证顺序与落盘顺序一致）。
				if handler.rewriting {
					handler.rewriteBuf = append(handler.rewriteBuf, data)
				}
				handler.mu.Unlock()
			}

			if task.startRewriteDone != nil {
				// 只有写协程会读写 rewriting 状态，因此这里不需要额外锁。
				if handler.rewriting {
					task.startRewriteDone <- errors.New("rewrite already in progress")
				} else {
					handler.rewriting = true
					handler.rewriteBuf = handler.rewriteBuf[:0]
					task.startRewriteDone <- nil
				}
			}

			if task.abortRewriteDone != nil {
				handler.rewriting = false
				handler.rewriteBuf = handler.rewriteBuf[:0]
				close(task.abortRewriteDone)
			}

			if task.finishRewrite != nil {
				err := handler.finishRewrite(task.finishRewrite.tmpFilename)
				task.finishRewrite.done <- err
			}

			// flush 屏障：保证在它之前入队的 payload 都已经写入文件，然后做一次 Sync
			if task.flushDone != nil {
				handler.mu.Lock()
				_ = handler.aofFile.Sync()
				handler.mu.Unlock()
				close(task.flushDone)
			}
		case <-ticker.C:
			handler.mu.Lock()
			_ = handler.aofFile.Sync()
			handler.mu.Unlock()
		}
	}
}

func (handler *AofHandler) finishRewrite(tmpFilename string) error {
	if !handler.rewriting {
		return errors.New("rewrite not started")
	}
	if tmpFilename == "" {
		return errors.New("empty tmp filename")
	}

	// 1) 追加 rewrite buffer 到 tmp 文件
	tmpFile, err := os.OpenFile(tmpFilename, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	for _, data := range handler.rewriteBuf {
		if _, err := tmpFile.Write(data); err != nil {
			_ = tmpFile.Close()
			return err
		}
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	// 2) 原子替换：关闭旧文件 -> rename -> reopen
	handler.mu.Lock()
	defer handler.mu.Unlock()

	_ = handler.aofFile.Sync()
	_ = handler.aofFile.Close()

	// Windows rename 不能覆盖，先删再改名
	_ = os.Remove(handler.aofFilename)
	if err := os.Rename(tmpFilename, handler.aofFilename); err != nil {
		// 尝试恢复：重新打开旧文件（即使 rename 失败，旧文件可能仍在）
		file, openErr := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
		if openErr == nil {
			handler.aofFile = file
		}
		return err
	}

	file, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	handler.aofFile = file

	// 3) 清理状态
	handler.rewriting = false
	handler.rewriteBuf = handler.rewriteBuf[:0]
	return nil
}

func (handler *AofHandler) Close() {
	handler.chMu.Lock()
	if handler.closed {
		handler.chMu.Unlock()
		return
	}
	handler.closed = true
	close(handler.aofChan) // Close channel
	handler.chMu.Unlock()

	handler.wg.Wait() // Wait for background routine to finish draining

	// Final sync and close
	handler.mu.Lock()
	defer handler.mu.Unlock()
	_ = handler.aofFile.Sync()
	_ = handler.aofFile.Close()
}
