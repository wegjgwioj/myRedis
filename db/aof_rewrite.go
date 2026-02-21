// AOF rewrite（重写/压缩）实现：提供 REWRITEAOF（同步）与 BGREWRITEAOF（后台）能力。
//
// 为什么需要 AOF rewrite：
// - AOF 是“追加日志”，运行久了会变大；重写可以把“历史命令”压缩成“重建当前状态所需的最小命令集”。
// - 同时也为后续 replication 的 full sync 提供基础（用快照重建状态）。
//
// 实现要点（对齐 Redis 思路，但简化）：
// - StartRewrite：通知 AOF 写协程开始缓冲后续写命令（rewrite buffer）
// - 生成快照：在 Actor 线程深拷贝当前数据
// - 后台写入：用快照生成临时 AOF 文件
// - FinishRewrite：在 Actor 线程触发“追加 rewrite buffer + 原子替换 + reopen”
package db

import (
	"bufio"
	"errors"
	"fmt"
	"myredis/rdb"
	"myredis/resp"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

type aofRewriteResult struct {
	tmpFilename string
	err         error
}

func (db *StandaloneDB) rewriteaof() resp.Reply {
	if db.aofHandler == nil {
		return resp.MakeErrReply("ERR aof is disabled")
	}
	if db.aofRewriting {
		return resp.MakeErrReply("ERR Background append only file rewriting already in progress")
	}
	db.aofRewriting = true
	defer func() { db.aofRewriting = false }()

	if err := db.aofHandler.StartRewrite(); err != nil {
		return resp.MakeErrReply("ERR start rewrite failed: " + err.Error())
	}

	entries, err := db.snapshotEntries()
	if err != nil {
		_ = db.aofHandler.AbortRewrite()
		return resp.MakeErrReply("ERR snapshot failed: " + err.Error())
	}

	tmp := makeAofTmpFilename(db.aofHandler.Filename())
	if err := writeAofFromSnapshot(tmp, entries); err != nil {
		_ = db.aofHandler.AbortRewrite()
		_ = os.Remove(tmp)
		return resp.MakeErrReply("ERR rewrite write failed: " + err.Error())
	}

	if err := db.aofHandler.FinishRewrite(tmp); err != nil {
		_ = db.aofHandler.AbortRewrite()
		_ = os.Remove(tmp)
		return resp.MakeErrReply("ERR rewrite finish failed: " + err.Error())
	}
	return resp.OkReply
}

func (db *StandaloneDB) bgrewriteaof() resp.Reply {
	if db.aofHandler == nil {
		return resp.MakeErrReply("ERR aof is disabled")
	}
	if db.aofRewriting {
		return resp.MakeErrReply("ERR Background append only file rewriting already in progress")
	}
	db.aofRewriting = true

	if err := db.aofHandler.StartRewrite(); err != nil {
		db.aofRewriting = false
		return resp.MakeErrReply("ERR start rewrite failed: " + err.Error())
	}

	entries, err := db.snapshotEntries()
	if err != nil {
		_ = db.aofHandler.AbortRewrite()
		db.aofRewriting = false
		return resp.MakeErrReply("ERR snapshot failed: " + err.Error())
	}

	tmp := makeAofTmpFilename(db.aofHandler.Filename())
	go func() {
		err := writeAofFromSnapshot(tmp, entries)
		db.aofRewriteDone <- aofRewriteResult{tmpFilename: tmp, err: err}
	}()

	return resp.MakeStatusReply("Background append only file rewriting started")
}

// handleAofRewriteDone 在 Actor 线程调用：负责把后台生成的 tmp 文件与 rewrite buffer 合并并原子替换。
func (db *StandaloneDB) handleAofRewriteDone(done aofRewriteResult) {
	if !db.aofRewriting || db.aofHandler == nil {
		_ = os.Remove(done.tmpFilename)
		return
	}

	if done.err != nil {
		_ = db.aofHandler.AbortRewrite()
		db.aofRewriting = false
		_ = os.Remove(done.tmpFilename)
		return
	}

	if err := db.aofHandler.FinishRewrite(done.tmpFilename); err != nil {
		_ = db.aofHandler.AbortRewrite()
		db.aofRewriting = false
		_ = os.Remove(done.tmpFilename)
		return
	}

	db.aofRewriting = false
}

func makeAofTmpFilename(aofFilename string) string {
	dir := filepath.Dir(aofFilename)
	base := filepath.Base(aofFilename)
	return filepath.Join(dir, fmt.Sprintf(".%s.rewrite.%d.tmp", base, time.Now().UnixNano()))
}

func writeAofFromSnapshot(tmpFilename string, entries []rdb.Entry) error {
	if tmpFilename == "" {
		return errors.New("empty tmp filename")
	}
	if err := os.MkdirAll(filepath.Dir(tmpFilename), 0o755); err != nil {
		return err
	}

	f, err := os.OpenFile(tmpFilename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 256*1024)

	// 为了输出稳定，按 key 排序（不影响语义）。
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

	for _, e := range entries {
		cmds, err := snapshotEntryToCommands(e)
		if err != nil {
			return err
		}
		for _, cmd := range cmds {
			if _, err := w.Write(resp.MakeMultiBulkReply(cmd).ToBytes()); err != nil {
				return err
			}
		}
	}

	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func snapshotEntryToCommands(e rdb.Entry) ([][][]byte, error) {
	const batch = 512
	key := []byte(e.Key)

	var out [][][]byte

	switch e.Type {
	case rdb.TypeString:
		out = append(out, [][]byte{[]byte("SET"), key, e.String})
	case rdb.TypeList:
		// 为了重建顺序，按从左到右的顺序 RPUSH。
		for i := 0; i < len(e.List); i += batch {
			end := i + batch
			if end > len(e.List) {
				end = len(e.List)
			}
			cmd := make([][]byte, 0, 2+(end-i))
			cmd = append(cmd, []byte("RPUSH"), key)
			cmd = append(cmd, e.List[i:end]...)
			out = append(out, cmd)
		}
	case rdb.TypeHash:
		fields := make([]string, 0, len(e.Hash))
		for f := range e.Hash {
			fields = append(fields, f)
		}
		sort.Strings(fields)
		pairs := make([][]byte, 0, len(fields)*2)
		for _, f := range fields {
			pairs = append(pairs, []byte(f))
			pairs = append(pairs, e.Hash[f])
		}
		for i := 0; i < len(pairs); i += batch * 2 {
			end := i + batch*2
			if end > len(pairs) {
				end = len(pairs)
			}
			cmd := make([][]byte, 0, 2+(end-i))
			cmd = append(cmd, []byte("HSET"), key)
			cmd = append(cmd, pairs[i:end]...)
			out = append(out, cmd)
		}
	case rdb.TypeSet:
		members := append([]string(nil), e.Set...)
		sort.Strings(members)
		for i := 0; i < len(members); i += batch {
			end := i + batch
			if end > len(members) {
				end = len(members)
			}
			cmd := make([][]byte, 0, 2+(end-i))
			cmd = append(cmd, []byte("SADD"), key)
			for _, m := range members[i:end] {
				cmd = append(cmd, []byte(m))
			}
			out = append(out, cmd)
		}
	default:
		return nil, errors.New("unknown snapshot entry type")
	}

	if e.ExpireAtUnixMs > 0 {
		out = append(out, [][]byte{
			[]byte("PEXPIREAT"),
			key,
			[]byte(strconv.FormatInt(e.ExpireAtUnixMs, 10)),
		})
	}
	return out, nil
}
