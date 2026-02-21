// RDB（快照）持久化：实现 SAVE/BGSAVE 与启动加载。
//
// 说明：
// - 本项目的 rdb 文件为自定义格式（见 rdb/ 包），目标是提供“快照 + 增量 AOF”的恢复路径。
// - SAVE：同步保存（会阻塞 Actor，一般用于测试或小数据量）
// - BGSAVE：后台保存（Actor 仅负责生成快照，写文件在 goroutine 中完成）
package db

import (
	"log"
	"myredis/rdb"
	"myredis/resp"
	"os"
)

func (db *StandaloneDB) loadRdb() {
	if db.rdbFilename == "" {
		return
	}
	if _, err := os.Stat(db.rdbFilename); err != nil {
		return
	}

	entries, err := rdb.Load(db.rdbFilename)
	if err != nil {
		log.Printf("RDB load error (%s): %v", db.rdbFilename, err)
		return
	}

	// 在 Actor 线程内恢复快照，避免与 ticker/其它操作并发产生数据竞争。
	req := &commandRequest{
		fn: func() resp.Reply {
			db.applySnapshot(entries)
			return resp.OkReply
		},
		result: make(chan resp.Reply, 1),
		noAof:  true,
	}
	select {
	case <-db.closing:
		return
	case db.ops <- req:
	}
	<-req.result
}

func (db *StandaloneDB) save() resp.Reply {
	if db.rdbFilename == "" {
		return resp.MakeErrReply("ERR rdb is disabled (use --rdb to enable)")
	}
	entries, err := db.snapshotEntries()
	if err != nil {
		return resp.MakeErrReply("ERR snapshot failed: " + err.Error())
	}
	if err := rdb.Save(db.rdbFilename, entries); err != nil {
		return resp.MakeErrReply("ERR rdb save failed: " + err.Error())
	}
	return resp.OkReply
}

func (db *StandaloneDB) bgsave() resp.Reply {
	if db.rdbFilename == "" {
		return resp.MakeErrReply("ERR rdb is disabled (use --rdb to enable)")
	}

	db.rdbMu.Lock()
	if db.rdbSaving {
		db.rdbMu.Unlock()
		return resp.MakeErrReply("ERR Background save already in progress")
	}
	db.rdbSaving = true
	db.rdbMu.Unlock()

	entries, err := db.snapshotEntries()
	if err != nil {
		db.rdbMu.Lock()
		db.rdbSaving = false
		db.rdbMu.Unlock()
		return resp.MakeErrReply("ERR snapshot failed: " + err.Error())
	}

	filename := db.rdbFilename
	go func() {
		if err := rdb.Save(filename, entries); err != nil {
			log.Printf("BGSAVE error (%s): %v", filename, err)
		}
		db.rdbMu.Lock()
		db.rdbSaving = false
		db.rdbMu.Unlock()
	}()

	return resp.MakeStatusReply("Background saving started")
}
