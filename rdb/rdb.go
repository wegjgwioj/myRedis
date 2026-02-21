// rdb 包实现一个“快照文件”（类似 Redis 的 RDB，但格式为本项目自定义）。
//
// 设计目标：
// - 启动加载更快：相比 AOF 需要重放大量命令，RDB 直接恢复内存状态。
// - 可作为复制/故障恢复的基础能力：后续做 replication 时可复用 LoadFromReader/SaveToWriter。
//
// 注意：
// - 这里不追求 100% 兼容 Redis 官方 RDB 格式（那会非常复杂且需要大量兼容测试）。
// - 只覆盖当前项目支持的数据类型：String/List/Hash/Set，并携带绝对过期时间（UnixMilli）。
package rdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	// magicHeader 用于快速识别文件类型与版本。
	magicHeader = "MYRDB1"
)

// EntryType 表示一个 key 的数据类型。
type EntryType uint8

const (
	TypeString EntryType = 1
	TypeList   EntryType = 2
	TypeHash   EntryType = 3
	TypeSet    EntryType = 4
)

// Entry 表示快照中的一个键值条目。
//
// ExpireAtUnixMs：
// - 0 表示不过期
// - >0 表示绝对过期时间（UnixMilli），用于保证“重启不续命”
type Entry struct {
	Key            string
	Type           EntryType
	ExpireAtUnixMs int64

	// 数据按类型落在不同字段中；未使用的字段保持空。
	String []byte
	List   [][]byte
	Hash   map[string][]byte
	Set    []string
}

// Save 将 entries 写入 filename（使用 tmp 文件 + 原子替换）。
func Save(filename string, entries []Entry) error {
	if filename == "" {
		return errors.New("empty rdb filename")
	}
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		return err
	}

	tmp := filename + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	buf := bufio.NewWriterSize(f, 256*1024)

	if err := SaveToWriter(buf, entries); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := buf.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	// Windows 上 Rename 不能覆盖已存在文件，因此先删除再改名。
	_ = os.Remove(filename)
	return os.Rename(tmp, filename)
}

// Load 从 filename 读取并返回 entries。
func Load(filename string) ([]Entry, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return LoadFromReader(bufio.NewReaderSize(f, 256*1024))
}

// SaveToWriter 将 entries 写入 w。
func SaveToWriter(w io.Writer, entries []Entry) error {
	// 为了让输出更稳定可比较，这里按 key 排序（不会影响语义）。
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

	if _, err := io.WriteString(w, magicHeader); err != nil {
		return err
	}
	createdAt := time.Now().UnixMilli()
	if err := writeInt64(w, createdAt); err != nil {
		return err
	}
	if err := writeUint32(w, uint32(len(entries))); err != nil {
		return err
	}

	for _, e := range entries {
		if err := writeUint8(w, uint8(e.Type)); err != nil {
			return err
		}
		if err := writeString(w, e.Key); err != nil {
			return err
		}
		if err := writeInt64(w, e.ExpireAtUnixMs); err != nil {
			return err
		}

		switch e.Type {
		case TypeString:
			if err := writeBytes(w, e.String); err != nil {
				return err
			}
		case TypeList:
			if err := writeUint32(w, uint32(len(e.List))); err != nil {
				return err
			}
			for _, b := range e.List {
				if err := writeBytes(w, b); err != nil {
					return err
				}
			}
		case TypeHash:
			if e.Hash == nil {
				if err := writeUint32(w, 0); err != nil {
					return err
				}
				break
			}
			fields := make([]string, 0, len(e.Hash))
			for k := range e.Hash {
				fields = append(fields, k)
			}
			sort.Strings(fields)
			if err := writeUint32(w, uint32(len(fields))); err != nil {
				return err
			}
			for _, field := range fields {
				if err := writeString(w, field); err != nil {
					return err
				}
				if err := writeBytes(w, e.Hash[field]); err != nil {
					return err
				}
			}
		case TypeSet:
			members := append([]string(nil), e.Set...)
			sort.Strings(members)
			if err := writeUint32(w, uint32(len(members))); err != nil {
				return err
			}
			for _, m := range members {
				if err := writeString(w, m); err != nil {
					return err
				}
			}
		default:
			return errors.New("unknown entry type")
		}
	}

	return nil
}

// LoadFromReader 从 r 读取并返回 entries。
func LoadFromReader(r io.Reader) ([]Entry, error) {
	header := make([]byte, len(magicHeader))
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	if string(header) != magicHeader {
		return nil, errors.New("invalid rdb header")
	}

	_, err := readInt64(r) // createdAt（暂不使用）
	if err != nil {
		return nil, err
	}

	n, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, n)

	for i := uint32(0); i < n; i++ {
		typ, err := readUint8(r)
		if err != nil {
			return nil, err
		}
		key, err := readString(r)
		if err != nil {
			return nil, err
		}
		expireAt, err := readInt64(r)
		if err != nil {
			return nil, err
		}

		e := Entry{Key: key, Type: EntryType(typ), ExpireAtUnixMs: expireAt}

		switch e.Type {
		case TypeString:
			b, err := readBytes(r)
			if err != nil {
				return nil, err
			}
			e.String = b
		case TypeList:
			cnt, err := readUint32(r)
			if err != nil {
				return nil, err
			}
			e.List = make([][]byte, 0, cnt)
			for j := uint32(0); j < cnt; j++ {
				b, err := readBytes(r)
				if err != nil {
					return nil, err
				}
				e.List = append(e.List, b)
			}
		case TypeHash:
			cnt, err := readUint32(r)
			if err != nil {
				return nil, err
			}
			e.Hash = make(map[string][]byte, cnt)
			for j := uint32(0); j < cnt; j++ {
				field, err := readString(r)
				if err != nil {
					return nil, err
				}
				val, err := readBytes(r)
				if err != nil {
					return nil, err
				}
				e.Hash[field] = val
			}
		case TypeSet:
			cnt, err := readUint32(r)
			if err != nil {
				return nil, err
			}
			e.Set = make([]string, 0, cnt)
			for j := uint32(0); j < cnt; j++ {
				m, err := readString(r)
				if err != nil {
					return nil, err
				}
				e.Set = append(e.Set, m)
			}
		default:
			return nil, errors.New("unknown entry type")
		}

		entries = append(entries, e)
	}

	return entries, nil
}

func writeUint8(w io.Writer, v uint8) error {
	var b [1]byte
	b[0] = v
	_, err := w.Write(b[:])
	return err
}

func writeUint32(w io.Writer, v uint32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeInt64(w io.Writer, v int64) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(v))
	_, err := w.Write(b[:])
	return err
}

func writeString(w io.Writer, s string) error {
	if err := writeUint32(w, uint32(len(s))); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

func writeBytes(w io.Writer, b []byte) error {
	if b == nil {
		return writeUint32(w, 0)
	}
	if err := writeUint32(w, uint32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func readUint8(r io.Reader) (uint8, error) {
	var b [1]byte
	_, err := io.ReadFull(r, b[:])
	return b[0], err
}

func readUint32(r io.Reader) (uint32, error) {
	var b [4]byte
	_, err := io.ReadFull(r, b[:])
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b[:]), nil
}

func readInt64(r io.Reader) (int64, error) {
	var b [8]byte
	_, err := io.ReadFull(r, b[:])
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b[:])), nil
}

func readString(r io.Reader) (string, error) {
	n, err := readUint32(r)
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readBytes(r io.Reader) ([]byte, error) {
	n, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
