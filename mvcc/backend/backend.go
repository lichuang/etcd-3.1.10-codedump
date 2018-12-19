// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltdb/bolt"
	"github.com/coreos/pkg/capnslog"
)

var (
	defaultBatchLimit    = 10000
	defaultBatchInterval = 100 * time.Millisecond

	defragLimit = 10000

	// InitialMmapSize is the initial size of the mmapped region. Setting this larger than
	// the potential max db size can prevent writer from blocking reader.
	// This only works for linux.
	InitialMmapSize = int64(10 * 1024 * 1024 * 1024)

	plog = capnslog.NewPackageLogger("github.com/coreos/etcd", "mvcc/backend")
)

const (
	// DefaultQuotaBytes is the number of bytes the backend Size may
	// consume before exceeding the space quota.
	DefaultQuotaBytes = int64(2 * 1024 * 1024 * 1024) // 2GB
	// MaxQuotaBytes is the maximum number of bytes suggested for a backend
	// quota. A larger quota may lead to degraded performance.
	MaxQuotaBytes = int64(8 * 1024 * 1024 * 1024) // 8GB
)

type Backend interface {
	BatchTx() BatchTx
	Snapshot() Snapshot
	Hash(ignores map[IgnoreKey]struct{}) (uint32, error)
	// Size returns the current size of the backend.
	Size() int64
	Defrag() error
	ForceCommit()
	Close() error
}

type Snapshot interface {
	// Size gets the size of the snapshot.
	Size() int64
	// WriteTo writes the snapshot into the given writer.
	WriteTo(w io.Writer) (n int64, err error)
	// Close closes the snapshot.
	Close() error
}

type backend struct {
	// size and commits are used with atomic operations so they must be
	// 64-bit aligned, otherwise 32-bit tests will crash

	// size is the number of bytes in the backend
	// 存储数据的字节数
	size int64
	// commits counts number of commits since start
	// commit数量
	commits int64

	mu sync.RWMutex
	// 实际存储数据的boltdb实例
	db *bolt.DB

	// 批量提交的时间间隔
	batchInterval time.Duration
	// 批量提交的阈值
	batchLimit    int
	batchTx       *batchTx

	stopc chan struct{}
	donec chan struct{}
}

func New(path string, d time.Duration, limit int) Backend {
	return newBackend(path, d, limit)
}

func NewDefaultBackend(path string) Backend {
	return newBackend(path, defaultBatchInterval, defaultBatchLimit)
}

func newBackend(path string, d time.Duration, limit int) *backend {
	db, err := bolt.Open(path, 0600, boltOpenOptions)
	if err != nil {
		plog.Panicf("cannot open database at %s (%v)", path, err)
	}

	b := &backend{
		db: db,

		batchInterval: d,
		batchLimit:    limit,

		stopc: make(chan struct{}),
		donec: make(chan struct{}),
	}
	b.batchTx = newBatchTx(b)
	go b.run()
	return b
}

// BatchTx returns the current batch tx in coalescer. The tx can be used for read and
// write operations. The write result can be retrieved within the same tx immediately.
// The write result is isolated with other txs until the current one get committed.
func (b *backend) BatchTx() BatchTx {
	return b.batchTx
}

// ForceCommit forces the current batching tx to commit.
func (b *backend) ForceCommit() {
	b.batchTx.Commit()
}

func (b *backend) Snapshot() Snapshot {
	b.batchTx.Commit()

	b.mu.RLock()
	defer b.mu.RUnlock()
	tx, err := b.db.Begin(false)
	if err != nil {
		plog.Fatalf("cannot begin tx (%s)", err)
	}
	return &snapshot{tx}
}

type IgnoreKey struct {
	Bucket string
	Key    string
}

func (b *backend) Hash(ignores map[IgnoreKey]struct{}) (uint32, error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	b.mu.RLock()
	defer b.mu.RUnlock()
	err := b.db.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()
		for next, _ := c.First(); next != nil; next, _ = c.Next() {
			b := tx.Bucket(next)
			if b == nil {
				return fmt.Errorf("cannot get hash of bucket %s", string(next))
			}
			h.Write(next)
			b.ForEach(func(k, v []byte) error {
				bk := IgnoreKey{Bucket: string(next), Key: string(k)}
				if _, ok := ignores[bk]; !ok {
					h.Write(k)
					h.Write(v)
				}
				return nil
			})
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	return h.Sum32(), nil
}

func (b *backend) Size() int64 {
	return atomic.LoadInt64(&b.size)
}

func (b *backend) run() {
	defer close(b.donec)
	// 定时提交数据落盘
	t := time.NewTimer(b.batchInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-b.stopc:
			b.batchTx.CommitAndStop()
			return
		}
		b.batchTx.Commit()
		t.Reset(b.batchInterval)
	}
}

func (b *backend) Close() error {
	close(b.stopc)
	<-b.donec
	return b.db.Close()
}

// Commits returns total number of commits since start
// 返回当前提交事务数量
func (b *backend) Commits() int64 {
	return atomic.LoadInt64(&b.commits)
}

// 进行碎片整理，提高其中bucket的填充率。
// 整理碎片操作通过创建新的blotdb数据库文件并将旧数据库文件的数据写入新数据库文件中。
// 因为新文件是顺序写入的，所以会提高填充率（FillPercent）
// 整理过程中，需要加锁
func (b *backend) Defrag() error {
	// 碎片整理
	err := b.defrag()
	if err != nil {
		return err
	}

	// commit to update metadata like db.size
	// 整理碎片完成后马上进行一次事务提交操作
	b.batchTx.Commit()

	return nil
}

func (b *backend) defrag() error {
	// TODO: make this non-blocking?
	// lock batchTx to ensure nobody is using previous tx, and then
	// close previous ongoing tx.
	b.batchTx.Lock()
	defer b.batchTx.Unlock()

	// lock database after lock tx to avoid deadlock.
	b.mu.Lock()
	defer b.mu.Unlock()

	// 提交当前的事务，由于传入了true参数，所以提交后不会立即打开新的读写事务
	b.batchTx.commit(true)
	b.batchTx.tx = nil

	// 创建一个boltdb临时文件
	tmpdb, err := bolt.Open(b.db.Path()+".tmp", 0600, boltOpenOptions)
	if err != nil {
		return err
	}

	// 进行碎片整理操作
	err = defragdb(b.db, tmpdb, defragLimit)

	if err != nil {
		tmpdb.Close()
		os.RemoveAll(tmpdb.Path())
		return err
	}

	// 旧文件的文件路径
	dbp := b.db.Path()
	// 新文件的文件路径
	tdbp := tmpdb.Path()

	err = b.db.Close()
	if err != nil {
		plog.Fatalf("cannot close database (%s)", err)
	}
	err = tmpdb.Close()
	if err != nil {
		plog.Fatalf("cannot close database (%s)", err)
	}
	// 重命名数据库文件，用新的覆盖旧的文件
	err = os.Rename(tdbp, dbp)
	if err != nil {
		plog.Fatalf("cannot rename database (%s)", err)
	}

	// 重新创建bolt.db实例，此时使用的数据库文件就是整理过后的文件
	b.db, err = bolt.Open(dbp, 0600, boltOpenOptions)
	if err != nil {
		plog.Panicf("cannot open database at %s (%v)", dbp, err)
	}
	// 开启新的读写事务以及只读事务
	b.batchTx.tx, err = b.db.Begin(true)
	if err != nil {
		plog.Fatalf("cannot begin tx (%s)", err)
	}

	return nil
}

func defragdb(odb, tmpdb *bolt.DB, limit int) error {
	// open a tx on tmpdb for writes
	// 在新数据库上开启一个读写事务
	tmptx, err := tmpdb.Begin(true)
	if err != nil {
		return err
	}

	// open a tx on old db for read
	// 旧数据上开启一个只读事务
	tx, err := odb.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	c := tx.Cursor()

	count := 0
	// 遍历旧数据库实例
	for next, _ := c.First(); next != nil; next, _ = c.Next() {
		// 获取bucket
		b := tx.Bucket(next)
		if b == nil {
			return fmt.Errorf("backend: cannot defrag bucket %s", string(next))
		}

		// 在新数据库上创建
		tmpb, berr := tmptx.CreateBucketIfNotExists(next)
		tmpb.FillPercent = 0.9 // for seq write in for each
		if berr != nil {
			return berr
		}

		b.ForEach(func(k, v []byte) error {
			count++
			if count > limit {
				err = tmptx.Commit()
				if err != nil {
					return err
				}
				tmptx, err = tmpdb.Begin(true)
				if err != nil {
					return err
				}
				tmpb = tmptx.Bucket(next)
				// 把填充比例设置成0.9，提高填充比例
				// 因为是顺序写入新数据库的
				tmpb.FillPercent = 0.9 // for seq write in for each

				count = 0
			}
			return tmpb.Put(k, v)
		})
	}

	return tmptx.Commit()
}

// NewTmpBackend creates a backend implementation for testing.
func NewTmpBackend(batchInterval time.Duration, batchLimit int) (*backend, string) {
	dir, err := ioutil.TempDir(os.TempDir(), "etcd_backend_test")
	if err != nil {
		plog.Fatal(err)
	}
	tmpPath := filepath.Join(dir, "database")
	return newBackend(tmpPath, batchInterval, batchLimit), tmpPath
}

func NewDefaultTmpBackend() (*backend, string) {
	return NewTmpBackend(defaultBatchInterval, defaultBatchLimit)
}

type snapshot struct {
	// 继承自bolt.Tx Size和WriteTo使用它的实现
	*bolt.Tx
}

func (s *snapshot) Close() error { return s.Tx.Rollback() }
