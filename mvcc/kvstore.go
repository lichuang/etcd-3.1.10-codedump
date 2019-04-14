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

package mvcc

import (
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/coreos/etcd/lease"
	"github.com/coreos/etcd/mvcc/backend"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/coreos/etcd/pkg/schedule"
	"github.com/coreos/pkg/capnslog"
	"golang.org/x/net/context"
	"runtime/debug"
)

var (
	keyBucketName  = []byte("key")
	metaBucketName = []byte("meta")

	// markedRevBytesLen is the byte length of marked revision.
	// The first `revBytesLen` bytes represents a normal revision. The last
	// one byte is the mark.
	markedRevBytesLen      = revBytesLen + 1
	markBytePosition       = markedRevBytesLen - 1
	markTombstone     byte = 't'

	consistentIndexKeyName  = []byte("consistent_index")
	scheduledCompactKeyName = []byte("scheduledCompactRev")
	finishedCompactKeyName  = []byte("finishedCompactRev")

	ErrTxnIDMismatch = errors.New("mvcc: txn id mismatch")
	ErrCompacted     = errors.New("mvcc: required revision has been compacted")
	ErrFutureRev     = errors.New("mvcc: required revision is a future revision")
	ErrCanceled      = errors.New("mvcc: watcher is canceled")

	plog = capnslog.NewPackageLogger("github.com/coreos/etcd", "mvcc")
)

// ConsistentIndexGetter is an interface that wraps the Get method.
// Consistent index is the offset of an entry in a consistent replicated log.
type ConsistentIndexGetter interface {
	// ConsistentIndex returns the consistent index of current executing entry.
	ConsistentIndex() uint64
}

type store struct {
	mu sync.Mutex // guards the following

	ig ConsistentIndexGetter

	// 保存持久化存储数据
	b       backend.Backend
	// 保存key索引
	kvindex index

	le lease.Lessor

	currentRev revision
	// the main revision of the last compaction
	compactMainRev int64

	tx        backend.BatchTx
	txnID     int64 // tracks the current txnID to verify txn operations
	txnModify bool

	// bytesBuf8 is a byte slice of length 8
	// to avoid a repetitive allocation in saveIndex.
	bytesBuf8 []byte

	// 保存修改的数据数组，用于在watch事件中通知客户端
	changes   []mvccpb.KeyValue
	fifoSched schedule.Scheduler

	stopc chan struct{}
}

// NewStore returns a new store. It is useful to create a store inside
// mvcc pkg. It should only be used for testing externally.
func NewStore(b backend.Backend, le lease.Lessor, ig ConsistentIndexGetter) *store {
	s := &store{
		b:       b,
		ig:      ig,
		kvindex: newTreeIndex(),

		le: le,

		currentRev:     revision{main: 1},
		compactMainRev: -1,

		bytesBuf8: make([]byte, 8, 8),
		fifoSched: schedule.NewFIFOScheduler(),

		stopc: make(chan struct{}),
	}

	if s.le != nil {
		s.le.SetRangeDeleter(s)
	}

	tx := s.b.BatchTx()
	tx.Lock()
	// 创建key和meta两个bucket
	tx.UnsafeCreateBucket(keyBucketName)
	tx.UnsafeCreateBucket(metaBucketName)
	tx.Unlock()
	// 强制提交事务
	s.b.ForceCommit()

	if err := s.restore(); err != nil {
		// TODO: return the error instead of panic here?
		panic("failed to recover store from backend")
	}

	return s
}

// 返回当前的main revision
func (s *store) Rev() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.currentRev.main
}

func (s *store) FirstRev() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.compactMainRev
}

func (s *store) Put(key, value []byte, lease lease.LeaseID) int64 {
	id := s.TxnBegin()
	s.put(key, value, lease)
	s.txnEnd(id)

	putCounter.Inc()

	return int64(s.currentRev.main)
}

func (s *store) Range(key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	id := s.TxnBegin()
	kvs, count, rev, err := s.rangeKeys(key, end, ro.Limit, ro.Rev, ro.Count)
	s.txnEnd(id)

	rangeCounter.Inc()

	r = &RangeResult{
		KVs:   kvs,
		Count: count,
		Rev:   rev,
	}

	return r, err
}

func (s *store) DeleteRange(key, end []byte) (n, rev int64) {
	id := s.TxnBegin()
	n = s.deleteRange(key, end)
	s.txnEnd(id)

	deleteCounter.Inc()

	return n, int64(s.currentRev.main)
}

// 一次事务开始
func (s *store) TxnBegin() int64 {
	// mu加锁是保护store本身的数据
	s.mu.Lock()
	// 一次事务开始时，将本次事务的sub revision置零
	s.currentRev.sub = 0
	s.tx = s.b.BatchTx()
	// batchtx加锁是为了保护boltdb内部的数据
	s.tx.Lock()

	// 随机分配一个事务ID
	s.txnID = rand.Int63()
	return s.txnID
}

// 一次事务结束
func (s *store) TxnEnd(txnID int64) error {
	err := s.txnEnd(txnID)
	if err != nil {
		return err
	}

	txnCounter.Inc()
	return nil
}

// txnEnd is used for unlocking an internal txn. It does
// not increase the txnCounter.
func (s *store) txnEnd(txnID int64) error {
	// 事务ID对不上
	if txnID != s.txnID {
		return ErrTxnIDMismatch
	}

	// only update index if the txn modifies the mvcc state.
	// read only txn might execute with one write txn concurrently,
	// it should not write its index to mvcc.
	// 有数据变化，保存索引
	if s.txnModify {
		s.saveIndex()
	}
	s.txnModify = false

	// 存储可以解锁了
	s.tx.Unlock()
	// 如果本次事务进行了修改，那么就将main revision + 1，表示到下一个事务
	if s.currentRev.sub != 0 {
		s.currentRev.main += 1
	}
	// sub revision置零
	s.currentRev.sub = 0

	dbTotalSize.Set(float64(s.b.Size()))
	s.mu.Unlock()
	return nil
}

// 事务range操作，，适用于txnID不是noTxn的情况
func (s *store) TxnRange(txnID int64, key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	// 事务ID对不上
	if txnID != s.txnID {
		return nil, ErrTxnIDMismatch
	}

	kvs, count, rev, err := s.rangeKeys(key, end, ro.Limit, ro.Rev, ro.Count)

	r = &RangeResult{
		KVs:   kvs,
		Count: count,
		Rev:   rev,
	}
	return r, err
}

// 事务put操作，适用于txnID不是noTxn的情况
func (s *store) TxnPut(txnID int64, key, value []byte, lease lease.LeaseID) (rev int64, err error) {
	if txnID != s.txnID {
		return 0, ErrTxnIDMismatch
	}

	s.put(key, value, lease)
	// 返回下一次事务的main revision
	return int64(s.currentRev.main + 1), nil
}

// 事务delete，适用于txnID不是noTxn的情况
func (s *store) TxnDeleteRange(txnID int64, key, end []byte) (n, rev int64, err error) {
	if txnID != s.txnID {
		return 0, 0, ErrTxnIDMismatch
	}

	n = s.deleteRange(key, end)
	if n != 0 || s.currentRev.sub != 0 {
		// 有修改操作，返回下一个事务main revision
		rev = int64(s.currentRev.main + 1)
	} else {
		rev = int64(s.currentRev.main)
	}
	return n, rev, nil
}

// 进行压缩的屏障
func (s *store) compactBarrier(ctx context.Context, ch chan struct{}) {
	if ctx == nil || ctx.Err() != nil {
		// 如果出错了
		s.mu.Lock()
		select {
		case <-s.stopc:	// 不做任何事
		default:	// 默认的行为就是继续调度调用这个函数
			f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
			s.fifoSched.Schedule(f)
		}
		s.mu.Unlock()
		return
	}
	// 只有在不出错的情况下，关闭channel返回
	close(ch)
}

// 压缩操作，将rev这个main revision之前的数据进行压缩
func (s *store) Compact(rev int64) (<-chan struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 如果小于等于compactMainRev，说明rev之前的数据已经被压缩了
	if rev <= s.compactMainRev {
		ch := make(chan struct{})
		f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
		s.fifoSched.Schedule(f)
		// 返回已经压缩的错误
		return ch, ErrCompacted
	}
	// 如果rev大于当前main revision，也是不合法的
	if rev > s.currentRev.main {
		return nil, ErrFutureRev
	}

	// 记录开始的时间
	start := time.Now()

	// 修改compactMainRev
	s.compactMainRev = rev

	rbytes := newRevBytes()
	revToBytes(revision{main: rev}, rbytes)

	tx := s.b.BatchTx()
	tx.Lock()
	// 向meta数据中记录下compact revision
	tx.UnsafePut(metaBucketName, scheduledCompactKeyName, rbytes)
	tx.Unlock()
	// ensure that desired compaction is persisted
	// 强制提交之前的数据
	s.b.ForceCommit()

	// 索引进行compact操作
	keep := s.kvindex.Compact(rev)
	ch := make(chan struct{})
	var j = func(ctx context.Context) {
		if ctx.Err() != nil {
			s.compactBarrier(ctx, ch)
			return
		}
		if !s.scheduleCompaction(rev, keep) {
			s.compactBarrier(nil, ch)
			return
		}
		close(ch)
	}

	// 调度进行scheduleCompaction操作
	s.fifoSched.Schedule(j)

	// 记录compact操作的耗时
	indexCompactionPauseDurations.Observe(float64(time.Since(start) / time.Millisecond))
	return ch, nil
}

// DefaultIgnores is a map of keys to ignore in hash checking.
var DefaultIgnores map[backend.IgnoreKey]struct{}

func init() {
	DefaultIgnores = map[backend.IgnoreKey]struct{}{
		// consistent index might be changed due to v2 internal sync, which
		// is not controllable by the user.
		{Bucket: string(metaBucketName), Key: string(consistentIndexKeyName)}: {},
	}
}

func (s *store) Hash() (uint32, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b.ForceCommit()

	h, err := s.b.Hash(DefaultIgnores)
	rev := s.currentRev.main
	return h, rev, err
}

// 提交事务
func (s *store) Commit() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.tx = s.b.BatchTx()
	s.tx.Lock()
	// 保存索引
	s.saveIndex()
	s.tx.Unlock()
	// 强制提交
	s.b.ForceCommit()
}

func (s *store) Restore(b backend.Backend) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	close(s.stopc)
	s.fifoSched.Stop()

	s.b = b
	s.kvindex = newTreeIndex()
	s.currentRev = revision{main: 1}
	s.compactMainRev = -1
	s.tx = b.BatchTx()
	s.txnID = -1
	s.fifoSched = schedule.NewFIFOScheduler()
	s.stopc = make(chan struct{})

	return s.restore()
}

func (s *store) restore() error {
	// 最大和最小 revision
	min, max := newRevBytes(), newRevBytes()
	revToBytes(revision{main: 1}, min)
	revToBytes(revision{main: math.MaxInt64, sub: math.MaxInt64}, max)

	// 保存key与lease的对应关系
	keyToLease := make(map[string]lease.LeaseID)

	// use an unordered map to hold the temp index data to speed up
	// the initial key index recovery.
	// we will convert this unordered map into the tree index later.
	unordered := make(map[string]*keyIndex, 100000)

	// restore index
	tx := s.b.BatchTx()
	tx.Lock()
	// 查询上一次压缩完成时的revision
	_, finishedCompactBytes := tx.UnsafeRange(metaBucketName, finishedCompactKeyName, nil, 0)
	if len(finishedCompactBytes) != 0 {
		// 保存到compactMainRev中
		s.compactMainRev = bytesToRev(finishedCompactBytes[0]).main
		plog.Printf("restore compact to %d", s.compactMainRev)
	}

	// TODO: limit N to reduce max memory usage
	// 查询数据
	keys, vals := tx.UnsafeRange(keyBucketName, min, max, 0)
	for i, key := range keys {
		// 反序列化
		var kv mvccpb.KeyValue
		if err := kv.Unmarshal(vals[i]); err != nil {
			plog.Fatalf("cannot unmarshal event: %v", err)
		}

		rev := bytesToRev(key[:revBytesLen])

		// restore index
		switch {
		case isTombstone(key):	// tombstone的数据
			if ki, ok := unordered[string(kv.Key)]; ok {
				ki.tombstone(rev.main, rev.sub)
			}
			// 删除key与lease的对应关系
			delete(keyToLease, string(kv.Key))

		default:	// 非tombstone的情况
			ki, ok := unordered[string(kv.Key)]
			if ok {
				// 已经存在这个键值了，所以直接put操作进行覆盖
				ki.put(rev.main, rev.sub)
			} else {
				// 否则restore，保存对应的keyIndex
				ki = &keyIndex{key: kv.Key}
				ki.restore(revision{kv.CreateRevision, 0}, rev, kv.Version)
				unordered[string(kv.Key)] = ki
			}

			if lid := lease.LeaseID(kv.Lease); lid != lease.NoLease {
				keyToLease[string(kv.Key)] = lid
			} else {
				delete(keyToLease, string(kv.Key))
			}
		}

		// update revision
		s.currentRev = rev
	}

	// restore the tree index from the unordered index.
	// 保存keyIndex索引
	for _, v := range unordered {
		s.kvindex.Insert(v)
	}

	// keys in the range [compacted revision -N, compaction] might all be deleted due to compaction.
	// the correct revision should be set to compaction revision in the case, not the largest revision
	// we have seen.
	if s.currentRev.main < s.compactMainRev {
		s.currentRev.main = s.compactMainRev
	}

	// key与lease的对应关系
	for key, lid := range keyToLease {
		if s.le == nil {
			panic("no lessor to attach lease")
		}
		err := s.le.Attach(lid, []lease.LeaseItem{{Key: key}})
		if err != nil {
			plog.Errorf("unexpected Attach error: %v", err)
		}
	}

	_, scheduledCompactBytes := tx.UnsafeRange(metaBucketName, scheduledCompactKeyName, nil, 0)
	scheduledCompact := int64(0)
	if len(scheduledCompactBytes) != 0 {
		scheduledCompact = bytesToRev(scheduledCompactBytes[0]).main
		if scheduledCompact <= s.compactMainRev {
			scheduledCompact = 0
		}
	}

	tx.Unlock()

	if scheduledCompact != 0 {
		s.Compact(scheduledCompact)
		plog.Printf("resume scheduled compaction at %d", scheduledCompact)
	}

	return nil
}

func (s *store) Close() error {
	close(s.stopc)
	s.fifoSched.Stop()
	return nil
}

// 判断两个store是否相等，对比当前revision、compact main revision、kvIndex
func (a *store) Equal(b *store) bool {
	if a.currentRev != b.currentRev {
		return false
	}
	if a.compactMainRev != b.compactMainRev {
		return false
	}
	return a.kvindex.Equal(b.kvindex)
}

// range is a keyword in Go, add Keys suffix.
func (s *store) rangeKeys(key, end []byte, limit, rangeRev int64, countOnly bool) (kvs []mvccpb.KeyValue, count int, curRev int64, err error) {
	// 当前main revision
	curRev = int64(s.currentRev.main)
	// sub revision大于0说明当前事务有修改操作
	if s.currentRev.sub > 0 {
		// 这种情况下将main revision + 1，到下一次事务ID
		curRev += 1
	}

	// 传入的revision不能大于当前的revision
	if rangeRev > curRev {
		return nil, -1, s.currentRev.main, ErrFutureRev
	}
	var rev int64
	// rev: 如果rangeRev<=0，那么取curRev；否则取rangeRev
	if rangeRev <= 0 {
		rev = curRev
	} else {
		rev = rangeRev
	}
	// 小于compactMainRev也是不合法的
	if rev < s.compactMainRev {
		return nil, -1, 0, ErrCompacted
	}

	// 从索引中根据revision和key范围取出数据范围
	_, revpairs := s.kvindex.Range(key, end, int64(rev))
	if len(revpairs) == 0 {
		return nil, 0, curRev, nil
	}
	// 如果只是想简单计数就直接返回计数就好了
	if countOnly {
		return nil, len(revpairs), curRev, nil
	}

	// 遍历前面的数据反序列化之后返回
	for _, revpair := range revpairs {
		start, end := revBytesRange(revpair)

		_, vs := s.tx.UnsafeRange(keyBucketName, start, end, 0)
		if len(vs) != 1 {
			plog.Fatalf("range cannot find rev (%d,%d)", revpair.main, revpair.sub)
		}

		var kv mvccpb.KeyValue
		if err := kv.Unmarshal(vs[0]); err != nil {
			plog.Fatalf("cannot unmarshal event: %v", err)
		}
		kvs = append(kvs, kv)
		if limit > 0 && len(kvs) >= int(limit) {
			break
		}
	}
	return kvs, len(revpairs), curRev, nil
}

func (s *store) put(key, value []byte, leaseID lease.LeaseID) {
	plog.Infof("put stack: %s", string(debug.Stack()))
	s.txnModify = true

	rev := s.currentRev.main + 1
	c := rev
	oldLease := lease.NoLease

	// if the key exists before, use its previous created and
	// get its previous leaseID
	// 检查key索引中之前是否已经存在该key
	_, created, ver, err := s.kvindex.Get(key, rev)
	if err == nil {
		// 使用之前的main版本号
		c = created.main
		// 拿到旧的lease实例
		oldLease = s.le.GetLease(lease.LeaseItem{Key: string(key)})
	}

	ibytes := newRevBytes()
	revToBytes(revision{main: rev, sub: s.currentRev.sub}, ibytes)

	ver = ver + 1
	kv := mvccpb.KeyValue{
		Key:            key,
		Value:          value,
		CreateRevision: c,
		ModRevision:    rev,
		Version:        ver,
		Lease:          int64(leaseID),
	}

	d, err := kv.Marshal()
	if err != nil {
		plog.Fatalf("cannot marshal event: %v", err)
	}

	// 写入持久化存储
	s.tx.UnsafeSeqPut(keyBucketName, ibytes, d)
	// 写入key索引
	s.kvindex.Put(key, revision{main: rev, sub: s.currentRev.sub})
	// 保存变更到changes数组中用于通知客户端
	s.changes = append(s.changes, kv)
	s.currentRev.sub += 1

	if oldLease != lease.NoLease {
		// 如果旧的lease实例存在
		if s.le == nil {
			panic("no lessor to detach lease")
		}

		// 将key从实例中解绑
		err = s.le.Detach(oldLease, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			plog.Errorf("unexpected error from lease detach: %v", err)
		}
	}

	if leaseID != lease.NoLease {
		if s.le == nil {
			panic("no lessor to attach lease")
		}

		// 然后绑定到新的lease中
		err = s.le.Attach(leaseID, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			panic("unexpected error from lease Attach")
		}
	}
}

func (s *store) deleteRange(key, end []byte) int64 {
	s.txnModify = true

	rrev := s.currentRev.main
	// 如果sub>0，说明当前已经有事务操作，将main revision指向下一个事务
	if s.currentRev.sub > 0 {
		rrev += 1
	}
	// 从内存中查询待删除的key
	keys, revs := s.kvindex.Range(key, end, rrev)

	if len(keys) == 0 {
		return 0
	}

	// 遍历进行删除
	for i, key := range keys {
		s.delete(key, revs[i])
	}
	return int64(len(keys))
}

func (s *store) delete(key []byte, rev revision) {
	mainrev := s.currentRev.main + 1

	// 对持久化存储的删除操作是往里面添加一段tombstone数据
	ibytes := newRevBytes()
	revToBytes(revision{main: mainrev, sub: s.currentRev.sub}, ibytes)
	// 追加t标识
	ibytes = appendMarkTombstone(ibytes)

	kv := mvccpb.KeyValue{
		Key: key,
	}

	d, err := kv.Marshal()
	if err != nil {
		plog.Fatalf("cannot marshal event: %v", err)
	}

	// 将序列化之后的数据写入持久化存储
	s.tx.UnsafeSeqPut(keyBucketName, ibytes, d)
	// kvIndex进行tombstone操作
	err = s.kvindex.Tombstone(key, revision{main: mainrev, sub: s.currentRev.sub})
	if err != nil {
		plog.Fatalf("cannot tombstone an existing key (%s): %v", string(key), err)
	}
	s.changes = append(s.changes, kv)
	s.currentRev.sub += 1

	item := lease.LeaseItem{Key: string(key)}
	leaseID := s.le.GetLease(item)

	if leaseID != lease.NoLease {
		// 如果这个key有关联的lease，则解绑
		err = s.le.Detach(leaseID, []lease.LeaseItem{item})
		if err != nil {
			plog.Errorf("cannot detach %v", err)
		}
	}
}

func (s *store) getChanges() []mvccpb.KeyValue {
	changes := s.changes
	s.changes = make([]mvccpb.KeyValue, 0, 4)
	return changes
}

func (s *store) saveIndex() {
	if s.ig == nil {
		return
	}
	tx := s.tx
	bs := s.bytesBuf8
	binary.BigEndian.PutUint64(bs, s.ig.ConsistentIndex())
	// put the index into the underlying backend
	// tx has been locked in TxnBegin, so there is no need to lock it again
	tx.UnsafePut(metaBucketName, consistentIndexKeyName, bs)
}

func (s *store) ConsistentIndex() uint64 {
	// TODO: cache index in a uint64 field?
	tx := s.b.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	_, vs := tx.UnsafeRange(metaBucketName, consistentIndexKeyName, nil, 0)
	if len(vs) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(vs[0])
}

// appendMarkTombstone appends tombstone mark to normal revision bytes.
// 向缓冲区添加一个tombstone标志位表示tombstone
func appendMarkTombstone(b []byte) []byte {
	if len(b) != revBytesLen {
		plog.Panicf("cannot append mark to non normal revision bytes")
	}
	return append(b, markTombstone)
}

// isTombstone checks whether the revision bytes is a tombstone.
func isTombstone(b []byte) bool {
	return len(b) == markedRevBytesLen && b[markBytePosition] == markTombstone
}

// revBytesRange returns the range of revision bytes at
// the given revision.
// 根据传入的revision返回start和end范围
func revBytesRange(rev revision) (start, end []byte) {
	start = newRevBytes()
	revToBytes(rev, start)

	end = newRevBytes()
	endRev := revision{main: rev.main, sub: rev.sub + 1}
	revToBytes(endRev, end)

	return start, end
}
