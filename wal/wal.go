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

package wal

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/wal/walpb"

	"github.com/coreos/pkg/capnslog"
)

const (
	// 以下是WAL存放的数据类型
	// 元数据
	metadataType int64 = iota + 1
	// 日志数据
	entryType
	// 状态数据
	stateType
	// 校验数据
	crcType
	// 快照数据
	snapshotType

	// warnSyncDuration is the amount of time allotted to an fsync before
	// logging a warning
	warnSyncDuration = time.Second
)

var (
	// SegmentSizeBytes is the preallocated size of each wal segment file.
	// The actual size might be larger than this. In general, the default
	// value should be used, but this is defined as an exported variable
	// so that tests can set a different segment size.
	SegmentSizeBytes int64 = 64 * 1000 * 1000 // 64MB

	plog = capnslog.NewPackageLogger("github.com/coreos/etcd", "wal")

	ErrMetadataConflict = errors.New("wal: conflicting metadata found")
	ErrFileNotFound     = errors.New("wal: file not found")
	ErrCRCMismatch      = errors.New("wal: crc mismatch")
	ErrSnapshotMismatch = errors.New("wal: snapshot mismatch")
	ErrSnapshotNotFound = errors.New("wal: snapshot not found")
	crcTable            = crc32.MakeTable(crc32.Castagnoli)
)

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
// WAL用于以append记录的方式快速记录数据到持久化存储中
// 只可能处于append模式或者读模式，但是不能同时处于两种模式中
// 新创建的WAL处于append模式
// 刚打开的WAL处于读模式
type WAL struct {
	// 存放WAL文件的目录
	dir string // the living directory of the underlay files

	// dirFile is a fd for the wal directory for syncing on Rename
	// 根据前面的dir成员创建的File指针
	dirFile *os.File

	// 每个WAL文件最开始的地方，都需要写入的元数据
	metadata []byte           // metadata recorded at the head of each WAL
	// 每次写入entryType类型的记录之后，都需要追加一条stateType类型的数据，state成员就用于存储当前状态
	state    raftpb.HardState // hardstate recorded at the head of WAL

	// 记录当前快照数据。每次读取WAL文件时，并不会从头开始读取。而是根据这里的快照数据定位到位置。
	// Snapshot.Index记录了对应快照数据的最后一条entry记录的索引，而Snapshot.Term记录的是对应记录的任期号。
	start     walpb.Snapshot // snapshot to start reading
	// 负责反序列化数据到Record
	decoder   *decoder       // decoder to decode records
	readClose func() error   // closer for decode reader

	// 读写WAL文件时的锁
	mu      sync.Mutex
	// WAL中最后一条entry记录的索引数据
	enti    uint64   // index of the last entry saved to the wal
	// 负责将Record记录序列化成二进制数据
	encoder *encoder // encoder to encode records

	// 当前所有的WAL日志文件实例
	locks []*fileutil.LockedFile // the locked files the WAL holds (the name is increasing)
	// 负责创建新的WAL临时文件
	fp    *filePipeline
}

// Create creates a WAL ready for appending records. The given metadata is
// recorded at the head of each WAL file, and can be retrieved with ReadAll.
// 负责创建WAL文件，dirpath指定路径，metadata指定了元数据
func Create(dirpath string, metadata []byte) (*WAL, error) {
	// 路径已经存在
	if Exist(dirpath) {
		return nil, os.ErrExist
	}

	// keep temporary wal directory so WAL initialization appears atomic
	tmpdirpath := filepath.Clean(dirpath) + ".tmp"
	if fileutil.Exist(tmpdirpath) {
		if err := os.RemoveAll(tmpdirpath); err != nil {
			return nil, err
		}
	}
	// 创建临时文件目录
	if err := fileutil.CreateDirAll(tmpdirpath); err != nil {
		return nil, err
	}

	// 第一个WAL文件名0-0.wal
	p := filepath.Join(tmpdirpath, walName(0, 0))
	// 创建临时文件
	f, err := fileutil.LockFile(p, os.O_WRONLY|os.O_CREATE, fileutil.PrivateFileMode)
	if err != nil {
		return nil, err
	}
	// 移动到文件结尾
	if _, err = f.Seek(0, os.SEEK_END); err != nil {
		return nil, err
	}
	// 预分配WAL文件空间，大小为SegmentSizeBytes（64MB）
	if err = fileutil.Preallocate(f.File, SegmentSizeBytes, true); err != nil {
		return nil, err
	}

	w := &WAL{
		dir:      dirpath,
		metadata: metadata,
	}
	// 创建文件encoder
	w.encoder, err = newFileEncoder(f.File, 0)
	if err != nil {
		return nil, err
	}
	// 添加lockfile实例
	w.locks = append(w.locks, f)
	// 写入一条crcType类型的记录
	if err = w.saveCrc(0); err != nil {
		return nil, err
	}
	// 写入一条metaType类型的记录
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: metadata}); err != nil {
		return nil, err
	}
	// 紧跟着是snapshotType类型记录
	if err = w.SaveSnapshot(walpb.Snapshot{}); err != nil {
		return nil, err
	}
	// 将临时目录重命名为正式文件目录
	if w, err = w.renameWal(tmpdirpath); err != nil {
		return nil, err
	}

	// directory was renamed; sync parent dir to persist rename
	// 临时目录重命名之后，将重命名操作刷新到磁盘
	pdir, perr := fileutil.OpenDir(filepath.Dir(w.dir))
	if perr != nil {
		return nil, perr
	}
	if perr = fileutil.Fsync(pdir); perr != nil {
		return nil, perr
	}
	if perr = pdir.Close(); err != nil {
		return nil, perr
	}

	return w, nil
}

// Open opens the WAL at the given snap.
// The snap SHOULD have been previously saved to the WAL, or the following
// ReadAll will fail.
// The returned WAL is ready to read and the first record will be the one after
// the given snap. The WAL cannot be appended to before reading out all of its
// previous records.
func Open(dirpath string, snap walpb.Snapshot) (*WAL, error) {
	w, err := openAtIndex(dirpath, snap, true)
	if err != nil {
		return nil, err
	}
	if w.dirFile, err = fileutil.OpenDir(w.dir); err != nil {
		return nil, err
	}
	return w, nil
}

// OpenForRead only opens the wal files for read.
// Write on a read only wal panics.
func OpenForRead(dirpath string, snap walpb.Snapshot) (*WAL, error) {
	return openAtIndex(dirpath, snap, false)
}

// 打开已经存在的WAL目录，用这个目录中的WAL文件创建WAL实例，最后返回WAL实例指针
// 传入的snap参数中，Snapshot.Index用于定位哪个索引位置之后的数据需要从wal中读取
func openAtIndex(dirpath string, snap walpb.Snapshot, write bool) (*WAL, error) {
	// 返回目录中所有wal文件名
	names, err := readWalNames(dirpath)
	if err != nil {
		return nil, err
	}

	// 根据传入的快照数据索引，查找这些wal文件中第一个满足这个索引的wal文件索引
	nameIndex, ok := searchIndex(names, snap.Index)
	if !ok || !isValidSeq(names[nameIndex:]) {
		return nil, ErrFileNotFound
	}

	// open the wal files
	// 保存读文件
	rcs := make([]io.ReadCloser, 0)
	rs := make([]io.Reader, 0)
	// ls用于记录该WAL实例管理的wal文件数组
	ls := make([]*fileutil.LockedFile, 0)
	// 遍历在这之后的wal文件
	for _, name := range names[nameIndex:] {
		p := filepath.Join(dirpath, name)
		if write {	// 如果需要写文件
			l, err := fileutil.TryLockFile(p, os.O_RDWR, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(rcs...)
				return nil, err
			}
			ls = append(ls, l)
			rcs = append(rcs, l)
		} else {	// 只读模式
			rf, err := os.OpenFile(p, os.O_RDONLY, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(rcs...)
				return nil, err
			}
			ls = append(ls, nil)
			rcs = append(rcs, rf)
		}
		rs = append(rs, rcs[len(rcs)-1])
	}

	closer := func() error { return closeAll(rcs...) }

	// create a WAL ready for reading
	w := &WAL{
		dir:       dirpath,
		// 保存快照数据
		start:     snap,
		decoder:   newDecoder(rs...),
		readClose: closer,
		locks:     ls,
	}

	if write {
		// write reuses the file descriptors from read; don't close so
		// WAL can append without dropping the file lock
		w.readClose = nil
		if _, _, err := parseWalName(filepath.Base(w.tail().Name())); err != nil {
			closer()
			return nil, err
		}
		// 如果是写模式，创建FilePipeline实例
		w.fp = newFilePipeline(w.dir, SegmentSizeBytes)
	}

	return w, nil
}

// ReadAll reads out records of the current WAL.
// If opened in write mode, it must read out all records until EOF. Or an error
// will be returned.
// If opened in read mode, it will try to read all records if possible.
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// If loaded snap doesn't match with the expected one, it will return
// all the records and error ErrSnapshotMismatch.
// ReadAll函数负责从当前WAL实例中读取所有的记录
// 如果是可写模式，那么必须独处所有的记录，否则将报错
// 如果是只读模式，将尝试读取所有的记录，但是如果读出的记录没有满足快照数据的要求，将返回ErrSnapshotNotFound
// 而如果读出来的快照数据与要求的快照数据不匹配，返回所有的记录以及ErrSnapshotMismatch
// TODO: detect not-last-snap error.
// TODO: maybe loose the checking of match.
// After ReadAll, the WAL will be ready for appending new records.
func (w *WAL) ReadAll() (metadata []byte, state raftpb.HardState, ents []raftpb.Entry, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{}
	decoder := w.decoder

	var match bool
	// 循环读出record记录
	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case entryType:	// entry类型
			// 反序列化数据成entry返回
			e := mustUnmarshalEntry(rec.Data)
			if e.Index > w.start.Index {	// 这条记录是在快照数据之后的数据
				// 添加到ents数组
				ents = append(ents[:e.Index-w.start.Index-1], e)
			}
			// 保存entry记录索引
			w.enti = e.Index
		case stateType:	// state类型
			// 反序列化保存在state中
			state = mustUnmarshalState(rec.Data)
		case metadataType:	// meta数据类型
			// 如果当前已经存在meta数据，而且两者不匹配
			if metadata != nil && !bytes.Equal(metadata, rec.Data) {
				// 返回ErrMetadataConflict错误
				state.Reset()
				return nil, state, nil, ErrMetadataConflict
			}
			// 保存下来
			metadata = rec.Data
		case crcType:	// crc类型
			crc := decoder.crc.Sum32()
			// current crc of decoder must match the crc of the record.
			// do no need to match 0 crc, since the decoder is a new one at this case.
			// crc数据不匹配
			if crc != 0 && rec.Validate(crc) != nil {
				state.Reset()
				// 返回ErrCRCMismatch错误
				return nil, state, nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		case snapshotType:	// 快照数据
			var snap walpb.Snapshot
			pbutil.MustUnmarshal(&snap, rec.Data)
			if snap.Index == w.start.Index {	// 两者的索引相同
				if snap.Term != w.start.Term {	// 但是任期号不同
					state.Reset()
					// 返回ErrSnapshotMismatch错误
					return nil, state, nil, ErrSnapshotMismatch
				}
				// 保存快照数据匹配的标志位
				match = true
			}
		default:
			state.Reset()
			return nil, state, nil, fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	// 到了这里，读取WAL日志的循环就结束了，中间可能出错
	// 如果读完了所有文件，则err = io.EOF
	// 如果是没有读完文件而中间有其他错误，那么err就不是EOF了，下面会分别处理只读模式和写模式

	switch w.tail() {
	case nil:
		// We do not have to read out all entries in read mode.
		// The last record maybe a partial written one, so
		// ErrunexpectedEOF might be returned.
		// 在只读模式下，可能没有读完全部的记录。最后一条记录可能是只写了一部分，此时就会返回ErrunexpectedEOF错误
		if err != io.EOF && err != io.ErrUnexpectedEOF {	// 如果不是EOF以及ErrunexpectedEOF错误的情况就返回错误
			state.Reset()
			return nil, state, nil, err
		}
	default:
		// 写模式下必须读完全部的记录
		// We must read all of the entries if WAL is opened in write mode.
		if err != io.EOF {	// 如果不是EOF错误，说明没有读完数据就报错了，这种情况也是返回错误
			state.Reset()
			return nil, state, nil, err
		}
		// decodeRecord() will return io.EOF if it detects a zero record,
		// but this zero record may be followed by non-zero records from
		// a torn write. Overwriting some of these non-zero records, but
		// not all, will cause CRC errors on WAL open. Since the records
		// were never fully synced to disk in the first place, it's safe
		// to zero them out to avoid any CRC errors from new writes.
		// 将最后一个WAL文件的后续部分全部填充为0
		if _, err = w.tail().Seek(w.decoder.lastOffset(), os.SEEK_SET); err != nil {
			return nil, state, nil, err
		}
		if err = fileutil.ZeroToEnd(w.tail().File); err != nil {
			return nil, state, nil, err
		}
	}

	err = nil
	if !match {
		// 快照数据不匹配
		err = ErrSnapshotNotFound
	}

	// close decoder, disable reading
	if w.readClose != nil {
		w.readClose()
		w.readClose = nil
	}
	w.start = walpb.Snapshot{}

	w.metadata = metadata

	if w.tail() != nil {	// 写模式下还有wal文件，因此下面创建encoder
		// create encoder (chain crc with the decoder), enable appending
		w.encoder, err = newFileEncoder(w.tail().File, w.decoder.lastCRC())
		if err != nil {
			return
		}
	}
	w.decoder = nil

	return metadata, state, ents, err
}

// cut closes current file written and creates a new one ready to append.
// cut first creates a temp wal file and writes necessary headers into it.
// Then cut atomically rename temp wal file to a wal file.
// 切换WAL文件，创建一个新的WAL文件出来
func (w *WAL) cut() error {
	// close old wal file; truncate to avoid wasting space if an early cut
	// 拿到当前wal文件的偏移位置
	off, serr := w.tail().Seek(0, os.SEEK_CUR)
	if serr != nil {
		return serr
	}
	// 根据当前文件偏移量清空后续填充内容
	if err := w.tail().Truncate(off); err != nil {
		return err
	}
	// 将修改落盘
	if err := w.sync(); err != nil {
		return err
	}
	// 根据最后一个wal文件的名称，得到新创建wal文件的文件名
	// 其中seq()返回的最后一个日志文件的编号，enti则是最后一条entry数据的索引
	// 这样新创建的wal文件保存的entry数据都是在enti之后的
	fpath := filepath.Join(w.dir, walName(w.seq()+1, w.enti+1))

	// create a temp wal file with name sequence + 1, or truncate the existing one
	// 创建一个新的临时文件
	newTail, err := w.fp.Open()
	if err != nil {
		return err
	}

	// update writer and save the previous crc
	// 添加到locks数组中
	w.locks = append(w.locks, newTail)
	// 前一个文件的crc值
	prevCrc := w.encoder.crc.Sum32()
	// 创建临时文件对应的encoder
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}
	// 保存上一个文件的crc值
	if err = w.saveCrc(prevCrc); err != nil {
		return err
	}
	// 保存meta元数据
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: w.metadata}); err != nil {
		return err
	}
	// 保存集群状态信息
	if err = w.saveState(&w.state); err != nil {
		return err
	}
	// atomically move temp wal file to wal file
	// 将修改落盘
	if err = w.sync(); err != nil {
		return err
	}

	// 得到当前文件偏移量
	off, err = w.tail().Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	// 将临时文件重命名为新的日志文件
	if err = os.Rename(newTail.Name(), fpath); err != nil {
		return err
	}
	// 落盘
	if err = fileutil.Fsync(w.dirFile); err != nil {
		return err
	}

	newTail.Close()

	// 打开重命名之后的新文件
	if newTail, err = fileutil.LockFile(fpath, os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return err
	}
	// 将文件指针移动到前面保存的索引值
	if _, err = newTail.Seek(off, os.SEEK_SET); err != nil {
		return err
	}
	// locks数组最后一项保存新的重命名之后的文件
	w.locks[len(w.locks)-1] = newTail

	prevCrc = w.encoder.crc.Sum32()
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}

	plog.Infof("segmented wal file %v is created", fpath)
	return nil
}

func (w *WAL) sync() error {
	if w.encoder != nil {
		if err := w.encoder.flush(); err != nil {
			return err
		}
	}
	start := time.Now()
	// 调用操作系统的Fdatasync将数据落盘
	err := fileutil.Fdatasync(w.tail().File)

	// 记录落盘花费的时间
	duration := time.Since(start)
	if duration > warnSyncDuration {	// 大于告警阈值
		plog.Warningf("sync duration of %v, expected less than %v", duration, warnSyncDuration)
	}
	// 记录下来
	syncDurations.Observe(duration.Seconds())

	return err
}

// ReleaseLockTo releases the locks, which has smaller index than the given index
// except the largest one among them.
// For example, if WAL is holding lock 1,2,3,4,5,6, ReleaseLockTo(4) will release
// lock 1,2 but keep 3. ReleaseLockTo(5) will release 1,2,3 but keep 4.
// ReleaseLockTo函数将释放小于index的文件，但是保留这些文件中最大的那个
// 比如[1,2,3,4,5,6]，ReleaseLockTo(4) 将释放1，2但是保存3
func (w *WAL) ReleaseLockTo(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var smaller int
	found := false

	for i, l := range w.locks {
		_, lockIndex, err := parseWalName(filepath.Base(l.Name()))
		if err != nil {
			return err
		}
		// 寻找第一个索引值不小于index的文件
		if lockIndex >= index {
			// 减一是因为要保留其中最大的那个文件
			smaller = i - 1
			found = true
			break
		}
	}

	// if no lock index is greater than the release index, we can
	// release lock up to the last one(excluding).
	if !found && len(w.locks) != 0 {
		smaller = len(w.locks) - 1
	}

	if smaller <= 0 {
		return nil
	}

	// 好了，smaller之前的文件都可以释放了
	for i := 0; i < smaller; i++ {
		if w.locks[i] == nil {
			continue
		}
		w.locks[i].Close()
	}
	w.locks = w.locks[smaller:]

	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fp != nil {
		w.fp.Close()
		w.fp = nil
	}

	if w.tail() != nil {
		if err := w.sync(); err != nil {
			return err
		}
	}
	for _, l := range w.locks {
		if l == nil {
			continue
		}
		if err := l.Close(); err != nil {
			plog.Errorf("failed to unlock during closing wal: %s", err)
		}
	}

	return w.dirFile.Close()
}

func (w *WAL) saveEntry(e *raftpb.Entry) error {
	// TODO: add MustMarshalTo to reduce one allocation.
	// 序列化entry
	b := pbutil.MustMarshal(e)
	// 构造一条record记录
	rec := &walpb.Record{Type: entryType, Data: b}
	// 通过encoder追加记录
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	// 更新最后一条entry索引值
	w.enti = e.Index
	return nil
}

func (w *WAL) saveState(s *raftpb.HardState) error {
	if raft.IsEmptyHardState(*s) {
		return nil
	}
	// 保存state
	w.state = *s
	// 序列化之后通过encoder保存记录
	b := pbutil.MustMarshal(s)
	rec := &walpb.Record{Type: stateType, Data: b}
	return w.encoder.encode(rec)
}

// 把HardState和日志条目Entry数组写入WAL文件
func (w *WAL) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// short cut, do not call sync
	// 没有数据直接返回nil了
	if raft.IsEmptyHardState(st) && len(ents) == 0 {
		return nil
	}

	// 这些数据是否需要落盘
	mustSync := mustSync(st, w.state, len(ents))

	// TODO(xiangli): no more reference operator
	for i := range ents {
		// 遍历记录进行保存
		if err := w.saveEntry(&ents[i]); err != nil {
			return err
		}
	}
	// 保存集群状态信息
	if err := w.saveState(&st); err != nil {
		return err
	}

	// 获得当前日志的文件偏移位置
	curOff, err := w.tail().Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	// 没有写满预分配空间
	if curOff < SegmentSizeBytes {
		if mustSync {	// 落盘之后就可以返回了
			return w.sync()
		}
		return nil
	}

	// 到了这里，说明写满了预分配空间，就需要切换文件了
	return w.cut()
}

// 保存快照数据到WAL文件中
func (w *WAL) SaveSnapshot(e walpb.Snapshot) error {
	b := pbutil.MustMarshal(&e)

	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{Type: snapshotType, Data: b}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	// update enti only when snapshot is ahead of last index
	if w.enti < e.Index {
		w.enti = e.Index
	}
	return w.sync()
}

func (w *WAL) saveCrc(prevCrc uint32) error {
	return w.encoder.encode(&walpb.Record{Type: crcType, Crc: prevCrc})
}

// 返回最后一个wal文件指针，也可以理解为当前wal文件指针
func (w *WAL) tail() *fileutil.LockedFile {
	if len(w.locks) > 0 {
		return w.locks[len(w.locks)-1]
	}
	return nil
}

func (w *WAL) seq() uint64 {
	t := w.tail()
	if t == nil {
		return 0
	}
	seq, _, err := parseWalName(filepath.Base(t.Name()))
	if err != nil {
		plog.Fatalf("bad wal name %s (%v)", t.Name(), err)
	}
	return seq
}

// 以下这些条件满足其一时意味着必须sync
func mustSync(st, prevst raftpb.HardState, entsnum int) bool {
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	// currentTerm
	// votedFor
	// log entries[]
	// 有记录或者投票ID不同或者任期号不同
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}

func closeAll(rcs ...io.ReadCloser) error {
	for _, f := range rcs {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}
