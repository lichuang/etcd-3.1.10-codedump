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

package raft

import pb "github.com/coreos/etcd/raft/raftpb"

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
// unstable.entries[i]保存raft log position i+unstable.offset.

// entry数组中位置为i的元素保存的数据位置在raft log位置i + offset位置的数据
// 其中offset可能小于持久化存储的最大索引偏移量，这意味着持久化存储中在offset的数据
// 可能会被截断

// unstable用来保存还未持久化的数据，其可能保存在两个地方：
// 快照数据snapshot或者entry数组
// 两者中同时只可能存在其中之一
// 快照数据用于开始启动时需要恢复的数据较多，所以一次性的使用快照数据来恢复
// entry数组则是用于逐条数据进行接收时使用
// 其中offset与entry数组配合着使用，可能会出现小于持久化最大索引偏移量的数据，所以需要做截断处理
type unstable struct {
	// the incoming unstable snapshot, if any.
	// 保存还没有持久化的快照数据
	snapshot *pb.Snapshot
	// all entries that have not yet been written to storage.
	// 还未持久化的数据
	entries []pb.Entry
	// offset用于保存entries数组中的数据的起始index
	offset  uint64

	logger Logger
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
// 只有在快照存在的情况下，返回快照中的meta数据
// 注意到与取lastIndex值不同的是，firstindex只会从快照中尝试。
// 这是因为快照中的数据用于刚启动时，此时可能是最早的index，
// 而entry数组存在数据时，持久化中已经有数据了，此时不可能从entry数组中取firstindex
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
// 如果entries存在，返回最后一个entry的索引
// 否则如果快照存在，返回快照的meta数据中的索引
// 以上都不成立，则返回false
func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index, true
	}
	return 0, false
}

// 返回索引i的term，如果不存在则返回0,false
// maybeTerm returns the term of the entry at index i, if there
// is any.
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		// 如果索引比offset小，那么尝试从快照中获取
		if u.snapshot == nil {
			return 0, false
		}
		if u.snapshot.Metadata.Index == i {
			// 只有在正好快照meta数据的index的情况下才查得到，在这之前的都查不到term了
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	// 到了这里就是i>=offset的情况了

	last, ok := u.maybeLastIndex()
	if !ok {
		return 0, false
	}
	// 如果比lastindex还大，那也查不到
	if i > last {
		return 0, false
	}
	return u.entries[i-u.offset].Term, true
}

// 传入索引i和term，表示目前这块数据已经持久化了
func (u *unstable) stableTo(i, t uint64) {
	gt, ok := u.maybeTerm(i)
	if !ok {
		return
	}
	// if i < offset, term is matched with the snapshot
	// only update the unstable entries if term is matched with
	// an unstable entry.
	// 只有在term相同，同时索引大于等于当前offset的情况下
	if gt == t && i >= u.offset {
		// 因为前面的数据被持久化了，所以将entries缩容，从i开始
		u.entries = u.entries[i+1-u.offset:]
		u.logger.Infof("stable to %d, entries size: %d", i, len(u.entries))
		// offset也要从i开始
		u.offset = i + 1
	}
}

// 传入索引，对快照进行操作
func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		// 如果索引刚好是快照的索引，说明快照的数据已经保存，所以当前快照可以置空了
		u.snapshot = nil
	}
}

// 传入快照，从快照数据中恢复
func (u *unstable) restore(s pb.Snapshot) {
	// 偏移量从快照索引之后开始，entries置空
	u.offset = s.Metadata.Index + 1
	// 清空entries数组
	u.entries = nil
	// 保存到snapshot中，注意这里保存的是指针，因为快照数据可能很大，如果值拷贝可能会涉及很多的数据
	u.snapshot = &s
}

// 传入entries，可能会导致原先数据的截断或者添加操作
// 这就是最开始注释中说明的offset可能比持久化索引小的情况，需要做截断
func (u *unstable) truncateAndAppend(ents []pb.Entry) {
	// 先拿到这些数据的第一个索引
	after := ents[0].Index
	switch {
	case after == u.offset+uint64(len(u.entries)):
		// 如果正好是紧接着当前数据的，就直接append
		// after is the next index in the u.entries
		// directly append
		u.entries = append(u.entries, ents...)
	case after <= u.offset:
		u.logger.Infof("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		// 如果比当前偏移量小，那用新的数据替换当前数据，需要同时更改offset和entries
		u.offset = after
		u.entries = ents
	default:
		// truncate to after and copy to u.entries
		// then append
		// 到了这里，说明 u.offset < after < u.offset+uint64(len(u.entries))
		// 那么新的entries需要拼接而成
		u.logger.Infof("truncate the unstable entries before index %d", after)
		u.entries = append([]pb.Entry{}, u.slice(u.offset, after)...)
		u.entries = append(u.entries, ents...)
	}
}

// 返回索引范围在[lo-u.offset : hi-u.offset]之间的数据
func (u *unstable) slice(lo uint64, hi uint64) []pb.Entry {
	u.mustCheckOutOfBounds(lo, hi)
	return u.entries[lo-u.offset : hi-u.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.offset)
// 检查传入的索引范围是否合法，不合法直接panic
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}