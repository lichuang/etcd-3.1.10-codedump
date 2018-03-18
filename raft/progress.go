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

import "fmt"

const (
	ProgressStateProbe ProgressStateType = iota
	ProgressStateReplicate
	ProgressStateSnapshot
)

type ProgressStateType uint64

var prstmap = [...]string{
	"ProgressStateProbe",
	"ProgressStateReplicate",
	"ProgressStateSnapshot",
}

func (st ProgressStateType) String() string { return prstmap[uint64(st)] }

// 该数据结构用于在leader中保存每个follower的状态信息，leader将根据这些信息决定发送给节点的日志
// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	// Next保存的是下一次leader发送append消息时传送过来的日志索引
	// 当选举出新的leader时，首先初始化Next为该leader最后一条日志+1
	// 然后如果向该节点append日志失败，则递减Next回退日志，一直回退到索引匹配为止

	// Match保存在该节点上保存的日志的最大索引，初始化为0
	Match, Next uint64
	// State defines how the leader should interact with the follower.
	//
	// When in ProgressStateProbe, leader sends at most one replication message
	// per heartbeat interval. It also probes actual progress of the follower.
	//
	// When in ProgressStateReplicate, leader optimistically increases next
	// to the latest entry sent after sending replication message. This is
	// an optimized state for fast replicating log entries to the follower.
	//
	// When in ProgressStateSnapshot, leader should have sent out snapshot
	// before and stops sending any replication message.
	State ProgressStateType
	// Paused is used in ProgressStateProbe.
	// When Paused is true, raft should pause sending replication message to this peer.
	Paused bool
	// PendingSnapshot is used in ProgressStateSnapshot.
	// If there is a pending snapshot, the pendingSnapshot will be set to the
	// index of the snapshot. If pendingSnapshot is set, the replication process of
	// this Progress will be paused. raft will not resend snapshot until the pending one
	// is reported to be failed.
	PendingSnapshot uint64

	// RecentActive is true if the progress is recently active. Receiving any messages
	// from the corresponding follower indicates the progress is active.
	// RecentActive can be reset to false after an election timeout.
	RecentActive bool

	// inflights is a sliding window for the inflight messages.
	// Each inflight message contains one or more log entries.
	// The max number of entries per message is defined in raft config as MaxSizePerMsg.
	// Thus inflight effectively limits both the number of inflight messages
	// and the bandwidth each Progress can use.
	// When inflights is full, no more message should be sent.
	// When a leader sends out a message, the index of the last
	// entry should be added to inflights. The index MUST be added
	// into inflights in order.
	// When a leader receives a reply, the previous inflights should
	// be freed by calling inflights.freeTo with the index of the last
	// received entry.
	// 用于实现滑动窗口，用来做流量控制
	ins *inflights
}

// 重置状态
func (pr *Progress) resetState(state ProgressStateType) {
	pr.Paused = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.ins.reset()
}

// 修改为probe状态
func (pr *Progress) becomeProbe() {
	// If the original state is ProgressStateSnapshot, progress knows that
	// the pending snapshot has been sent to this peer successfully, then
	// probes from pendingSnapshot + 1.
	if pr.State == ProgressStateSnapshot {
		// 如果当前状态是接受快照状态，那么可以知道该节点已经成功接受处理了该快照，此时修改next索引需要根据max和快照索引来判断
		pendingSnapshot := pr.PendingSnapshot
		pr.resetState(ProgressStateProbe)
		// 取两者的最大值+1
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		pr.resetState(ProgressStateProbe)
		pr.Next = pr.Match + 1
	}
}

func (pr *Progress) becomeReplicate() {
	pr.resetState(ProgressStateReplicate)
	pr.Next = pr.Match + 1
}

func (pr *Progress) becomeSnapshot(snapshoti uint64) {
	pr.resetState(ProgressStateSnapshot)
	pr.PendingSnapshot = snapshoti
}

// maybeUpdate returns false if the given n index comes from an outdated message.
// Otherwise it updates the progress and returns true.
// 如果传入的n小于等于当前的match索引，则索引就不会更新，返回false；否则更新索引返回true
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
		pr.resume()
	}
	if pr.Next < n+1 {
		pr.Next = n + 1
	}
	return updated
}

func (pr *Progress) optimisticUpdate(n uint64) { pr.Next = n + 1 }

// maybeDecrTo returns false if the given to index comes from an out of order message.
// Otherwise it decreases the progress next index to min(rejected, last) and returns true.
// rejected是拒绝该append消息时的索引，last是该节点的最后一条日志索引
func (pr *Progress) maybeDecrTo(rejected, last uint64) bool {
	if pr.State == ProgressStateReplicate {	// 如果当前在接收副本状态
		// the rejection must be stale if the progress has matched and "rejected"
		// is smaller than "match".
		if rejected <= pr.Match {
			// 这种情况说明返回的情况已经过期，中间有其他添加成功的情况，导致match索引递增，此时不需要回退索引，返回false
			return false
		}
		// directly decrease next to match + 1
		// 否则直接修改next到match+1
		pr.Next = pr.Match + 1
		return true
	}

	// 以下都不是接收副本状态的情况

	// the rejection must be stale if "rejected" does not match next - 1
	if pr.Next-1 != rejected {
		// 这种情况说明返回的情况已经过期，不需要回退索引，返回false
		return false
	}

	// 到了这里就回退Next为两者的较小值
	if pr.Next = min(rejected, last+1); pr.Next < 1 {
		pr.Next = 1
	}
	pr.resume()
	return true
}

func (pr *Progress) pause()  { pr.Paused = true }
func (pr *Progress) resume() { pr.Paused = false }

// IsPaused returns whether sending log entries to this node has been
// paused. A node may be paused because it has rejected recent
// MsgApps, is currently waiting for a snapshot, or has reached the
// MaxInflightMsgs limit.
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case ProgressStateProbe:
		return pr.Paused
	case ProgressStateReplicate:
		// 如果在replicate状态，pause与否取决于infilght数组是否满了
		return pr.ins.full()
	case ProgressStateSnapshot:
		// 处理快照时一定是paused的
		return true
	default:
		panic("unexpected state")
	}
}

func (pr *Progress) snapshotFailure() { pr.PendingSnapshot = 0 }

// needSnapshotAbort returns true if snapshot progress's Match
// is equal or higher than the pendingSnapshot.
// 可以中断快照的情况：当前为接收快照，同时match已经大于等于快照索引
func (pr *Progress) needSnapshotAbort() bool {
	return pr.State == ProgressStateSnapshot && pr.Match >= pr.PendingSnapshot
}

func (pr *Progress) String() string {
	return fmt.Sprintf("next = %d, match = %d, state = %s, waiting = %v, pendingSnapshot = %d", pr.Next, pr.Match, pr.State, pr.IsPaused(), pr.PendingSnapshot)
}

type inflights struct {
	// the starting index in the buffer
	start int
	// number of inflights in the buffer
	count int

	// the size of the buffer
	size int

	// buffer contains the index of the last entry
	// inside one message.
	buffer []uint64
}

func newInflights(size int) *inflights {
	return &inflights{
		size: size,
	}
}

// add adds an inflight into inflights
func (in *inflights) add(inflight uint64) {
	if in.full() {
		panic("cannot add into a full inflights")
	}
	next := in.start + in.count
	size := in.size
	if next >= size {
		// 索引如果超过size了，要回绕回来
		next -= size
	}
	// buffer不够就要增加
	if next >= len(in.buffer) {
		in.growBuf()
	}
	in.buffer[next] = inflight
	in.count++
}

// grow the inflight buffer by doubling up to inflights.size. We grow on demand
// instead of preallocating to inflights.size to handle systems which have
// thousands of Raft groups per process.
// 增加buffer
func (in *inflights) growBuf() {
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1
	} else if newSize > in.size {
		newSize = in.size
	}
	newBuffer := make([]uint64, newSize)
	copy(newBuffer, in.buffer)
	in.buffer = newBuffer
}

// freeTo frees the inflights smaller or equal to the given `to` flight.
func (in *inflights) freeTo(to uint64) {
	if in.count == 0 || to < in.buffer[in.start] {
		// 窗口为空，或者传入的参数小于窗口第一个元素的值
		// out of the left side of the window
		return
	}

	i, idx := 0, in.start
	for i = 0; i < in.count; i++ {
		// 寻找比to大的最小的值，找到了就退出循环
		if to < in.buffer[idx] { // found the first large inflight
			break
		}

		// increase index and maybe rotate
		// 处理需要回绕的情况
		size := in.size
		if idx++; idx >= size {
			idx -= size
		}
	}
	// free i inflights and set new start index
	// 数量减少那些小于to的数据
	in.count -= i
	// 开始索引从第一个大于to的元素开始
	in.start = idx
	if in.count == 0 {
		// 如果经过这次缩减，count为0，那么start从0开始
		// inflights is empty, reset the start index so that we don't grow the
		// buffer unnecessarily.
		in.start = 0
	}
}

func (in *inflights) freeFirstOne() { in.freeTo(in.buffer[in.start]) }

// full returns true if the inflights is full.
func (in *inflights) full() bool {
	return in.count == in.size
}

// resets frees all inflights.
func (in *inflights) reset() {
	in.count = 0
	in.start = 0
}
