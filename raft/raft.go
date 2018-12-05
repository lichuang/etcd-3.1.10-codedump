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

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/coreos/etcd/raft/raftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0
const noLimit = math.MaxUint64

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
	numStates
)

type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyLeaseBased
)

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	// 由于leader转让发起的竞选
	campaignTransfer CampaignType = "CampaignTransfer"
)

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization. Only the methods needed by the code are exposed
// (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// StateType represents the role of a node in a cluster.
type StateType uint64

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	// 每个节点的ID
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	// 保存集群所有节点ID的数组（包括自身）
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	// 选举超时tick
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	// 心跳超时tick
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64

	// MaxSizePerMsg limits the max size of each append message. Smaller value
	// lowers the raft recovery cost(initial probing and message lost during normal
	// operation). On the other side, it might affect the throughput during normal
	// replication. Note: math.MaxUint64 for unlimited, 0 for at most one entry per
	// message.
	MaxSizePerMsg uint64
	// MaxInflightMsgs limits the max number of in-flight append messages during
	// optimistic replication phase. The application transportation layer usually
	// has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
	// overflowing that sending buffer. TODO (xiangli): feedback to application to
	// limit the proposal rate?
	MaxInflightMsgs int

	// CheckQuorum specifies if the leader should check quorum activity. Leader
	// steps down when quorum is not active for an electionTimeout.
	// 标记leader是否需要检查集群中超过半数节点的活跃性，如果在选举超时内没有满足该条件，leader切换到follower状态
	CheckQuorum bool

	// PreVote enables the Pre-Vote algorithm described in raft thesis section
	// 9.6. This prevents disruption when a node that has been partitioned away
	// rejoins the cluster.
	PreVote bool

	// ReadOnlyOption specifies how the read only request is processed.
	//
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	//
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyOption ReadOnlyOption

	// Logger is the logger used for raft log. For multinode which can host
	// multiple raft group, each raft group can have its own logger
	Logger Logger
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight messages must be greater than 0")
	}

	if c.Logger == nil {
		c.Logger = raftLogger
	}

	return nil
}

type raft struct {
	id uint64

	// 任期号
	Term uint64
	// 投票给哪个节点ID
	Vote uint64

	readStates []ReadState

	// the log
	raftLog *raftLog

	maxInflight int
	maxMsgSize  uint64
	prs         map[uint64]*Progress

	state StateType

	// 该map存放哪些节点投票给了本节点
	votes map[uint64]bool

	msgs []pb.Message

	// the leader id
	lead uint64
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
	// leader转让的目标节点id
	leadTransferee uint64
	// New configuration is ignored if there exists unapplied configuration.
	// 标识当前还有没有applied的配置数据
	pendingConf bool

	readOnly *readOnly

	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int

	checkQuorum bool
	preVote     bool

	heartbeatTimeout int
	electionTimeout  int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int

	// tick函数，在到期的时候调用，不同的角色该函数不同
	tick func()
	step stepFunc

	logger Logger
}

func newRaft(c *Config) *raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newLog(c.Storage, c.Logger)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	peers := c.peers
	if len(cs.Nodes) > 0 {
		if len(peers) > 0 {
			// TODO(bdarnell): the peers argument is always nil except in
			// tests; the argument should be removed and these tests should be
			// updated to specify their nodes through a snapshot.
			panic("cannot specify both newRaft(peers) and ConfState.Nodes)")
		}
		peers = cs.Nodes
	}
	r := &raft{
		id:               c.ID,
		lead:             None,
		raftLog:          raftlog,
		maxMsgSize:       c.MaxSizePerMsg,
		maxInflight:      c.MaxInflightMsgs,
		prs:              make(map[uint64]*Progress),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		logger:           c.Logger,
		checkQuorum:      c.CheckQuorum,
		preVote:          c.PreVote,
		readOnly:         newReadOnly(c.ReadOnlyOption),
	}
	for _, p := range peers {
		r.prs[p] = &Progress{Next: 1, ins: newInflights(r.maxInflight)}
	}
	// 如果不是第一次启动而是从之前的数据进行恢复
	if !isHardStateEqual(hs, emptyState) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	// 启动都是follower状态
	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range r.nodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return r
}

func (r *raft) hasLeader() bool { return r.lead != None }

func (r *raft) softState() *SoftState { return &SoftState{Lead: r.lead, RaftState: r.state} }

func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.raftLog.committed,
	}
}

// 超过半数的节点数量
func (r *raft) quorum() int { return len(r.prs)/2 + 1 }

// 返回排序之后的节点ID数组
func (r *raft) nodes() []uint64 {
	nodes := make([]uint64, 0, len(r.prs))
	for id := range r.prs {
		nodes = append(nodes, id)
	}
	// 注意这里进行了排序
	sort.Sort(uint64Slice(nodes))
	return nodes
}

// send persists state to stable storage and then sends to its mailbox.
func (r *raft) send(m pb.Message) {
	m.From = r.id
	if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
		// 投票时term不能为空
		if m.Term == 0 {
			// PreVote RPCs are sent at a term other than our actual term, so the code
			// that sends these messages is responsible for setting the term.
			panic(fmt.Sprintf("term should be set when sending %s", m.Type))
		}
	} else {
		// 其他的消息类型，term必须为空
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.Type, m.Term))
		}
		// do not attach term to MsgProp, MsgReadIndex
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		// MsgReadIndex is also forwarded to leader.
		// prop消息和readindex消息不需要带上term参数
		if m.Type != pb.MsgProp && m.Type != pb.MsgReadIndex {
			m.Term = r.Term
		}
	}
	// 注意这里只是添加到msgs中
	r.msgs = append(r.msgs, m)
}

// sendAppend sends RPC, with entries to the given peer.
// 向to节点发送append消息
func (r *raft) sendAppend(to uint64) {
	pr := r.prs[to]
	if pr.IsPaused() {
		r.logger.Infof("node %d paused", to)
		return
	}
	m := pb.Message{}
	m.To = to

	// 从该节点的Next的上一条数据获取term
	term, errt := r.raftLog.term(pr.Next - 1)
	// 获取从该节点的Next之后的entries，总和不超过maxMsgSize
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)

	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		// 如果前面过程中有错，那说明之前的数据都写到快照里了，尝试发送快照数据过去
		if !pr.RecentActive {
			// 如果该节点当前不可用
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return
		}

		// 尝试发送快照
		m.Type = pb.MsgSnap
		snapshot, err := r.raftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return
			}
			panic(err) // TODO(bdarnell)
		}
		// 不能发送空快照
		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
			r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		// 该节点进入接收快照的状态
		pr.becomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
	} else { // 否则就是简单的发送append消息
		m.Type = pb.MsgApp
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ents
		// append消息需要告知当前leader的commit索引
		m.Commit = r.raftLog.committed
		if n := len(m.Entries); n != 0 {	// 如果发送过去的entries不为空
			switch pr.State {
			// optimistically increase the next when in ProgressStateReplicate
			case ProgressStateReplicate:
				// 如果该节点在接受副本的状态
				// 得到待发送数据的最后一条索引
				last := m.Entries[n-1].Index
				// 直接使用该索引更新Next索引
				pr.optimisticUpdate(last)
				pr.ins.add(last)
			case ProgressStateProbe:
				// 在probe状态时，每次只能发送一条app消息
				pr.pause()
			default:
				r.logger.Panicf("%x is sending append in unhandled state %s", r.id, pr.State)
			}
		}
	}
	r.send(m)
}

// sendHeartbeat sends an empty MsgApp
func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	// commit index取需要发送过去的节点的match已经当前leader的commited中的较小值
	commit := min(r.prs[to].Match, r.raftLog.committed)
	m := pb.Message{
		To:      to,
		Type:    pb.MsgHeartbeat,
		Commit:  commit,
		Context: ctx,
	}

	r.send(m)
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
// 向所有follower发送append消息
func (r *raft) bcastAppend() {
	for id := range r.prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *raft) bcastHeartbeat() {
	lastCtx := r.readOnly.lastPendingRequestCtx()
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}

func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	for id := range r.prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id, ctx)
	}
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
// 尝试commit当前的日志，如果commit日志索引发生变化了就返回true
func (r *raft) maybeCommit() bool {
	// TODO(bmizerany): optimize.. Currently naive
	mis := make(uint64Slice, 0, len(r.prs))
	// 拿到当前所有节点的Match到数组中
	for id := range r.prs {
		mis = append(mis, r.prs[id].Match)
	}
	// 逆序排列
	sort.Sort(sort.Reverse(mis))
	// 排列之后拿到中位数的Match，因为如果这个位置的Match对应的Term也等于当前的Term
	// 说明有过半的节点至少comit了mci这个索引的数据，这样leader就可以以这个索引进行commit了
	mci := mis[r.quorum()-1]
	// raft日志尝试commit
	return r.raftLog.maybeCommit(mci, r.Term)
}

// 重置raft的一些状态
func (r *raft) reset(term uint64) {
	if r.Term != term {
		// 如果是新的任期，那么保存任期号，同时将投票节点置空
		r.Term = term
		r.Vote = None
	}
	r.lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// 重置选举超时
	r.resetRandomizedElectionTimeout()

	r.abortLeaderTransfer()

	r.votes = make(map[uint64]bool)
	// 似乎对于非leader节点来说，重置progress数组状态没有太多的意义？
	for id := range r.prs {
		r.prs[id] = &Progress{Next: r.raftLog.lastIndex() + 1, ins: newInflights(r.maxInflight)}
		if id == r.id {
			r.prs[id].Match = r.raftLog.lastIndex()
		}
	}
	r.pendingConf = false
	r.readOnly = newReadOnly(r.readOnly.option)
}

// 批量append一堆entries
func (r *raft) appendEntry(es ...pb.Entry) {
	li := r.raftLog.lastIndex()
	for i := range es {
		// 设置这些entries的Term以及index
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	r.raftLog.append(es...)
	// 更新本节点的Next以及Match索引
	r.prs[r.id].maybeUpdate(r.raftLog.lastIndex())
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	// append之后，尝试一下是否可以进行commit
	r.maybeCommit()
}

// tickElection is run by followers and candidates after r.electionTimeout.
// follower以及candidate的tick函数，在r.electionTimeout之后被调用
func (r *raft) tickElection() {
	r.electionElapsed++

	if r.promotable() && r.pastElectionTimeout() {
		// 如果可以被提升为leader，同时选举时间也到了
		r.electionElapsed = 0
		// 发送HUP消息是为了重新开始选举
		r.Step(pb.Message{From: r.id, Type: pb.MsgHup})
	}
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
// tickHeartbeat是leader的tick函数
func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		// 如果超过了选举时间
		r.electionElapsed = 0
		if r.checkQuorum {
			r.Step(pb.Message{From: r.id, Type: pb.MsgCheckQuorum})
		}
		// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
		if r.state == StateLeader && r.leadTransferee != None {
			// 当前在迁移leader的流程，但是过了选举超时新的leader还没有产生，那么旧的leader重新成为leader
			r.abortLeaderTransfer()
		}
	}

	if r.state != StateLeader {
		// 不是leader的就不用往下走了
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		// 向集群中其他节点发送广播消息
		r.heartbeatElapsed = 0
		// 尝试发送MsgBeat消息
		r.Step(pb.Message{From: r.id, Type: pb.MsgBeat})
	}
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

func (r *raft) becomeCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	// 因为进入candidate状态，意味着需要重新进行选举了，所以reset的时候传入的是Term+1
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	// 给自己投票
	r.Vote = r.id
	r.state = StateCandidate
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

func (r *raft) becomePreCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	// Becoming a pre-candidate changes our step functions and state,
	// but doesn't change anything else. In particular it does not increase
	// r.Term or change r.Vote.
	// prevote不会递增term，也不会先进行投票，而是等prevote结果出来再进行决定
	r.step = stepCandidate
	r.tick = r.tickElection
	r.state = StatePreCandidate
	r.logger.Infof("%x became pre-candidate at term %d", r.id, r.Term)
}

func (r *raft) becomeLeader() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = StateLeader
	ents, err := r.raftLog.entries(r.raftLog.committed+1, noLimit)
	if err != nil {
		r.logger.Panicf("unexpected error getting uncommitted entries (%v)", err)
	}

	// 变成leader之前，这里还有没commit的配置变化消息
	nconf := numOfPendingConf(ents)
	if nconf > 1 {
		panic("unexpected multiple uncommitted config entry")
	}
	if nconf == 1 {
		r.pendingConf = true
	}

	// 为什么成为leader之后需要传入一个空数据？
	r.appendEntry(pb.Entry{Data: nil})
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}

func (r *raft) campaign(t CampaignType) {
	var term uint64
	var voteMsg pb.MessageType
	if t == campaignPreElection {
		r.becomePreCandidate()
		voteMsg = pb.MsgPreVote
		// PreVote RPCs are sent for the next term before we've incremented r.Term.
		term = r.Term + 1
	} else {
		r.becomeCandidate()
		voteMsg = pb.MsgVote
		term = r.Term
	}
	// 调用poll函数给自己投票，同时返回当前投票给本节点的节点数量
	if r.quorum() == r.poll(r.id, voteRespMsgType(voteMsg), true) {
		// We won the election after voting for ourselves (which must mean that
		// this is a single-node cluster). Advance to the next state.
		// 有半数投票，说明通过，切换到下一个状态
		if t == campaignPreElection {
			r.campaign(campaignElection)
		} else {
			// 如果给自己投票之后，刚好超过半数的通过，那么就成为新的leader
			r.becomeLeader()
		}
		return
	}

	// 向集群里的其他节点发送投票消息
	for id := range r.prs {
		if id == r.id {
			// 过滤掉自己
			continue
		}
		r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

		var ctx []byte
		if t == campaignTransfer {
			ctx = []byte(t)
		}
		r.send(pb.Message{Term: term, To: id, Type: voteMsg, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm(), Context: ctx})
	}
}

// 轮询集群中所有节点，返回一共有多少节点已经进行了投票
func (r *raft) poll(id uint64, t pb.MessageType, v bool) (granted int) {
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	// 如果id没有投票过，那么更新id的投票情况
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
	// 计算下都有多少节点已经投票给自己了
	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}

// raft的状态机
func (r *raft) Step(m pb.Message) error {
	r.logger.Infof("from:%d, to:%d, type:%s, term:%d, state:%v", m.From, m.To, m.Type, r.Term, r.state)

	// Handle the message term, which may result in our stepping down to a follower.
	switch {
	case m.Term == 0:
		// local message
		// 来自本地的消息
	case m.Term > r.Term:
		// 消息的Term大于节点当前的Term
		lead := m.From
		if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
			// 如果收到的是投票类消息

			// 当context为campaignTransfer时表示强制要求进行竞选
			force := bytes.Equal(m.Context, []byte(campaignTransfer))
			// 是否在租约期以内
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
			if !force && inLease {
				// 如果非强制，而且又在租约期以内，就不做任何处理
				// 非强制又在租约期内可以忽略选举消息，见论文的4.2.3，这是为了阻止已经离开集群的节点再次发起投票请求
				// If a server receives a RequestVote request within the minimum election timeout
				// of hearing from a current leader, it does not update its term or grant its vote
				r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
				return nil
			}
			// 否则将lead置为空
			lead = None
		}
		switch {
		// 注意Go的switch case不做处理的话是不会默认走到default情况的
		case m.Type == pb.MsgPreVote:
			// Never change our term in response to a PreVote
			// 在应答一个prevote消息时不对任期term做修改
		case m.Type == pb.MsgPreVoteResp && !m.Reject:
			// We send pre-vote requests with a term in our future. If the
			// pre-vote is granted, we will increment our term when we get a
			// quorum. If it is not, the term comes from the node that
			// rejected our vote so we should become a follower at the new
			// term.
		default:
			r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
			// 变成follower状态
			r.becomeFollower(m.Term, lead)
		}

	case m.Term < r.Term:
		// 消息的Term小于节点自身的Term，同时消息类型是心跳消息或者是append消息
		if r.checkQuorum && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp) {
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases
			// 收到了一个节点发送过来的更小的term消息。这种情况可能是因为消息的网络延时导致，但是也可能因为该节点由于网络分区导致了它递增了term到一个新的任期。
			// ，这种情况下该节点不能赢得一次选举，也不能使用旧的任期号重新再加入集群中。如果checkQurom为false，这种情况可以使用递增任期号应答来处理。
			// 但是如果checkQurom为True，
			// 此时收到了一个更小的term的节点发出的HB或者APP消息，于是应答一个appresp消息，试图纠正它的状态
			r.send(pb.Message{To: m.From, Type: pb.MsgAppResp})
		} else {
			// ignore other cases
			// 除了上面的情况以外，忽略任何term小于当前节点所在任期号的消息
			r.logger.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
		}
		// 在消息的term小于当前节点的term时，不往下处理直接返回了
		return nil
	}

	switch m.Type {
	case pb.MsgHup:
		// 收到HUP消息，说明准备进行选举
		if r.state != StateLeader {
			// 当前不是leader

			// 取出[applied+1,committed+1]之间的消息，即得到还未进行applied的日志列表
			ents, err := r.raftLog.slice(r.raftLog.applied+1, r.raftLog.committed+1, noLimit)
			if err != nil {
				r.logger.Panicf("unexpected error getting unapplied entries (%v)", err)
			}
			// 如果其中有config消息，并且commited > applied，说明当前还有没有apply的config消息，这种情况下不能开始投票
			if n := numOfPendingConf(ents); n != 0 && r.raftLog.committed > r.raftLog.applied {
				r.logger.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
				return nil
			}

			r.logger.Infof("%x is starting a new election at term %d", r.id, r.Term)
			// 进行选举
			if r.preVote {
				r.campaign(campaignPreElection)
			} else {
				r.campaign(campaignElection)
			}
		} else {
			r.logger.Debugf("%x ignoring MsgHup because already leader", r.id)
		}

	case pb.MsgVote, pb.MsgPreVote:
		// 收到投票类的消息
		// The m.Term > r.Term clause is for MsgPreVote. For MsgVote m.Term should
		// always equal r.Term.
		if (r.Vote == None || m.Term > r.Term || r.Vote == m.From) && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			// 如果当前没有给任何节点投票（r.Vote == None）或者投票的节点term大于本节点的（m.Term > r.Term）
			// 或者是之前已经投票的节点（r.Vote == m.From）
			// 同时还满足该节点的消息是最新的（r.raftLog.isUpToDate(m.Index, m.LogTerm)），那么就接收这个节点的投票
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Type: voteRespMsgType(m.Type)})
			if m.Type == pb.MsgVote {
				// Only record real votes.
				// 保存下来给哪个节点投票了
				r.electionElapsed = 0
				r.Vote = m.From
			}
		} else {
			// 否则拒绝投票
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Type: voteRespMsgType(m.Type), Reject: true})
		}

	default:
		// 其他情况下进入各种状态下自己定制的状态机函数
		r.step(r, m)
	}
	return nil
}

type stepFunc func(r *raft, m pb.Message)

// leader的状态机
func stepLeader(r *raft, m pb.Message) {
	// These message types do not require any progress for m.From.
	switch m.Type {
	case pb.MsgBeat:
		// 广播HB消息
		r.bcastHeartbeat()
		return
	case pb.MsgCheckQuorum:
		// 检查集群可用性
		if !r.checkQuorumActive() {
			// 如果超过半数的服务器没有活跃
			r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
			// 变成follower状态
			r.becomeFollower(r.Term, None)
		}
		return
	case pb.MsgProp:
		// 不能提交空数据
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MsgProp", r.id)
		}
		// 检查是否在集群中
		if _, ok := r.prs[r.id]; !ok {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			// 这里检查本节点是否还在集群以内，如果已经不在集群中了，不处理该消息直接返回。
      // 这种情况出现在本节点已经通过配置变化被移除出了集群的场景。
			return
		}
		// 当前正在转换leader过程中，不能提交
		if r.leadTransferee != None {
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return
		}

		for i, e := range m.Entries {
			if e.Type == pb.EntryConfChange {
				if r.pendingConf {
					// 如果当前为还没有处理的配置变化请求，则其他配置变化的数据暂时忽略
					r.logger.Infof("propose conf %s ignored since pending unapplied configuration", e.String())
					// 将这个数据置空
					m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
				}
				// 置位有还没有处理的配置变化数据
				r.pendingConf = true
			}
		}
		// 添加数据到log中
		r.appendEntry(m.Entries...)
		// 向集群其他节点广播append消息
		r.bcastAppend()
		return
	case pb.MsgReadIndex:
		if r.quorum() > 1 {
			// 这个表达式用于判断是否commttied log索引对应的term是否与当前term不相等，
			// 如果不相等说明这是一个新的leader，而在它当选之后还没有提交任何数据
			if r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) != r.Term {
				// Reject read only request when this leader has not committed any log entry at its term.
				return
			}

			// thinking: use an interally defined context instead of the user given context.
			// We can express this in terms of the term and index instead of a user-supplied value.
			// This would allow multiple reads to piggyback on the same message.
			switch r.readOnly.option {
			case ReadOnlySafe:
				// 把读请求到来时的committed索引保存下来
				r.readOnly.addRequest(r.raftLog.committed, m)
				// 广播消息出去，其中消息的CTX是该读请求的唯一标识
				// 在应答是Context要原样返回，将使用这个ctx操作readOnly相关数据
				r.bcastHeartbeatWithCtx(m.Entries[0].Data)
			case ReadOnlyLeaseBased:
				var ri uint64
				if r.checkQuorum {
					ri = r.raftLog.committed
				}
				if m.From == None || m.From == r.id { // from local member
					r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
				} else {
					r.send(pb.Message{To: m.From, Type: pb.MsgReadIndexResp, Index: ri, Entries: m.Entries})
				}
			}
		} else {
			r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
		}

		return
	}

	// All other message types require a progress for m.From (pr).
	// 检查消息发送者当前是否在集群中
	pr, prOk := r.prs[m.From]
	if !prOk {
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return
	}
	switch m.Type {
	case pb.MsgAppResp:	// 对append消息的应答
		// 置位该节点当前是活跃的
		pr.RecentActive = true

		if m.Reject {	// 如果拒绝了append消息，说明term、index不匹配
			r.logger.Debugf("%x received msgApp rejection(lastindex: %d) from %x for index %d",
				r.id, m.RejectHint, m.From, m.Index)
			// rejecthint带来的是拒绝该app请求的节点，其最大日志的索引
			if pr.maybeDecrTo(m.Index, m.RejectHint) {	// 尝试回退关于该节点的Match、Next索引
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				if pr.State == ProgressStateReplicate {
					pr.becomeProbe()
				}
				// 再次发送append消息
				r.sendAppend(m.From)
			}
		} else {	// 通过该append请求
			oldPaused := pr.IsPaused()
			if pr.maybeUpdate(m.Index) {	// 如果该节点的索引发生了更新
				switch {
				case pr.State == ProgressStateProbe:
					// 如果当前该节点在探测状态，切换到可以接收副本状态
					pr.becomeReplicate()
				case pr.State == ProgressStateSnapshot && pr.needSnapshotAbort():
					// 如果当前该接在在接受快照状态，而且已经快照数据同步完成了
					r.logger.Debugf("%x snapshot aborted, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					// 切换到探测状态
					pr.becomeProbe()
				case pr.State == ProgressStateReplicate:
					// 如果当前该节点在接收副本状态，因为通过了该请求，所以滑动窗口可以释放在这之前的索引了
					pr.ins.freeTo(m.Index)
				}

				if r.maybeCommit() {
					// 如果可以commit日志，那么广播append消息
					r.bcastAppend()
				} else if oldPaused {
					// update() reset the wait state on this node. If we had delayed sending
					// an update before, send it now.
					// 如果该节点之前状态是暂停，继续发送append消息给它
					r.sendAppend(m.From)
				}
				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					// 要迁移过去的新leader，其日志已经追上了旧的leader
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MsgHeartbeatResp:
		// 该节点当前处于活跃状态
		pr.RecentActive = true
		// 这里调用resume是因为当前可能处于probe状态，而这个状态在两个heartbeat消息的间隔期只能收一条同步日志消息，因此在收到HB消息时就停止pause标记
		pr.resume()

		// free one slot for the full inflights window to allow progress.
		if pr.State == ProgressStateReplicate && pr.ins.full() {
			pr.ins.freeFirstOne()
		}
		// 该节点的match节点小于当前最大日志索引，可能已经过期了，尝试添加日志
		if pr.Match < r.raftLog.lastIndex() {
			r.sendAppend(m.From)
		}

		// 只有readonly safe方案，才会继续往下走
		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return
		}

		// 收到应答调用recvAck函数返回当前针对该消息已经应答的节点数量
		ackCount := r.readOnly.recvAck(m)
		if ackCount < r.quorum() {
			// 小于集群半数以上就返回不往下走了
			return
		}

		// 调用advance函数尝试丢弃已经被确认的read index状态
		rss := r.readOnly.advance(m)
		for _, rs := range rss {	// 遍历准备被丢弃的readindex状态
			req := rs.req
			if req.From == None || req.From == r.id { // from local member
				// 如果来自本地
				r.readStates = append(r.readStates, ReadState{Index: rs.index, RequestCtx: req.Entries[0].Data})
			} else {
				// 否则就是来自外部，需要应答
				r.send(pb.Message{To: req.From, Type: pb.MsgReadIndexResp, Index: rs.index, Entries: req.Entries})
			}
		}
	case pb.MsgSnapStatus:
		// 当前该节点状态已经不是在接受快照的状态了，直接返回
		if pr.State != ProgressStateSnapshot {
			return
		}
		if !m.Reject {
			// 接收快照成功
			pr.becomeProbe()
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		} else {
			// 接收快照失败
			pr.snapshotFailure()
			pr.becomeProbe()
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		}
		// If snapshot finish, wait for the msgAppResp from the remote node before sending
		// out the next msgApp.
		// If snapshot failure, wait for a heartbeat interval before next try
		// 先暂停等待下一次被唤醒
		pr.pause()
	case pb.MsgUnreachable:
		// During optimistic replication, if the remote becomes unreachable,
		// there is huge probability that a MsgApp is lost.
		if pr.State == ProgressStateReplicate {
			pr.becomeProbe()
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
	case pb.MsgTransferLeader:
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			// 判断是否已经有相同节点的leader转让流程在进行中
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, leadTransferee, leadTransferee)
				// 如果是，直接返回
				return
			}
			// 否则中断之前的转让流程
			r.abortLeaderTransfer()
			r.logger.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
		}
		// 判断是否转让过来的leader是否本节点，如果是也直接返回，因为本节点已经是leader了
		if leadTransferee == r.id {
			r.logger.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
			return
		}
		// Transfer leadership to third party.
		r.logger.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.raftLog.lastIndex() {
			// 如果日志已经匹配了，那么就发送timeoutnow协议过去
			r.sendTimeoutNow(leadTransferee)
			r.logger.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else {
			// 否则继续追加日志
			r.sendAppend(leadTransferee)
		}
	}
}

// stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
// whether they respond to MsgVoteResp or MsgPreVoteResp.
func stepCandidate(r *raft, m pb.Message) {
	// Only handle vote responses corresponding to our candidacy (while in
	// StateCandidate, we may get stale MsgPreVoteResp messages in this term from
	// our pre-candidate state).
	var myVoteRespType pb.MessageType
	if r.state == StatePreCandidate {
		myVoteRespType = pb.MsgPreVoteResp
	} else {
		myVoteRespType = pb.MsgVoteResp
	}
	// 以下转换成follower状态时，为什么不判断消息的term是否至少大于当前节点的term？？？

	switch m.Type {
	case pb.MsgProp:
		// 当前没有leader，所以忽略掉提交的消息
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return
	case pb.MsgApp:
		// 收到append消息，说明集群中已经有leader，转换为follower
		r.becomeFollower(r.Term, m.From)
		// 添加日志
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		// 收到HB消息，说明集群已经有leader，转换为follower
		r.becomeFollower(r.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		// 收到快照消息，说明集群已经有leader，转换为follower
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case myVoteRespType:
		// 计算当前集群中有多少节点给自己投了票
		gr := r.poll(m.From, m.Type, !m.Reject)
		r.logger.Infof("%x [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.quorum(), gr, m.Type, len(r.votes)-gr)
		switch r.quorum() {
		case gr:	// 如果进行投票的节点数量正好是半数以上节点数量
			if r.state == StatePreCandidate {
				r.campaign(campaignElection)
			} else {
				// 变成leader
				r.becomeLeader()
				r.bcastAppend()
			}
		case len(r.votes) - gr:	// 如果是半数以上节点拒绝了投票
			// 变成follower
			r.becomeFollower(r.Term, None)
		}
	case pb.MsgTimeoutNow:
		// candidate收到timeout指令忽略不处理
		r.logger.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.state, m.From)
	}
}

// follower的状态机函数
func stepFollower(r *raft, m pb.Message) {
	switch m.Type {
	case pb.MsgProp:
		// 本节点提交的值
		if r.lead == None {
			// 没有leader则提交失败，忽略
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return
		}
		// 向leader进行redirect
		m.To = r.lead
		r.send(m)
	case pb.MsgApp:
		// append消息
		// 收到leader的app消息，重置选举tick计时器，因为这样证明leader还存活
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		// HB消息
		// 收到leader的HB消息，重置选举tick计时器，因为这样证明leader还存活
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		// 快照消息
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case pb.MsgTransferLeader:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return
		}
		// 向leader转发leader转让消息
		m.To = r.lead
		r.send(m)
	case pb.MsgTimeoutNow:
		if r.promotable() {	// 如果本节点可以提升为leader，那么就发起新一轮的竞选
			r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
			// Leadership transfers never use pre-vote even if r.preVote is true; we
			// know we are not recovering from a partition so there is no need for the
			// extra round trip.
			// timeout消息用在leader转让中，所以不需要prevote即使开了这个选项
			r.campaign(campaignTransfer)
		} else {
			r.logger.Infof("%x received MsgTimeoutNow from %x but is not promotable", r.id, m.From)
		}
	case pb.MsgReadIndex:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return
		}
		// 向leader转发此类型消息
		m.To = r.lead
		r.send(m)
	case pb.MsgReadIndexResp:
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return
		}
		// 更新readstates数组
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
}

// 添加日志
func (r *raft) handleAppendEntries(m pb.Message) {
	// 先检查消息消息的合法性
	if m.Index < r.raftLog.committed {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
		return
	}

	r.logger.Infof("%x -> %x index %d", m.From, r.id, m.Index)
	// 尝试添加到日志模块中
	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		// 添加成功，返回的index是添加成功之后的最大index
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: mlastIndex})
	} else {
		// 添加失败
		r.logger.Debugf("%x [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %x",
			r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		// 添加失败的时候，返回的Index是传入过来的Index，RejectHint是该节点当前日志的最后索引
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: m.Index, Reject: true, RejectHint: r.raftLog.lastIndex()})
	}
}

// 处理HB消息
func (r *raft) handleHeartbeat(m pb.Message) {
	r.raftLog.commitTo(m.Commit)
	// 要把HB消息带过来的context原样返回
	r.send(pb.Message{To: m.From, Type: pb.MsgHeartbeatResp, Context: m.Context})
}

func (r *raft) handleSnapshot(m pb.Message) {
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	// 注意这里成功与失败，只是返回的Index参数不同
	if r.restore(m.Snapshot) {
		r.logger.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.lastIndex()})
	} else {
		r.logger.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
	}
}

// restore recovers the state machine from a snapshot. It restores the log and the
// configuration of state machine.
// 使用快照数据进行恢复
func (r *raft) restore(s pb.Snapshot) bool {
	// 首先判断快照索引的合法性
	if s.Metadata.Index <= r.raftLog.committed {
		return false
	}

	// matchTerm返回true，说明本节点的日志中已经有对应的日志了
	if r.raftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
		// 提交到快照所在的索引
		r.raftLog.commitTo(s.Metadata.Index)
		// 为什么这里返回false？
		return false
	}

	r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] starts to restore snapshot [index: %d, term: %d]",
		r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)

	// 使用快照数据进行日志的恢复
	r.raftLog.restore(s)
	r.prs = make(map[uint64]*Progress)
	// 包括集群中其他节点的状态也使用快照中的状态数据进行恢复
	for _, n := range s.Metadata.ConfState.Nodes {
		match, next := uint64(0), r.raftLog.lastIndex()+1
		if n == r.id {
			match = next - 1
		}
		r.setProgress(n, match, next)
		r.logger.Infof("%x restored progress of %x [%s]", r.id, n, r.prs[n])
	}
	return true
}

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
// 返回是否可以被提升为leader
func (r *raft) promotable() bool {
	// 在prs数组中找到本节点，说明该节点在集群中，这就具备了可以被选为leader的条件
	_, ok := r.prs[r.id]
	return ok
}

// 增加一个节点
func (r *raft) addNode(id uint64) {
	// 重置pengdingConf标志位
	r.pendingConf = false
	// 检查是否已经存在节点列表中
	if _, ok := r.prs[id]; ok {
		// Ignore any redundant addNode calls (which can happen because the
		// initial bootstrapping entries are applied twice).
		return
	}

	// 这里才真的添加进来
	r.setProgress(id, 0, r.raftLog.lastIndex()+1)
}

// 删除一个节点
func (r *raft) removeNode(id uint64) {
	r.delProgress(id)
	// 重置pengdingConf标志位
	r.pendingConf = false

	// do not try to commit or abort transferring if there is no nodes in the cluster.
	if len(r.prs) == 0 {
		return
	}

	// The quorum size is now smaller, so see if any pending entries can
	// be committed.
	// 由于删除了节点，所以半数节点的数量变少了，于是去查看是否有可以认为提交成功的数据
	if r.maybeCommit() {
		r.bcastAppend()
	}
	// If the removed node is the leadTransferee, then abort the leadership transferring.
	if r.state == StateLeader && r.leadTransferee == id {
		// 如果在leader迁移过程中发生了删除节点的操作，那么中断迁移leader流程
		r.abortLeaderTransfer()
	}
}

func (r *raft) resetPendingConf() { r.pendingConf = false }

func (r *raft) setProgress(id, match, next uint64) {
	r.prs[id] = &Progress{Next: next, Match: match, ins: newInflights(r.maxInflight)}
}

func (r *raft) delProgress(id uint64) {
	delete(r.prs, id)
}

func (r *raft) loadState(state pb.HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// pastElectionTimeout returns true iff r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

// checkQuorumActive returns true if the quorum is active from
// the view of the local raft state machine. Otherwise, it returns
// false.
// checkQuorumActive also resets all RecentActive to false.
// 检查集群的可用于，当小于半数的节点不可用时返回false
func (r *raft) checkQuorumActive() bool {
	var act int

	for id := range r.prs {
		if id == r.id { // self is always active
			act++
			continue
		}

		if r.prs[id].RecentActive {
			act++
		}

		r.prs[id].RecentActive = false
	}

	return act >= r.quorum()
}

func (r *raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, Type: pb.MsgTimeoutNow})
}

// 中断leader转让流程
func (r *raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

// 返回消息数组中配置变化的消息数量
func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].Type == pb.EntryConfChange {
			n++
		}
	}
	return n
}
