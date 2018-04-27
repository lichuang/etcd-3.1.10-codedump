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

/*
This file contains tests which verify that the scenarios described
in the raft paper (https://ramcloud.stanford.edu/raft.pdf) are
handled by the raft implementation correctly. Each test focuses on
several sentences written in the paper. This could help us to prevent
most implementation bugs.

Each test is composed of three parts: init, test and check.
Init part uses simple and understandable way to simulate the init state.
Test part uses Step function to generate the scenario. Check part checks
outgoing messages and state.
*/
package raft

import (
	"fmt"
	"testing"

	"reflect"
	"sort"

	pb "github.com/coreos/etcd/raft/raftpb"
)

func TestFollowerUpdateTermFromMessage(t *testing.T) {
	testUpdateTermFromMessage(t, StateFollower)
}
func TestCandidateUpdateTermFromMessage(t *testing.T) {
	testUpdateTermFromMessage(t, StateCandidate)
}
func TestLeaderUpdateTermFromMessage(t *testing.T) {
	testUpdateTermFromMessage(t, StateLeader)
}

// testUpdateTermFromMessage tests that if one server’s current term is
// smaller than the other’s, then it updates its current term to the larger
// value. If a candidate or leader discovers that its term is out of date,
// it immediately reverts to follower state.
// Reference: section 5.1
// 收到更大term提交的消息时，切换到follower状态
func testUpdateTermFromMessage(t *testing.T, state StateType) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	switch state {
	case StateFollower:
		r.becomeFollower(1, 2)
	case StateCandidate:
		r.becomeCandidate()
	case StateLeader:
		r.becomeCandidate()
		r.becomeLeader()
	}

	r.Step(pb.Message{Type: pb.MsgApp, Term: 2})

	if r.Term != 2 {
		t.Errorf("term = %d, want %d", r.Term, 2)
	}
	if r.state != StateFollower {
		t.Errorf("state = %v, want %v", r.state, StateFollower)
	}
}

// TestRejectStaleTermMessage tests that if a server receives a request with
// a stale term number, it rejects the request.
// Our implementation ignores the request instead.
// Reference: section 5.1
// 收到过期的term消息时，该消息不会被处理
func TestRejectStaleTermMessage(t *testing.T) {
	called := false
	fakeStep := func(r *raft, m pb.Message) {
		called = true
	}
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	r.step = fakeStep
	r.loadState(pb.HardState{Term: 2})

	r.Step(pb.Message{Type: pb.MsgApp, Term: r.Term - 1})

	if called {
		t.Errorf("stepFunc called = %v, want %v", called, false)
	}
}

// TestStartAsFollower tests that when servers start up, they begin as followers.
// Reference: section 5.2
// 服务启动时是follower状态
func TestStartAsFollower(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	if r.state != StateFollower {
		t.Errorf("state = %s, want %s", r.state, StateFollower)
	}
}

// TestLeaderBcastBeat tests that if the leader receives a heartbeat tick,
// it will send a msgApp with m.Index = 0, m.LogTerm=0 and empty entries as
// heartbeat to all followers.
// Reference: section 5.2
// leader在tick时会向follower发送HB消息
func TestLeaderBcastBeat(t *testing.T) {
	// heartbeat interval
	hi := 1
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, hi, NewMemoryStorage())
	r.becomeCandidate()
	r.becomeLeader()
	for i := 0; i < 10; i++ {
		r.appendEntry(pb.Entry{Index: uint64(i) + 1})
	}

	for i := 0; i < hi; i++ {
		r.tick()
	}

	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 1, Type: pb.MsgHeartbeat},
		{From: 1, To: 3, Term: 1, Type: pb.MsgHeartbeat},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %v, want %v", msgs, wmsgs)
	}
}

func TestFollowerStartElection(t *testing.T) {
	testNonleaderStartElection(t, StateFollower)
}
func TestCandidateStartNewElection(t *testing.T) {
	testNonleaderStartElection(t, StateCandidate)
}

// testNonleaderStartElection tests that if a follower receives no communication
// over election timeout, it begins an election to choose a new leader. It
// increments its current term and transitions to candidate state. It then
// votes for itself and issues RequestVote RPCs in parallel to each of the
// other servers in the cluster.
// Reference: section 5.2
// Also if a candidate fails to obtain a majority, it will time out and
// start a new election by incrementing its term and initiating another
// round of RequestVote RPCs.
// Reference: section 5.2
// 当一个follower在选举超时内没有收到leader的消息，那么将递增term并且切换到candidate状态并且向其他server
// 发送MsgVote消息选举自己为leader
// 同样的，一个candidate在没有收到过半选票时，也会递增term进行下一轮选举，直到出现了leader
func testNonleaderStartElection(t *testing.T, state StateType) {
	// election timeout
	et := 10
	r := newTestRaft(1, []uint64{1, 2, 3}, et, 1, NewMemoryStorage())
	switch state {
	case StateFollower:
		r.becomeFollower(1, 2)
	case StateCandidate:
		r.becomeCandidate()
	}

	// 模拟超时的场景
	for i := 1; i < 2*et; i++ {
		r.tick()
	}

	if r.Term != 2 {
		t.Errorf("term = %d, want 2", r.Term)
	}
	if r.state != StateCandidate {
		t.Errorf("state = %s, want %s", r.state, StateCandidate)
	}
	if !r.votes[r.id] {
		t.Errorf("vote for self = false, want true")
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 2, Type: pb.MsgVote},
		{From: 1, To: 3, Term: 2, Type: pb.MsgVote},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %v, want %v", msgs, wmsgs)
	}
}

// TestLeaderElectionInOneRoundRPC tests all cases that may happen in
// leader election during one round of RequestVote RPC:
// a) it wins the election
// b) it loses the election
// c) it is unclear about the result
// Reference: section 5.2
// 测试不同场景下选举的情况：
// a) 赢得超过半数选票成为leader
// b) 超过半数拒绝，转换为follower
// c) 没有赢得半数投票，仍然在candidate状态
// 需要区分以上两种bc情况：情况b是明确拒绝，c是没有收到选票。
func TestLeaderElectionInOneRoundRPC(t *testing.T) {
	tests := []struct {
		size  int
		votes map[uint64]bool
		state StateType
	}{
		// win the election when receiving votes from a majority of the servers
		{1, map[uint64]bool{}, StateLeader},
		{3, map[uint64]bool{2: true, 3: true}, StateLeader},
		{3, map[uint64]bool{2: true}, StateLeader},
		{5, map[uint64]bool{2: true, 3: true, 4: true, 5: true}, StateLeader},
		{5, map[uint64]bool{2: true, 3: true, 4: true}, StateLeader},
		{5, map[uint64]bool{2: true, 3: true}, StateLeader},

		// return to follower state if it receives vote denial from a majority
		{3, map[uint64]bool{2: false, 3: false}, StateFollower},
		{5, map[uint64]bool{2: false, 3: false, 4: false, 5: false}, StateFollower},
		{5, map[uint64]bool{2: true, 3: false, 4: false, 5: false}, StateFollower},

		// stay in candidate if it does not obtain the majority
		{3, map[uint64]bool{}, StateCandidate},
		{5, map[uint64]bool{2: true}, StateCandidate},
		{5, map[uint64]bool{2: false, 3: false}, StateCandidate},
		{5, map[uint64]bool{}, StateCandidate},
	}
	for i, tt := range tests {
		r := newTestRaft(1, idsBySize(tt.size), 10, 1, NewMemoryStorage())

		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
		for id, vote := range tt.votes {
			r.Step(pb.Message{From: id, To: 1, Type: pb.MsgVoteResp, Reject: !vote})
		}

		if r.state != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, r.state, tt.state)
		}
		if g := r.Term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

// TestFollowerVote tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2
// 测试每次选举只能进行一次投票，先到先得
func TestFollowerVote(t *testing.T) {
	tests := []struct {
		vote    uint64
		nvote   uint64
		wreject bool
	}{
		{None, 1, false},
		{None, 2, false},
		{1, 1, false},
		{2, 2, false},
		{1, 2, true},
		{2, 1, true},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		r.loadState(pb.HardState{Term: 1, Vote: tt.vote})

		r.Step(pb.Message{From: tt.nvote, To: 1, Term: 1, Type: pb.MsgVote})

		msgs := r.readMessages()
		wmsgs := []pb.Message{
			{From: 1, To: tt.nvote, Term: 1, Type: pb.MsgVoteResp, Reject: tt.wreject},
		}
		if !reflect.DeepEqual(msgs, wmsgs) {
			t.Errorf("#%d: msgs = %v, want %v", i, msgs, wmsgs)
		}
	}
}

// TestCandidateFallback tests that while waiting for votes,
// if a candidate receives an AppendEntries RPC from another server claiming
// to be leader whose term is at least as large as the candidate's current term,
// it recognizes the leader as legitimate and returns to follower state.
// Reference: section 5.2
// 验证在选举期间，收到其他节点发送过来的消息，且消息中的term至少比当前选举时的term大，那么将返回到follower状态
func TestCandidateFallback(t *testing.T) {
	tests := []pb.Message{
		{From: 2, To: 1, Term: 1, Type: pb.MsgApp},
		{From: 2, To: 1, Term: 2, Type: pb.MsgApp},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
		if r.state != StateCandidate {
			t.Fatalf("unexpected state = %s, want %s", r.state, StateCandidate)
		}

		r.Step(tt)

		if g := r.state; g != StateFollower {
			t.Errorf("#%d: state = %s, want %s", i, g, StateFollower)
		}
		if g := r.Term; g != tt.Term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.Term)
		}
	}
}

func TestFollowerElectionTimeoutRandomized(t *testing.T) {
	SetLogger(discardLogger)
	defer SetLogger(defaultLogger)
	testNonleaderElectionTimeoutRandomized(t, StateFollower)
}
func TestCandidateElectionTimeoutRandomized(t *testing.T) {
	SetLogger(discardLogger)
	defer SetLogger(defaultLogger)
	testNonleaderElectionTimeoutRandomized(t, StateCandidate)
}

// testNonleaderElectionTimeoutRandomized tests that election timeout for
// follower or candidate is randomized.
// Reference: section 5.2
// 验证follower或者candidate的选举超时时间是随机的
func testNonleaderElectionTimeoutRandomized(t *testing.T, state StateType) {
	et := 10
	r := newTestRaft(1, []uint64{1, 2, 3}, et, 1, NewMemoryStorage())
	timeouts := make(map[int]bool)
	for round := 0; round < 50*et; round++ {
		switch state {
		case StateFollower:
			r.becomeFollower(r.Term+1, 2)
		case StateCandidate:
			r.becomeCandidate()
		}

		time := 0
		for len(r.readMessages()) == 0 {
			r.tick()
			time++
		}
		timeouts[time] = true
	}

	for d := et + 1; d < 2*et; d++ {
		if !timeouts[d] {
			t.Errorf("timeout in %d ticks should happen", d)
		}
	}
}

func TestFollowersElectioinTimeoutNonconflict(t *testing.T) {
	SetLogger(discardLogger)
	defer SetLogger(defaultLogger)
	testNonleadersElectionTimeoutNonconflict(t, StateFollower)
}
func TestCandidatesElectionTimeoutNonconflict(t *testing.T) {
	SetLogger(discardLogger)
	defer SetLogger(defaultLogger)
	testNonleadersElectionTimeoutNonconflict(t, StateCandidate)
}

// testNonleadersElectionTimeoutNonconflict tests that in most cases only a
// single server(follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election.
// Reference: section 5.2
// 验证在绝大多数情况下，只有可能有一个server超时，避免出现了脑裂的情况
func testNonleadersElectionTimeoutNonconflict(t *testing.T, state StateType) {
	et := 10
	size := 5
	rs := make([]*raft, size)
	ids := idsBySize(size)
	for k := range rs {
		rs[k] = newTestRaft(ids[k], ids, et, 1, NewMemoryStorage())
	}
	conflicts := 0
	for round := 0; round < 1000; round++ {
		for _, r := range rs {
			switch state {
			case StateFollower:
				r.becomeFollower(r.Term+1, None)
			case StateCandidate:
				r.becomeCandidate()
			}
		}

		timeoutNum := 0
		for timeoutNum == 0 {
			for _, r := range rs {
				r.tick()
				if len(r.readMessages()) > 0 {
					timeoutNum++
				}
			}
		}
		// several rafts time out at the same tick
		if timeoutNum > 1 {
			conflicts++
		}
	}

	// 同时有两个以上节点超时的比例，不超过千分之0.3
	if g := float64(conflicts) / 1000; g > 0.3 {
		t.Errorf("probability of conflicts = %v, want <= 0.3", g)
	}
}

// TestLeaderStartReplication tests that when receiving client proposals,
// the leader appends the proposal to its log as a new entry, then issues
// AppendEntries RPCs in parallel to each of the other servers to replicate
// the entry. Also, when sending an AppendEntries RPC, the leader includes
// the index and term of the entry in its log that immediately precedes
// the new entries.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
// 验证leader收到proposal消息时，将向其他节点发送Append消息，该消息的
func TestLeaderStartReplication(t *testing.T) {
	s := NewMemoryStorage()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, s)
	r.becomeCandidate()
	r.becomeLeader()
	commitNoopEntry(r, s)
	li := r.raftLog.lastIndex()

	ents := []pb.Entry{{Data: []byte("some data")}}
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: ents})

	if g := r.raftLog.lastIndex(); g != li+1 {
		t.Errorf("lastIndex = %d, want %d", g, li+1)
	}
	if g := r.raftLog.committed; g != li {
		t.Errorf("committed = %d, want %d", g, li)
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wents := []pb.Entry{{Index: li + 1, Term: 1, Data: []byte("some data")}}
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 1, Type: pb.MsgApp, Index: li, LogTerm: 1, Entries: wents, Commit: li},
		{From: 1, To: 3, Term: 1, Type: pb.MsgApp, Index: li, LogTerm: 1, Entries: wents, Commit: li},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %+v, want %+v", msgs, wmsgs)
	}
	if g := r.raftLog.unstableEntries(); !reflect.DeepEqual(g, wents) {
		t.Errorf("ents = %+v, want %+v", g, wents)
	}
}

// TestLeaderCommitEntry tests that when the entry has been safely replicated,
// the leader gives out the applied entries, which can be applied to its state
// machine.
// Also, the leader keeps track of the highest index it knows to be committed,
// and it includes that index in future AppendEntries RPCs so that the other
// servers eventually find out.
// Reference: section 5.3
// 验证当MsgProp消息被集群的半数以上节点通过时，leader将修改自己日志的commit索引值，然后向
// 集群的其他节点发出MsgApp消息，对该通过的消息进行append，同时消息中会带上leader当前日志commit索引的信息
func TestLeaderCommitEntry(t *testing.T) {
	s := NewMemoryStorage()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, s)
	// 成为leader
	r.becomeCandidate()
	r.becomeLeader()
	commitNoopEntry(r, s)
	// 得到当前日志的最后一条索引
	li := r.raftLog.lastIndex()
	// 提交一个新的值
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})
	// 模拟集群中其他节点都通过该提交
	for _, m := range r.readMessages() {
		r.Step(acceptAndReply(m))
	}
	// 验证提交完成之后，commit索引是前面保存的最后一条索引+1
	if g := r.raftLog.committed; g != li+1 {
		t.Errorf("committed = %d, want %d", g, li+1)
	}
	// 验证提交完成之后，leader上面日志数据
	wents := []pb.Entry{{Index: li + 1, Term: 1, Data: []byte("some data")}}
	if g := r.raftLog.nextEnts(); !reflect.DeepEqual(g, wents) {
		t.Errorf("nextEnts = %+v, want %+v", g, wents)
	}
	// 验证向集群中其他节点发出了append消息，append刚刚提交成功的值，同时带上了commit索引信息
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	for i, m := range msgs {
		if w := uint64(i + 2); m.To != w {
			t.Errorf("to = %x, want %x", m.To, w)
		}
		if m.Type != pb.MsgApp {
			t.Errorf("type = %v, want %v", m.Type, pb.MsgApp)
		}
		if m.Commit != li+1 {
			t.Errorf("commit = %d, want %d", m.Commit, li+1)
		}
	}
}

// TestLeaderAcknowledgeCommit tests that a log entry is committed once the
// leader that created the entry has replicated it on a majority of the servers.
// Reference: section 5.3
// 验证当leader收到超过半数的节点应答，那么将commit提交的日志条目
func TestLeaderAcknowledgeCommit(t *testing.T) {
	tests := []struct {
		// 集群节点的数量
		size      int
		// 每个节点是否接收proposal
		acceptors map[uint64]bool
		wack      bool
	}{
		{1, nil, true},
		{3, nil, false},
		{3, map[uint64]bool{2: true}, true},
		{3, map[uint64]bool{2: true, 3: true}, true},
		{5, nil, false},
		{5, map[uint64]bool{2: true}, false},
		{5, map[uint64]bool{2: true, 3: true}, true},
		{5, map[uint64]bool{2: true, 3: true, 4: true}, true},
		{5, map[uint64]bool{2: true, 3: true, 4: true, 5: true}, true},
	}
	for i, tt := range tests {
		s := NewMemoryStorage()
		r := newTestRaft(1, idsBySize(tt.size), 10, 1, s)
		r.becomeCandidate()
		r.becomeLeader()
		commitNoopEntry(r, s)
		li := r.raftLog.lastIndex()
		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

		for _, m := range r.readMessages() {
			if tt.acceptors[m.To] {
				r.Step(acceptAndReply(m))
			}
		}

		if g := r.raftLog.committed > li; g != tt.wack {
			t.Errorf("#%d: ack commit = %v, want %v", i, g, tt.wack)
		}
	}
}

// TestLeaderCommitPrecedingEntries tests that when leader commits a log entry,
// it also commits all preceding entries in the leader’s log, including
// entries created by previous leaders.
// Also, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
// 验证当leader commit一条日志条目，在这之前的所有日志都会被提交，包括以前任期内提交的日志
// 而这些提交的日志排序是根据index来进行排序的
func TestLeaderCommitPrecedingEntries(t *testing.T) {
	tests := [][]pb.Entry{
		{},
		{{Term: 2, Index: 1}},
		{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
		{{Term: 1, Index: 1}},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		// 模拟storage中已经存在了一些日志条目
		storage.Append(tt)
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, storage)
		// 模拟启动时以任期2开始
		r.loadState(pb.HardState{Term: 2})
		// 成为leader
		r.becomeCandidate()
		r.becomeLeader()
		// 成为leader之后提交一条消息
		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

		// 模拟集群中其他机器都同意了提交
		for _, m := range r.readMessages() {
			r.Step(acceptAndReply(m))
		}

		li := uint64(len(tt))
		// 日志数组为：前面通过storage.Append添加进去的测试日志（代表在任期之前的日志）、成为leader之后提交的一条空日志、调用Step提交的数据
		wents := append(tt, pb.Entry{Term: 3, Index: li + 1}, pb.Entry{Term: 3, Index: li + 2, Data: []byte("some data")})
		// 验证提交但是还没有apply的日志是不是正确
		if g := r.raftLog.nextEnts(); !reflect.DeepEqual(g, wents) {
			t.Errorf("#%d: ents = %+v, want %+v", i, g, wents)
		}
	}
}

// TestFollowerCommitEntry tests that once a follower learns that a log entry
// is committed, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
// TODO
// 验证当follower收到来自leader的append消息时，apply日志条目的顺序
func TestFollowerCommitEntry(t *testing.T) {
	tests := []struct {
		ents   []pb.Entry
		commit uint64
	}{
		{
			[]pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
			},
			1,
		},
		{
			[]pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
				{Term: 1, Index: 2, Data: []byte("some data2")},
			},
			2,
		},
		{
			[]pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data2")},
				{Term: 1, Index: 2, Data: []byte("some data")},
			},
			2,
		},
		{
			[]pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
				{Term: 1, Index: 2, Data: []byte("some data2")},
			},
			1,
		},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		r.becomeFollower(1, 2)

		r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgApp, Term: 1, Entries: tt.ents, Commit: tt.commit})

		if g := r.raftLog.committed; g != tt.commit {
			t.Errorf("#%d: committed = %d, want %d", i, g, tt.commit)
		}
		wents := tt.ents[:int(tt.commit)]
		if g := r.raftLog.nextEnts(); !reflect.DeepEqual(g, wents) {
			t.Errorf("#%d: nextEnts = %v, want %v", i, g, wents)
		}
	}
}

// TestFollowerCheckMsgApp tests that if the follower does not find an
// entry in its log with the same index and term as the one in AppendEntries RPC,
// then it refuses the new entries. Otherwise it replies that it accepts the
// append entries.
// Reference: section 5.3
func TestFollowerCheckMsgApp(t *testing.T) {
	// 提前准备好的已持久化的数据
	ents := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		term        uint64
		index       uint64
		windex      uint64
		wreject     bool
		wrejectHint uint64
	}{
		// match with committed entries
		{0, 0, 1, false, 0},
		{ents[0].Term, ents[0].Index, 1, false, 0},
		// match with uncommitted entries
		{ents[1].Term, ents[1].Index, 2, false, 0},

		// unmatch with existing entry
		{ents[0].Term, ents[1].Index, ents[1].Index, true, 2},
		// unexisting entry
		{ents[1].Term + 1, ents[1].Index + 1, ents[1].Index + 1, true, 2},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append(ents)
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, storage)
		r.loadState(pb.HardState{Commit: 1})
		r.becomeFollower(2, 2)

		r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgApp, Term: 2, LogTerm: tt.term, Index: tt.index})

		msgs := r.readMessages()
		wmsgs := []pb.Message{
			{From: 1, To: 2, Type: pb.MsgAppResp, Term: 2, Index: tt.windex, Reject: tt.wreject, RejectHint: tt.wrejectHint},
		}
		if !reflect.DeepEqual(msgs, wmsgs) {
			t.Errorf("#%d: msgs = %+v, want %+v", i, msgs, wmsgs)
		}
	}
}

// TestFollowerAppendEntries tests that when AppendEntries RPC is valid,
// the follower will delete the existing conflict entry and all that follow it,
// and append any new entries not already in the log.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
// 验证当follower收到合法的Append请求时，将删除冲突的日志条目，然后将还没有的日志条目添加进来
func TestFollowerAppendEntries(t *testing.T) {
	tests := []struct {
		index, term uint64
		// 通过append消息尝试添加进去的日志条目
		ents        []pb.Entry
		// 所有的日志数组
		wents       []pb.Entry
		// 未持久化的日志数组
		wunstable   []pb.Entry
	}{
		{
			2, 2,
			[]pb.Entry{{Term: 3, Index: 3}},
			[]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}},
			[]pb.Entry{{Term: 3, Index: 3}},
		},
		{
			1, 1,
			[]pb.Entry{{Term: 3, Index: 2}, {Term: 4, Index: 3}},
			[]pb.Entry{{Term: 1, Index: 1}, {Term: 3, Index: 2}, {Term: 4, Index: 3}},
			[]pb.Entry{{Term: 3, Index: 2}, {Term: 4, Index: 3}},
		},
		{
			0, 0,
			[]pb.Entry{{Term: 1, Index: 1}},
			[]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
			nil,
		},
		{
			0, 0,
			[]pb.Entry{{Term: 3, Index: 1}},
			[]pb.Entry{{Term: 3, Index: 1}},
			[]pb.Entry{{Term: 3, Index: 1}},
		},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append([]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}})
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, storage)
		// 角色为follower
		r.becomeFollower(2, 2)

		// 模拟收到leader的append消息
		r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgApp, Term: 2, LogTerm: tt.term, Index: tt.index, Entries: tt.ents})

		// 验证在收到消息之后的日志条目
		if g := r.raftLog.allEntries(); !reflect.DeepEqual(g, tt.wents) {
			t.Errorf("#%d: ents = %+v, want %+v", i, g, tt.wents)
		}
		if g := r.raftLog.unstableEntries(); !reflect.DeepEqual(g, tt.wunstable) {
			t.Errorf("#%d: unstableEnts = %+v, want %+v", i, g, tt.wunstable)
		}
	}
}

// TestLeaderSyncFollowerLog tests that the leader could bring a follower's log
// into consistency with its own.
// Reference: section 5.3, figure 7
// 验证leader最终会将follower的日志同步成与leader端一致的日志
func TestLeaderSyncFollowerLog(t *testing.T) {
	ents := []pb.Entry{
		{},
		{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
		{Term: 4, Index: 4}, {Term: 4, Index: 5},
		{Term: 5, Index: 6}, {Term: 5, Index: 7},
		{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10},
	}
	term := uint64(8)
	tests := [][]pb.Entry{
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5},
			{Term: 5, Index: 6}, {Term: 5, Index: 7},
			{Term: 6, Index: 8}, {Term: 6, Index: 9},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5},
			{Term: 5, Index: 6}, {Term: 5, Index: 7},
			{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10}, {Term: 6, Index: 11},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5},
			{Term: 5, Index: 6}, {Term: 5, Index: 7},
			{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10},
			{Term: 7, Index: 11}, {Term: 7, Index: 12},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5}, {Term: 4, Index: 6}, {Term: 4, Index: 7},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 2, Index: 4}, {Term: 2, Index: 5}, {Term: 2, Index: 6},
			{Term: 3, Index: 7}, {Term: 3, Index: 8}, {Term: 3, Index: 9}, {Term: 3, Index: 10}, {Term: 3, Index: 11},
		},
	}
	for i, tt := range tests {
		leadStorage := NewMemoryStorage()
		// leader的存储预先加载测试数据
		leadStorage.Append(ents)
		lead := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, leadStorage)
		lead.loadState(pb.HardState{Commit: lead.raftLog.lastIndex(), Term: term})
		followerStorage := NewMemoryStorage()
		// follower的存储加载的是测试用例数据
		followerStorage.Append(tt)
		follower := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, followerStorage)
		follower.loadState(pb.HardState{Term: term - 1})
		// It is necessary to have a three-node cluster.
		// The second may have more up-to-date log than the first one, so the
		// first node needs the vote from the third node to become the leader.
		n := newNetwork(lead, follower, nopStepper)
		// 选举
		n.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
		// The election occurs in the term after the one we loaded with
		// lead.loadState above.
		n.send(pb.Message{From: 3, To: 1, Type: pb.MsgVoteResp, Term: term + 1})

		n.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

		if g := diffu(ltoa(lead.raftLog), ltoa(follower.raftLog)); g != "" {
			t.Errorf("#%d: log diff:\n%s", i, g)
		}
	}
}

// TestVoteRequest tests that the vote request includes information about the candidate’s log
// and are sent to all of the other nodes.
// Reference: section 5.4.1
// 模拟candidate的投票请求中，会带上当前该节点日志的term、index、logterm信息
func TestVoteRequest(t *testing.T) {
	tests := []struct {
		ents  []pb.Entry
		wterm uint64
	}{
		{[]pb.Entry{{Term: 1, Index: 1}}, 2},
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}, 3},
	}
	for j, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		// 先append测试的数据
		r.Step(pb.Message{
			From: 2, To: 1, Type: pb.MsgApp, Term: tt.wterm - 1, LogTerm: 0, Index: 0, Entries: tt.ents,
		})
		r.readMessages()

		// 模拟超时
		for i := 1; i < r.electionTimeout*2; i++ {
			r.tickElection()
		}

		msgs := r.readMessages()
		sort.Sort(messageSlice(msgs))
		if len(msgs) != 2 {
			t.Fatalf("#%d: len(msg) = %d, want %d", j, len(msgs), 2)
		}
		// 验证发出去的消息都是投票消息，并且带上了相关的信息
		for i, m := range msgs {
			if m.Type != pb.MsgVote {
				t.Errorf("#%d: msgType = %d, want %d", i, m.Type, pb.MsgVote)
			}
			if m.To != uint64(i+2) {
				t.Errorf("#%d: to = %d, want %d", i, m.To, i+2)
			}
			if m.Term != tt.wterm {
				t.Errorf("#%d: term = %d, want %d", i, m.Term, tt.wterm)
			}
			windex, wlogterm := tt.ents[len(tt.ents)-1].Index, tt.ents[len(tt.ents)-1].Term
			if m.Index != windex {
				t.Errorf("#%d: index = %d, want %d", i, m.Index, windex)
			}
			if m.LogTerm != wlogterm {
				t.Errorf("#%d: logterm = %d, want %d", i, m.LogTerm, wlogterm)
			}
		}
	}
}

// TestVoter tests the voter denies its vote if its own log is more up-to-date
// than that of the candidate.
// Reference: section 5.4.1
// 验证投票者不会通过比自己当前日志还旧的candidate投票
func TestVoter(t *testing.T) {
	tests := []struct {
		ents    []pb.Entry
		logterm uint64
		index   uint64

		wreject bool
	}{
		// same logterm
		// logterm相同，index相同
		{[]pb.Entry{{Term: 1, Index: 1}}, 1, 1, false},
		// logterm相同，index更大
		{[]pb.Entry{{Term: 1, Index: 1}}, 1, 2, false},
		// logterm,index都小，拒绝
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, 1, 1, true},
		// candidate higher logterm
		{[]pb.Entry{{Term: 1, Index: 1}}, 2, 1, false},
		{[]pb.Entry{{Term: 1, Index: 1}}, 2, 2, false},
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, 2, 1, false},
		// voter higher logterm
		{[]pb.Entry{{Term: 2, Index: 1}}, 1, 1, true},
		{[]pb.Entry{{Term: 2, Index: 1}}, 1, 2, true},
		{[]pb.Entry{{Term: 2, Index: 1}, {Term: 1, Index: 2}}, 1, 1, true},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append(tt.ents)
		r := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)

		r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgVote, Term: 3, LogTerm: tt.logterm, Index: tt.index})

		msgs := r.readMessages()
		if len(msgs) != 1 {
			t.Fatalf("#%d: len(msg) = %d, want %d", i, len(msgs), 1)
		}
		m := msgs[0]
		if m.Type != pb.MsgVoteResp {
			t.Errorf("#%d: msgType = %d, want %d", i, m.Type, pb.MsgVoteResp)
		}
		if m.Reject != tt.wreject {
			t.Errorf("#%d: reject = %t, want %t", i, m.Reject, tt.wreject)
		}
	}
}

// TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the leader’s
// current term are committed by counting replicas.
// Reference: section 5.4.2
// 验证leader不会提交使用过期任期term提交的数据
func TestLeaderOnlyCommitsLogFromCurrentTerm(t *testing.T) {
	ents := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		index   uint64
		wcommit uint64
	}{
		// do not commit log entries in previous terms
		// 索引1和2是以前term提交的数据，所以不会改变当前的commit
		{1, 0},
		{2, 0},
		// commit log in current term
		{3, 3},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		// 存储先append进去数据，模拟这些数据已经在新leader之前提交过了
		storage.Append(ents)
		r := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
		r.loadState(pb.HardState{Term: 2})
		// become leader at term 3
		r.becomeCandidate()
		r.becomeLeader()
		r.readMessages()
		// propose a entry to current term
		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

		r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgAppResp, Term: r.Term, Index: tt.index})
		if r.raftLog.committed != tt.wcommit {
			t.Errorf("#%d: commit = %d, want %d", i, r.raftLog.committed, tt.wcommit)
		}
	}
}

type messageSlice []pb.Message

func (s messageSlice) Len() int           { return len(s) }
func (s messageSlice) Less(i, j int) bool { return fmt.Sprint(s[i]) < fmt.Sprint(s[j]) }
func (s messageSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func commitNoopEntry(r *raft, s *MemoryStorage) {
	if r.state != StateLeader {
		panic("it should only be used when it is the leader")
	}
	r.bcastAppend()
	// simulate the response of MsgApp
	msgs := r.readMessages()
	for _, m := range msgs {
		if m.Type != pb.MsgApp || len(m.Entries) != 1 || m.Entries[0].Data != nil {
			panic("not a message to append noop entry")
		}
		r.Step(acceptAndReply(m))
	}
	// ignore further messages to refresh followers' commit index
	r.readMessages()
	s.Append(r.raftLog.unstableEntries())
	r.raftLog.appliedTo(r.raftLog.committed)
	r.raftLog.stableTo(r.raftLog.lastIndex(), r.raftLog.lastTerm())
}

func acceptAndReply(m pb.Message) pb.Message {
	if m.Type != pb.MsgApp {
		panic("type should be MsgApp")
	}
	return pb.Message{
		From:  m.To,
		To:    m.From,
		Term:  m.Term,
		Type:  pb.MsgAppResp,
		Index: m.Index + uint64(len(m.Entries)),
	}
}
