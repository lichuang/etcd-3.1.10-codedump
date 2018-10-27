// Copyright 2016 The etcd Authors
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

// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, It's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// RequestCtx
type ReadState struct {
	// 保存接收该读请求时的committed index
	Index      uint64
	// 保存读请求ID，全局唯一的定义一次读请求
	RequestCtx []byte
}

type readIndexStatus struct {
	// 保存原始的readIndex请求消息
	req   pb.Message
	// 保存收到该readIndex请求时的leader commit索引
	index uint64
	// 保存有什么节点进行了应答，从这里可以计算出来是否有超过半数应答了
	acks  map[uint64]struct{}
}

type readOnly struct {
	option           ReadOnlyOption
	// 使用entry的数据为key，保存当前pending的readIndex状态
	pendingReadIndex map[string]*readIndexStatus
	// 保存entry的数据为的队列，pending的readindex状态在这个队列中进行排队
	readIndexQueue   []string
}

func newReadOnly(option ReadOnlyOption) *readOnly {
	return &readOnly{
		option:           option,
		pendingReadIndex: make(map[string]*readIndexStatus),
	}
}

// addRequest adds a read only reuqest into readonly struct.
// `index` is the commit index of the raft state machine when it received
// the read only request.
// `m` is the original read only request message from the local or remote node.
func (ro *readOnly) addRequest(index uint64, m pb.Message) {
	ctx := string(m.Entries[0].Data)
	// 判断是否重复添加
	if _, ok := ro.pendingReadIndex[ctx]; ok {
		return
	}
	ro.pendingReadIndex[ctx] = &readIndexStatus{index: index, req: m, acks: make(map[uint64]struct{})}
	ro.readIndexQueue = append(ro.readIndexQueue, ctx)
}

// recvAck notifies the readonly struct that the raft state machine received
// an acknowledgment of the heartbeat that attached with the read only request
// context.
// 收到某个节点对一个HB消息的应答，这个函数中尝试查找该消息是否在readonly数据中
func (ro *readOnly) recvAck(m pb.Message) int {
	// 根据context内容到map中进行查找
	rs, ok := ro.pendingReadIndex[string(m.Context)]
	if !ok {
		// 找不到就返回，说明这个HB消息没有带上readindex相关的数据
		return 0
	}

	// 将该消息的ack map加上该应答节点
	rs.acks[m.From] = struct{}{}
	// add one to include an ack from local node
	// 返回当前ack的节点数量，+1是因为包含了leader
	return len(rs.acks) + 1
}

// advance advances the read only request queue kept by the readonly struct.
// It dequeues the requests until it finds the read only request that has
// the same context as the given `m`.
// 在确保某HB消息被集群中半数以上节点应答了，此时尝试在readindex队列中查找，看一下队列中的readindex数据有哪些可以丢弃了（也就是已经被应答过了）
// 最后返回被丢弃的数据队列
func (ro *readOnly) advance(m pb.Message) []*readIndexStatus {
	var (
		i     int
		found bool
	)

	ctx := string(m.Context)
	rss := []*readIndexStatus{}

	for _, okctx := range ro.readIndexQueue {
		i++
		rs, ok := ro.pendingReadIndex[okctx]
		if !ok {
			// 不可能出现在map中找不到该数据的情况
			panic("cannot find corresponding read state from pending map")
		}
		// 都加入应答队列中，后面用于根据这个队列的数据来删除pendingReadIndex中对应的数据
		rss = append(rss, rs)
		if okctx == ctx {
			// 找到了就终止循环
			found = true
			break
		}
	}

	if found {
		// 找到了，就丢弃在这之前的队列readonly数据了
		ro.readIndexQueue = ro.readIndexQueue[i:]
		for _, rs := range rss {
			// 遍历队列从map中删除
			delete(ro.pendingReadIndex, string(rs.req.Entries[0].Data))
		}
		return rss
	}

	return nil
}

// lastPendingRequestCtx returns the context of the last pending read only
// request in readonly struct.
// 从readonly队列中返回最后一个数据
func (ro *readOnly) lastPendingRequestCtx() string {
	if len(ro.readIndexQueue) == 0 {
		return ""
	}
	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}
