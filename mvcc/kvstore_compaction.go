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
	"time"
)

func (s *store) scheduleCompaction(compactMainRev int64, keep map[revision]struct{}) bool {
	totalStart := time.Now()
	defer dbCompactionTotalDurations.Observe(float64(time.Since(totalStart) / time.Millisecond))

	// 结束范围为compactMainRev+1
	end := make([]byte, 8)
	binary.BigEndian.PutUint64(end, uint64(compactMainRev+1))

	// 每次多少批量
	batchsize := int64(10000)
	// 起始key，最开始为空
	last := make([]byte, 8+1+8)
	for {
		var rev revision

		start := time.Now()
		// 拿到事务
		tx := s.b.BatchTx()
		// 拿到事务锁
		tx.Lock()

		// 查询范围内的key数组
		keys, _ := tx.UnsafeRange(keyBucketName, last, end, batchsize)
		for _, key := range keys {
			rev = bytesToRev(key)
			if _, ok := keep[rev]; !ok {
				// 删除这些key的数据
				tx.UnsafeDelete(keyBucketName, key)
			}
		}

		if len(keys) < int(batchsize) {	// 小于批量，说明是最后一次查询
			rbytes := make([]byte, 8+1+8)
			// 将compactMainRev存放为finishedCompactKeyName
			revToBytes(revision{main: compactMainRev}, rbytes)
			tx.UnsafePut(metaBucketName, finishedCompactKeyName, rbytes)
			tx.Unlock()
			plog.Printf("finished scheduled compaction at %d (took %v)", compactMainRev, time.Since(totalStart))
			return true
		}

		// update last
		// 更新起始key的值，为上一次删除的{main,sub+1}
		revToBytes(revision{main: rev.main, sub: rev.sub + 1}, last)
		tx.Unlock()
		dbCompactionPauseDurations.Observe(float64(time.Since(start) / time.Millisecond))

		// 每删除一批数据就sleep一段时间
		select {
		case <-time.After(100 * time.Millisecond):
		case <-s.stopc:
			return false
		}
	}
}
