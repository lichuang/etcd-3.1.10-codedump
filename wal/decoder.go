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
	"bufio"
	"encoding/binary"
	"hash"
	"io"
	"sync"

	"github.com/coreos/etcd/pkg/crc"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/wal/walpb"
)

const minSectorSize = 512

type decoder struct {
	mu  sync.Mutex
	// 在WAL.openAtIndex方法中打开的日志文件
	brs []*bufio.Reader

	// lastValidOff file offset following the last valid decoded record
	// 紧跟着最后一条合法记录的文件偏移量
	lastValidOff int64
	crc          hash.Hash32
}

func newDecoder(r ...io.Reader) *decoder {
	readers := make([]*bufio.Reader, len(r))
	for i := range r {
		readers[i] = bufio.NewReader(r[i])
	}
	return &decoder{
		brs: readers,
		crc: crc.New(0, crcTable),
	}
}

func (d *decoder) decode(rec *walpb.Record) error {
	rec.Reset()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.decodeRecord(rec)
}

func (d *decoder) decodeRecord(rec *walpb.Record) error {
	// 没有日志文件可以读取了，返回EOF
	if len(d.brs) == 0 {
		return io.EOF
	}

	// 尝试读一个Int64的值
	l, err := readInt64(d.brs[0])
	if err == io.EOF || (err == nil && l == 0) {	// 如果EOF（读到了文件尾）或者读不到数据
		// hit end of file or preallocated space
		// 这时可能读到了这个文件的最后
		// 尝试读下一个wal文件
		d.brs = d.brs[1:]
		if len(d.brs) == 0 {
			// 但是已经没有wal文件了，也是返回EOF
			return io.EOF
		}
		// 既然换了一个新文件，就重置lastValidOff
		d.lastValidOff = 0
		// 调用decodeRecord重新开始读取
		return d.decodeRecord(rec)
	}
	if err != nil {
		return err
	}

	// 返回记录长度和填充长度
	recBytes, padBytes := decodeFrameSize(l)

	// 分配足够大小的buffer
	data := make([]byte, recBytes+padBytes)
	// 从文件中读取数据到buffer
	if _, err = io.ReadFull(d.brs[0], data); err != nil {
		// ReadFull returns io.EOF only if no bytes were read
		// the decoder should treat this as an ErrUnexpectedEOF instead.
		if err == io.EOF {	// 读不到数据
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	// 反序列化，注意只用了buffer中记录长度的数据
	if err := rec.Unmarshal(data[:recBytes]); err != nil {
		if d.isTornEntry(data) {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	// skip crc checking if the record type is crcType
	// 校验记录的crc值
	if rec.Type != crcType {
		d.crc.Write(rec.Data)
		if err := rec.Validate(d.crc.Sum32()); err != nil {
			if d.isTornEntry(data) {
				return io.ErrUnexpectedEOF
			}
			return err
		}
	}
	// 移动lastValidOff跳过这条记录
	// record decoded as valid; point last valid offset to end of record
	d.lastValidOff += recBytes + padBytes + 8
	return nil
}

// 返回记录长度和填充数据长度
func decodeFrameSize(lenField int64) (recBytes int64, padBytes int64) {
	// the record size is stored in the lower 56 bits of the 64-bit length
	// 记录长度存放在低56位
	recBytes = int64(uint64(lenField) & ^(uint64(0xff) << 56))
	// non-zero padding is indicated by set MSb / a negative length
	// 填充长度在高7位，只有在lenField<0的情况下才存在
	if lenField < 0 {
		// padding is stored in lower 3 bits of length MSB
		padBytes = int64((uint64(lenField) >> 56) & 0x7)
	}
	return
}

// isTornEntry determines whether the last entry of the WAL was partially written
// and corrupted because of a torn write.
func (d *decoder) isTornEntry(data []byte) bool {
	if len(d.brs) != 1 {
		return false
	}

	fileOff := d.lastValidOff + 8
	curOff := 0
	chunks := [][]byte{}
	// split data on sector boundaries
	for curOff < len(data) {
		chunkLen := int(minSectorSize - (fileOff % minSectorSize))
		if chunkLen > len(data)-curOff {
			chunkLen = len(data) - curOff
		}
		chunks = append(chunks, data[curOff:curOff+chunkLen])
		fileOff += int64(chunkLen)
		curOff += chunkLen
	}

	// if any data for a sector chunk is all 0, it's a torn write
	for _, sect := range chunks {
		isZero := true
		for _, v := range sect {
			if v != 0 {
				isZero = false
				break
			}
		}
		if isZero {
			return true
		}
	}
	return false
}

func (d *decoder) updateCRC(prevCrc uint32) {
	d.crc = crc.New(prevCrc, crcTable)
}

func (d *decoder) lastCRC() uint32 {
	return d.crc.Sum32()
}

func (d *decoder) lastOffset() int64 { return d.lastValidOff }

func mustUnmarshalEntry(d []byte) raftpb.Entry {
	var e raftpb.Entry
	pbutil.MustUnmarshal(&e, d)
	return e
}

func mustUnmarshalState(d []byte) raftpb.HardState {
	var s raftpb.HardState
	pbutil.MustUnmarshal(&s, d)
	return s
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}
