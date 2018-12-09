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
	"errors"
	"fmt"
	"strings"

	"github.com/coreos/etcd/pkg/fileutil"
)

var (
	badWalName = errors.New("bad wal name")
)

func Exist(dirpath string) bool {
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		return false
	}
	return len(names) != 0
}

// searchIndex returns the last array index of names whose raft index section is
// equal to or smaller than the given index.
// The given names MUST be sorted.
// 传入一个wal文件名数组和index，返回第一个索引在index之后的wal文件在这个数组中的索引
func searchIndex(names []string, index uint64) (int, bool) {
	// 从后往前查找
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		// 根据文件名得到文件中存储数据的最小索引
		_, curIndex, err := parseWalName(name)
		if err != nil {
			plog.Panicf("parse correct name should never fail: %v", err)
		}
		// 如果index大于该wal文件索引就返回索引值
		if index >= curIndex {
			return i, true
		}
	}
	return -1, false
}

// names should have been sorted based on sequence number.
// isValidSeq checks whether seq increases continuously.
func isValidSeq(names []string) bool {
	var lastSeq uint64
	for _, name := range names {
		curSeq, _, err := parseWalName(name)
		if err != nil {
			plog.Panicf("parse correct name should never fail: %v", err)
		}
		if lastSeq != 0 && lastSeq != curSeq-1 {
			return false
		}
		lastSeq = curSeq
	}
	return true
}

// 传入WAL文件目录，扫描其中的所有文件，返回所有文件名格式为"16位整数-16位整数.wal"的文件名数组
func readWalNames(dirpath string) ([]string, error) {
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		return nil, err
	}
	wnames := checkWalNames(names)
	if len(wnames) == 0 {
		return nil, ErrFileNotFound
	}
	return wnames, nil
}

// 传入一个文件数组，返回其中文件名格式为"16位整数-16位整数.wal"的文件名数组
func checkWalNames(names []string) []string {
	wnames := make([]string, 0)
	for _, name := range names {
		if _, _, err := parseWalName(name); err != nil {
			// don't complain about left over tmp files
			if !strings.HasSuffix(name, ".tmp") {
				plog.Warningf("ignored file %v in wal", name)
			}
			continue
		}
		wnames = append(wnames, name)
	}
	return wnames
}

// 分析一个文件名，如果符合"16位整数-16位整数.wal"格式，则返回seq和index
func parseWalName(str string) (seq, index uint64, err error) {
	if !strings.HasSuffix(str, ".wal") {
		return 0, 0, badWalName
	}
	_, err = fmt.Sscanf(str, "%016x-%016x.wal", &seq, &index)
	return seq, index, err
}

// 格式：16位整数-16位整数.wal
// 其中前16位是wal文件编号，后面的整数则是这个wal文件第一条entry数据的索引值，至少都比这个index值大
// 这样就能知道某个wal文件保存的都是哪个索引之后的entry数据了
func walName(seq, index uint64) string {
	return fmt.Sprintf("%016x-%016x.wal", seq, index)
}
