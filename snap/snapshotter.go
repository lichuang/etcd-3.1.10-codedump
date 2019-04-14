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

// Package snap stores raft nodes' states with snapshots.
package snap

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	pioutil "github.com/coreos/etcd/pkg/ioutil"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap/snappb"

	"github.com/coreos/pkg/capnslog"
)

const (
	snapSuffix = ".snap"
)

var (
	plog = capnslog.NewPackageLogger("github.com/coreos/etcd", "snap")

	ErrNoSnapshot    = errors.New("snap: no available snapshot")
	ErrEmptySnapshot = errors.New("snap: empty snapshot")
	ErrCRCMismatch   = errors.New("snap: crc mismatch")
	crcTable         = crc32.MakeTable(crc32.Castagnoli)

	// A map of valid files that can be present in the snap folder.
	validFiles = map[string]bool{
		"db": true,
	}
)

type Snapshotter struct {
	dir string
}

func New(dir string) *Snapshotter {
	return &Snapshotter{
		dir: dir,
	}
}

func (s *Snapshotter) SaveSnap(snapshot raftpb.Snapshot) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}
	return s.save(&snapshot)
}

// 保存快照数据
func (s *Snapshotter) save(snapshot *raftpb.Snapshot) error {
	start := time.Now()

	// 快照文件名：最后一条日志记录任期号-最后一条记录索引号.snap
	fname := fmt.Sprintf("%016x-%016x%s", snapshot.Metadata.Term, snapshot.Metadata.Index, snapSuffix)
	b := pbutil.MustMarshal(snapshot)
	crc := crc32.Update(0, crcTable, b)
	// 序列化之后的数据封装成Snapshot
	snap := snappb.Snapshot{Crc: crc, Data: b}
	// 序列化
	d, err := snap.Marshal()
	if err != nil {
		return err
	} else {
		marshallingDurations.Observe(float64(time.Since(start)) / float64(time.Second))
	}

	// 写入文件并且刷新磁盘
	err = pioutil.WriteAndSyncFile(filepath.Join(s.dir, fname), d, 0666)
	if err == nil {
		// 记录一下花了多少时间
		saveDurations.Observe(float64(time.Since(start)) / float64(time.Second))
	} else {
		err1 := os.Remove(filepath.Join(s.dir, fname))
		if err1 != nil {
			plog.Errorf("failed to remove broken snapshot file %s", filepath.Join(s.dir, fname))
		}
	}
	return err
}

// 加载目录下的所有快照文件，返回最近的快照数据
func (s *Snapshotter) Load() (*raftpb.Snapshot, error) {
	// 返回快照目录中的快照文件名
	names, err := s.snapNames()
	if err != nil {
		return nil, err
	}
	var snap *raftpb.Snapshot
	for _, name := range names {
		// 依次加载
		if snap, err = loadSnap(s.dir, name); err == nil {
			break
		}
	}
	if err != nil {
		return nil, ErrNoSnapshot
	}
	// 返回最近的快照数据
	return snap, nil
}

func loadSnap(dir, name string) (*raftpb.Snapshot, error) {
	fpath := filepath.Join(dir, name)
	snap, err := Read(fpath)
	if err != nil {
		renameBroken(fpath)
	}
	return snap, err
}

// Read reads the snapshot named by snapname and returns the snapshot.
// 从快照文件中读取快照数据
func Read(snapname string) (*raftpb.Snapshot, error) {
	// 读文件内容
	b, err := ioutil.ReadFile(snapname)
	if err != nil {
		plog.Errorf("cannot read file %v: %v", snapname, err)
		return nil, err
	}

	if len(b) == 0 {
		plog.Errorf("unexpected empty snapshot")
		return nil, ErrEmptySnapshot
	}

	// 反序列化文件内容
	var serializedSnap snappb.Snapshot
	if err = serializedSnap.Unmarshal(b); err != nil {
		plog.Errorf("corrupted snapshot file %v: %v", snapname, err)
		return nil, err
	}

	if len(serializedSnap.Data) == 0 || serializedSnap.Crc == 0 {
		plog.Errorf("unexpected empty snapshot")
		return nil, ErrEmptySnapshot
	}

	crc := crc32.Update(0, crcTable, serializedSnap.Data)
	// 校验crc编码
	if crc != serializedSnap.Crc {
		plog.Errorf("corrupted snapshot file %v: crc mismatch", snapname)
		return nil, ErrCRCMismatch
	}

	var snap raftpb.Snapshot
	// 再从数据中反序列化出Snapshot
	if err = snap.Unmarshal(serializedSnap.Data); err != nil {
		plog.Errorf("corrupted snapshot file %v: %v", snapname, err)
		return nil, err
	}
	return &snap, nil
}

// snapNames returns the filename of the snapshots in logical time order (from newest to oldest).
// If there is no available snapshots, an ErrNoSnapshot will be returned.
// 查找快照目录中的所有快照文件，从最新到最旧文件进行排序，返回文件名数组
func (s *Snapshotter) snapNames() ([]string, error) {
	dir, err := os.Open(s.dir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	snaps := checkSuffix(names)
	if len(snaps) == 0 {
		return nil, ErrNoSnapshot
	}
	sort.Sort(sort.Reverse(sort.StringSlice(snaps)))
	return snaps, nil
}

func checkSuffix(names []string) []string {
	snaps := []string{}
	for i := range names {
		if strings.HasSuffix(names[i], snapSuffix) {
			snaps = append(snaps, names[i])
		} else {
			// If we find a file which is not a snapshot then check if it's
			// a vaild file. If not throw out a warning.
			if _, ok := validFiles[names[i]]; !ok {
				plog.Warningf("skipped unexpected non snapshot file %v", names[i])
			}
		}
	}
	return snaps
}

func renameBroken(path string) {
	brokenPath := path + ".broken"
	if err := os.Rename(path, brokenPath); err != nil {
		plog.Warningf("cannot rename broken snapshot file %v to %v: %v", path, brokenPath, err)
	}
}
