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

package wal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/coreos/etcd/pkg/fileutil"
)

// filePipeline pipelines allocating disk space
// 这个结构体负责创建新的WAL临时文件
// 它将启动一个goroutine，一直等待被唤醒，然后创建新的WAL文件
type filePipeline struct {
	// 文件路径
	// dir to put files
	dir string
	// 创建文件的预分配空间大小
	// size of files to make, in bytes
	size int64
	// count number of files generated
	// 当前filePipeline实例创建的文件数量
	count int

	// 这个channel将创建好的文件返回WAL
	filec chan *fileutil.LockedFile
	errc  chan error
	donec chan struct{}
}

func newFilePipeline(dir string, fileSize int64) *filePipeline {
	fp := &filePipeline{
		dir:   dir,
		size:  fileSize,
		filec: make(chan *fileutil.LockedFile),
		errc:  make(chan error, 1),
		donec: make(chan struct{}),
	}
	go fp.run()
	return fp
}

// Open returns a fresh file for writing. Rename the file before calling
// Open again or there will be file collisions.
func (fp *filePipeline) Open() (f *fileutil.LockedFile, err error) {
	select {
	case f = <-fp.filec:
	case err = <-fp.errc:
	}
	return
}

func (fp *filePipeline) Close() error {
	close(fp.donec)
	return <-fp.errc
}

func (fp *filePipeline) alloc() (f *fileutil.LockedFile, err error) {
	// count % 2 so this file isn't the same as the one last published
	// 为了防止文件名冲突，所以%2，这样名称就在0、1两个之间轮流
	fpath := filepath.Join(fp.dir, fmt.Sprintf("%d.tmp", fp.count%2))
	// 创建文件
	if f, err = fileutil.LockFile(fpath, os.O_CREATE|os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return nil, err
	}
	// 预分配空间
	if err = fileutil.Preallocate(f.File, fp.size, true); err != nil {
		plog.Errorf("failed to allocate space when creating new wal file (%v)", err)
		f.Close()
		return nil, err
	}
	// 递增创建的文件数量
	fp.count++
	return f, nil
}

// 主循环监听channel，调用alloc函数创建新的WAL文件
func (fp *filePipeline) run() {
	defer close(fp.errc)
	for {
		// 创建文件
		f, err := fp.alloc()
		if err != nil {
			// 如果失败，通过channel返回错误
			fp.errc <- err
			return
		}
		select {
		// 将创建好的文件写入channel
		case fp.filec <- f:
			// 等待关闭
		case <-fp.donec:
			os.Remove(f.Name())
			f.Close()
			return
		}
	}
}
