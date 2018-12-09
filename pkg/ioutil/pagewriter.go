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

package ioutil

import (
	"io"
)

var defaultBufferBytes = 128 * 1024

// PageWriter implements the io.Writer interface so that writes will
// either be in page chunks or from flushing.
// 带缓冲区的writer，每次写满一个page的情况下才进行Flush，page的大小可以指定
type PageWriter struct {
	w io.Writer
	// pageOffset tracks the page offset of the base of the buffer
	// 记录缓冲页数据偏移量
	pageOffset int
	// pageBytes is the number of bytes per page
	// 每一页都有多少byte数据
	pageBytes int
	// bufferedBytes counts the number of bytes pending for write in the buffer
	// 当前buf共有多少数据，因为buf空间是预分配好的
	bufferedBytes int
	// buf holds the write buffer
	// 存储buffer数据
	buf []byte
	// bufWatermarkBytes is the number of bytes the buffer can hold before it needs
	// to be flushed. It is less than len(buf) so there is space for slack writes
	// to bring the writer to page alignment.
	// buffer水位，超过这个水位的数据写入磁盘
	bufWatermarkBytes int
}

// NewPageWriter creates a new PageWriter. pageBytes is the number of bytes
// to write per page. pageOffset is the starting offset of io.Writer.
func NewPageWriter(w io.Writer, pageBytes, pageOffset int) *PageWriter {
	return &PageWriter{
		w:                 w,
		pageOffset:        pageOffset,
		pageBytes:         pageBytes,
		// 预分配buffer，为什么是defaultBufferBytes+pageBytes？
		buf:               make([]byte, defaultBufferBytes+pageBytes),
		bufWatermarkBytes: defaultBufferBytes,
	}
}

func (pw *PageWriter) Write(p []byte) (n int, err error) {
	if len(p)+pw.bufferedBytes <= pw.bufWatermarkBytes {
		// 还没有到写入磁盘的水位，拷贝到buf中，增加bufferedBytes然后返回
		// no overflow
		copy(pw.buf[pw.bufferedBytes:], p)
		pw.bufferedBytes += len(p)
		return len(p), nil
	}
	// 因为缓冲区会回绕，所以下面的计算要用 %
	// complete the slack page in the buffer if unaligned
	// slack存储的是页剩余空间
	slack := pw.pageBytes - ((pw.pageOffset + pw.bufferedBytes) % pw.pageBytes)
	if slack != pw.pageBytes {	// 不足一页
		partial := slack > len(p)
		if partial {
			// not enough data to complete the slack page
			// 数据不足以填充该页剩余空间
			slack = len(p)
		}
		// special case: writing to slack page in buffer
		copy(pw.buf[pw.bufferedBytes:], p[:slack])
		pw.bufferedBytes += slack
		n = slack
		p = p[slack:]
		if partial {
			// avoid forcing an unaligned flush
			return n, nil
		}
	}
	// buffer contents are now page-aligned; clear out
	if err = pw.Flush(); err != nil {
		return n, err
	}
	// directly write all complete pages without copying
	if len(p) > pw.pageBytes {
		pages := len(p) / pw.pageBytes
		c, werr := pw.w.Write(p[:pages*pw.pageBytes])
		n += c
		if werr != nil {
			return n, werr
		}
		p = p[pages*pw.pageBytes:]
	}
	// write remaining tail to buffer
	c, werr := pw.Write(p)
	n += c
	return n, werr
}

func (pw *PageWriter) Flush() error {
	if pw.bufferedBytes == 0 {
		return nil
	}
	// 写入磁盘
	_, err := pw.w.Write(pw.buf[:pw.bufferedBytes])
	// 更新页偏移量
	pw.pageOffset = (pw.pageOffset + pw.bufferedBytes) % pw.pageBytes
	pw.bufferedBytes = 0
	return err
}
