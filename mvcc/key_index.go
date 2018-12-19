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
	"bytes"
	"errors"
	"fmt"

	"github.com/google/btree"
)

var (
	ErrRevisionNotFound = errors.New("mvcc: revision not found")
)

// keyIndex stores the revisions of a key in the backend.
// Each keyIndex has at least one key generation.
// Each generation might have several key versions.
// Tombstone on a key appends an tombstone version at the end
// of the current generation and creates a new empty generation.
// Each version of a key has an index pointing to the backend.
//
// For example: put(1.0);put(2.0);tombstone(3.0);put(4.0);tombstone(5.0) on key "foo"
// generate a keyIndex:
// key:     "foo"
// rev: 5
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {1.0, 2.0, 3.0(t)}
//
// Compact a keyIndex removes the versions with smaller or equal to
// rev except the largest one. If the generation becomes empty
// during compaction, it will be removed. if all the generations get
// removed, the keyIndex should be removed.

// For example:
// compact(2) on the previous example
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {2.0, 3.0(t)}
//
// compact(4)
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//
// compact(5):
// generations:
//    {empty} -> key SHOULD be removed.
//
// compact(6):
// generations:
//    {empty} -> key SHOULD be removed.
// key索引数据结构
type keyIndex struct {
	// key
	key         []byte
	// 最后一次修改的revision
	modified    revision // the main rev of the last modification
	// generation数组
	generations []generation
}

// put puts a revision to the keyIndex.
func (ki *keyIndex) put(main int64, sub int64) {
	rev := revision{main: main, sub: sub}

	// 不比当前最后一次修改的revision大则panic
	if !rev.GreaterThan(ki.modified) {
		plog.Panicf("store.keyindex: put with unexpected smaller revision [%v / %v]", rev, ki.modified)
	}
	// 没有generation，构造一个空的
	if len(ki.generations) == 0 {
		ki.generations = append(ki.generations, generation{})
	}
	// 拿到最后一个generation
	g := &ki.generations[len(ki.generations)-1]
	if len(g.revs) == 0 { // create a new key
		// 连revision都没有，说明是一个新的key
		// 注意这里的key并不是KV数据中的key，而是对应一个generation
		keysGauge.Inc()
		g.created = rev
	}
	// 保存revision
	g.revs = append(g.revs, rev)
	// 递增版本号
	g.ver++
	// 保存最后一次修改的revision
	ki.modified = rev
}

func (ki *keyIndex) restore(created, modified revision, ver int64) {
	if len(ki.generations) != 0 {
		plog.Panicf("store.keyindex: cannot restore non-empty keyIndex")
	}

	ki.modified = modified
	g := generation{created: created, ver: ver, revs: []revision{modified}}
	ki.generations = append(ki.generations, g)
	keysGauge.Inc()
}

// tombstone puts a revision, pointing to a tombstone, to the keyIndex.
// It also creates a new empty generation in the keyIndex.
// It returns ErrRevisionNotFound when tombstone on an empty generation.
func (ki *keyIndex) tombstone(main int64, sub int64) error {
	// 不能为空
	if ki.isEmpty() {
		plog.Panicf("store.keyindex: unexpected tombstone on empty keyIndex %s", string(ki.key))
	}
	// 最后一个generation为空，返回ErrRevisionNotFound
	if ki.generations[len(ki.generations)-1].isEmpty() {
		return ErrRevisionNotFound
	}
	ki.put(main, sub)
	// 增加一个空的generation，于是上一个generation就是tombstone了，这么做类似于链表最后一个元素的next指针为NULL的操作
	ki.generations = append(ki.generations, generation{})
	// tombstone了，所以key数量减一
	keysGauge.Dec()
	return nil
}

// get gets the modified, created revision and version of the key that satisfies the given atRev.
// Rev must be higher than or equal to the given atRev.
// 找到第一个main > rev的revision数据
func (ki *keyIndex) get(atRev int64) (modified, created revision, ver int64, err error) {
	if ki.isEmpty() {
		plog.Panicf("store.keyindex: unexpected get on empty keyIndex %s", string(ki.key))
	}
	// 查找指定revision的数据
	g := ki.findGeneration(atRev)
	if g.isEmpty() {
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}

	// 找到最后第一个满足main <= rev的位置（因为walk函数是逆序查找）
	// 而由于数组是0开始，因此根据该位置拿到的就是第一个main > rev的revision位置
	n := g.walk(func(rev revision) bool { return rev.main > atRev })
	if n != -1 {
		// 找到了
		return g.revs[n], g.created, g.ver - int64(len(g.revs)-n-1), nil
	}

	// 找不到
	return revision{}, revision{}, 0, ErrRevisionNotFound
}

// since returns revisions since the given rev. Only the revision with the
// largest sub revision will be returned if multiple revisions have the same
// main revision.
// 找到所有revision大于传入值的revision数组返回
func (ki *keyIndex) since(rev int64) []revision {
	if ki.isEmpty() {
		plog.Panicf("store.keyindex: unexpected get on empty keyIndex %s", string(ki.key))
	}
	since := revision{rev, 0}
	var gi int
	// find the generations to start checking
	// 从后往前查找第一个小于rev的generation数组索引保存到gi
	for gi = len(ki.generations) - 1; gi > 0; gi-- {
		g := ki.generations[gi]
		if g.isEmpty() {
			continue
		}
		// 找到了从后往前第一个小于rev的revision，终止循环
		if since.GreaterThan(g.created) {
			break
		}
	}

	// 从前面的gi索引开始，查找大于rev的revision返回
	var revs []revision
	var last int64
	for ; gi < len(ki.generations); gi++ {	// 遍历gi之后的每个generation
		for _, r := range ki.generations[gi].revs {	// 遍历每个generation中的revision
			if since.GreaterThan(r) {
				// 略过小于rev的revision
				continue
			}
			// 到这里都是大于rev的revision

			if r.main == last {
				// 如果main版本跟last一致，修改当前最后一个revision为该revision
				// 原因在于，相同main版本的，sub版本更小的revision不被外部看到
				// replace the revision with a new one that has higher sub value,
				// because the original one should not be seen by external
				revs[len(revs)-1] = r
				continue
			}
			// 保存revision以及main版本号
			revs = append(revs, r)
			last = r.main
		}
	}
	return revs
}

// compact compacts a keyIndex by removing the versions with smaller or equal
// revision than the given atRev except the largest one (If the largest one is
// a tombstone, it will not be kept).
// If a generation becomes empty during compaction, it will be removed.
// 压缩操作将删除小于该rev版本的revision数据，如果在此过程之后一个generation为空，那么这个generation
// 也将被删除
func (ki *keyIndex) compact(atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		plog.Panicf("store.keyindex: unexpected compact on empty keyIndex %s", string(ki.key))
	}

	// walk until reaching the first revision that has an revision smaller or equal to
	// the atRev.
	// add it to the available map
	f := func(rev revision) bool {
		if rev.main <= atRev {
			// 找到main版本更小的revision之后置空
			available[rev] = struct{}{}
			return false
		}
		return true
	}

	i, g := 0, &ki.generations[0]
	// find first generation includes atRev or created after atRev
	// 从前往后找到第一个最后一个revision数据main版本大于rev的generation
	for i < len(ki.generations)-1 {
		if tomb := g.revs[len(g.revs)-1].main; tomb > atRev {
			break
		}
		i++
		g = &ki.generations[i]
	}

	// 如果找到的generation不为空，删除这个generation的revision中小于rev的revision数据
	if !g.isEmpty() {
		// 查找generation的revision中有没有小于rev的revision
		n := g.walk(f)
		// remove the previous contents.
		if n != -1 {
			// 找到了，那么在这之前的revision全部删除
			g.revs = g.revs[n:]
		}
		// remove any tombstone
		// 如果上面的操作完毕之后generation的revision数组为1，说明已经被删空了
		// 同时如果不是ki的最后一个generation
		if len(g.revs) == 1 && i != len(ki.generations)-1 {
			// 记录下这个generation位置索引到i中，同时删除available，因为已经没有revision了
			delete(available, g.revs[0])
			i++
		}
	}
	// 将之前的generation删除
	// remove the previous generations.
	ki.generations = ki.generations[i:]
	return
}

func (ki *keyIndex) isEmpty() bool {
	// 只有一个generation同时这个generation还是空的
	return len(ki.generations) == 1 && ki.generations[0].isEmpty()
}

// findGeneration finds out the generation of the keyIndex that the
// given rev belongs to. If the given rev is at the gap of two generations,
// which means that the key does not exist at the given rev, it returns nil.
// 根据revision的main版本来查找generation返回
// 如果指定的rev在两个generation中间，那么表示不存在该指定revision的数据，返回nil
func (ki *keyIndex) findGeneration(rev int64) *generation {
	// 拿到generation数量
	lastg := len(ki.generations) - 1
	// 从后往前查找
	cg := lastg

	for cg >= 0 {
		// revision数组为空，往前查下一个generation
		if len(ki.generations[cg].revs) == 0 {
			cg--
			continue
		}
		// 拿到当前generation元素
		g := ki.generations[cg]
		if cg != lastg {	// 如果不是最后一个generation
			// 如果该generation的最后一个revision数据的main版本小于传入的rev，表示查找失败返回。
			if tomb := g.revs[len(g.revs)-1].main; tomb <= rev {
				return nil
			}
		}
		// 如果第一个revision的main版本小于等于rev，则返回
		if g.revs[0].main <= rev {
			return &ki.generations[cg]
		}
		// 继续往前查找
		cg--
	}
	return nil
}

func (a *keyIndex) Less(b btree.Item) bool {
	return bytes.Compare(a.key, b.(*keyIndex).key) == -1
}

func (a *keyIndex) equal(b *keyIndex) bool {
	if !bytes.Equal(a.key, b.key) {
		return false
	}
	if a.modified != b.modified {
		return false
	}
	if len(a.generations) != len(b.generations) {
		return false
	}
	for i := range a.generations {
		ag, bg := a.generations[i], b.generations[i]
		if !ag.equal(bg) {
			return false
		}
	}
	return true
}

func (ki *keyIndex) String() string {
	var s string
	for _, g := range ki.generations {
		s += g.String()
	}
	return s
}

// generation contains multiple revisions of a key.
type generation struct {
	// 这个generation存放的修改数量，其实就是revs数组长度
	ver     int64
	// 创建这个generation时的revision
	created revision // when the generation is created (put in first revision).
	// 这个generation的revision数组
	revs    []revision
}

func (g *generation) isEmpty() bool { return g == nil || len(g.revs) == 0 }

// walk walks through the revisions in the generation in descending order.
// It passes the revision to the given function.
// walk returns until: 1. it finishes walking all pairs 2. the function returns false.
// walk returns the position at where it stopped. If it stopped after
// finishing walking, -1 will be returned.
// 逆序遍历revision数组，针对每个元素调用f函数访问，一直到：
// 1. 函数返回false，此时返回该位置
// 2.	数组遍历完毕，此时返回-1
func (g *generation) walk(f func(rev revision) bool) int {
	l := len(g.revs)
	for i := range g.revs {
		ok := f(g.revs[l-i-1])
		if !ok {
			return l - i - 1
		}
	}
	return -1
}

func (g *generation) String() string {
	return fmt.Sprintf("g: created[%d] ver[%d], revs %#v\n", g.created, g.ver, g.revs)
}

func (a generation) equal(b generation) bool {
	if a.ver != b.ver {
		return false
	}
	if len(a.revs) != len(b.revs) {
		return false
	}

	for i := range a.revs {
		ar, br := a.revs[i], b.revs[i]
		if ar != br {
			return false
		}
	}
	return true
}
