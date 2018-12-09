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

package mvcc

import (
	"math"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/coreos/etcd/pkg/adt"
)

var (
	// watchBatchMaxRevs is the maximum distinct revisions that
	// may be sent to an unsynced watcher at a time. Declared as
	// var instead of const for testing purposes.
	// 最多一次batch放入多少revision
	watchBatchMaxRevs = 1000
)

// 存储批量事件
type eventBatch struct {
	// evs is a batch of revision-ordered events
	// 事件数组
	evs []mvccpb.Event
	// revs is the minimum unique revisions observed for this batch
	// 该eventBatch中一共有多少不同的revision事件
	revs int
	// moreRev is first revision with more events following this batch
	// 下一个eventBatch发送时的第一个revision
	moreRev int64
}

func (eb *eventBatch) add(ev mvccpb.Event) {
	// 超过了最多revision
	if eb.revs > watchBatchMaxRevs {
		// maxed out batch size
		return
	}

	if len(eb.evs) == 0 {	// 没有元素，说明第一次添加event进来
		// base case
		eb.revs = 1
		eb.evs = append(eb.evs, ev)
		return
	}

	// revision accounting
	// 拿到已经存储的event的最大revision
	ebRev := eb.evs[len(eb.evs)-1].Kv.ModRevision
	// 拿到事件的revision
	evRev := ev.Kv.ModRevision
	if evRev > ebRev {	// 如果事件的revision更大
		// revision计数递增
		eb.revs++
		// 超过阈值了
		if eb.revs > watchBatchMaxRevs {
			eb.moreRev = evRev
			return
		}
	}

	// 添加进来
	eb.evs = append(eb.evs, ev)
}

// watcher关注的eventBatch
type watcherBatch map[*watcher]*eventBatch

func (wb watcherBatch) add(w *watcher, ev mvccpb.Event) {
	eb := wb[w]
	if eb == nil {	// 找到watcher关注的eventBatch，就创建新的出来
		eb = &eventBatch{}
		wb[w] = eb
	}
	// 添加事件到该eventBatch中
	eb.add(ev)
}

// newWatcherBatch maps watchers to their matched events. It enables quick
// events look up by watcher.
// 传入event数组，从watcherGroup的watcher中找到对这些事件中的event关注而且满足revision的watcher返回
func newWatcherBatch(wg *watcherGroup, evs []mvccpb.Event) watcherBatch {
	// 没有watcher，直接返回
	if len(wg.watchers) == 0 {
		return nil
	}

	wb := make(watcherBatch)
	for _, ev := range evs {	// 遍历事件
		for w := range wg.watcherSetByKey(string(ev.Kv.Key)) {	// 找到关注这些事件中的key的watcher
			if ev.Kv.ModRevision >= w.minRev {	// 如果事件的revision大于等于watcher要求的最小revision
				// don't double notify
				// 就添加到watcherBatch中
				wb.add(w, ev)
			}
		}
	}
	return wb
}

// 存储watcher的set，但是go里面没有set所以只能存value是一个空结构体
type watcherSet map[*watcher]struct{}

func (w watcherSet) add(wa *watcher) {
	if _, ok := w[wa]; ok {
		panic("add watcher twice!")
	}
	w[wa] = struct{}{}
}

func (w watcherSet) union(ws watcherSet) {
	for wa := range ws {
		w.add(wa)
	}
}

func (w watcherSet) delete(wa *watcher) {
	if _, ok := w[wa]; !ok {
		panic("removing missing watcher!")
	}
	delete(w, wa)
}

// 存放关注相同key的watcher集合
type watcherSetByKey map[string]watcherSet

func (w watcherSetByKey) add(wa *watcher) {
	set := w[string(wa.key)]
	if set == nil {
		set = make(watcherSet)
		w[string(wa.key)] = set
	}
	set.add(wa)
}

func (w watcherSetByKey) delete(wa *watcher) bool {
	k := string(wa.key)
	if v, ok := w[k]; ok {
		if _, ok := v[wa]; ok {
			delete(v, wa)
			if len(v) == 0 {
				// remove the set; nothing left
				delete(w, k)
			}
			return true
		}
	}
	return false
}

// watcherGroup is a collection of watchers organized by their ranges
type watcherGroup struct {
	// keyWatchers has the watchers that watch on a single key
	// 关注单个key的watcher集合
	keyWatchers watcherSetByKey
	// ranges has the watchers that watch a range; it is sorted by interval
	// 关注范围key的watcher集合
	ranges adt.IntervalTree
	// watchers is the set of all watchers
	// 所有watcher集合
	watchers watcherSet
}

func newWatcherGroup() watcherGroup {
	return watcherGroup{
		keyWatchers: make(watcherSetByKey),
		watchers:    make(watcherSet),
	}
}

// add puts a watcher in the group.
func (wg *watcherGroup) add(wa *watcher) {
	// 先添加进全体watcher集合中
	wg.watchers.add(wa)
	// end为nil，说明只关注单个key
	if wa.end == nil {
		// 添加到单个key watcher集合中，返回
		wg.keyWatchers.add(wa)
		return
	}

	// 到了这里说明是关注返回key的watcher

	// interval already registered?
	// 先创建一个范围
	ivl := adt.NewStringAffineInterval(string(wa.key), string(wa.end))
	// 根据此范围在ranges中查找
	if iv := wg.ranges.Find(ivl); iv != nil {
		// 找到了，说明已经存在此范围，添加watcher进来就好
		iv.Val.(watcherSet).add(wa)
		return
	}

	// not registered, put in interval tree
	// 没有找到，新创建一个
	ws := make(watcherSet)
	ws.add(wa)
	wg.ranges.Insert(ivl, ws)
}

// contains is whether the given key has a watcher in the group.
func (wg *watcherGroup) contains(key string) bool {
	_, ok := wg.keyWatchers[key]
	// 先在单key中查找，然后在ranges中查找
	return ok || wg.ranges.Contains(adt.NewStringAffinePoint(key))
}

// size gives the number of unique watchers in the group.
// 返回所有watcher的数量
func (wg *watcherGroup) size() int { return len(wg.watchers) }

// delete removes a watcher from the group.
func (wg *watcherGroup) delete(wa *watcher) bool {
	if _, ok := wg.watchers[wa]; !ok {
		// 全体watcher中查找不到，那么不存在这个watcher，直接返回了
		return false
	}
	wg.watchers.delete(wa)
	if wa.end == nil {
		// 没有end，说明是关注单个key的watcher
		wg.keyWatchers.delete(wa)
		return true
	}

	// 到这里就是关注范围key的watcher
	ivl := adt.NewStringAffineInterval(string(wa.key), string(wa.end))
	iv := wg.ranges.Find(ivl)
	if iv == nil {
		return false
	}

	ws := iv.Val.(watcherSet)
	delete(ws, wa)
	if len(ws) == 0 {
		// remove interval missing watchers
		if ok := wg.ranges.Delete(ivl); !ok {
			panic("could not remove watcher from interval tree")
		}
	}

	return true
}

// choose selects watchers from the watcher group to update
func (wg *watcherGroup) choose(maxWatchers int, curRev, compactRev int64) (*watcherGroup, int64) {
	if len(wg.watchers) < maxWatchers {	// 当前watcher数量不足要求，直接从该wg中选择返回
		return wg, wg.chooseAll(curRev, compactRev)
	}
	// 否则创建一个临时的wg返回
	ret := newWatcherGroup()
	for w := range wg.watchers {
		if maxWatchers <= 0 {
			// 达到数量了，退出循环
			break
		}
		maxWatchers--
		ret.add(w)
	}
	return &ret, ret.chooseAll(curRev, compactRev)
}

func (wg *watcherGroup) chooseAll(curRev, compactRev int64) int64 {
	minRev := int64(math.MaxInt64)
	for w := range wg.watchers {
		if w.minRev > curRev {
			panic("watcher current revision should not exceed current revision")
		}
		if w.minRev < compactRev {
			// 小于compact revision，则先发送watch response
			select {
			case w.ch <- WatchResponse{WatchID: w.id, CompactRevision: compactRev}:
				w.compacted = true
				wg.delete(w)
			default:
				// retry next time
			}
			continue
		}
		if minRev > w.minRev {
			minRev = w.minRev
		}
	}
	return minRev
}

// watcherSetByKey gets the set of watchers that receive events on the given key.
// 返回关注某个key的watcher集合，可能包含在单个key中，也可能在ranges里，要全部返回
func (wg *watcherGroup) watcherSetByKey(key string) watcherSet {
	wkeys := wg.keyWatchers[key]
	wranges := wg.ranges.Stab(adt.NewStringAffinePoint(key))

	// zero-copy cases
	switch {
	case len(wranges) == 0:
		// ranges没找到
		// no need to merge ranges or copy; reuse single-key set
		return wkeys
	case len(wranges) == 0 && len(wkeys) == 0:
		// 两个都没有
		return nil
	case len(wranges) == 1 && len(wkeys) == 0:
		// 只有ranges有
		return wranges[0].Val.(watcherSet)
	}

	// 两个都有，进行合并

	// copy case
	ret := make(watcherSet)
	ret.union(wg.keyWatchers[key])
	for _, item := range wranges {
		ret.union(item.Val.(watcherSet))
	}
	return ret
}
