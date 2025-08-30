package main

import (
	"math/rand"
	"strings"
	"time"
)

type Entry struct {
	Member string
	Score  float64
}

const (
	maxLevel = 32   // Reasonable upper bound
	pFactor  = 0.25 // Probability for level promotion
)

type node struct {
	entry   Entry
	forward []*node
}

// SkipList is an ordered set by (score, member)
type SkipList struct {
	header *node
	level  int // current max level (1..maxLevel)
	len    int
	// fast path for updates/removals
	byMember map[string]*node
	rnd      *rand.Rand
}

func New() *SkipList {
	h := &node{
		forward: make([]*node, maxLevel),
	}
	return &SkipList{
		header:   h,
		level:    1,
		len:      0,
		byMember: make(map[string]*node),
		rnd:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Len returns number of elements
func (sl *SkipList) Len() int { return sl.len }

// compare returns -1 if a<b, 0 if equal key, +1 if a>b, by (score, member)
func compare(a, b Entry) int {
	switch {
	case a.Score < b.Score:
		return -1
	case a.Score > b.Score:
		return 1
	default:
		// tie-break by member (case-sensitive, lexicographic)
		return strings.Compare(a.Member, b.Member)
	}
}

// randomLevel returns level in [1, maxLevel]
func (sl *SkipList) randomLevel() int {
	l := 1
	for l < maxLevel && sl.rnd.Float64() < pFactor {
		l++
	}
	return l
}

// Get returns the entry with the given member, and true if found.
func (sl *SkipList) Get(member string) (Entry, bool) {
	n, ok := sl.byMember[member]
	if !ok {
		return Entry{}, false
	}
	return n.entry, true
}

// Add inserts a new entry or updates the score if the member exists.
// If the member exists and the score is unchanged, it's a no-op.
// If the score changed, the element is reinserted in the correct position.
func (sl *SkipList) Add(e Entry) {
	// Update path: if member exists
	if old, ok := sl.byMember[e.Member]; ok {
		if old.entry.Score == e.Score {
			// no change in position, just ensure entry updated
			old.entry = e
			return
		}
		// remove and reinsert (simpler and still O(log N))
		sl.Remove(e.Member)
	}

	update := make([]*node, maxLevel)
	x := sl.header
	// find update pointers for each level
	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && compare(x.forward[i].entry, e) < 0 {
			x = x.forward[i]
		}
		update[i] = x
	}

	lvl := sl.randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			update[i] = sl.header
		}
		sl.level = lvl
	}

	n := &node{
		entry:   e,
		forward: make([]*node, lvl),
	}
	for i := 0; i < lvl; i++ {
		n.forward[i] = update[i].forward[i]
		update[i].forward[i] = n
	}

	sl.byMember[e.Member] = n
	sl.len++
}

// Remove deletes by member. Returns true if something was removed.
func (sl *SkipList) Remove(member string) bool {
	n, found := sl.byMember[member]
	if !found {
		return false
	}

	update := make([]*node, maxLevel)
	x := sl.header
	e := n.entry

	// locate update pointers for the node
	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && compare(x.forward[i].entry, e) < 0 {
			x = x.forward[i]
		}
		update[i] = x
	}

	// verify target and unlink
	target := x.forward[0]
	if target == nil || compare(target.entry, e) != 0 {
		// not found in order (shouldn't happen given byMember), bail defensively
		return false
	}
	for i := 0; i < len(target.forward); i++ {
		if update[i].forward[i] == target {
			update[i].forward[i] = target.forward[i]
		}
	}
	// adjust current level if top levels became empty
	for sl.level > 1 && sl.header.forward[sl.level-1] == nil {
		sl.level--
	}
	delete(sl.byMember, member)
	sl.len--
	return true
}

// RangeByScore returns up to `limit` entries with min<=score<=max (or open bounds),
// ordered ascending. If limit<=0, returns all in range.
func (sl *SkipList) RangeByScore(min, max float64, inclusiveMin, inclusiveMax bool, limit int) []Entry {
	out := make([]Entry, 0)
	if sl.len == 0 {
		return out
	}
	// Find first >= min
	x := sl.header
	startKey := Entry{Member: "", Score: min}
	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil {
			cmp := compare(x.forward[i].entry, startKey)
			if cmp < 0 || (cmp == 0 && !inclusiveMin) {
				x = x.forward[i]
			} else {
				break
			}
		}
	}
	x = x.forward[0]

	// Collect while <= max
	for x != nil {
		e := x.entry
		if e.Score < min || (!inclusiveMin && e.Score == min) {
			x = x.forward[0]
			continue
		}
		if e.Score > max || (!inclusiveMax && e.Score == max) {
			break
		}
		out = append(out, e)
		if limit > 0 && len(out) >= limit {
			break
		}
		x = x.forward[0]
	}
	return out
}

// RangeByRank returns entries from rank start to stop (inclusive), 0-based.
// If start<0 it is treated as 0. If stop>=Len() it is clamped.
func (sl *SkipList) RangeByRank(start, stop int) []Entry {
	if sl.len == 0 {
		return nil
	}
	if start < 0 {
		start = 0
	}
	if stop < start {
		return nil
	}
	if stop >= sl.len {
		stop = sl.len - 1
	}
	out := make([]Entry, 0, stop-start+1)

	// Linear walk from head (skip list without spans). For large lists, consider spans.
	x := sl.header.forward[0]
	idx := 0
	for x != nil && idx <= stop {
		if idx >= start {
			out = append(out, x.entry)
		}
		x = x.forward[0]
		idx++
	}
	return out
}

// Iterate provides a forward iterator from the smallest element.
// Example:
//
//	it := sl.Iterate()
//	for it.Next() {
//	    e := it.Value()
//	}
func (sl *SkipList) Iterate() *Iterator {
	return &Iterator{curr: sl.header}
}

type Iterator struct {
	curr *node
}

func (it *Iterator) Next() bool {
	if it.curr == nil {
		return false
	}
	it.curr = it.curr.forward[0]
	return it.curr != nil
}

func (it *Iterator) Value() Entry {
	return it.curr.entry
}
