package partition

import (
	"cmp"
	"slices"
)

type RangeLookup[K cmp.Ordered, V any] struct {
	ends   []K // sorted end keys
	values []V // parallel array, values[i] owns range ending at ends[i]
}

// Insert end into sorted position in r.ends
// Insert value at same position in r.values
// Maintain sorted order
func (r *RangeLookup[K, V]) Add(end K, value V) {
	idx, _ := slices.BinarySearchFunc(r.ends, end, func(e K, target K) int {
		return cmp.Compare(e, target)
	})
	r.ends = slices.Insert(r.ends, idx, end)
	r.values = slices.Insert(r.values, idx, value)
}

// Binary search r.ends for first end > point
// Return corresponding value
// If point >= all ends, return zero, false
func (r *RangeLookup[K, V]) Find(point K) (V, bool) {
	idx, found := slices.BinarySearchFunc(r.ends, point, func(end K, p K) int {
		return cmp.Compare(end, p)
	})
	if found {
		idx++
	}
	if idx < len(r.values) {
		return r.values[idx], true
	}
	var zero V
	return zero, false
}

// Find first range where end > low
// Collect all ranges until start of range > high
// Return slice of matching values
func (r *RangeLookup[K, V]) FindRange(low, high K) []V {
	idx, found := slices.BinarySearchFunc(r.ends, low, func(end K, l K) int {
		return cmp.Compare(end, l)
	})
	if found {
		idx++
	}
	var results []V
	for idx < len(r.values) {
		// Include range if its start < high.
		// Start of range idx is ends[idx-1]; for idx==0 it is implicitly -inf.
		if idx > 0 && cmp.Compare(r.ends[idx-1], high) >= 0 {
			break
		}
		results = append(results, r.values[idx])
		idx++
	}
	return results
}

// Return a copy of the values slice.
func (r *RangeLookup[K, V]) Values() []V {
	out := make([]V, len(r.values))
	copy(out, r.values)
	return out
}

// Find and remove entry with matching end key
// Remove corresponding value
func (r *RangeLookup[K, V]) Remove(end K) {
	idx, found := slices.BinarySearchFunc(r.ends, end, func(e K, target K) int {
		return cmp.Compare(e, target)
	})
	if found {
		r.ends = append(r.ends[:idx], r.ends[idx+1:]...)
		r.values = append(r.values[:idx], r.values[idx+1:]...)
	}
}
