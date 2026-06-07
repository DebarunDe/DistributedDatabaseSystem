package ap

import "sync"

// TimestampStore tracks the last-write timestamp (unix nanos) per key for
// last-writer-wins conflict resolution in AP mode.
type TimestampStore struct {
	mu     sync.RWMutex
	stamps map[uint64]int64 // key → unix nanos of last write
}

func NewTimestampStore() *TimestampStore {
	return &TimestampStore{stamps: make(map[uint64]int64)}
}

// Get returns the stored timestamp for key, or 0 if none.
func (ts *TimestampStore) Get(key uint64) int64 {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.stamps[key]
}

// Set unconditionally records t as the timestamp for key.
func (ts *TimestampStore) Set(key uint64, t int64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.stamps[key] = t
}

// CompareAndSet updates the timestamp to remoteTs if remoteTs > current,
// returning true when the remote write wins (caller should apply the write).
func (ts *TimestampStore) CompareAndSet(key uint64, remoteTs int64) bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if remoteTs > ts.stamps[key] {
		ts.stamps[key] = remoteTs
		return true
	}
	return false
}

// Delete removes the timestamp entry for key (used after a delete is applied).
func (ts *TimestampStore) Delete(key uint64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	delete(ts.stamps, key)
}

// Snapshot returns a copy of all timestamps (for testing / diagnostics).
func (ts *TimestampStore) Snapshot() map[uint64]int64 {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	out := make(map[uint64]int64, len(ts.stamps))
	for k, v := range ts.stamps {
		out[k] = v
	}
	return out
}
