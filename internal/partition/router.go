package partition

import (
	"fmt"
	"maps"
	"sync"
)

type Router struct {
	mu          sync.RWMutex
	cache       RangeLookup[uint64, *RangeDescriptor]
	coordinator *Coordinator
}

func (r *Router) Refresh() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.cache = RangeLookup[uint64, *RangeDescriptor]{}

	// populate cache with all ranges from coordinator
	ranges := r.coordinator.GetAllRanges()
	for _, rd := range ranges {
		r.cache.Add(rd.EndKey, rd)
	}

	return nil
}

func NewRouter(coordinator *Coordinator) (*Router, error) {
	router := &Router{
		cache:       RangeLookup[uint64, *RangeDescriptor]{},
		coordinator: coordinator,
	}

	if err := router.Refresh(); err != nil {
		return nil, err
	}
	return router, nil
}

// RouteKey returns the cached range descriptor for key. It never refreshes
// internally — callers must call Refresh() when a node signals a stale route.
// The returned descriptor is a fresh copy; callers may mutate it freely.
func (r *Router) RouteKey(key uint64) (*RangeDescriptor, error) {
	r.mu.RLock()
	rd, ok := r.cache.Find(key)
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("RouteKey: no range found for key %d", key)
	}
	cp := *rd
	cp.Replicas = maps.Clone(rd.Replicas)
	return &cp, nil
}

// RouteRange returns the cached range descriptors covering [startKey, endKey).
// Each descriptor is a fresh copy; callers may mutate them freely.
func (r *Router) RouteRange(startKey, endKey uint64) ([]*RangeDescriptor, error) {
	r.mu.RLock()
	rds := r.cache.FindRange(startKey, endKey)
	r.mu.RUnlock()

	if len(rds) == 0 {
		return nil, fmt.Errorf("RouteRange: no ranges found for range [%d, %d)", startKey, endKey)
	}
	copies := make([]*RangeDescriptor, len(rds))
	for i, rd := range rds {
		cp := *rd
		cp.Replicas = maps.Clone(rd.Replicas)
		copies[i] = &cp
	}
	return copies, nil
}
