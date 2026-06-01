package partition

import (
	"fmt"
	"log"
	"maps"
	"sync"
)

type Coordinator struct {
	mu          sync.RWMutex
	rl          RangeLookup[uint64, *RangeDescriptor]
	nextRangeID uint64
	allNodes    map[uint64]string
}

func NewCoordinator(allNodes map[uint64]string) *Coordinator {
	c := &Coordinator{
		nextRangeID: 2,
		allNodes:    allNodes,
	}
	c.rl.Add(^uint64(0), &RangeDescriptor{
		RangeID:  1,
		StartKey: 0,
		EndKey:   ^uint64(0),
		Replicas: allNodes,
		Size:     0,
	})
	return c
}

func (c *Coordinator) LookupKey(key uint64) *RangeDescriptor {
	c.mu.RLock()
	defer c.mu.RUnlock()

	rd, ok := c.rl.Find(key)
	if !ok {
		log.Printf("LookupKey: no range found for key %d", key)
		return nil
	}
	return rd
}

func (c *Coordinator) LookupRange(startKey, endKey uint64) []*RangeDescriptor {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.rl.FindRange(startKey, endKey)
}

func (c *Coordinator) RequestSplit(rangeID uint64, splitKey uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find range to split
	rd, ok := c.rl.Find(splitKey)
	if !ok {
		return fmt.Errorf("RequestSplit: no range found for split key %d", splitKey)
	}

	if rd.RangeID != rangeID {
		return fmt.Errorf("RequestSplit: rangeID mismatch for split key %d", splitKey)
	}

	if splitKey == rd.StartKey {
		return fmt.Errorf("RequestSplit: splitKey %d must be strictly inside range [%d, %d)",
			splitKey, rd.StartKey, rd.EndKey)
	}

	// Create new range descriptor for lower half
	lowRd := &RangeDescriptor{
		RangeID:  c.nextRangeID,
		StartKey: rd.StartKey,
		EndKey:   splitKey,
		Replicas: maps.Clone(rd.Replicas),
		Size:     rd.Size / 2, // approximate
	}

	// Update existing range descriptor for upper half
	rd.StartKey = splitKey
	rd.Size = rd.Size / 2 // approximate

	//add lower half to range lookup
	c.rl.Add(lowRd.EndKey, lowRd)

	c.nextRangeID++

	return nil
}

func (c *Coordinator) GetAllRanges() []*RangeDescriptor {
	c.mu.RLock()
	defer c.mu.RUnlock()

	src := c.rl.Values()
	ranges := make([]*RangeDescriptor, len(src))
	for i, rd := range src {
		copy := *rd
		copy.Replicas = maps.Clone(rd.Replicas)
		ranges[i] = &copy
	}
	return ranges
}

func (c *Coordinator) UpdateLeader(rangeID uint64, leaderID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, rd := range c.rl.Values() {
		if rd.RangeID == rangeID {
			rd.LeaderID = leaderID
			return
		}
	}
}
