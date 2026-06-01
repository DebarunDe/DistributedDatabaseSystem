package partition

import (
	"sync"
	"testing"
)

func makeNodes() map[uint64]string {
	return map[uint64]string{
		1: "node1:8080",
		2: "node2:8080",
		3: "node3:8080",
	}
}

// ---- NewCoordinator -----------------------------------------------------

func TestNewCoordinator_SingleInitialRange(t *testing.T) {
	c := NewCoordinator(makeNodes())

	ranges := c.GetAllRanges()
	if len(ranges) != 1 {
		t.Fatalf("expected 1 initial range, got %d", len(ranges))
	}
}

func TestNewCoordinator_InitialRangeFields(t *testing.T) {
	c := NewCoordinator(makeNodes())

	rd := c.GetAllRanges()[0]
	if rd.RangeID != 1 {
		t.Errorf("RangeID = %d, want 1", rd.RangeID)
	}
	if rd.StartKey != 0 {
		t.Errorf("StartKey = %d, want 0", rd.StartKey)
	}
	if rd.EndKey != ^uint64(0) {
		t.Errorf("EndKey = %d, want MaxUint64", rd.EndKey)
	}
	if rd.LeaderID != 0 {
		t.Errorf("LeaderID = %d, want 0 (unknown)", rd.LeaderID)
	}
	if len(rd.Replicas) != 3 {
		t.Errorf("Replicas count = %d, want 3", len(rd.Replicas))
	}
}

func TestNewCoordinator_NextRangeID(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if c.nextRangeID != 2 {
		t.Errorf("nextRangeID = %d, want 2", c.nextRangeID)
	}
}

func TestNewCoordinator_CoversFullKeySpace(t *testing.T) {
	c := NewCoordinator(makeNodes())

	keys := []uint64{0, 1, 1 << 16, 1 << 32, ^uint64(0) - 1}
	for _, k := range keys {
		if rd := c.LookupKey(k); rd == nil {
			t.Errorf("LookupKey(%d) returned nil on fresh coordinator", k)
		}
	}
}

// ---- LookupKey ----------------------------------------------------------

func TestLookupKey_MaxUint64_NotFound(t *testing.T) {
	c := NewCoordinator(makeNodes())

	// MaxUint64 is the exclusive end key of the initial range — it is never owned.
	if rd := c.LookupKey(^uint64(0)); rd != nil {
		t.Fatalf("LookupKey(MaxUint64) should return nil, got %+v", rd)
	}
}

func TestLookupKey_MaxUint64MinusOne_Found(t *testing.T) {
	c := NewCoordinator(makeNodes())

	rd := c.LookupKey(^uint64(0) - 1)
	if rd == nil {
		t.Fatal("LookupKey(MaxUint64-1) returned nil")
	}
	if rd.RangeID != 1 {
		t.Errorf("RangeID = %d, want 1", rd.RangeID)
	}
}

func TestLookupKey_AfterSplit_RoutesCorrectly(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatal(err)
	}
	// After split: [0,500) → rangeID 2, [500,MaxUint64) → rangeID 1

	cases := []struct {
		key       uint64
		wantID    uint64
		wantStart uint64
	}{
		{0, 2, 0},
		{499, 2, 0},
		{500, 1, 500},
		{1000, 1, 500},
	}
	for _, tc := range cases {
		rd := c.LookupKey(tc.key)
		if rd == nil {
			t.Fatalf("LookupKey(%d) returned nil", tc.key)
		}
		if rd.RangeID != tc.wantID {
			t.Errorf("LookupKey(%d).RangeID = %d, want %d", tc.key, rd.RangeID, tc.wantID)
		}
		if rd.StartKey != tc.wantStart {
			t.Errorf("LookupKey(%d).StartKey = %d, want %d", tc.key, rd.StartKey, tc.wantStart)
		}
	}
}

func TestLookupKey_SplitBoundaryKey(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatal(err)
	}

	// The split key 500 is the start of the upper half — it belongs to the upper range.
	rd := c.LookupKey(500)
	if rd == nil || rd.StartKey != 500 {
		t.Fatalf("LookupKey(500) should return upper range [500,...), got %+v", rd)
	}
}

// ---- LookupRange --------------------------------------------------------

func TestLookupRange_SingleRange(t *testing.T) {
	c := NewCoordinator(makeNodes())

	got := c.LookupRange(0, 1000)
	if len(got) != 1 || got[0].RangeID != 1 {
		t.Fatalf("LookupRange(0, 1000) = %v, want [rangeID=1]", got)
	}
}

func TestLookupRange_SpansMultipleRanges(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil { // [0,500) id=2, [500,MaxUint64) id=1
		t.Fatal(err)
	}
	if err := c.RequestSplit(1, 750); err != nil { // [500,750) id=3, [750,MaxUint64) id=1
		t.Fatal(err)
	}

	// Query [400, 600) touches [0,500) and [500,750).
	got := c.LookupRange(400, 600)
	if len(got) != 2 {
		t.Fatalf("LookupRange(400, 600) returned %d ranges, want 2", len(got))
	}
}

func TestLookupRange_ExactRangeBoundary_ExcludesNext(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}

	// Query [0, 500): the split key 500 is the exclusive high boundary.
	// Only the lower range [0,500) should match.
	got := c.LookupRange(0, 500)
	if len(got) != 1 {
		t.Fatalf("LookupRange(0, 500) returned %d ranges, want 1", len(got))
	}
	if got[0].StartKey != 0 {
		t.Errorf("expected lower range (StartKey=0), got StartKey=%d", got[0].StartKey)
	}
}

func TestLookupRange_AllRangesAfterMultipleSplits(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := c.RequestSplit(2, 250); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := c.RequestSplit(1, 750); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	// Ranges: [0,250), [250,500), [500,750), [750,MaxUint64)

	got := c.LookupRange(0, ^uint64(0))
	if len(got) != 4 {
		t.Fatalf("LookupRange over full key space returned %d ranges, want 4", len(got))
	}
}

// ---- RequestSplit -------------------------------------------------------

func TestRequestSplit_Valid_ProducesTwoRanges(t *testing.T) {
	c := NewCoordinator(makeNodes())

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := len(c.GetAllRanges()); got != 2 {
		t.Fatalf("expected 2 ranges after split, got %d", got)
	}
}

func TestRequestSplit_LowerHalfHasCorrectFields(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}

	rd := c.LookupKey(0)
	if rd == nil {
		t.Fatal("lower range not found")
	}
	if rd.StartKey != 0 || rd.EndKey != 500 {
		t.Errorf("lower range = [%d, %d), want [0, 500)", rd.StartKey, rd.EndKey)
	}
	if rd.RangeID != 2 {
		t.Errorf("lower rangeID = %d, want 2", rd.RangeID)
	}
}

func TestRequestSplit_UpperHalfHasCorrectFields(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}

	rd := c.LookupKey(500)
	if rd == nil {
		t.Fatal("upper range not found")
	}
	if rd.StartKey != 500 || rd.EndKey != ^uint64(0) {
		t.Errorf("upper range = [%d, %d), want [500, MaxUint64)", rd.StartKey, rd.EndKey)
	}
	if rd.RangeID != 1 {
		t.Errorf("upper rangeID = %d, want 1", rd.RangeID)
	}
}

func TestRequestSplit_SplitKeyEqualsStartKey_Error(t *testing.T) {
	c := NewCoordinator(makeNodes())

	// 0 is the StartKey of the initial range.
	if err := c.RequestSplit(1, 0); err == nil {
		t.Fatal("expected error when splitKey == StartKey, got nil")
	}
}

func TestRequestSplit_SplitKeyEqualsStartKey_UpperRange_Error(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	} // upper range [500, MaxUint64) keeps rangeID=1

	// 500 is now the StartKey of the upper range (rangeID=1).
	if err := c.RequestSplit(1, 500); err == nil {
		t.Fatal("expected error when splitKey == StartKey of upper range, got nil")
	}
}

func TestRequestSplit_SplitKeyEqualsEndKey_Error(t *testing.T) {
	c := NewCoordinator(makeNodes())

	// The end key of the initial range is MaxUint64 (exclusive); Find returns not found.
	if err := c.RequestSplit(1, ^uint64(0)); err == nil {
		t.Fatal("expected error when splitKey == EndKey, got nil")
	}
}

func TestRequestSplit_WrongRangeID_Error(t *testing.T) {
	c := NewCoordinator(makeNodes())

	if err := c.RequestSplit(99, 500); err == nil {
		t.Fatal("expected error for non-existent rangeID, got nil")
	}
}

func TestRequestSplit_SplitKeyInDifferentRange_Error(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	} // [0,500) id=2, [500,MaxUint64) id=1

	// splitKey=100 belongs to rangeID=2, but we claim rangeID=1.
	if err := c.RequestSplit(1, 100); err == nil {
		t.Fatal("expected error when splitKey is in a different range")
	}
}

func TestRequestSplit_ReplicasAreIndependent(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}

	lower := c.LookupKey(0)   // live pointer to lower range
	upper := c.LookupKey(500) // live pointer to upper range

	// Mutate lower's Replicas; upper must be unaffected.
	lower.Replicas[99] = "extra-node:9090"

	if _, exists := upper.Replicas[99]; exists {
		t.Fatal("modifying lower range Replicas leaked into upper range Replicas — maps were not cloned")
	}
}

func TestRequestSplit_NextRangeIDIncrements(t *testing.T) {
	c := NewCoordinator(makeNodes())

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if c.nextRangeID != 3 {
		t.Errorf("nextRangeID after 1st split = %d, want 3", c.nextRangeID)
	}

	if err := c.RequestSplit(1, 750); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if c.nextRangeID != 4 {
		t.Errorf("nextRangeID after 2nd split = %d, want 4", c.nextRangeID)
	}
}

func TestRequestSplit_MultipleSplits_AllKeysBoundaries(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	} // [0,500) id=2, [500,MaxUint64) id=1
	if err := c.RequestSplit(2, 250); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	} // [0,250) id=3, [250,500) id=2
	if err := c.RequestSplit(1, 750); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	} // [500,750) id=4, [750,MaxUint64) id=1
	// Final: [0,250) id=3, [250,500) id=2, [500,750) id=4, [750,MaxUint64) id=1

	if got := len(c.GetAllRanges()); got != 4 {
		t.Fatalf("expected 4 ranges, got %d", got)
	}

	cases := []struct {
		key       uint64
		wantStart uint64
		wantEnd   uint64
	}{
		{0, 0, 250},
		{249, 0, 250},
		{250, 250, 500},
		{499, 250, 500},
		{500, 500, 750},
		{749, 500, 750},
		{750, 750, ^uint64(0)},
		{1000, 750, ^uint64(0)},
	}
	for _, tc := range cases {
		rd := c.LookupKey(tc.key)
		if rd == nil {
			t.Fatalf("LookupKey(%d) returned nil", tc.key)
		}
		if rd.StartKey != tc.wantStart || rd.EndKey != tc.wantEnd {
			t.Errorf("LookupKey(%d) = [%d,%d), want [%d,%d)",
				tc.key, rd.StartKey, rd.EndKey, tc.wantStart, tc.wantEnd)
		}
	}
}

func TestRequestSplit_SizeIsHalved(t *testing.T) {
	c := NewCoordinator(makeNodes())
	// Directly set size on the live descriptor before splitting.
	c.rl.values[0].Size = 2000

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}

	lower := c.LookupKey(0)
	upper := c.LookupKey(500)
	if lower.Size != 1000 {
		t.Errorf("lower.Size = %d, want 1000", lower.Size)
	}
	if upper.Size != 1000 {
		t.Errorf("upper.Size = %d, want 1000", upper.Size)
	}
}

// ---- GetAllRanges -------------------------------------------------------

func TestGetAllRanges_ReturnsCopies(t *testing.T) {
	c := NewCoordinator(makeNodes())

	ranges := c.GetAllRanges()
	ranges[0].LeaderID = 42

	// The live descriptor should be unaffected.
	rd := c.LookupKey(0)
	if rd.LeaderID == 42 {
		t.Fatal("GetAllRanges returned a live pointer instead of a copy")
	}
}

func TestGetAllRanges_CountMatchesSplits(t *testing.T) {
	c := NewCoordinator(makeNodes())

	if got := len(c.GetAllRanges()); got != 1 {
		t.Errorf("initial count = %d, want 1", got)
	}
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if got := len(c.GetAllRanges()); got != 2 {
		t.Errorf("after 1 split = %d, want 2", got)
	}
	if err := c.RequestSplit(2, 250); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if got := len(c.GetAllRanges()); got != 3 {
		t.Errorf("after 2 splits = %d, want 3", got)
	}
}

func TestGetAllRanges_CopiesAreSelfConsistent(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}

	ranges := c.GetAllRanges()
	// Each copy should have the same fields as the live descriptor.
	for _, rd := range ranges {
		live := c.LookupKey(rd.StartKey)
		if live == nil {
			t.Fatalf("LookupKey(%d) returned nil", rd.StartKey)
		}
		if rd.RangeID != live.RangeID || rd.StartKey != live.StartKey || rd.EndKey != live.EndKey {
			t.Errorf("copy and live diverge: copy=%+v live=%+v", rd, live)
		}
	}
}

// ---- UpdateLeader -------------------------------------------------------

func TestUpdateLeader_SetsLeaderOnCorrectRange(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	} // lower rangeID=2, upper rangeID=1

	c.UpdateLeader(2, 3) // set leader of rangeID=2 to nodeID=3

	lower := c.LookupKey(0)
	if lower.LeaderID != 3 {
		t.Errorf("lower.LeaderID = %d, want 3", lower.LeaderID)
	}
}

func TestUpdateLeader_DoesNotAffectOtherRanges(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}

	c.UpdateLeader(2, 3)

	upper := c.LookupKey(500)
	if upper.LeaderID != 0 {
		t.Errorf("upper.LeaderID = %d, want 0 (unchanged)", upper.LeaderID)
	}
}

func TestUpdateLeader_UnknownRangeID_IsNoOp(t *testing.T) {
	c := NewCoordinator(makeNodes())

	c.UpdateLeader(99, 3) // rangeID 99 does not exist

	if rd := c.LookupKey(0); rd.LeaderID != 0 {
		t.Errorf("LeaderID changed unexpectedly to %d", rd.LeaderID)
	}
}

func TestUpdateLeader_VisibleViaLookupKey(t *testing.T) {
	c := NewCoordinator(makeNodes())
	c.UpdateLeader(1, 2)

	rd := c.LookupKey(0)
	if rd == nil || rd.LeaderID != 2 {
		t.Fatalf("LookupKey after UpdateLeader: LeaderID = %v, want 2", rd)
	}
}

func TestUpdateLeader_VisibleViaGetAllRanges(t *testing.T) {
	c := NewCoordinator(makeNodes())
	c.UpdateLeader(1, 2)

	ranges := c.GetAllRanges()
	if len(ranges) == 0 || ranges[0].LeaderID != 2 {
		t.Errorf("GetAllRanges after UpdateLeader: LeaderID = %d, want 2", ranges[0].LeaderID)
	}
}

func TestUpdateLeader_CanBeUpdatedMultipleTimes(t *testing.T) {
	c := NewCoordinator(makeNodes())

	c.UpdateLeader(1, 1)
	c.UpdateLeader(1, 2)
	c.UpdateLeader(1, 3)

	rd := c.LookupKey(0)
	if rd.LeaderID != 3 {
		t.Errorf("LeaderID = %d, want 3 (last update wins)", rd.LeaderID)
	}
}

// ---- Concurrency --------------------------------------------------------

func TestCoordinator_ConcurrentLookupKey(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}

	var wg sync.WaitGroup
	for i := uint64(0); i < 100; i++ {
		wg.Add(1)
		go func(key uint64) {
			defer wg.Done()
			if c.LookupKey(key) == nil {
				t.Errorf("LookupKey(%d) returned nil under concurrent reads", key)
			}
		}(i * 4) // covers keys 0, 4, 8, … 396 (all in lower range)
	}
	wg.Wait()
}

func TestCoordinator_ConcurrentGetAllRanges(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ranges := c.GetAllRanges()
			if len(ranges) == 0 {
				t.Error("GetAllRanges returned empty slice under concurrent reads")
			}
		}()
	}
	wg.Wait()
}

func TestCoordinator_ConcurrentUpdateLeaderAndLookup(t *testing.T) {
	c := NewCoordinator(makeNodes())

	var wg sync.WaitGroup
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(leaderID uint64) {
			defer wg.Done()
			c.UpdateLeader(1, leaderID)
		}(uint64(i))
	}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.LookupKey(0) // must not panic or deadlock
		}()
	}
	wg.Wait()
}

func TestCoordinator_ConcurrentLookupAndGetAllRanges(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			c.LookupKey(100)
		}()
		go func() {
			defer wg.Done()
			c.GetAllRanges()
		}()
	}
	wg.Wait()
}
