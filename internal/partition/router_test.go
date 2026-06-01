package partition

import (
	"sync"
	"testing"
)

func mustNewRouter(t *testing.T, c *Coordinator) *Router {
	t.Helper()
	r, err := NewRouter(c)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return r
}

// ---- NewRouter ----------------------------------------------------------

func TestNewRouter_CacheIsPopulated(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if _, err := r.RouteKey(0); err != nil {
		t.Fatalf("RouteKey(0) on fresh router: %v", err)
	}
}

func TestNewRouter_CoversFullKeySpace(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	keys := []uint64{0, 1, 1 << 16, 1 << 32, ^uint64(0) - 1}
	for _, k := range keys {
		if _, err := r.RouteKey(k); err != nil {
			t.Errorf("RouteKey(%d) on fresh router: %v", k, err)
		}
	}
}

func TestNewRouter_PreexistingSplits_AllRangesAccessible(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := c.RequestSplit(2, 250); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	// Ranges at construction time: [0,250), [250,500), [500,MaxUint64)
	r := mustNewRouter(t, c)

	cases := []struct {
		key       uint64
		wantStart uint64
	}{
		{0, 0},
		{249, 0},
		{250, 250},
		{499, 250},
		{500, 500},
	}
	for _, tc := range cases {
		rd, err := r.RouteKey(tc.key)
		if err != nil {
			t.Fatalf("RouteKey(%d): %v", tc.key, err)
		}
		if rd.StartKey != tc.wantStart {
			t.Errorf("RouteKey(%d).StartKey = %d, want %d", tc.key, rd.StartKey, tc.wantStart)
		}
	}
}

// ---- Refresh ------------------------------------------------------------

func TestRefresh_PicksUpSplit(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	} // [0,500) id=2, [500,MaxUint64) id=1

	// Cache is stale — key 300 still maps to the pre-split range.
	rd, _ := r.RouteKey(300)
	if rd.RangeID != 1 {
		t.Fatalf("before Refresh: expected stale RangeID=1, got %d", rd.RangeID)
	}

	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	rd, err := r.RouteKey(300)
	if err != nil {
		t.Fatalf("RouteKey(300) after Refresh: %v", err)
	}
	if rd.RangeID != 2 {
		t.Errorf("after Refresh: RangeID = %d, want 2 (lower half)", rd.RangeID)
	}
}

func TestRefresh_PicksUpLeaderUpdate(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	c.UpdateLeader(1, 3)

	rd, _ := r.RouteKey(0)
	if rd.LeaderID != 0 {
		t.Fatalf("before Refresh: expected stale LeaderID=0, got %d", rd.LeaderID)
	}

	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	rd, _ = r.RouteKey(0)
	if rd.LeaderID != 3 {
		t.Errorf("after Refresh: LeaderID = %d, want 3", rd.LeaderID)
	}
}

func TestRefresh_MultipleSplits_AllRangesVisible(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	} // [0,500) id=2, [500,MaxUint64) id=1
	if err := c.RequestSplit(2, 250); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	} // [0,250) id=3, [250,500) id=2
	if err := c.RequestSplit(1, 750); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	} // [500,750) id=4, [750,MaxUint64) id=1
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
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
		{999, 750, ^uint64(0)},
	}
	for _, tc := range cases {
		rd, err := r.RouteKey(tc.key)
		if err != nil {
			t.Fatalf("RouteKey(%d): %v", tc.key, err)
		}
		if rd.StartKey != tc.wantStart || rd.EndKey != tc.wantEnd {
			t.Errorf("RouteKey(%d) = [%d,%d), want [%d,%d)",
				tc.key, rd.StartKey, rd.EndKey, tc.wantStart, tc.wantEnd)
		}
	}
}

func TestRefresh_IsIdempotent(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	for i := 0; i < 5; i++ {
		if err := r.Refresh(); err != nil {
			t.Fatalf("Refresh %d: %v", i, err)
		}
	}

	rd, err := r.RouteKey(0)
	if err != nil || rd.RangeID != 1 {
		t.Errorf("after repeated Refreshes: RouteKey(0) = %v, err=%v", rd, err)
	}
}

func TestRefresh_ReplacesEntireCache(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	} // adds a second range
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	rds, err := r.RouteRange(0, ^uint64(0))
	if err != nil {
		t.Fatalf("RouteRange full span: %v", err)
	}
	if len(rds) != 2 {
		t.Errorf("after Refresh with 2 ranges: len = %d, want 2", len(rds))
	}
}

// ---- RouteKey -----------------------------------------------------------

func TestRouteKey_KeyZero(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	rd, err := r.RouteKey(0)
	if err != nil {
		t.Fatalf("RouteKey(0): %v", err)
	}
	if rd.StartKey != 0 {
		t.Errorf("StartKey = %d, want 0", rd.StartKey)
	}
}

func TestRouteKey_MaxUint64MinusOne(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	rd, err := r.RouteKey(^uint64(0) - 1)
	if err != nil {
		t.Fatalf("RouteKey(MaxUint64-1): %v", err)
	}
	if rd.RangeID != 1 {
		t.Errorf("RangeID = %d, want 1", rd.RangeID)
	}
}

func TestRouteKey_MaxUint64_NotFound(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	// MaxUint64 is the exclusive end key of the initial range — it is never owned.
	if _, err := r.RouteKey(^uint64(0)); err == nil {
		t.Fatal("RouteKey(MaxUint64) should return error (exclusive end key)")
	}
}

func TestRouteKey_SplitBoundary_GoesToUpperRange(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	// The split key 500 is the inclusive start of the upper range.
	rd, err := r.RouteKey(500)
	if err != nil {
		t.Fatalf("RouteKey(500): %v", err)
	}
	if rd.StartKey != 500 {
		t.Errorf("split key should route to upper range (StartKey=500), got StartKey=%d", rd.StartKey)
	}
}

func TestRouteKey_BelowSplitKey_GoesToLowerRange(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	rd, err := r.RouteKey(499)
	if err != nil {
		t.Fatalf("RouteKey(499): %v", err)
	}
	if rd.EndKey != 500 {
		t.Errorf("key 499 should route to lower range (EndKey=500), got EndKey=%d", rd.EndKey)
	}
}

func TestRouteKey_ReturnsCorrectFields(t *testing.T) {
	nodes := makeNodes()
	c := NewCoordinator(nodes)
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	} // [0,500) id=2, [500,MaxUint64) id=1
	c.UpdateLeader(2, 2)     // lower half, leader = node 2
	r := mustNewRouter(t, c) // cache populated with both ranges and leader info

	rd, err := r.RouteKey(0)
	if err != nil {
		t.Fatalf("RouteKey(0): %v", err)
	}
	if rd.RangeID != 2 {
		t.Errorf("RangeID = %d, want 2", rd.RangeID)
	}
	if rd.StartKey != 0 || rd.EndKey != 500 {
		t.Errorf("range = [%d, %d), want [0, 500)", rd.StartKey, rd.EndKey)
	}
	if rd.LeaderID != 2 {
		t.Errorf("LeaderID = %d, want 2", rd.LeaderID)
	}
	if len(rd.Replicas) != len(nodes) {
		t.Errorf("Replicas count = %d, want %d", len(rd.Replicas), len(nodes))
	}
}

func TestRouteKey_IsStaleUntilRefresh(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	} // split happens after router is constructed

	// Router cache still has old (pre-split) descriptor for key 300.
	rd, err := r.RouteKey(300)
	if err != nil {
		t.Fatalf("RouteKey(300) pre-Refresh: %v", err)
	}
	if rd.RangeID != 1 {
		t.Errorf("expected stale RangeID=1 before Refresh, got %d", rd.RangeID)
	}

	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	rd, err = r.RouteKey(300)
	if err != nil {
		t.Fatalf("RouteKey(300) post-Refresh: %v", err)
	}
	if rd.RangeID != 2 {
		t.Errorf("expected updated RangeID=2 after Refresh, got %d", rd.RangeID)
	}
}

func TestRouteKey_LeaderIsStaleUntilRefresh(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	c.UpdateLeader(1, 2)

	rd, _ := r.RouteKey(0)
	if rd.LeaderID != 0 {
		t.Fatalf("before Refresh: expected stale LeaderID=0, got %d", rd.LeaderID)
	}

	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	rd, _ = r.RouteKey(0)
	if rd.LeaderID != 2 {
		t.Errorf("after Refresh: LeaderID = %d, want 2", rd.LeaderID)
	}
}

func TestRouteKey_AllBoundaries_AfterMultipleSplits(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if err := c.RequestSplit(1, 1000); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := c.RequestSplit(2, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	// Ranges: [0,500), [500,1000), [1000,MaxUint64)

	cases := []struct {
		key       uint64
		wantStart uint64
		wantEnd   uint64
	}{
		{0, 0, 500},
		{499, 0, 500},
		{500, 500, 1000},
		{999, 500, 1000},
		{1000, 1000, ^uint64(0)},
		{9999, 1000, ^uint64(0)},
	}
	for _, tc := range cases {
		rd, err := r.RouteKey(tc.key)
		if err != nil {
			t.Fatalf("RouteKey(%d): %v", tc.key, err)
		}
		if rd.StartKey != tc.wantStart || rd.EndKey != tc.wantEnd {
			t.Errorf("RouteKey(%d) = [%d,%d), want [%d,%d)",
				tc.key, rd.StartKey, rd.EndKey, tc.wantStart, tc.wantEnd)
		}
	}
}

// ---- RouteRange ---------------------------------------------------------

func TestRouteRange_SingleRange(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	rds, err := r.RouteRange(0, 1000)
	if err != nil {
		t.Fatalf("RouteRange(0, 1000): %v", err)
	}
	if len(rds) != 1 || rds[0].RangeID != 1 {
		t.Fatalf("len = %d, RangeID = %v; want 1 range with RangeID=1", len(rds), rds)
	}
}

func TestRouteRange_SpansMultipleRanges(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	rds, err := r.RouteRange(400, 600)
	if err != nil {
		t.Fatalf("RouteRange(400, 600): %v", err)
	}
	if len(rds) != 2 {
		t.Fatalf("len = %d, want 2", len(rds))
	}
}

func TestRouteRange_ExactBoundary_ExcludesNextRange(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	// [0, 500) covers exactly the lower range; the upper range starts at 500 == high.
	rds, err := r.RouteRange(0, 500)
	if err != nil {
		t.Fatalf("RouteRange(0, 500): %v", err)
	}
	if len(rds) != 1 {
		t.Fatalf("len = %d, want 1", len(rds))
	}
	if rds[0].StartKey != 0 {
		t.Errorf("expected lower range (StartKey=0), got StartKey=%d", rds[0].StartKey)
	}
}

func TestRouteRange_EntireKeySpace_CoverAllRanges(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := c.RequestSplit(2, 250); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	// Ranges: [0,250), [250,500), [500,MaxUint64)

	rds, err := r.RouteRange(0, ^uint64(0))
	if err != nil {
		t.Fatalf("RouteRange full span: %v", err)
	}
	if len(rds) != 3 {
		t.Fatalf("len = %d, want 3", len(rds))
	}
}

func TestRouteRange_SingleKeyWindow(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	// A single-key window [300, 301) is entirely within the lower range [0,500).
	rds, err := r.RouteRange(300, 301)
	if err != nil {
		t.Fatalf("RouteRange(300, 301): %v", err)
	}
	if len(rds) != 1 || rds[0].StartKey != 0 {
		t.Errorf("expected lower range [0,500), got %+v", rds)
	}
}

func TestRouteRange_WindowAtSplitBoundary(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	// Window [499, 501) straddles the split: covers both [0,500) and [500,MaxUint64).
	rds, err := r.RouteRange(499, 501)
	if err != nil {
		t.Fatalf("RouteRange(499, 501): %v", err)
	}
	if len(rds) != 2 {
		t.Fatalf("len = %d, want 2 (window straddles split at 500)", len(rds))
	}
}

func TestRouteRange_IsStaleUntilRefresh(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}

	// Before refresh: span [400,600) sees only the stale pre-split range.
	rds, err := r.RouteRange(400, 600)
	if err != nil {
		t.Fatalf("RouteRange(400, 600) pre-Refresh: %v", err)
	}
	if len(rds) != 1 {
		t.Errorf("before Refresh: expected 1 stale range, got %d", len(rds))
	}

	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	rds, err = r.RouteRange(400, 600)
	if err != nil {
		t.Fatalf("RouteRange(400, 600) post-Refresh: %v", err)
	}
	if len(rds) != 2 {
		t.Errorf("after Refresh: expected 2 ranges, got %d", len(rds))
	}
}

func TestRouteRange_AllRangesSorted(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if err := c.RequestSplit(1, 750); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := c.RequestSplit(2, 250); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := c.RequestSplit(2, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	// Ranges (sorted): [0,250), [250,500), [500,750), [750,MaxUint64)

	rds, err := r.RouteRange(0, ^uint64(0))
	if err != nil {
		t.Fatalf("RouteRange full span: %v", err)
	}
	for i := 1; i < len(rds); i++ {
		if rds[i].StartKey < rds[i-1].StartKey {
			t.Errorf("ranges not sorted: rds[%d].StartKey=%d < rds[%d].StartKey=%d",
				i, rds[i].StartKey, i-1, rds[i-1].StartKey)
		}
	}
}

// ---- Cache isolation ----------------------------------------------------

func TestRouteKey_MutatingReturnedReplicas_DoesNotCorruptCoordinator(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	rd, _ := r.RouteKey(0)
	rd.Replicas[99] = "rogue:9090"

	live := c.LookupKey(0)
	if _, exists := live.Replicas[99]; exists {
		t.Fatal("mutating returned descriptor Replicas leaked into coordinator data")
	}
}

func TestRouteRange_MutatingReturnedReplicas_DoesNotCorruptCoordinator(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	rds, _ := r.RouteRange(0, ^uint64(0))
	for _, rd := range rds {
		rd.Replicas[99] = "rogue:9090"
	}

	for _, live := range c.GetAllRanges() {
		if _, exists := live.Replicas[99]; exists {
			t.Fatal("mutating returned RouteRange Replicas leaked into coordinator data")
		}
	}
}

func TestRouteKey_DescriptorFromBeforeRefresh_IsUnchangedAfterRefresh(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	old, _ := r.RouteKey(0)
	wantRangeID := old.RangeID

	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	// The descriptor obtained before Refresh must not be mutated by it.
	if old.RangeID != wantRangeID {
		t.Errorf("old descriptor changed after Refresh: RangeID = %d, want %d", old.RangeID, wantRangeID)
	}
}

func TestRouteKey_TwoCallsReturnIndependentDescriptors(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	rd1, _ := r.RouteKey(0)
	rd2, _ := r.RouteKey(0)

	rd1.Replicas[99] = "rogue:9090"

	if _, exists := rd2.Replicas[99]; exists {
		t.Fatal("mutating one returned descriptor's Replicas affected another — RouteKey must return independent copies")
	}
}

// ---- Concurrency --------------------------------------------------------

func TestRouter_ConcurrentRouteKey(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	var wg sync.WaitGroup
	for i := uint64(0); i < 100; i++ {
		wg.Add(1)
		go func(key uint64) {
			defer wg.Done()
			if _, err := r.RouteKey(key); err != nil {
				t.Errorf("RouteKey(%d): %v", key, err)
			}
		}(i * 1000)
	}
	wg.Wait()
}

func TestRouter_ConcurrentRouteRange(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	r := mustNewRouter(t, c)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rds, err := r.RouteRange(0, 1000)
			if err != nil || len(rds) == 0 {
				t.Errorf("RouteRange concurrent: err=%v, len=%d", err, len(rds))
			}
		}()
	}
	wg.Wait()
}

func TestRouter_ConcurrentRefreshAndRouteKey(t *testing.T) {
	c := NewCoordinator(makeNodes())
	r := mustNewRouter(t, c)

	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := r.Refresh(); err != nil {
					t.Errorf("Refresh: %v", err)
				}
			}
		}()
	}

	for i := uint64(0); i < 20; i++ {
		wg.Add(1)
		go func(key uint64) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if _, err := r.RouteKey(key); err != nil {
					t.Errorf("RouteKey(%d): %v", key, err)
				}
			}
		}(i * 100)
	}

	wg.Wait()
}

func TestRouter_ConcurrentRefreshAndRouteRange(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	r := mustNewRouter(t, c)

	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Refresh(); err != nil {
				t.Errorf("Refresh: %v", err)
			}
		}()
	}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := r.RouteRange(0, 1000); err != nil {
				t.Errorf("RouteRange: %v", err)
			}
		}()
	}

	wg.Wait()
}

func TestRouter_ConcurrentRefreshes_DoNotCorruptState(t *testing.T) {
	c := NewCoordinator(makeNodes())
	if err := c.RequestSplit(1, 500); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	r := mustNewRouter(t, c)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Refresh(); err != nil {
				t.Errorf("Refresh: %v", err)
			}
		}()
	}
	wg.Wait()

	// After all concurrent refreshes, routing must still be correct.
	cases := []struct {
		key     uint64
		wantEnd uint64
	}{
		{0, 500},
		{499, 500},
		{500, ^uint64(0)},
	}
	for _, tc := range cases {
		rd, err := r.RouteKey(tc.key)
		if err != nil {
			t.Fatalf("RouteKey(%d) after concurrent Refreshes: %v", tc.key, err)
		}
		if rd.EndKey != tc.wantEnd {
			t.Errorf("RouteKey(%d).EndKey = %d, want %d", tc.key, rd.EndKey, tc.wantEnd)
		}
	}
}
