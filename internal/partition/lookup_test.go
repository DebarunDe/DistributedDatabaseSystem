package partition

import (
	"testing"
)

// ---- Add ----------------------------------------------------------------

func TestRangeLookup_Add_Single(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a")

	if len(rl.ends) != 1 || rl.ends[0] != 100 {
		t.Fatalf("ends = %v, want [100]", rl.ends)
	}
	if rl.values[0] != "a" {
		t.Fatalf("values = %v, want [a]", rl.values)
	}
}

func TestRangeLookup_Add_MaintainsSortedOrder_Reverse(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(300, "c")
	rl.Add(100, "a")
	rl.Add(200, "b")

	wantEnds := []uint64{100, 200, 300}
	wantVals := []string{"a", "b", "c"}
	for i := range rl.ends {
		if rl.ends[i] != wantEnds[i] || rl.values[i] != wantVals[i] {
			t.Fatalf("index %d: got end=%d val=%q, want end=%d val=%q",
				i, rl.ends[i], rl.values[i], wantEnds[i], wantVals[i])
		}
	}
}

func TestRangeLookup_Add_MaintainsSortedOrder_Ascending(t *testing.T) {
	var rl RangeLookup[uint64, int]
	rl.Add(10, 1)
	rl.Add(20, 2)
	rl.Add(30, 3)

	for i, e := range rl.ends {
		want := uint64((i + 1) * 10)
		if e != want {
			t.Fatalf("ends[%d] = %d, want %d", i, e, want)
		}
	}
}

func TestRangeLookup_Add_MaintainsSortedOrder_Interleaved(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(200, "b")
	rl.Add(400, "d")
	rl.Add(100, "a")
	rl.Add(300, "c")

	wantEnds := []uint64{100, 200, 300, 400}
	wantVals := []string{"a", "b", "c", "d"}
	for i := range rl.ends {
		if rl.ends[i] != wantEnds[i] || rl.values[i] != wantVals[i] {
			t.Fatalf("index %d: got end=%d val=%q, want end=%d val=%q",
				i, rl.ends[i], rl.values[i], wantEnds[i], wantVals[i])
		}
	}
}

// ---- Find ---------------------------------------------------------------

func TestRangeLookup_Find_Empty(t *testing.T) {
	var rl RangeLookup[uint64, string]
	_, ok := rl.Find(0)
	if ok {
		t.Fatal("Find on empty lookup should return false")
	}
}

func TestRangeLookup_Find_SingleRange_FirstKey(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a") // covers [0, 100)

	v, ok := rl.Find(0)
	if !ok || v != "a" {
		t.Fatalf("Find(0) = %q, %v; want \"a\", true", v, ok)
	}
}

func TestRangeLookup_Find_SingleRange_LastKey(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a")

	v, ok := rl.Find(99)
	if !ok || v != "a" {
		t.Fatalf("Find(99) = %q, %v; want \"a\", true", v, ok)
	}
}

func TestRangeLookup_Find_EndKeyIsExclusive(t *testing.T) {
	// End key 100 is the boundary between range "a" [0,100) and range "b" [100,200).
	// Find(100) must return "b", not "a".
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a")
	rl.Add(200, "b")

	v, ok := rl.Find(100)
	if !ok || v != "b" {
		t.Fatalf("Find(100) = %q, %v; want \"b\", true", v, ok)
	}
}

func TestRangeLookup_Find_BeyondAllRanges(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a")

	// Exact end key with no successor range.
	_, ok := rl.Find(100)
	if ok {
		t.Fatal("Find(100) on single range ending at 100 should return false")
	}

	_, ok = rl.Find(200)
	if ok {
		t.Fatal("Find(200) beyond all ranges should return false")
	}
}

func TestRangeLookup_Find_MultipleRanges(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a") // [0, 100)
	rl.Add(200, "b") // [100, 200)
	rl.Add(300, "c") // [200, 300)

	cases := []struct {
		key  uint64
		want string
	}{
		{0, "a"}, {50, "a"}, {99, "a"},
		{100, "b"}, {150, "b"}, {199, "b"},
		{200, "c"}, {250, "c"}, {299, "c"},
	}
	for _, tc := range cases {
		v, ok := rl.Find(tc.key)
		if !ok || v != tc.want {
			t.Errorf("Find(%d) = %q, %v; want %q, true", tc.key, v, ok, tc.want)
		}
	}
}

func TestRangeLookup_Find_MaxUint64EndKey(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(^uint64(0), "all") // covers [0, MaxUint64)

	v, ok := rl.Find(0)
	if !ok || v != "all" {
		t.Fatalf("Find(0) = %q, %v; want \"all\", true", v, ok)
	}
	v, ok = rl.Find(^uint64(0) - 1)
	if !ok || v != "all" {
		t.Fatalf("Find(MaxUint64-1) = %q, %v; want \"all\", true", v, ok)
	}
	// MaxUint64 itself is the exclusive end: not found.
	_, ok = rl.Find(^uint64(0))
	if ok {
		t.Fatal("Find(MaxUint64) should return false (exclusive end key)")
	}
}

// ---- FindRange ----------------------------------------------------------

func TestRangeLookup_FindRange_Empty(t *testing.T) {
	var rl RangeLookup[uint64, string]
	got := rl.FindRange(0, 100)
	if len(got) != 0 {
		t.Fatalf("FindRange on empty lookup: got %v, want []", got)
	}
}

func TestRangeLookup_FindRange_SingleRange_FullSpan(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(1000, "a")

	got := rl.FindRange(0, 999)
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("FindRange(0, 999) = %v; want [a]", got)
	}
}

func TestRangeLookup_FindRange_NoOverlap(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a") // [0, 100)
	rl.Add(200, "b") // [100, 200)

	// Query [200, 300): range "b" ends at 200 which equals low, so no overlap.
	got := rl.FindRange(200, 300)
	if len(got) != 0 {
		t.Fatalf("FindRange(200, 300) = %v; want []", got)
	}
}

func TestRangeLookup_FindRange_LastRangeIncluded(t *testing.T) {
	// Regression test for the bug where the last overlapping range was dropped
	// because its end exceeds the query high boundary.
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a") // [0, 100)
	rl.Add(200, "b") // [100, 200)
	rl.Add(300, "c") // [200, 300)

	// Query [50, 150) spans "a" and "b"; "b" ends at 200 > 150 but starts at 100 < 150.
	got := rl.FindRange(50, 150)
	if len(got) != 2 {
		t.Fatalf("FindRange(50, 150) returned %d ranges, want 2: %v", len(got), got)
	}
	if got[0] != "a" || got[1] != "b" {
		t.Errorf("FindRange(50, 150) = %v; want [a b]", got)
	}
}

func TestRangeLookup_FindRange_AllRanges(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a")
	rl.Add(200, "b")
	rl.Add(300, "c")

	got := rl.FindRange(0, 300)
	if len(got) != 3 {
		t.Fatalf("FindRange(0, 300) returned %d ranges, want 3: %v", len(got), got)
	}
}

func TestRangeLookup_FindRange_ExactRangeBoundary(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a") // [0, 100)
	rl.Add(200, "b") // [100, 200)

	// Query [0, 100) is exactly range "a"; "b" starts at 100 == high, so excluded.
	got := rl.FindRange(0, 100)
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("FindRange(0, 100) = %v; want [a]", got)
	}
}

func TestRangeLookup_FindRange_MiddleRangeOnly(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a")
	rl.Add(200, "b")
	rl.Add(300, "c")

	// Query [150, 160) is entirely within "b" [100, 200).
	got := rl.FindRange(150, 160)
	if len(got) != 1 || got[0] != "b" {
		t.Fatalf("FindRange(150, 160) = %v; want [b]", got)
	}
}

func TestRangeLookup_FindRange_QueryAtRangeBoundary(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a") // [0, 100)
	rl.Add(200, "b") // [100, 200)
	rl.Add(300, "c") // [200, 300)

	// Query [100, 200) is exactly range "b".
	got := rl.FindRange(100, 200)
	if len(got) != 1 || got[0] != "b" {
		t.Fatalf("FindRange(100, 200) = %v; want [b]", got)
	}
}

func TestRangeLookup_FindRange_SingleKeyQuery(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a")
	rl.Add(200, "b")

	// Query [150, 151) — a single-key window entirely in "b".
	got := rl.FindRange(150, 151)
	if len(got) != 1 || got[0] != "b" {
		t.Fatalf("FindRange(150, 151) = %v; want [b]", got)
	}
}

// ---- Remove -------------------------------------------------------------

func TestRangeLookup_Remove_Middle(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a")
	rl.Add(200, "b")
	rl.Add(300, "c")

	rl.Remove(200)

	if len(rl.ends) != 2 {
		t.Fatalf("len(ends) = %d, want 2", len(rl.ends))
	}
	if rl.ends[0] != 100 || rl.ends[1] != 300 {
		t.Errorf("ends = %v, want [100 300]", rl.ends)
	}
	if rl.values[0] != "a" || rl.values[1] != "c" {
		t.Errorf("values = %v, want [a c]", rl.values)
	}
}

func TestRangeLookup_Remove_First(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a")
	rl.Add(200, "b")

	rl.Remove(100)

	if len(rl.ends) != 1 || rl.ends[0] != 200 || rl.values[0] != "b" {
		t.Fatalf("after Remove(100): ends=%v values=%v", rl.ends, rl.values)
	}
}

func TestRangeLookup_Remove_Last(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a")
	rl.Add(200, "b")

	rl.Remove(200)

	if len(rl.ends) != 1 || rl.ends[0] != 100 || rl.values[0] != "a" {
		t.Fatalf("after Remove(200): ends=%v values=%v", rl.ends, rl.values)
	}
}

func TestRangeLookup_Remove_NotFound_IsNoOp(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a")

	rl.Remove(999)

	if len(rl.ends) != 1 {
		t.Fatalf("Remove of non-existent key should be a no-op, got ends=%v", rl.ends)
	}
}

func TestRangeLookup_Remove_All_LeavesEmptyLookup(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a")
	rl.Remove(100)

	_, ok := rl.Find(50)
	if ok {
		t.Fatal("Find on empty lookup after Remove should return false")
	}
	if len(rl.ends) != 0 || len(rl.values) != 0 {
		t.Fatalf("expected empty state, got ends=%v values=%v", rl.ends, rl.values)
	}
}

func TestRangeLookup_Remove_FindStillWorksAfter(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a")
	rl.Add(200, "b")
	rl.Add(300, "c")

	rl.Remove(200) // remove middle range

	// Keys that were in [0,100) and [200,300) should still be found correctly.
	if v, ok := rl.Find(50); !ok || v != "a" {
		t.Errorf("Find(50) after remove = %q, %v; want \"a\", true", v, ok)
	}
	if v, ok := rl.Find(250); !ok || v != "c" {
		t.Errorf("Find(250) after remove = %q, %v; want \"c\", true", v, ok)
	}
}

// ---- Values -------------------------------------------------------------

func TestRangeLookup_Values_Empty(t *testing.T) {
	var rl RangeLookup[uint64, string]
	v := rl.Values()
	if len(v) != 0 {
		t.Fatalf("Values() on empty lookup = %v, want []", v)
	}
}

func TestRangeLookup_Values_ReturnsCopy(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(100, "a")
	rl.Add(200, "b")

	v := rl.Values()
	v[0] = "modified"

	orig := rl.Values()
	if orig[0] != "a" {
		t.Fatal("modifying the returned slice affected the internal state")
	}
}

func TestRangeLookup_Values_OrderMatchesSortedEnds(t *testing.T) {
	var rl RangeLookup[uint64, string]
	rl.Add(300, "c")
	rl.Add(100, "a")
	rl.Add(200, "b")

	v := rl.Values()
	want := []string{"a", "b", "c"}
	for i, got := range v {
		if got != want[i] {
			t.Errorf("Values()[%d] = %q, want %q", i, got, want[i])
		}
	}
}
