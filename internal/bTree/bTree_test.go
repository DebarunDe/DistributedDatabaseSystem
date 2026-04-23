package btree

import (
	"encoding/binary"
	"fmt"
	"sort"
	"testing"

	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
)

// makeRecord builds a record with an 8-byte BigEndian key followed by the given value bytes.
func makeRecord(key uint64, value []byte) []byte {
	rec := make([]byte, 8+len(value))
	binary.BigEndian.PutUint64(rec[:8], key)
	copy(rec[8:], value)
	return rec
}

// newLeaf creates a fresh leaf page with no siblings.
func newLeaf() *pagemanager.Page {
	return pagemanager.NewLeafPage(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID)
}

// insertSorted inserts records in key order, panics if insert fails.
func insertSorted(t *testing.T, page *pagemanager.Page, keys []uint64) {
	t.Helper()
	for _, k := range keys {
		rec := makeRecord(k, []byte("val"))
		_, ok := page.InsertRecord(rec)
		if !ok {
			t.Fatalf("InsertRecord failed for key %d", k)
		}
	}
}

// TestSearchLeaf_EmptyPage verifies no crash and correct miss on an empty page.
func TestSearchLeaf_EmptyPage(t *testing.T) {
	page := newLeaf()
	rec, found := searchLeaf(42, page)
	if found || rec != nil {
		t.Errorf("empty page: expected (nil, false), got (%v, %v)", rec, found)
	}
}

// TestSearchLeaf_SingleRecord_Hit finds the only record on the page.
func TestSearchLeaf_SingleRecord_Hit(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{100})

	rec, found := searchLeaf(100, page)
	if !found {
		t.Fatal("expected found=true")
	}
	if RecordKey(rec) != 100 {
		t.Errorf("key mismatch: got %d", RecordKey(rec))
	}
}

// TestSearchLeaf_SingleRecord_Miss misses on a page with one different key.
func TestSearchLeaf_SingleRecord_Miss(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{100})

	rec, found := searchLeaf(99, page)
	if found || rec != nil {
		t.Errorf("expected miss, got (%v, %v)", rec, found)
	}
}

// TestSearchLeaf_FirstKey finds the lowest key (left boundary of binary search).
func TestSearchLeaf_FirstKey(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{10, 20, 30, 40, 50})

	rec, found := searchLeaf(10, page)
	if !found || RecordKey(rec) != 10 {
		t.Errorf("first key: expected (10, true), got (%v, %v)", RecordKey(rec), found)
	}
}

// TestSearchLeaf_LastKey finds the highest key (right boundary of binary search).
func TestSearchLeaf_LastKey(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{10, 20, 30, 40, 50})

	rec, found := searchLeaf(50, page)
	if !found || RecordKey(rec) != 50 {
		t.Errorf("last key: expected (50, true), got (%v, %v)", RecordKey(rec), found)
	}
}

// TestSearchLeaf_MiddleKey finds a key in the middle of the page.
func TestSearchLeaf_MiddleKey(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{10, 20, 30, 40, 50})

	rec, found := searchLeaf(30, page)
	if !found || RecordKey(rec) != 30 {
		t.Errorf("middle key: expected (30, true), got (%v, %v)", RecordKey(rec), found)
	}
}

// TestSearchLeaf_AllKeys verifies every key on a multi-record page is found.
func TestSearchLeaf_AllKeys(t *testing.T) {
	keys := []uint64{5, 15, 25, 35, 45, 55, 65, 75}
	page := newLeaf()
	insertSorted(t, page, keys)

	for _, k := range keys {
		rec, found := searchLeaf(k, page)
		if !found {
			t.Errorf("key %d: expected found", k)
			continue
		}
		if RecordKey(rec) != k {
			t.Errorf("key %d: record returned wrong key %d", k, RecordKey(rec))
		}
	}
}

// TestSearchLeaf_KeyBelowAll misses when the search key is less than all stored keys.
func TestSearchLeaf_KeyBelowAll(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{10, 20, 30})

	rec, found := searchLeaf(1, page)
	if found || rec != nil {
		t.Errorf("below-all miss: expected (nil, false), got (%v, %v)", rec, found)
	}
}

// TestSearchLeaf_KeyAboveAll misses when the search key exceeds all stored keys.
func TestSearchLeaf_KeyAboveAll(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{10, 20, 30})

	rec, found := searchLeaf(999, page)
	if found || rec != nil {
		t.Errorf("above-all miss: expected (nil, false), got (%v, %v)", rec, found)
	}
}

// TestSearchLeaf_KeyInGap misses when the key falls between two stored keys.
func TestSearchLeaf_KeyInGap(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{10, 30, 50})

	for _, miss := range []uint64{11, 20, 29, 31, 49} {
		rec, found := searchLeaf(miss, page)
		if found || rec != nil {
			t.Errorf("gap key %d: expected miss, got (%v, %v)", miss, rec, found)
		}
	}
}

// TestSearchLeaf_KeyZero verifies key=0 (minimum uint64) works correctly.
func TestSearchLeaf_KeyZero(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{0, 1, 2})

	rec, found := searchLeaf(0, page)
	if !found || RecordKey(rec) != 0 {
		t.Errorf("key=0 hit: expected (0, true), got (%v, %v)", RecordKey(rec), found)
	}
}

// TestSearchLeaf_MaxUint64 verifies the maximum uint64 key is found correctly.
func TestSearchLeaf_MaxUint64(t *testing.T) {
	const maxKey = ^uint64(0)
	page := newLeaf()
	insertSorted(t, page, []uint64{1, 100, maxKey})

	rec, found := searchLeaf(maxKey, page)
	if !found || RecordKey(rec) != maxKey {
		t.Errorf("maxKey hit: expected (maxKey, true), got (%v, %v)", RecordKey(rec), found)
	}
}

// TestSearchLeaf_MaxUint64_Miss misses on maxKey-1 when only maxKey is stored.
func TestSearchLeaf_MaxUint64_Miss(t *testing.T) {
	const maxKey = ^uint64(0)
	page := newLeaf()
	insertSorted(t, page, []uint64{maxKey})

	rec, found := searchLeaf(maxKey-1, page)
	if found || rec != nil {
		t.Errorf("maxKey-1 miss: expected (nil, false), got (%v, %v)", rec, found)
	}
}

// TestSearchLeaf_ValueBytes confirms the full record bytes (key+value) are returned, not just the key.
func TestSearchLeaf_ValueBytes(t *testing.T) {
	page := newLeaf()
	want := []byte("hello")
	rec := makeRecord(42, want)
	page.InsertRecord(rec)

	got, found := searchLeaf(42, page)
	if !found {
		t.Fatal("expected found")
	}
	if string(got[8:]) != string(want) {
		t.Errorf("value bytes: want %q, got %q", want, got[8:])
	}
}

// TestSearchLeaf_DeletedSlot_Miss verifies that searching for a deleted slot's key returns false.
// searchLeaf relies on GetRecord which returns (nil, false) for unused slots; the binary search
// short-circuits on that and returns false rather than scanning around the hole.
func TestSearchLeaf_DeletedSlot_Miss(t *testing.T) {
	page := newLeaf()
	// Insert three records in sorted order: slot 0=10, slot 1=20, slot 2=30
	insertSorted(t, page, []uint64{10, 20, 30})

	// Delete the middle slot (key 20) — slot 1 is now unused
	page.DeleteRecord(1)

	// Searching for 20 may return false because its slot is deleted.
	// searchLeaf does not skip holes; a deleted mid-slot terminates the search immediately.
	_, found := searchLeaf(20, page)
	if found {
		t.Error("deleted slot key: expected not found after slot deletion")
	}
}

// TestSearchLeaf_LargeEvenPage exercises binary search across a larger sorted set.
func TestSearchLeaf_LargeEvenPage(t *testing.T) {
	page := newLeaf()
	var keys []uint64
	for i := uint64(0); i < 50; i++ {
		keys = append(keys, i*10)
	}
	insertSorted(t, page, keys)

	for _, k := range keys {
		rec, found := searchLeaf(k, page)
		if !found || RecordKey(rec) != k {
			t.Errorf("large page: key %d not found correctly", k)
		}
	}

	// Gaps between entries should all miss
	for i := uint64(0); i < 49; i++ {
		_, found := searchLeaf(i*10+5, page)
		if found {
			t.Errorf("large page gap: key %d should not be found", i*10+5)
		}
	}
}

// ---- searchInternal helpers ----

// newInternal creates an internal page with the given rightmost child ID.
func newInternal(rightmostChild uint32) *pagemanager.Page {
	return pagemanager.NewInternalPage(1, 1, rightmostChild)
}

// insertInternalRecords inserts (key, childID) pairs into an internal page in order.
// Panics via t.Fatalf if any insert fails.
func insertInternalRecords(t *testing.T, page *pagemanager.Page, keys []uint64, childIDs []uint32) {
	t.Helper()
	for i, k := range keys {
		rec := EncodeInternalRecord(k, childIDs[i])
		_, ok := page.InsertRecord(rec)
		if !ok {
			t.Fatalf("InsertRecord failed for internal key %d", k)
		}
	}
}

// ---- searchInternal tests ----

// TestSearchInternal_EmptyPage returns the rightmost child when there are no records.
func TestSearchInternal_EmptyPage(t *testing.T) {
	const rightmost uint32 = 99
	page := newInternal(rightmost)
	got := searchInternal(42, page)
	if got != rightmost {
		t.Errorf("empty page: want rightmost=%d, got %d", rightmost, got)
	}
}

// TestSearchInternal_SingleRecord_KeyLessThan routes to the record's child when key < separator.
func TestSearchInternal_SingleRecord_KeyLessThan(t *testing.T) {
	// Layout: [ key=50, child=10 ] rightmost=99
	// key=30 <= 50, so route to child 10
	page := newInternal(99)
	insertInternalRecords(t, page, []uint64{50}, []uint32{10})
	got := searchInternal(30, page)
	if got != 10 {
		t.Errorf("key<separator: want 10, got %d", got)
	}
}

// TestSearchInternal_SingleRecord_KeyEqual routes to the record's child when key == separator.
func TestSearchInternal_SingleRecord_KeyEqual(t *testing.T) {
	page := newInternal(99)
	insertInternalRecords(t, page, []uint64{50}, []uint32{10})
	got := searchInternal(50, page)
	if got != 10 {
		t.Errorf("key==separator: want 10, got %d", got)
	}
}

// TestSearchInternal_SingleRecord_KeyGreaterThan routes to rightmost child when key > separator.
func TestSearchInternal_SingleRecord_KeyGreaterThan(t *testing.T) {
	page := newInternal(99)
	insertInternalRecords(t, page, []uint64{50}, []uint32{10})
	got := searchInternal(75, page)
	if got != 99 {
		t.Errorf("key>separator: want rightmost=99, got %d", got)
	}
}

// TestSearchInternal_MultipleRecords_KeyBelowAll routes to the first child.
func TestSearchInternal_MultipleRecords_KeyBelowAll(t *testing.T) {
	// keys: 10, 20, 30 | children: 1, 2, 3 | rightmost: 4
	// key=5 <= 10, so first child = 1
	page := newInternal(4)
	insertInternalRecords(t, page, []uint64{10, 20, 30}, []uint32{1, 2, 3})
	got := searchInternal(5, page)
	if got != 1 {
		t.Errorf("below-all: want 1, got %d", got)
	}
}

// TestSearchInternal_MultipleRecords_KeyEqualFirst routes to the first record's child.
func TestSearchInternal_MultipleRecords_KeyEqualFirst(t *testing.T) {
	page := newInternal(4)
	insertInternalRecords(t, page, []uint64{10, 20, 30}, []uint32{1, 2, 3})
	got := searchInternal(10, page)
	if got != 1 {
		t.Errorf("key==first separator: want 1, got %d", got)
	}
}

// TestSearchInternal_MultipleRecords_KeyInFirstGap routes to the second child (10 < key <= 20).
func TestSearchInternal_MultipleRecords_KeyInFirstGap(t *testing.T) {
	page := newInternal(4)
	insertInternalRecords(t, page, []uint64{10, 20, 30}, []uint32{1, 2, 3})
	got := searchInternal(15, page)
	if got != 2 {
		t.Errorf("key in gap (10,20]: want 2, got %d", got)
	}
}

// TestSearchInternal_MultipleRecords_KeyEqualMiddle routes to the middle child when key == middle separator.
func TestSearchInternal_MultipleRecords_KeyEqualMiddle(t *testing.T) {
	page := newInternal(4)
	insertInternalRecords(t, page, []uint64{10, 20, 30}, []uint32{1, 2, 3})
	got := searchInternal(20, page)
	if got != 2 {
		t.Errorf("key==middle separator: want 2, got %d", got)
	}
}

// TestSearchInternal_MultipleRecords_KeyInSecondGap routes to the third child (20 < key <= 30).
func TestSearchInternal_MultipleRecords_KeyInSecondGap(t *testing.T) {
	page := newInternal(4)
	insertInternalRecords(t, page, []uint64{10, 20, 30}, []uint32{1, 2, 3})
	got := searchInternal(25, page)
	if got != 3 {
		t.Errorf("key in gap (20,30]: want 3, got %d", got)
	}
}

// TestSearchInternal_MultipleRecords_KeyEqualLast routes to the last record's child.
func TestSearchInternal_MultipleRecords_KeyEqualLast(t *testing.T) {
	page := newInternal(4)
	insertInternalRecords(t, page, []uint64{10, 20, 30}, []uint32{1, 2, 3})
	got := searchInternal(30, page)
	if got != 3 {
		t.Errorf("key==last separator: want 3, got %d", got)
	}
}

// TestSearchInternal_MultipleRecords_KeyAboveAll routes to the rightmost child.
func TestSearchInternal_MultipleRecords_KeyAboveAll(t *testing.T) {
	page := newInternal(4)
	insertInternalRecords(t, page, []uint64{10, 20, 30}, []uint32{1, 2, 3})
	got := searchInternal(40, page)
	if got != 4 {
		t.Errorf("key>all separators: want rightmost=4, got %d", got)
	}
}

// TestSearchInternal_AllRoutingSlots verifies every routing boundary across a multi-record page.
func TestSearchInternal_AllRoutingSlots(t *testing.T) {
	// separators: 100, 200, 300, 400
	// children:     1,   2,   3,   4   rightmost: 5
	keys := []uint64{100, 200, 300, 400}
	children := []uint32{1, 2, 3, 4}
	const rightmost uint32 = 5

	page := newInternal(rightmost)
	insertInternalRecords(t, page, keys, children)

	tests := []struct {
		searchKey uint64
		wantChild uint32
	}{
		{50, 1},   // below all
		{100, 1},  // == first separator
		{150, 2},  // in (100, 200]
		{200, 2},  // == second separator
		{250, 3},  // in (200, 300]
		{300, 3},  // == third separator
		{350, 4},  // in (300, 400]
		{400, 4},  // == fourth separator
		{401, 5},  // above all → rightmost
		{9999, 5}, // far above all → rightmost
	}

	for _, tt := range tests {
		got := searchInternal(tt.searchKey, page)
		if got != tt.wantChild {
			t.Errorf("key=%d: want child=%d, got %d", tt.searchKey, tt.wantChild, got)
		}
	}
}

// TestSearchInternal_KeyZero_BelowAll routes key=0 to the first child.
func TestSearchInternal_KeyZero_BelowAll(t *testing.T) {
	page := newInternal(99)
	insertInternalRecords(t, page, []uint64{10, 20}, []uint32{1, 2})
	got := searchInternal(0, page)
	if got != 1 {
		t.Errorf("key=0: want 1, got %d", got)
	}
}

// TestSearchInternal_KeyZero_ExactMatch routes key=0 when 0 is a stored separator.
func TestSearchInternal_KeyZero_ExactMatch(t *testing.T) {
	page := newInternal(99)
	insertInternalRecords(t, page, []uint64{0, 10}, []uint32{1, 2})
	got := searchInternal(0, page)
	if got != 1 {
		t.Errorf("key=0 exact: want 1, got %d", got)
	}
}

// TestSearchInternal_MaxUint64_AboveAll routes the maximum uint64 to rightmost child.
func TestSearchInternal_MaxUint64_AboveAll(t *testing.T) {
	const maxKey = ^uint64(0)
	page := newInternal(99)
	insertInternalRecords(t, page, []uint64{100, 200}, []uint32{1, 2})
	got := searchInternal(maxKey, page)
	if got != 99 {
		t.Errorf("maxUint64: want rightmost=99, got %d", got)
	}
}

// TestSearchInternal_MaxUint64_ExactMatch routes when maxUint64 is itself a stored separator.
func TestSearchInternal_MaxUint64_ExactMatch(t *testing.T) {
	const maxKey = ^uint64(0)
	page := newInternal(99)
	insertInternalRecords(t, page, []uint64{100, maxKey}, []uint32{1, 2})
	got := searchInternal(maxKey, page)
	if got != 2 {
		t.Errorf("maxUint64 exact match: want 2, got %d", got)
	}
}

// TestSearchInternal_DistinctChildIDs confirms the actual stored child ID is returned, not a slot index.
func TestSearchInternal_DistinctChildIDs(t *testing.T) {
	// Use non-sequential, recognisable child IDs to catch off-by-one on ID vs index.
	page := newInternal(1000)
	insertInternalRecords(t, page,
		[]uint64{50, 150, 250},
		[]uint32{777, 888, 999},
	)

	tests := []struct {
		key   uint64
		child uint32
	}{
		{10, 777},
		{50, 777},
		{100, 888},
		{150, 888},
		{200, 999},
		{250, 999},
		{300, 1000},
	}
	for _, tt := range tests {
		got := searchInternal(tt.key, page)
		if got != tt.child {
			t.Errorf("distinct IDs: key=%d want=%d got=%d", tt.key, tt.child, got)
		}
	}
}

// TestSearchInternal_DeletedSlot_ReturnsInvalidPageID checks that a deleted middle slot causes
// searchInternal to return InvalidPageID, because GetRecord returns false for unused slots.
func TestSearchInternal_DeletedSlot_ReturnsInvalidPageID(t *testing.T) {
	// 3 records at slots 0,1,2 with keys 10,20,30.
	// Searching for key=20 will binary-search to slot 1 (mid of 3).
	// After deleting slot 1, GetRecord(1) returns false → InvalidPageID.
	page := newInternal(99)
	insertInternalRecords(t, page, []uint64{10, 20, 30}, []uint32{1, 2, 3})
	page.DeleteRecord(1)

	got := searchInternal(20, page)
	if got != pagemanager.InvalidPageID {
		t.Errorf("deleted slot: want InvalidPageID, got %d", got)
	}
}

// TestSearchInternal_LargePage_CorrectRouting verifies routing across a large number of separators.
func TestSearchInternal_LargePage_CorrectRouting(t *testing.T) {
	const n = 40
	var keys []uint64
	var children []uint32
	for i := 0; i < n; i++ {
		keys = append(keys, uint64((i+1)*100)) // 100, 200, ..., 4000
		children = append(children, uint32(i+1))
	}
	const rightmost uint32 = uint32(n + 1)

	page := newInternal(rightmost)
	insertInternalRecords(t, page, keys, children)

	// Exact matches: key == separator[i] → child[i]
	for i, k := range keys {
		got := searchInternal(k, page)
		if got != children[i] {
			t.Errorf("exact match key=%d: want %d, got %d", k, children[i], got)
		}
	}

	// Key just below each separator: routes to child[i] (first where key <= sep[i])
	for i, k := range keys {
		got := searchInternal(k-1, page)
		if got != children[i] {
			t.Errorf("key=%d (sep[%d]-1): want %d, got %d", k-1, i, children[i], got)
		}
	}

	// Key above all separators → rightmost
	got := searchInternal(keys[n-1]+1, page)
	if got != rightmost {
		t.Errorf("above all: want rightmost=%d, got %d", rightmost, got)
	}
}

// ---- findLeaf helpers ----

// mockPM is a minimal in-memory PageManager stub for testing findLeaf.
// Only ReadPage is functional; all other methods are no-ops.
type mockPM struct {
	pages   map[uint32]*pagemanager.Page
	readErr map[uint32]error
}

func newMockPM() *mockPM {
	return &mockPM{
		pages:   make(map[uint32]*pagemanager.Page),
		readErr: make(map[uint32]error),
	}
}

func (m *mockPM) ReadPage(pageId uint32) (*pagemanager.Page, error) {
	if err, ok := m.readErr[pageId]; ok {
		return nil, err
	}
	p, ok := m.pages[pageId]
	if !ok {
		return nil, fmt.Errorf("page %d not found", pageId)
	}
	return p, nil
}

func (m *mockPM) AllocatePage() (*pagemanager.Page, error) { return nil, nil }
func (m *mockPM) WritePage(_ *pagemanager.Page) error      { return nil }
func (m *mockPM) FreePage(_ uint32) error                  { return nil }
func (m *mockPM) GetRootPageId() uint32                    { return 0 }
func (m *mockPM) SetRootPageId(_ uint32) error             { return nil }
func (m *mockPM) Close() error                             { return nil }
func (m *mockPM) Delete() error                            { return nil }

// leafPage creates a leaf page with the given ID and pre-inserted keys.
func leafPage(id uint32, keys []uint64) *pagemanager.Page {
	p := pagemanager.NewLeafPage(id, pagemanager.InvalidPageID, pagemanager.InvalidPageID)
	for _, k := range keys {
		p.InsertRecord(makeRecord(k, []byte("v")))
	}
	return p
}

// internalPage creates an internal page with the given separators, left children, and rightmost child.
func internalPage(id uint32, seps []uint64, children []uint32, rightmost uint32) *pagemanager.Page {
	p := pagemanager.NewInternalPage(id, 1, rightmost)
	for i, sep := range seps {
		p.InsertRecord(EncodeInternalRecord(sep, children[i]))
	}
	return p
}

// ---- findLeaf tests ----

// TestFindLeaf_LeafPage_ReturnedDirectly verifies a leaf page is returned without calling ReadPage.
func TestFindLeaf_LeafPage_ReturnedDirectly(t *testing.T) {
	bt := NewBTree(newMockPM())
	root := leafPage(1, []uint64{10, 20, 30})

	got, err := bt.findLeaf(20, root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != root {
		t.Error("expected the same leaf page to be returned")
	}
}

// TestFindLeaf_EmptyLeafPage_ReturnedDirectly verifies an empty leaf is returned without traversal.
func TestFindLeaf_EmptyLeafPage_ReturnedDirectly(t *testing.T) {
	bt := NewBTree(newMockPM())
	root := pagemanager.NewLeafPage(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID)

	got, err := bt.findLeaf(42, root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != root {
		t.Error("empty leaf page should be returned directly")
	}
}

// TestFindLeaf_OneLevel_RoutesToLeftLeaf verifies routing to the left child when key < separator.
func TestFindLeaf_OneLevel_RoutesToLeftLeaf(t *testing.T) {
	pm := newMockPM()
	leaf2 := leafPage(2, []uint64{10, 20, 40})
	leaf3 := leafPage(3, []uint64{60, 70, 80})
	pm.pages[2] = leaf2
	pm.pages[3] = leaf3

	root := internalPage(1, []uint64{50}, []uint32{2}, 3)
	bt := NewBTree(pm)

	got, err := bt.findLeaf(30, root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != leaf2 {
		t.Error("key < separator: expected left leaf (page 2)")
	}
}

// TestFindLeaf_OneLevel_RoutesToRightLeaf verifies routing to the rightmost child when key > separator.
func TestFindLeaf_OneLevel_RoutesToRightLeaf(t *testing.T) {
	pm := newMockPM()
	leaf2 := leafPage(2, []uint64{10, 20, 40})
	leaf3 := leafPage(3, []uint64{60, 70, 80})
	pm.pages[2] = leaf2
	pm.pages[3] = leaf3

	root := internalPage(1, []uint64{50}, []uint32{2}, 3)
	bt := NewBTree(pm)

	got, err := bt.findLeaf(70, root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != leaf3 {
		t.Error("key > separator: expected rightmost leaf (page 3)")
	}
}

// TestFindLeaf_OneLevel_KeyEqualSeparator verifies key == separator routes left (lower-bound semantics).
func TestFindLeaf_OneLevel_KeyEqualSeparator(t *testing.T) {
	pm := newMockPM()
	leaf2 := leafPage(2, []uint64{10, 50})
	leaf3 := leafPage(3, []uint64{60, 70})
	pm.pages[2] = leaf2
	pm.pages[3] = leaf3

	root := internalPage(1, []uint64{50}, []uint32{2}, 3)
	bt := NewBTree(pm)

	got, err := bt.findLeaf(50, root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != leaf2 {
		t.Error("key == separator: expected left leaf (page 2)")
	}
}

// TestFindLeaf_OneLevel_KeyBelowAll verifies the first child is chosen when key is less than all separators.
func TestFindLeaf_OneLevel_KeyBelowAll(t *testing.T) {
	pm := newMockPM()
	leaf2 := leafPage(2, []uint64{5})
	leaf3 := leafPage(3, []uint64{200})
	pm.pages[2] = leaf2
	pm.pages[3] = leaf3

	root := internalPage(1, []uint64{100}, []uint32{2}, 3)
	bt := NewBTree(pm)

	got, err := bt.findLeaf(1, root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != leaf2 {
		t.Error("key below all: expected leftmost leaf (page 2)")
	}
}

// TestFindLeaf_OneLevel_KeyAboveAll verifies the rightmost child is chosen when key exceeds all separators.
func TestFindLeaf_OneLevel_KeyAboveAll(t *testing.T) {
	pm := newMockPM()
	leaf2 := leafPage(2, []uint64{10})
	leaf3 := leafPage(3, []uint64{200, 300})
	pm.pages[2] = leaf2
	pm.pages[3] = leaf3

	root := internalPage(1, []uint64{50}, []uint32{2}, 3)
	bt := NewBTree(pm)

	got, err := bt.findLeaf(999, root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != leaf3 {
		t.Error("key above all: expected rightmost leaf (page 3)")
	}
}

// TestFindLeaf_OneLevel_MultipleChildren verifies all routing slots across a root with four children.
func TestFindLeaf_OneLevel_MultipleChildren(t *testing.T) {
	pm := newMockPM()
	leaf2 := leafPage(2, []uint64{50})
	leaf3 := leafPage(3, []uint64{150})
	leaf4 := leafPage(4, []uint64{250})
	leaf5 := leafPage(5, []uint64{350})
	pm.pages[2] = leaf2
	pm.pages[3] = leaf3
	pm.pages[4] = leaf4
	pm.pages[5] = leaf5

	root := internalPage(1, []uint64{100, 200, 300}, []uint32{2, 3, 4}, 5)
	bt := NewBTree(pm)

	tests := []struct {
		key  uint64
		want *pagemanager.Page
		name string
	}{
		{50, leaf2, "below all seps"},
		{100, leaf2, "equal first sep"},
		{150, leaf3, "between first and second sep"},
		{200, leaf3, "equal second sep"},
		{250, leaf4, "between second and third sep"},
		{300, leaf4, "equal third sep"},
		{350, leaf5, "above all seps"},
		{9999, leaf5, "far above all seps"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := bt.findLeaf(tt.key, root)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("key=%d: got wrong leaf page", tt.key)
			}
		})
	}
}

// TestFindLeaf_OneLevel_NoSeparators_AlwaysRightmost tests an internal page with no records.
func TestFindLeaf_OneLevel_NoSeparators_AlwaysRightmost(t *testing.T) {
	pm := newMockPM()
	leaf5 := leafPage(5, []uint64{10, 20, 30})
	pm.pages[5] = leaf5

	root := internalPage(1, nil, nil, 5)
	bt := NewBTree(pm)

	for _, key := range []uint64{0, 1, 50, 1000, ^uint64(0)} {
		got, err := bt.findLeaf(key, root)
		if err != nil {
			t.Fatalf("key=%d: unexpected error: %v", key, err)
		}
		if got != leaf5 {
			t.Errorf("key=%d: expected rightmost-only leaf (page 5)", key)
		}
	}
}

// TestFindLeaf_TwoLevels_RoutesToCorrectLeaf verifies correct traversal across three levels.
//
// Tree layout:
//
//	        [sep=100, child=2, rm=3]   (page 1, root)
//	       /                            \
//	[sep=40,child=4,rm=5] (page 2)  [sep=150,child=6,rm=7] (page 3)
//	/            \                  /                \
//
// [10,20](4)  [50,70](5)     [110,130](6)     [160,180](7)
func TestFindLeaf_TwoLevels_RoutesToCorrectLeaf(t *testing.T) {
	pm := newMockPM()
	leaf4 := leafPage(4, []uint64{10, 20})
	leaf5 := leafPage(5, []uint64{50, 70})
	leaf6 := leafPage(6, []uint64{110, 130})
	leaf7 := leafPage(7, []uint64{160, 180})

	pm.pages[2] = internalPage(2, []uint64{40}, []uint32{4}, 5)
	pm.pages[3] = internalPage(3, []uint64{150}, []uint32{6}, 7)
	pm.pages[4] = leaf4
	pm.pages[5] = leaf5
	pm.pages[6] = leaf6
	pm.pages[7] = leaf7

	root := internalPage(1, []uint64{100}, []uint32{2}, 3)
	bt := NewBTree(pm)

	tests := []struct {
		key  uint64
		want *pagemanager.Page
		name string
	}{
		{10, leaf4, "leftmost leaf"},
		{40, leaf4, "equal left-subtree separator, routes left"},
		{60, leaf5, "right of left-subtree separator"},
		{100, leaf5, "equal root sep, enters left subtree then routes right"},
		{110, leaf6, "enters right subtree, routes left"},
		{150, leaf6, "equal right-subtree separator, routes left"},
		{160, leaf7, "right of right-subtree separator"},
		{200, leaf7, "above all, rightmost leaf"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := bt.findLeaf(tt.key, root)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("key=%d: wrong leaf returned", tt.key)
			}
		})
	}
}

// TestFindLeaf_ThreeLevels_DeepTraversal verifies traversal through four levels of pages.
//
// Tree layout:
//
//	Page 1 (root):   sep=1000, child=2, rightmost=9
//	Page 2 (L2 int): sep=500,  child=3, rightmost=8
//	Page 3 (L1 int): sep=250,  child=4, rightmost=5
//	Page 4 (leaf):   [10, 100, 200]
//	Page 5 (leaf):   [300, 400]
//	Page 8 (leaf):   [600, 700]
//	Page 9 (leaf):   [1100, 1200]
func TestFindLeaf_ThreeLevels_DeepTraversal(t *testing.T) {
	pm := newMockPM()
	leaf4 := leafPage(4, []uint64{10, 100, 200})
	leaf5 := leafPage(5, []uint64{300, 400})
	leaf8 := leafPage(8, []uint64{600, 700})
	leaf9 := leafPage(9, []uint64{1100, 1200})

	pm.pages[3] = internalPage(3, []uint64{250}, []uint32{4}, 5)
	pm.pages[2] = internalPage(2, []uint64{500}, []uint32{3}, 8)
	pm.pages[4] = leaf4
	pm.pages[5] = leaf5
	pm.pages[8] = leaf8
	pm.pages[9] = leaf9

	root := internalPage(1, []uint64{1000}, []uint32{2}, 9)
	bt := NewBTree(pm)

	tests := []struct {
		key  uint64
		want *pagemanager.Page
		name string
	}{
		{10, leaf4, "deep left path"},
		{250, leaf4, "equal L1 sep, routes left"},
		{300, leaf5, "right of L1 sep"},
		{500, leaf5, "equal L2 sep, routes into L1 then right"},
		{600, leaf8, "right of L2 sep, skips L1 entirely"},
		{1000, leaf8, "equal root sep, traverses left subtree to its rightmost"},
		{1100, leaf9, "above root sep, rightmost leaf"},
		{9999, leaf9, "far above all, rightmost leaf"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := bt.findLeaf(tt.key, root)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("key=%d: wrong leaf returned", tt.key)
			}
		})
	}
}

// TestFindLeaf_ReadPageError_FirstLevel verifies error propagation when the first child read fails.
func TestFindLeaf_ReadPageError_FirstLevel(t *testing.T) {
	pm := newMockPM()
	pm.readErr[2] = fmt.Errorf("disk error on page 2")

	root := internalPage(1, []uint64{50}, []uint32{2}, 3)
	bt := NewBTree(pm)

	got, err := bt.findLeaf(30, root)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got != nil {
		t.Error("expected nil page on error, got non-nil")
	}
}

// TestFindLeaf_ReadPageError_DeepLevel verifies error propagation when a read fails deep in the tree.
func TestFindLeaf_ReadPageError_DeepLevel(t *testing.T) {
	pm := newMockPM()
	pm.pages[2] = internalPage(2, []uint64{25}, []uint32{4}, 5)
	pm.readErr[4] = fmt.Errorf("disk error on page 4")

	root := internalPage(1, []uint64{100}, []uint32{2}, 3)
	bt := NewBTree(pm)

	_, err := bt.findLeaf(10, root)
	if err == nil {
		t.Fatal("expected error to propagate from deep level")
	}
}

// TestFindLeaf_MissingPage_ReturnsError verifies an error is returned when the target page is absent.
func TestFindLeaf_MissingPage_ReturnsError(t *testing.T) {
	pm := newMockPM()
	// page 2 is intentionally absent from pm.pages
	root := internalPage(1, []uint64{50}, []uint32{2}, 3)
	bt := NewBTree(pm)

	_, err := bt.findLeaf(10, root)
	if err == nil {
		t.Fatal("expected error for missing page, got nil")
	}
}

// TestFindLeaf_KeyZero_RoutesLeft verifies key=0 routes to the leftmost child.
func TestFindLeaf_KeyZero_RoutesLeft(t *testing.T) {
	pm := newMockPM()
	leaf2 := leafPage(2, []uint64{0, 1, 5})
	leaf3 := leafPage(3, []uint64{100, 200})
	pm.pages[2] = leaf2
	pm.pages[3] = leaf3

	root := internalPage(1, []uint64{50}, []uint32{2}, 3)
	bt := NewBTree(pm)

	got, err := bt.findLeaf(0, root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != leaf2 {
		t.Error("key=0: expected leftmost leaf (page 2)")
	}
}

// TestFindLeaf_MaxUint64_RoutesRightmost verifies key=MaxUint64 routes to the rightmost child.
func TestFindLeaf_MaxUint64_RoutesRightmost(t *testing.T) {
	const maxKey = ^uint64(0)
	pm := newMockPM()
	leaf2 := leafPage(2, []uint64{10})
	leaf3 := leafPage(3, []uint64{maxKey})
	pm.pages[2] = leaf2
	pm.pages[3] = leaf3

	root := internalPage(1, []uint64{100}, []uint32{2}, 3)
	bt := NewBTree(pm)

	got, err := bt.findLeaf(maxKey, root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != leaf3 {
		t.Error("key=MaxUint64: expected rightmost leaf (page 3)")
	}
}

// TestFindLeaf_MaxUint64_AsSeparator verifies routing when MaxUint64 is itself a separator.
func TestFindLeaf_MaxUint64_AsSeparator(t *testing.T) {
	const maxKey = ^uint64(0)
	pm := newMockPM()
	leaf2 := leafPage(2, []uint64{50, maxKey})
	leaf3 := leafPage(3, nil)
	pm.pages[2] = leaf2
	pm.pages[3] = leaf3

	root := internalPage(1, []uint64{maxKey}, []uint32{2}, 3)
	bt := NewBTree(pm)

	got, err := bt.findLeaf(maxKey, root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != leaf2 {
		t.Error("key=MaxUint64 equal to separator: expected left leaf (page 2)")
	}
}

// TestFindLeaf_AlwaysReturnsLeafPage guarantees the returned page type is always PageTypeLeaf.
func TestFindLeaf_AlwaysReturnsLeafPage(t *testing.T) {
	pm := newMockPM()
	leaf2 := leafPage(2, []uint64{10, 20})
	leaf3 := leafPage(3, []uint64{60, 70})
	pm.pages[2] = leaf2
	pm.pages[3] = leaf3

	root := internalPage(1, []uint64{50}, []uint32{2}, 3)
	bt := NewBTree(pm)

	for _, key := range []uint64{0, 10, 30, 50, 55, 70, 9999} {
		got, err := bt.findLeaf(key, root)
		if err != nil {
			t.Fatalf("key=%d: unexpected error: %v", key, err)
		}
		if got.GetPageType() != pagemanager.PageTypeLeaf {
			t.Errorf("key=%d: returned page type %d, want PageTypeLeaf", key, got.GetPageType())
		}
	}
}

// TestFindLeaf_AllLeavesReachable ensures every leaf in the tree can be reached by some key.
func TestFindLeaf_AllLeavesReachable(t *testing.T) {
	pm := newMockPM()
	leaf2 := leafPage(2, []uint64{50})
	leaf3 := leafPage(3, []uint64{150})
	leaf4 := leafPage(4, []uint64{250})
	leaf5 := leafPage(5, []uint64{350})
	pm.pages[2] = leaf2
	pm.pages[3] = leaf3
	pm.pages[4] = leaf4
	pm.pages[5] = leaf5

	root := internalPage(1, []uint64{100, 200, 300}, []uint32{2, 3, 4}, 5)
	bt := NewBTree(pm)

	tests := []struct {
		key  uint64
		want *pagemanager.Page
	}{
		{50, leaf2}, {100, leaf2},
		{150, leaf3}, {200, leaf3},
		{250, leaf4}, {300, leaf4},
		{350, leaf5}, {999, leaf5},
	}

	reached := make(map[*pagemanager.Page]bool)
	for _, tt := range tests {
		got, err := bt.findLeaf(tt.key, root)
		if err != nil {
			t.Fatalf("key=%d: unexpected error: %v", tt.key, err)
		}
		if got != tt.want {
			t.Errorf("key=%d: wrong leaf returned", tt.key)
		}
		reached[got] = true
	}

	for i, leaf := range []*pagemanager.Page{leaf2, leaf3, leaf4, leaf5} {
		if !reached[leaf] {
			t.Errorf("leaf page %d was never reached", i+2)
		}
	}
}

// ---- findLeafWithPath tests ----

// TestFindLeafWithPath_LeafPage_EmptyPath verifies a leaf root returns an empty (non-nil) path.
func TestFindLeafWithPath_LeafPage_EmptyPath(t *testing.T) {
	bt := NewBTree(newMockPM())
	root := leafPage(1, []uint64{10, 20, 30})

	leaf, path, err := bt.findLeafWithPath(20, root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if leaf != root {
		t.Error("expected root leaf page to be returned directly")
	}
	if path == nil {
		t.Error("path should be non-nil empty slice, got nil")
	}
	if len(path) != 0 {
		t.Errorf("leaf root: expected empty path, got %v", path)
	}
}

// TestFindLeafWithPath_OneLevel_LeftRoute verifies path contains the root's ID (direct parent of the leaf).
func TestFindLeafWithPath_OneLevel_LeftRoute(t *testing.T) {
	pm := newMockPM()
	leaf2 := leafPage(2, []uint64{10, 20, 40})
	leaf3 := leafPage(3, []uint64{60, 70, 80})
	pm.pages[2] = leaf2
	pm.pages[3] = leaf3

	root := internalPage(1, []uint64{50}, []uint32{2}, 3)
	bt := NewBTree(pm)

	leaf, path, err := bt.findLeafWithPath(30, root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if leaf != leaf2 {
		t.Error("expected left leaf (page 2)")
	}
	if len(path) != 1 || path[0] != 1 {
		t.Errorf("one-level left route: expected path=[1], got %v", path)
	}
}

// TestFindLeafWithPath_OneLevel_RightRoute verifies path contains the root's ID (direct parent of the leaf).
func TestFindLeafWithPath_OneLevel_RightRoute(t *testing.T) {
	pm := newMockPM()
	leaf2 := leafPage(2, []uint64{10, 20, 40})
	leaf3 := leafPage(3, []uint64{60, 70, 80})
	pm.pages[2] = leaf2
	pm.pages[3] = leaf3

	root := internalPage(1, []uint64{50}, []uint32{2}, 3)
	bt := NewBTree(pm)

	leaf, path, err := bt.findLeafWithPath(70, root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if leaf != leaf3 {
		t.Error("expected right leaf (page 3)")
	}
	if len(path) != 1 || path[0] != 1 {
		t.Errorf("one-level right route: expected path=[1], got %v", path)
	}
}

// TestFindLeafWithPath_TwoLevels_PathContainsIntermediateAndLeaf verifies a 3-level traversal
// produces a path of length 2: [rootID, internalID].
//
// Tree layout:
//
//	        [sep=100, child=2, rm=3]   (page 1, root)
//	       /                            \
//	[sep=40,child=4,rm=5] (page 2)  [sep=150,child=6,rm=7] (page 3)
//	/            \                  /                \
//
// [10,20](4)  [50,70](5)     [110,130](6)     [160,180](7)
func TestFindLeafWithPath_TwoLevels_PathContainsIntermediateAndLeaf(t *testing.T) {
	pm := newMockPM()
	leaf4 := leafPage(4, []uint64{10, 20})
	leaf5 := leafPage(5, []uint64{50, 70})
	leaf6 := leafPage(6, []uint64{110, 130})
	leaf7 := leafPage(7, []uint64{160, 180})

	pm.pages[2] = internalPage(2, []uint64{40}, []uint32{4}, 5)
	pm.pages[3] = internalPage(3, []uint64{150}, []uint32{6}, 7)
	pm.pages[4] = leaf4
	pm.pages[5] = leaf5
	pm.pages[6] = leaf6
	pm.pages[7] = leaf7

	root := internalPage(1, []uint64{100}, []uint32{2}, 3)
	bt := NewBTree(pm)

	tests := []struct {
		key      uint64
		wantLeaf *pagemanager.Page
		wantPath []uint32
		name     string
	}{
		{10, leaf4, []uint32{1, 2}, "leftmost leaf via page 2"},
		{60, leaf5, []uint32{1, 2}, "right of left subtree separator"},
		{100, leaf5, []uint32{1, 2}, "equal root sep, enters left subtree rightmost"},
		{110, leaf6, []uint32{1, 3}, "enters right subtree left child"},
		{200, leaf7, []uint32{1, 3}, "rightmost leaf via page 3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leaf, path, err := bt.findLeafWithPath(tt.key, root)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if leaf != tt.wantLeaf {
				t.Errorf("key=%d: wrong leaf returned", tt.key)
			}
			if len(path) != len(tt.wantPath) {
				t.Fatalf("key=%d: path length %d, want %d; path=%v", tt.key, len(path), len(tt.wantPath), path)
			}
			for i, id := range tt.wantPath {
				if path[i] != id {
					t.Errorf("key=%d: path[%d]=%d, want %d", tt.key, i, path[i], id)
				}
			}
		})
	}
}

// TestFindLeafWithPath_ThreeLevels_DeepPath verifies path length equals tree depth for deep trees.
//
// Tree: page1(root) → page2(L2 internal) → page3(L1 internal) → page4(leaf)
func TestFindLeafWithPath_ThreeLevels_DeepPath(t *testing.T) {
	pm := newMockPM()
	leaf4 := leafPage(4, []uint64{10, 100, 200})
	leaf5 := leafPage(5, []uint64{300, 400})
	leaf8 := leafPage(8, []uint64{600, 700})
	leaf9 := leafPage(9, []uint64{1100, 1200})

	pm.pages[3] = internalPage(3, []uint64{250}, []uint32{4}, 5)
	pm.pages[2] = internalPage(2, []uint64{500}, []uint32{3}, 8)
	pm.pages[4] = leaf4
	pm.pages[5] = leaf5
	pm.pages[8] = leaf8
	pm.pages[9] = leaf9

	root := internalPage(1, []uint64{1000}, []uint32{2}, 9)
	bt := NewBTree(pm)

	leaf, path, err := bt.findLeafWithPath(10, root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if leaf != leaf4 {
		t.Error("expected deepest left leaf (page 4)")
	}
	wantPath := []uint32{1, 2, 3}
	if len(path) != len(wantPath) {
		t.Fatalf("three-level path: want %v, got %v", wantPath, path)
	}
	for i, id := range wantPath {
		if path[i] != id {
			t.Errorf("path[%d]=%d, want %d", i, path[i], id)
		}
	}
}

// TestFindLeafWithPath_ReadPageError_NilPath verifies that on error the returned path is nil.
func TestFindLeafWithPath_ReadPageError_NilPath(t *testing.T) {
	pm := newMockPM()
	pm.readErr[2] = fmt.Errorf("disk error on page 2")

	root := internalPage(1, []uint64{50}, []uint32{2}, 3)
	bt := NewBTree(pm)

	leaf, path, err := bt.findLeafWithPath(30, root)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if leaf != nil {
		t.Error("expected nil leaf on error")
	}
	if path != nil {
		t.Errorf("expected nil path on error, got %v", path)
	}
}

// TestFindLeafWithPath_ReadPageError_DeepLevel verifies error propagation from a deep level.
func TestFindLeafWithPath_ReadPageError_DeepLevel(t *testing.T) {
	pm := newMockPM()
	pm.pages[2] = internalPage(2, []uint64{25}, []uint32{4}, 5)
	pm.readErr[4] = fmt.Errorf("disk error on page 4")

	root := internalPage(1, []uint64{100}, []uint32{2}, 3)
	bt := NewBTree(pm)

	_, path, err := bt.findLeafWithPath(10, root)
	if err == nil {
		t.Fatal("expected error from deep level, got nil")
	}
	if path != nil {
		t.Errorf("expected nil path on error, got %v", path)
	}
}

// TestFindLeafWithPath_NoSeparators_PathToRightmost verifies routing with an empty internal page.
func TestFindLeafWithPath_NoSeparators_PathToRightmost(t *testing.T) {
	pm := newMockPM()
	leaf5 := leafPage(5, []uint64{10, 20, 30})
	pm.pages[5] = leaf5

	root := internalPage(1, nil, nil, 5)
	bt := NewBTree(pm)

	for _, key := range []uint64{0, 1, 50, 1000, ^uint64(0)} {
		leaf, path, err := bt.findLeafWithPath(key, root)
		if err != nil {
			t.Fatalf("key=%d: unexpected error: %v", key, err)
		}
		if leaf != leaf5 {
			t.Errorf("key=%d: expected rightmost leaf (page 5)", key)
		}
		if len(path) != 1 || path[0] != 1 {
			t.Errorf("key=%d: expected path=[1], got %v", key, path)
		}
	}
}

// TestFindLeafWithPath_IndependentCallsDontShareState verifies separate calls return independent paths.
func TestFindLeafWithPath_IndependentCallsDontShareState(t *testing.T) {
	pm := newMockPM()
	leaf2 := leafPage(2, []uint64{10})
	leaf3 := leafPage(3, []uint64{100})
	pm.pages[2] = leaf2
	pm.pages[3] = leaf3

	root := internalPage(1, []uint64{50}, []uint32{2}, 3)
	bt := NewBTree(pm)

	_, path1, err := bt.findLeafWithPath(10, root)
	if err != nil {
		t.Fatalf("call 1 error: %v", err)
	}
	_, path2, err := bt.findLeafWithPath(100, root)
	if err != nil {
		t.Fatalf("call 2 error: %v", err)
	}

	if len(path1) != 1 || path1[0] != 1 {
		t.Errorf("call 1: expected path=[1], got %v", path1)
	}
	if len(path2) != 1 || path2[0] != 1 {
		t.Errorf("call 2: expected path=[1], got %v", path2)
	}
}

// =====================================
// Search end-to-end tests
// =====================================

// searchMockPM wraps mockPM with a configurable root page ID for Search tests.
type searchMockPM struct {
	*mockPM
	rootId uint32
}

func (s *searchMockPM) GetRootPageId() uint32 { return s.rootId }

func newSearchPM(rootId uint32) *searchMockPM {
	return &searchMockPM{mockPM: newMockPM(), rootId: rootId}
}

// encodeLeafRec encodes a leaf record or fatals the test.
func encodeLeafRec(t *testing.T, key uint64, fields []Field) []byte {
	t.Helper()
	rec, err := EncodeLeafRecord(key, fields)
	if err != nil {
		t.Fatalf("EncodeLeafRecord(key=%d): %v", key, err)
	}
	return rec
}

// leafPageF builds a leaf page pre-populated with properly encoded field records.
// Records must be provided in ascending key order (binary search relies on this).
func leafPageF(id uint32, records [][]byte) *pagemanager.Page {
	p := pagemanager.NewLeafPage(id, pagemanager.InvalidPageID, pagemanager.InvalidPageID)
	for _, rec := range records {
		if _, ok := p.InsertRecord(rec); !ok {
			panic(fmt.Sprintf("InsertRecord failed on page %d", id))
		}
	}
	return p
}

// strF / intF / nullF are shorthand Field constructors.
func strF(tag uint8, v string) Field { return Field{Tag: tag, Value: StringValue{V: v}} }
func intF(tag uint8, v int64) Field  { return Field{Tag: tag, Value: IntValue{V: v}} }
func nullF(tag uint8) Field          { return Field{Tag: tag, Value: NullValue{}} }

// assertFields verifies that got matches want field-by-field.
// Values are compared via fmt.Sprintf to handle non-comparable types (e.g. ListValue).
func assertFields(t *testing.T, got, want []Field) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("field count: got %d, want %d", len(got), len(want))
	}
	for i, wf := range want {
		gf := got[i]
		if gf.Tag != wf.Tag {
			t.Errorf("field[%d].Tag: got %d, want %d", i, gf.Tag, wf.Tag)
		}
		if fmt.Sprintf("%v", gf.Value) != fmt.Sprintf("%v", wf.Value) {
			t.Errorf("field[%d].Value: got %v, want %v", i, gf.Value, wf.Value)
		}
	}
}

// ---- Basic structural tests ----

// TestSearch_EmptyTree returns an error when there is no root page.
func TestSearch_EmptyTree(t *testing.T) {
	bt := NewBTree(newSearchPM(pagemanager.InvalidPageID))
	_, found, err := bt.Search(42)
	if err != nil {
		t.Fatal("did not expect error for empty tree")
	}
	if found {
		t.Error("expected found=false for empty tree")
	}
}

// TestSearch_RootReadError propagates an I/O error when the root page cannot be read.
func TestSearch_RootReadError(t *testing.T) {
	pm := newSearchPM(1)
	pm.readErr[1] = fmt.Errorf("disk failure on root")
	_, _, err := NewBTree(pm).Search(42)
	if err == nil {
		t.Fatal("expected error when root page read fails")
	}
}

// TestSearch_SingleLeafPage_Hit finds the only record in a one-page tree.
func TestSearch_SingleLeafPage_Hit(t *testing.T) {
	pm := newSearchPM(1)
	want := []Field{strF(1, "hello"), intF(2, 99)}
	pm.pages[1] = leafPageF(1, [][]byte{encodeLeafRec(t, 100, want)})

	got, found, err := NewBTree(pm).Search(100)
	if err != nil || !found {
		t.Fatalf("expected (fields, true, nil), got (_, %v, %v)", found, err)
	}
	assertFields(t, got, want)
}

// TestSearch_SingleLeafPage_Miss returns (nil, false, nil) for a key not in the tree.
func TestSearch_SingleLeafPage_Miss(t *testing.T) {
	pm := newSearchPM(1)
	pm.pages[1] = leafPageF(1, [][]byte{encodeLeafRec(t, 100, []Field{strF(1, "v")})})

	for _, miss := range []uint64{1, 99, 101, 9999} {
		_, found, err := NewBTree(pm).Search(miss)
		if err != nil || found {
			t.Errorf("key=%d: expected miss, got found=%v err=%v", miss, found, err)
		}
	}
}

// TestSearch_LeafReadError propagates an I/O error when a leaf page cannot be read.
func TestSearch_LeafReadError(t *testing.T) {
	pm := newSearchPM(1)
	pm.pages[1] = internalPage(1, []uint64{50}, []uint32{2}, 3)
	pm.readErr[2] = fmt.Errorf("disk failure on leaf page 2")
	_, _, err := NewBTree(pm).Search(30)
	if err == nil {
		t.Fatal("expected error when leaf page read fails")
	}
}

// TestSearch_NoFields_KeyOnlyRecord finds a record with no value fields.
func TestSearch_NoFields_KeyOnlyRecord(t *testing.T) {
	pm := newSearchPM(1)
	pm.pages[1] = leafPageF(1, [][]byte{encodeLeafRec(t, 42, nil)})

	fields, found, err := NewBTree(pm).Search(42)
	if err != nil || !found {
		t.Fatalf("expected ([], true, nil), got (_, %v, %v)", found, err)
	}
	if len(fields) != 0 {
		t.Errorf("expected no fields, got %d", len(fields))
	}
}

// TestSearch_CorruptRecord_ReturnsError returns an error when the value bytes are malformed.
func TestSearch_CorruptRecord_ReturnsError(t *testing.T) {
	pm := newSearchPM(1)
	// tag=1, type=int (0x01), but zero payload bytes follow — truncated int field.
	// searchLeaf returns the raw bytes; DecodeLeafRecord then rejects the 2 unparseable trailing bytes.
	corrupt := makeRecord(42, []byte{0x01, 0x01})
	pm.pages[1] = leafPageF(1, [][]byte{corrupt})

	_, _, err := NewBTree(pm).Search(42)
	if err == nil {
		t.Fatal("expected error for corrupt record, got nil")
	}
}

// ---- User Registry ----
// Schema: key=userID | tag1=username(string) | tag2=email(string) | tag3=age(int) | tag4=role(string)

// TestSearch_UserRegistry_LookupByID finds each user in a single-leaf tree and verifies all fields.
func TestSearch_UserRegistry_LookupByID(t *testing.T) {
	type user struct {
		id    uint64
		name  string
		email string
		age   int64
		role  string
	}
	users := []user{
		{1001, "Alice Chen", "alice@example.com", 28, "admin"},
		{1002, "Bob Smith", "bob@example.com", 35, "user"},
		{1003, "Carol White", "carol@example.com", 42, "moderator"},
		{1004, "Dave Jones", "dave@example.com", 31, "user"},
		{1005, "Eve Brown", "eve@example.com", 22, "user"},
	}

	pm := newSearchPM(1)
	var recs [][]byte
	for _, u := range users {
		recs = append(recs, encodeLeafRec(t, u.id, []Field{
			strF(1, u.name), strF(2, u.email), intF(3, u.age), strF(4, u.role),
		}))
	}
	pm.pages[1] = leafPageF(1, recs)
	bt := NewBTree(pm)

	for _, u := range users {
		t.Run(u.name, func(t *testing.T) {
			fields, found, err := bt.Search(u.id)
			if err != nil || !found {
				t.Fatalf("unexpected (found=%v, err=%v)", found, err)
			}
			assertFields(t, fields, []Field{
				strF(1, u.name), strF(2, u.email), intF(3, u.age), strF(4, u.role),
			})
		})
	}
}

// TestSearch_UserRegistry_UnknownUser returns not-found for IDs never inserted.
func TestSearch_UserRegistry_UnknownUser(t *testing.T) {
	pm := newSearchPM(1)
	recs := [][]byte{
		encodeLeafRec(t, 1001, []Field{strF(1, "Alice")}),
		encodeLeafRec(t, 1002, []Field{strF(1, "Bob")}),
		encodeLeafRec(t, 1003, []Field{strF(1, "Carol")}),
	}
	pm.pages[1] = leafPageF(1, recs)
	bt := NewBTree(pm)

	for _, missingID := range []uint64{1000, 1004, 9999} {
		_, found, err := bt.Search(missingID)
		if err != nil || found {
			t.Errorf("user %d: expected miss, got found=%v err=%v", missingID, found, err)
		}
	}
}

// TestSearch_UserRegistry_MultiLevel finds users spread across a two-level tree.
//
// Tree layout:
//
//	Root (page 1): sep=2000, child=2, rightmost=3
//	Leaf (page 2): users with ID ≤ 2000 (IDs: 1001, 1250, 1999, 2000)
//	Leaf (page 3): users with ID > 2000 (IDs: 2500, 2800, 2999)
//
// key=2000 equals the separator, so lower-bound routing sends it left to page 2.
func TestSearch_UserRegistry_MultiLevel(t *testing.T) {
	pm := newSearchPM(1)

	leaf2Recs := [][]byte{
		encodeLeafRec(t, 1001, []Field{strF(1, "Alice Chen"), strF(2, "alice@example.com"), intF(3, 28), strF(4, "admin")}),
		encodeLeafRec(t, 1250, []Field{strF(1, "Dave Jones"), strF(2, "dave@example.com"), intF(3, 31), strF(4, "user")}),
		encodeLeafRec(t, 1999, []Field{strF(1, "Grace Park"), strF(2, "grace@example.com"), intF(3, 19), strF(4, "user")}),
		encodeLeafRec(t, 2000, []Field{strF(1, "Henry Liu"), strF(2, "henry@example.com"), intF(3, 45), strF(4, "moderator")}),
	}
	leaf3Recs := [][]byte{
		encodeLeafRec(t, 2500, []Field{strF(1, "Iris Wang"), strF(2, "iris@example.com"), intF(3, 27), strF(4, "user")}),
		encodeLeafRec(t, 2800, []Field{strF(1, "Felix Müller"), strF(2, "felix@example.com"), intF(3, 38), strF(4, "user")}),
		encodeLeafRec(t, 2999, []Field{strF(1, "Jake Kim"), strF(2, "jake@example.com"), intF(3, 33), strF(4, "admin")}),
	}
	pm.pages[1] = internalPage(1, []uint64{2000}, []uint32{2}, 3)
	pm.pages[2] = leafPageF(2, leaf2Recs)
	pm.pages[3] = leafPageF(3, leaf3Recs)
	bt := NewBTree(pm)

	hits := []struct {
		id   uint64
		name string
		role string
	}{
		{1001, "Alice Chen", "admin"},
		{1250, "Dave Jones", "user"},
		{1999, "Grace Park", "user"},
		{2000, "Henry Liu", "moderator"}, // equal-separator: routes left
		{2500, "Iris Wang", "user"},
		{2800, "Felix Müller", "user"},
		{2999, "Jake Kim", "admin"},
	}
	for _, tt := range hits {
		t.Run(fmt.Sprintf("user_%d", tt.id), func(t *testing.T) {
			fields, found, err := bt.Search(tt.id)
			if err != nil || !found {
				t.Fatalf("expected found, got found=%v err=%v", found, err)
			}
			if sv, ok := fields[0].Value.(StringValue); !ok || sv.V != tt.name {
				t.Errorf("name: got %v, want %q", fields[0].Value, tt.name)
			}
			if sv, ok := fields[3].Value.(StringValue); !ok || sv.V != tt.role {
				t.Errorf("role: got %v, want %q", fields[3].Value, tt.role)
			}
		})
	}

	for _, missID := range []uint64{1000, 2001, 3000} {
		_, found, err := bt.Search(missID)
		if err != nil || found {
			t.Errorf("miss %d: expected not found, got found=%v err=%v", missID, found, err)
		}
	}
}

// TestSearch_UserRegistry_NullOptionalField finds a user whose optional field is null.
// Represents a user with no assigned role (role tag is null).
func TestSearch_UserRegistry_NullOptionalField(t *testing.T) {
	pm := newSearchPM(1)
	want := []Field{strF(1, "Anon User"), strF(2, "anon@example.com"), intF(3, 0), nullF(4)}
	pm.pages[1] = leafPageF(1, [][]byte{encodeLeafRec(t, 5000, want)})

	fields, found, err := NewBTree(pm).Search(5000)
	if err != nil || !found {
		t.Fatalf("expected found, got found=%v err=%v", found, err)
	}
	assertFields(t, fields, want)
}

// ---- Product Catalog ----
// Schema: key=SKU | tag1=name(string) | tag2=price_cents(int) | tag3=category(string) | tag4=stock(int)

// TestSearch_ProductCatalog_FindBySKU finds each product by SKU and verifies all fields.
func TestSearch_ProductCatalog_FindBySKU(t *testing.T) {
	type product struct {
		sku      uint64
		name     string
		price    int64
		category string
		stock    int64
	}
	products := []product{
		{1001, "Wireless Mouse", 2999, "Electronics", 150},
		{1002, "USB-C Keyboard", 4999, "Electronics", 80},
		{1003, "HDMI Cable 2m", 899, "Accessories", 300},
		{1004, "Laptop Stand", 3499, "Accessories", 60},
		{1005, "Webcam HD", 5999, "Electronics", 45},
		{1006, "Desk Lamp", 1999, "Office", 200},
	}

	pm := newSearchPM(1)
	var recs [][]byte
	for _, p := range products {
		recs = append(recs, encodeLeafRec(t, p.sku, []Field{
			strF(1, p.name), intF(2, p.price), strF(3, p.category), intF(4, p.stock),
		}))
	}
	pm.pages[1] = leafPageF(1, recs)
	bt := NewBTree(pm)

	for _, p := range products {
		t.Run(p.name, func(t *testing.T) {
			fields, found, err := bt.Search(p.sku)
			if err != nil || !found {
				t.Fatalf("SKU %d: expected found, got found=%v err=%v", p.sku, found, err)
			}
			if sv, ok := fields[0].Value.(StringValue); !ok || sv.V != p.name {
				t.Errorf("name: got %v, want %q", fields[0].Value, p.name)
			}
			if iv, ok := fields[1].Value.(IntValue); !ok || iv.V != p.price {
				t.Errorf("price: got %v, want %d", fields[1].Value, p.price)
			}
			if sv, ok := fields[2].Value.(StringValue); !ok || sv.V != p.category {
				t.Errorf("category: got %v, want %q", fields[2].Value, p.category)
			}
			if iv, ok := fields[3].Value.(IntValue); !ok || iv.V != p.stock {
				t.Errorf("stock: got %v, want %d", fields[3].Value, p.stock)
			}
		})
	}
}

// TestSearch_ProductCatalog_Discontinued verifies that discontinued SKUs return not-found.
func TestSearch_ProductCatalog_Discontinued(t *testing.T) {
	pm := newSearchPM(1)
	recs := [][]byte{
		encodeLeafRec(t, 1001, []Field{strF(1, "Wireless Mouse"), intF(2, 2999)}),
		encodeLeafRec(t, 1003, []Field{strF(1, "HDMI Cable 2m"), intF(2, 899)}),
		encodeLeafRec(t, 1005, []Field{strF(1, "Webcam HD"), intF(2, 5999)}),
	}
	pm.pages[1] = leafPageF(1, recs)
	bt := NewBTree(pm)

	// SKUs 1002, 1004, 1006 were never inserted (discontinued)
	for _, discontinuedSKU := range []uint64{1002, 1004, 1006, 9999} {
		_, found, err := bt.Search(discontinuedSKU)
		if err != nil || found {
			t.Errorf("SKU %d: expected not found, got found=%v err=%v", discontinuedSKU, found, err)
		}
	}
}

// TestSearch_ProductCatalog_MultiLevel finds products from a two-level tree split by price tier.
//
// Tree layout:
//
//	Root (page 1): sep=3000, child=2, rightmost=3
//	Leaf (page 2): budget/mid-range items (SKU ≤ 3000)
//	Leaf (page 3): premium items (SKU > 3000)
//
// SKU=3000 equals the separator → routes left to page 2.
func TestSearch_ProductCatalog_MultiLevel(t *testing.T) {
	pm := newSearchPM(1)

	leaf2Recs := [][]byte{
		encodeLeafRec(t, 1001, []Field{strF(1, "USB Hub"), intF(2, 1499), strF(3, "Accessories")}),
		encodeLeafRec(t, 1500, []Field{strF(1, "Mousepad XL"), intF(2, 799), strF(3, "Accessories")}),
		encodeLeafRec(t, 2999, []Field{strF(1, "HDMI Switch"), intF(2, 2499), strF(3, "Electronics")}),
		encodeLeafRec(t, 3000, []Field{strF(1, "4K Monitor"), intF(2, 39999), strF(3, "Electronics")}),
	}
	leaf3Recs := [][]byte{
		encodeLeafRec(t, 4500, []Field{strF(1, "Ergonomic Chair"), intF(2, 59999), strF(3, "Furniture")}),
		encodeLeafRec(t, 5000, []Field{strF(1, "Standing Desk"), intF(2, 69999), strF(3, "Furniture")}),
		encodeLeafRec(t, 5999, []Field{strF(1, "Premium Headphones"), intF(2, 24999), strF(3, "Electronics")}),
	}
	pm.pages[1] = internalPage(1, []uint64{3000}, []uint32{2}, 3)
	pm.pages[2] = leafPageF(2, leaf2Recs)
	pm.pages[3] = leafPageF(3, leaf3Recs)
	bt := NewBTree(pm)

	tests := []struct {
		sku   uint64
		name  string
		price int64
	}{
		{1001, "USB Hub", 1499},
		{1500, "Mousepad XL", 799},
		{2999, "HDMI Switch", 2499},
		{3000, "4K Monitor", 39999}, // equal-separator: routes left
		{4500, "Ergonomic Chair", 59999},
		{5000, "Standing Desk", 69999},
		{5999, "Premium Headphones", 24999},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields, found, err := bt.Search(tt.sku)
			if err != nil || !found {
				t.Fatalf("SKU %d: expected found, got found=%v err=%v", tt.sku, found, err)
			}
			if sv, ok := fields[0].Value.(StringValue); !ok || sv.V != tt.name {
				t.Errorf("name: got %v, want %q", fields[0].Value, tt.name)
			}
			if iv, ok := fields[1].Value.(IntValue); !ok || iv.V != tt.price {
				t.Errorf("price: got %v, want %d", fields[1].Value, tt.price)
			}
		})
	}

	for _, missID := range []uint64{999, 2000, 3001, 6000} {
		_, found, err := bt.Search(missID)
		if err != nil || found {
			t.Errorf("miss SKU %d: expected not found, got found=%v err=%v", missID, found, err)
		}
	}
}

// ---- Audit Event Log ----
// Schema: key=eventSeqID | tag1=event_type(string) | tag2=unix_ts(int) | tag3=source_ip(string) | tag4=user_id(int)
//
// Three-level tree layout:
//
//	Root (page 1):   sep=5000, child=2, rightmost=3
//	L2 left (page 2): sep=2500, child=4, rightmost=5
//	L2 right (page 3): sep=7500, child=6, rightmost=7
//	Leaf 4: seq ≤ 2500   Leaf 5: 2500 < seq ≤ 5000
//	Leaf 6: 5000 < seq ≤ 7500   Leaf 7: seq > 7500
//
// TestSearch_EventLog_ThreeLevelTree searches the full log and checks every routing path.
func TestSearch_EventLog_ThreeLevelTree(t *testing.T) {
	pm := newSearchPM(1)

	leaf4Recs := [][]byte{
		encodeLeafRec(t, 1001, []Field{strF(1, "LOGIN"), intF(2, 1700000001), strF(3, "192.168.1.10"), intF(4, 1001)}),
		encodeLeafRec(t, 2000, []Field{strF(1, "LOGOUT"), intF(2, 1700003600), strF(3, "192.168.1.10"), intF(4, 1001)}),
		encodeLeafRec(t, 2499, []Field{strF(1, "LOGIN"), intF(2, 1700007200), strF(3, "10.0.0.5"), intF(4, 1002)}),
		encodeLeafRec(t, 2500, []Field{strF(1, "PURCHASE"), intF(2, 1700010800), strF(3, "10.0.0.5"), intF(4, 1002)}),
	}
	leaf5Recs := [][]byte{
		encodeLeafRec(t, 2501, []Field{strF(1, "DOWNLOAD"), intF(2, 1700014400), strF(3, "172.16.0.3"), intF(4, 1003)}),
		encodeLeafRec(t, 3750, []Field{strF(1, "UPLOAD"), intF(2, 1700018000), strF(3, "172.16.0.3"), intF(4, 1003)}),
		encodeLeafRec(t, 4999, []Field{strF(1, "DELETE"), intF(2, 1700021600), strF(3, "172.16.0.3"), intF(4, 1003)}),
		encodeLeafRec(t, 5000, []Field{strF(1, "ADMIN_ACTION"), intF(2, 1700025200), strF(3, "10.10.0.1"), intF(4, 9999)}),
	}
	leaf6Recs := [][]byte{
		encodeLeafRec(t, 5001, []Field{strF(1, "PASSWORD_CHANGE"), intF(2, 1700028800), strF(3, "192.168.2.20"), intF(4, 1004)}),
		encodeLeafRec(t, 6500, []Field{strF(1, "FAILED_LOGIN"), intF(2, 1700032400), strF(3, "203.0.113.99"), intF(4, 0)}),
		encodeLeafRec(t, 7499, []Field{strF(1, "MFA_ENABLED"), intF(2, 1700036000), strF(3, "192.168.2.20"), intF(4, 1004)}),
		encodeLeafRec(t, 7500, []Field{strF(1, "API_CALL"), intF(2, 1700039600), strF(3, "10.0.1.15"), intF(4, 1005)}),
	}
	leaf7Recs := [][]byte{
		encodeLeafRec(t, 7501, []Field{strF(1, "EXPORT"), intF(2, 1700043200), strF(3, "10.0.1.15"), intF(4, 1005)}),
		encodeLeafRec(t, 9000, []Field{strF(1, "BACKUP"), intF(2, 1700046800), strF(3, "10.0.1.15"), intF(4, 1005)}),
		encodeLeafRec(t, 9999, []Field{strF(1, "SYSTEM_SHUTDOWN"), intF(2, 1700050400), strF(3, "10.0.1.15"), intF(4, 1005)}),
	}

	pm.pages[1] = internalPage(1, []uint64{5000}, []uint32{2}, 3)
	pm.pages[2] = internalPage(2, []uint64{2500}, []uint32{4}, 5)
	pm.pages[3] = internalPage(3, []uint64{7500}, []uint32{6}, 7)
	pm.pages[4] = leafPageF(4, leaf4Recs)
	pm.pages[5] = leafPageF(5, leaf5Recs)
	pm.pages[6] = leafPageF(6, leaf6Recs)
	pm.pages[7] = leafPageF(7, leaf7Recs)
	bt := NewBTree(pm)

	hits := []struct {
		seqID     uint64
		eventType string
		userID    int64
	}{
		{1001, "LOGIN", 1001},           // leaf 4, leftmost
		{2000, "LOGOUT", 1001},          // leaf 4, middle
		{2499, "LOGIN", 1002},           // leaf 4, just below sep(page2)
		{2500, "PURCHASE", 1002},        // leaf 4, equal to sep(page2) → routes left
		{2501, "DOWNLOAD", 1003},        // leaf 5, just above sep(page2)
		{3750, "UPLOAD", 1003},          // leaf 5, middle
		{4999, "DELETE", 1003},          // leaf 5, just below sep(root)
		{5000, "ADMIN_ACTION", 9999},    // leaf 5, equal to sep(root) → routes left-subtree, rightmost
		{5001, "PASSWORD_CHANGE", 1004}, // leaf 6, just above sep(root)
		{6500, "FAILED_LOGIN", 0},       // leaf 6, middle (anonymous attacker)
		{7499, "MFA_ENABLED", 1004},     // leaf 6, just below sep(page3)
		{7500, "API_CALL", 1005},        // leaf 6, equal to sep(page3) → routes left
		{7501, "EXPORT", 1005},          // leaf 7, just above sep(page3)
		{9000, "BACKUP", 1005},          // leaf 7, middle
		{9999, "SYSTEM_SHUTDOWN", 1005}, // leaf 7, rightmost
	}
	for _, tt := range hits {
		t.Run(fmt.Sprintf("event_%d_%s", tt.seqID, tt.eventType), func(t *testing.T) {
			fields, found, err := bt.Search(tt.seqID)
			if err != nil || !found {
				t.Fatalf("expected found, got found=%v err=%v", found, err)
			}
			if sv, ok := fields[0].Value.(StringValue); !ok || sv.V != tt.eventType {
				t.Errorf("event_type: got %v, want %q", fields[0].Value, tt.eventType)
			}
			if iv, ok := fields[3].Value.(IntValue); !ok || iv.V != tt.userID {
				t.Errorf("user_id: got %v, want %d", fields[3].Value, tt.userID)
			}
		})
	}

	// Gaps that were never inserted
	for _, missID := range []uint64{1000, 2502, 5002, 7502, 10000} {
		_, found, err := bt.Search(missID)
		if err != nil || found {
			t.Errorf("miss %d: expected not found, got found=%v err=%v", missID, found, err)
		}
	}
}

// ---- DNS Zone Records ----
// Schema: key=recordID | tag1=hostname(string) | tag2=record_type(string) | tag3=ttl(int) | tag4=ip_addresses(list<string>)

// TestSearch_DNSRecord_ListField finds a DNS record and verifies the list-of-IPs field.
func TestSearch_DNSRecord_ListField(t *testing.T) {
	pm := newSearchPM(1)

	dnsRecords := []struct {
		id         uint64
		hostname   string
		recordType string
		ttl        int64
		ips        []string
	}{
		{101, "example.com", "A", 300, []string{"93.184.216.34"}},
		{102, "www.example.com", "A", 300, []string{"93.184.216.34", "93.184.216.35"}},
		{103, "mail.example.com", "A", 3600, []string{"198.51.100.1", "198.51.100.2", "198.51.100.3"}},
		{104, "cdn.example.com", "CNAME", 60, []string{}},
	}

	var recs [][]byte
	for _, dr := range dnsRecords {
		elems := make([]Value, len(dr.ips))
		for i, ip := range dr.ips {
			elems[i] = StringValue{V: ip}
		}
		recs = append(recs, encodeLeafRec(t, dr.id, []Field{
			strF(1, dr.hostname),
			strF(2, dr.recordType),
			intF(3, dr.ttl),
			{Tag: 4, Value: ListValue{ElemType: FieldTypeString, Elems: elems}},
		}))
	}
	pm.pages[1] = leafPageF(1, recs)
	bt := NewBTree(pm)

	// Verify multi-IP record (example.com with round-robin)
	fields, found, err := bt.Search(102)
	if err != nil || !found {
		t.Fatalf("record 102: expected found, got found=%v err=%v", found, err)
	}
	if sv, ok := fields[0].Value.(StringValue); !ok || sv.V != "www.example.com" {
		t.Errorf("hostname: got %v, want www.example.com", fields[0].Value)
	}
	ipList, ok := fields[3].Value.(ListValue)
	if !ok {
		t.Fatal("tag4 is not a ListValue")
	}
	wantIPs := []string{"93.184.216.34", "93.184.216.35"}
	if len(ipList.Elems) != len(wantIPs) {
		t.Fatalf("ip count: got %d, want %d", len(ipList.Elems), len(wantIPs))
	}
	for i, wip := range wantIPs {
		if sv, ok := ipList.Elems[i].(StringValue); !ok || sv.V != wip {
			t.Errorf("ip[%d]: got %v, want %q", i, ipList.Elems[i], wip)
		}
	}

	// Verify single-IP record
	fields, found, err = bt.Search(101)
	if err != nil || !found {
		t.Fatalf("record 101: expected found, got found=%v err=%v", found, err)
	}
	ipList, ok = fields[3].Value.(ListValue)
	if !ok || len(ipList.Elems) != 1 {
		t.Fatalf("record 101: expected 1-element list, got %v", fields[3].Value)
	}
	if sv, ok := ipList.Elems[0].(StringValue); !ok || sv.V != "93.184.216.34" {
		t.Errorf("record 101 ip: got %v, want 93.184.216.34", ipList.Elems[0])
	}

	// Verify three-IP record
	fields, found, err = bt.Search(103)
	if err != nil || !found {
		t.Fatalf("record 103: expected found, got found=%v err=%v", found, err)
	}
	ipList, ok = fields[3].Value.(ListValue)
	if !ok || len(ipList.Elems) != 3 {
		t.Fatalf("record 103: expected 3-element list, got %v", fields[3].Value)
	}
}

// ---- Active Session Store ----
// Schema: key=sessionID | tag1=user_id(int) | tag2=created_at(int) | tag3=expires_at(int) | tag4=user_agent(string)

// TestSearch_SessionStore_MultiLevel looks up active sessions from a two-level tree.
//
// Tree layout:
//
//	Root (page 1): sep=500000, child=2, rightmost=3
//	Leaf (page 2): session IDs ≤ 500000 (older sessions)
//	Leaf (page 3): session IDs > 500000 (recent sessions)
func TestSearch_SessionStore_MultiLevel(t *testing.T) {
	pm := newSearchPM(1)

	leaf2Recs := [][]byte{
		encodeLeafRec(t, 100001, []Field{intF(1, 1001), intF(2, 1699990000), intF(3, 1700076400), strF(4, "Mozilla/5.0 (Windows NT 10.0)")}),
		encodeLeafRec(t, 250000, []Field{intF(1, 1002), intF(2, 1699995000), intF(3, 1700081400), strF(4, "Mozilla/5.0 (Macintosh)")}),
		encodeLeafRec(t, 499999, []Field{intF(1, 1003), intF(2, 1700000000), intF(3, 1700086400), strF(4, "curl/7.68.0")}),
		encodeLeafRec(t, 500000, []Field{intF(1, 1004), intF(2, 1700001000), intF(3, 1700087400), strF(4, "Go-http-client/1.1")}),
	}
	leaf3Recs := [][]byte{
		encodeLeafRec(t, 500001, []Field{intF(1, 1005), intF(2, 1700002000), intF(3, 1700088400), strF(4, "python-requests/2.28")}),
		encodeLeafRec(t, 750000, []Field{intF(1, 1006), intF(2, 1700005000), intF(3, 1700091400), strF(4, "Mozilla/5.0 (iPhone)")}),
		encodeLeafRec(t, 999999, []Field{intF(1, 1007), intF(2, 1700009999), intF(3, 1700096399), strF(4, "Mozilla/5.0 (Android)")}),
	}
	pm.pages[1] = internalPage(1, []uint64{500000}, []uint32{2}, 3)
	pm.pages[2] = leafPageF(2, leaf2Recs)
	pm.pages[3] = leafPageF(3, leaf3Recs)
	bt := NewBTree(pm)

	tests := []struct {
		sessionID uint64
		userID    int64
		userAgent string
	}{
		{100001, 1001, "Mozilla/5.0 (Windows NT 10.0)"},
		{250000, 1002, "Mozilla/5.0 (Macintosh)"},
		{499999, 1003, "curl/7.68.0"},
		{500000, 1004, "Go-http-client/1.1"},   // equal-separator: routes left
		{500001, 1005, "python-requests/2.28"}, // just above separator: routes right
		{750000, 1006, "Mozilla/5.0 (iPhone)"},
		{999999, 1007, "Mozilla/5.0 (Android)"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("session_%d", tt.sessionID), func(t *testing.T) {
			fields, found, err := bt.Search(tt.sessionID)
			if err != nil || !found {
				t.Fatalf("session %d: expected found, got found=%v err=%v", tt.sessionID, found, err)
			}
			if iv, ok := fields[0].Value.(IntValue); !ok || iv.V != tt.userID {
				t.Errorf("user_id: got %v, want %d", fields[0].Value, tt.userID)
			}
			if sv, ok := fields[3].Value.(StringValue); !ok || sv.V != tt.userAgent {
				t.Errorf("user_agent: got %v, want %q", fields[3].Value, tt.userAgent)
			}
		})
	}

	// Expired/never-created sessions
	for _, missID := range []uint64{1, 100000, 500002, 1000000} {
		_, found, err := bt.Search(missID)
		if err != nil || found {
			t.Errorf("miss session %d: expected not found, got found=%v err=%v", missID, found, err)
		}
	}
}

// ---- Edge case: boundary keys ----

// TestSearch_KeyZero_Found verifies that key=0 (minimum uint64) is found correctly.
func TestSearch_KeyZero_Found(t *testing.T) {
	pm := newSearchPM(1)
	want := []Field{strF(1, "root-entry"), intF(2, 0)}
	pm.pages[1] = leafPageF(1, [][]byte{
		encodeLeafRec(t, 0, want),
		encodeLeafRec(t, 1, []Field{strF(1, "next")}),
		encodeLeafRec(t, 100, []Field{strF(1, "far")}),
	})

	fields, found, err := NewBTree(pm).Search(0)
	if err != nil || !found {
		t.Fatalf("key=0: expected found, got found=%v err=%v", found, err)
	}
	assertFields(t, fields, want)
}

// TestSearch_MaxUint64_Found verifies the maximum uint64 key is found and decoded correctly.
func TestSearch_MaxUint64_Found(t *testing.T) {
	const maxKey = ^uint64(0)
	pm := newSearchPM(1)
	want := []Field{strF(1, "sentinel"), intF(2, -1)}
	pm.pages[1] = leafPageF(1, [][]byte{
		encodeLeafRec(t, 1, []Field{strF(1, "first")}),
		encodeLeafRec(t, maxKey-1, []Field{strF(1, "penultimate")}),
		encodeLeafRec(t, maxKey, want),
	})

	fields, found, err := NewBTree(pm).Search(maxKey)
	if err != nil || !found {
		t.Fatalf("maxKey: expected found, got found=%v err=%v", found, err)
	}
	assertFields(t, fields, want)
}

// TestSearch_MaxUint64_MultiLevel routes MaxUint64 through a two-level tree to the rightmost leaf.
func TestSearch_MaxUint64_MultiLevel(t *testing.T) {
	const maxKey = ^uint64(0)
	pm := newSearchPM(1)
	want := []Field{strF(1, "overflow-guard"), intF(2, -2)}
	pm.pages[1] = internalPage(1, []uint64{1000}, []uint32{2}, 3)
	pm.pages[2] = leafPageF(2, [][]byte{encodeLeafRec(t, 500, []Field{strF(1, "left")})})
	pm.pages[3] = leafPageF(3, [][]byte{
		encodeLeafRec(t, 2000, []Field{strF(1, "right-low")}),
		encodeLeafRec(t, maxKey, want),
	})

	fields, found, err := NewBTree(pm).Search(maxKey)
	if err != nil || !found {
		t.Fatalf("maxKey multi-level: expected found, got found=%v err=%v", found, err)
	}
	assertFields(t, fields, want)
}

// =====================================
// findInsertPosition tests
// =====================================

// TestFindInsertPosition_EmptyPage returns 0 for any key on an empty page.
func TestFindInsertPosition_EmptyPage(t *testing.T) {
	page := newLeaf()
	for _, key := range []uint64{0, 1, 42, ^uint64(0)} {
		if got := findInsertPosition(key, page); got != 0 {
			t.Errorf("empty page key=%d: want 0, got %d", key, got)
		}
	}
}

// TestFindInsertPosition_SingleRecord_KeyLess inserts before the only record.
func TestFindInsertPosition_SingleRecord_KeyLess(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{50})
	if got := findInsertPosition(30, page); got != 0 {
		t.Errorf("key < only record: want 0, got %d", got)
	}
}

// TestFindInsertPosition_SingleRecord_KeyEqual returns the slot of the existing record.
func TestFindInsertPosition_SingleRecord_KeyEqual(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{50})
	if got := findInsertPosition(50, page); got != 0 {
		t.Errorf("key == only record: want 0, got %d", got)
	}
}

// TestFindInsertPosition_SingleRecord_KeyGreater appends after the only record.
func TestFindInsertPosition_SingleRecord_KeyGreater(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{50})
	if got := findInsertPosition(75, page); got != 1 {
		t.Errorf("key > only record: want 1, got %d", got)
	}
}

// TestFindInsertPosition_BelowAll returns 0 when the key is less than every stored key.
func TestFindInsertPosition_BelowAll(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{10, 20, 30, 40, 50})
	if got := findInsertPosition(1, page); got != 0 {
		t.Errorf("below all: want 0, got %d", got)
	}
}

// TestFindInsertPosition_AboveAll returns rowCount when the key exceeds every stored key.
func TestFindInsertPosition_AboveAll(t *testing.T) {
	page := newLeaf()
	keys := []uint64{10, 20, 30, 40, 50}
	insertSorted(t, page, keys)
	if got := findInsertPosition(999, page); got != len(keys) {
		t.Errorf("above all: want %d, got %d", len(keys), got)
	}
}

// TestFindInsertPosition_InGaps verifies every gap between stored keys returns the correct slot.
func TestFindInsertPosition_InGaps(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{10, 20, 30, 40, 50})

	tests := []struct {
		key  uint64
		want int
	}{
		{11, 1}, // between 10 and 20 → insert before slot 1 (key=20)
		{15, 1},
		{19, 1},
		{21, 2}, // between 20 and 30 → insert before slot 2 (key=30)
		{25, 2},
		{29, 2},
		{31, 3},
		{39, 3},
		{41, 4},
		{49, 4},
	}
	for _, tt := range tests {
		got := findInsertPosition(tt.key, page)
		if got != tt.want {
			t.Errorf("key=%d: want %d, got %d", tt.key, tt.want, got)
		}
	}
}

// TestFindInsertPosition_ExactMatches returns the slot of each exact existing key.
func TestFindInsertPosition_ExactMatches(t *testing.T) {
	keys := []uint64{10, 20, 30, 40, 50}
	page := newLeaf()
	insertSorted(t, page, keys)

	for i, k := range keys {
		got := findInsertPosition(k, page)
		if got != i {
			t.Errorf("exact key=%d: want slot %d, got %d", k, i, got)
		}
	}
}

// TestFindInsertPosition_KeyZero places key=0 before all positive keys.
func TestFindInsertPosition_KeyZero(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{1, 2, 3})
	if got := findInsertPosition(0, page); got != 0 {
		t.Errorf("key=0: want 0, got %d", got)
	}
}

// TestFindInsertPosition_MaxUint64 appends at end when key is max uint64.
func TestFindInsertPosition_MaxUint64(t *testing.T) {
	const maxKey = ^uint64(0)
	page := newLeaf()
	insertSorted(t, page, []uint64{1, 100, 9999})
	if got := findInsertPosition(maxKey, page); got != 3 {
		t.Errorf("maxUint64: want 3, got %d", got)
	}
}

// TestFindInsertPosition_MaxUint64_ExistingKey returns the slot of maxUint64 when already present.
func TestFindInsertPosition_MaxUint64_ExistingKey(t *testing.T) {
	const maxKey = ^uint64(0)
	page := newLeaf()
	insertSorted(t, page, []uint64{1, 100, maxKey})
	if got := findInsertPosition(maxKey, page); got != 2 {
		t.Errorf("maxUint64 exists: want 2, got %d", got)
	}
}

// TestFindInsertPosition_DeletedSlot_ReturnsLow verifies the fallback when GetRecord returns false.
// Deleting the middle slot of a 3-record page and searching for its key returns 'low'
// (the lower bound of the contracted search window), not an out-of-bounds index.
func TestFindInsertPosition_DeletedSlot_ReturnsLow(t *testing.T) {
	page := newLeaf()
	insertSorted(t, page, []uint64{10, 20, 30})
	page.DeleteRecord(1) // slot 1 (key=20) is now unusable

	got := findInsertPosition(20, page)
	if got < 0 || got > 3 {
		t.Errorf("deleted slot: position %d out of valid range [0,3]", got)
	}
}

// TestFindInsertPosition_LargePage verifies correct positions across a 50-element page.
func TestFindInsertPosition_LargePage(t *testing.T) {
	page := newLeaf()
	var keys []uint64
	for i := uint64(0); i < 50; i++ {
		keys = append(keys, i*10) // 0, 10, 20, ..., 490
	}
	insertSorted(t, page, keys)

	for i, k := range keys {
		if got := findInsertPosition(k, page); got != i {
			t.Errorf("exact key=%d: want %d, got %d", k, i, got)
		}
	}

	for i := 0; i < 49; i++ {
		gapKey := keys[i] + 5
		if got := findInsertPosition(gapKey, page); got != i+1 {
			t.Errorf("gap key=%d: want %d, got %d", gapKey, i+1, got)
		}
	}

	if got := findInsertPosition(999, page); got != 50 {
		t.Errorf("above all: want 50, got %d", got)
	}
}

// =====================================
// splitLeaf tests
// =====================================

// splitPM extends mockPM with AllocatePage and WritePage support for splitLeaf tests.
type splitPM struct {
	*mockPM
	newPage  *pagemanager.Page
	allocErr error
	writeErr error
	written  []*pagemanager.Page
}

func newSplitPM(newPage *pagemanager.Page) *splitPM {
	return &splitPM{mockPM: newMockPM(), newPage: newPage}
}

func (s *splitPM) AllocatePage() (*pagemanager.Page, error) {
	if s.allocErr != nil {
		return nil, s.allocErr
	}
	return s.newPage, nil
}

func (s *splitPM) WritePage(p *pagemanager.Page) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	s.written = append(s.written, p)
	return nil
}

// fillLeaf creates a leaf page with the given IDs and inserts records for each key.
func fillLeaf(id, leftSib, rightSib uint32, keys []uint64) *pagemanager.Page {
	p := pagemanager.NewLeafPage(id, leftSib, rightSib)
	for _, k := range keys {
		if _, ok := p.InsertRecord(makeRecord(k, []byte("val"))); !ok {
			panic(fmt.Sprintf("fillLeaf: InsertRecord failed for key %d on page %d", k, id))
		}
	}
	return p
}

// pageKeys returns all keys stored in a page in slot order.
func pageKeys(p *pagemanager.Page) []uint64 {
	n := int(p.GetRowCount())
	keys := make([]uint64, 0, n)
	for i := range n {
		rec, ok := p.GetRecord(i)
		if !ok {
			continue
		}
		keys = append(keys, RecordKey(rec))
	}
	return keys
}

// isSortedAsc returns true if keys is strictly ascending.
func isSortedAsc(keys []uint64) bool {
	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			return false
		}
	}
	return true
}

// equalSlices returns true if two uint64 slices are identical.
func equalSlices(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---- Error propagation ----

// TestSplitLeaf_AllocateError verifies that an AllocatePage failure is propagated as an error.
func TestSplitLeaf_AllocateError(t *testing.T) {
	pm := newSplitPM(nil)
	pm.allocErr = fmt.Errorf("out of disk space")

	page := fillLeaf(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID, []uint64{10, 20, 30, 40})
	_, _, err := NewBTree(pm).splitLeaf(page, makeRecord(25, []byte("val")))
	if err == nil {
		t.Fatal("expected error from AllocatePage failure, got nil")
	}
}

// TestSplitLeaf_ReadSiblingError verifies that a ReadPage failure on the old right sibling propagates.
func TestSplitLeaf_ReadSiblingError(t *testing.T) {
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	pm.readErr[3] = fmt.Errorf("disk error on page 3")

	// page 1 has right sibling 3 — ReadPage(3) will fail
	page := fillLeaf(1, pagemanager.InvalidPageID, 3, []uint64{10, 20, 30, 40})
	_, _, err := NewBTree(pm).splitLeaf(page, makeRecord(25, []byte("val")))
	if err == nil {
		t.Fatal("expected error from ReadPage(rightSibling) failure, got nil")
	}
}

// TestSplitLeaf_WriteSiblingError verifies that a WritePage failure on the old right sibling propagates.
func TestSplitLeaf_WriteSiblingError(t *testing.T) {
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	pm.writeErr = fmt.Errorf("disk write failed")
	pm.pages[3] = pagemanager.NewLeafPage(3, 1, pagemanager.InvalidPageID)

	page := fillLeaf(1, pagemanager.InvalidPageID, 3, []uint64{10, 20, 30, 40})
	_, _, err := NewBTree(pm).splitLeaf(page, makeRecord(25, []byte("val")))
	if err == nil {
		t.Fatal("expected error from WritePage failure, got nil")
	}
}

// ---- Page type ----

// TestSplitLeaf_NewPageIsLeaf verifies the returned page has type PageTypeLeaf.
func TestSplitLeaf_NewPageIsLeaf(t *testing.T) {
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	page := fillLeaf(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID, []uint64{10, 20, 30, 40})

	got, _, err := NewBTree(pm).splitLeaf(page, makeRecord(25, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.GetPageType() != pagemanager.PageTypeLeaf {
		t.Errorf("new page type: got %d, want PageTypeLeaf", got.GetPageType())
	}
}

// ---- Record distribution ----

// TestSplitLeaf_FourPlusOne verifies the split with 4 existing + 1 new = 5 total records.
// splitPoint = 5/2 = 2: old gets [0,1], new gets [2,3,4].
func TestSplitLeaf_FourPlusOne(t *testing.T) {
	// all=[10,20,25,30,40], split=2 → old=[10,20], new=[25,30,40], sep=max(old)=20
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	page := fillLeaf(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID, []uint64{10, 20, 30, 40})

	got, sep, err := NewBTree(pm).splitLeaf(page, makeRecord(25, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !equalSlices(pageKeys(page), []uint64{10, 20}) {
		t.Errorf("old page keys: got %v, want [10 20]", pageKeys(page))
	}
	if !equalSlices(pageKeys(got), []uint64{25, 30, 40}) {
		t.Errorf("new page keys: got %v, want [25 30 40]", pageKeys(got))
	}
	if sep != 20 {
		t.Errorf("separator: got %d, want 20 (max of left page)", sep)
	}
}

// TestSplitLeaf_FivePlusOne verifies the split with 5 existing + 1 new = 6 total records.
// splitPoint = 6/2 = 3: old gets [0,1,2], new gets [3,4,5].
func TestSplitLeaf_FivePlusOne(t *testing.T) {
	// all=[10,20,30,40,50,60], split=3 → old=[10,20,30], new=[40,50,60], sep=30
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	page := fillLeaf(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID, []uint64{10, 20, 30, 50, 60})

	got, sep, err := NewBTree(pm).splitLeaf(page, makeRecord(40, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !equalSlices(pageKeys(page), []uint64{10, 20, 30}) {
		t.Errorf("old page keys: got %v, want [10 20 30]", pageKeys(page))
	}
	if !equalSlices(pageKeys(got), []uint64{40, 50, 60}) {
		t.Errorf("new page keys: got %v, want [40 50 60]", pageKeys(got))
	}
	if sep != 30 {
		t.Errorf("separator: got %d, want 30 (max of left page)", sep)
	}
}

// TestSplitLeaf_InsertAtStart verifies a split when the new record has the smallest key (insertPos=0).
// all=[10,20,30,40,50], split=2 → old=[10,20], new=[30,40,50], sep=max(old)=20.
func TestSplitLeaf_InsertAtStart(t *testing.T) {
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	page := fillLeaf(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID, []uint64{20, 30, 40, 50})

	got, sep, err := NewBTree(pm).splitLeaf(page, makeRecord(10, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !equalSlices(pageKeys(page), []uint64{10, 20}) {
		t.Errorf("old page keys: got %v, want [10 20]", pageKeys(page))
	}
	if !equalSlices(pageKeys(got), []uint64{30, 40, 50}) {
		t.Errorf("new page keys: got %v, want [30 40 50]", pageKeys(got))
	}
	if sep != 20 {
		t.Errorf("separator: got %d, want 20 (max of left page)", sep)
	}
}

// TestSplitLeaf_InsertAtEnd verifies a split when the new record has the largest key.
// all=[10,20,30,40,50], split=2 → old=[10,20], new=[30,40,50], sep=max(old)=20.
func TestSplitLeaf_InsertAtEnd(t *testing.T) {
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	page := fillLeaf(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID, []uint64{10, 20, 30, 40})

	got, sep, err := NewBTree(pm).splitLeaf(page, makeRecord(50, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !equalSlices(pageKeys(page), []uint64{10, 20}) {
		t.Errorf("old page keys: got %v, want [10 20]", pageKeys(page))
	}
	if !equalSlices(pageKeys(got), []uint64{30, 40, 50}) {
		t.Errorf("new page keys: got %v, want [30 40 50]", pageKeys(got))
	}
	if sep != 20 {
		t.Errorf("separator: got %d, want 20 (max of left page)", sep)
	}
}

// TestSplitLeaf_InsertAtSplitPoint verifies when the new record lands exactly at the split boundary.
// insertPos=2 == splitPoint: new record becomes the first key of the right page.
func TestSplitLeaf_InsertAtSplitPoint(t *testing.T) {
	// existing=[10,20,40,50], newKey=30 sorts to insertPos=2
	// all=[10,20,30,40,50], split=2 → old=[10,20], new=[30,40,50], sep=max(old)=20
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	page := fillLeaf(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID, []uint64{10, 20, 40, 50})

	got, sep, err := NewBTree(pm).splitLeaf(page, makeRecord(30, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !equalSlices(pageKeys(page), []uint64{10, 20}) {
		t.Errorf("old page keys: got %v, want [10 20]", pageKeys(page))
	}
	if !equalSlices(pageKeys(got), []uint64{30, 40, 50}) {
		t.Errorf("new page keys: got %v, want [30 40 50]", pageKeys(got))
	}
	if sep != 20 {
		t.Errorf("separator: got %d, want 20 (max of left page)", sep)
	}
}

// TestSplitLeaf_MinimalSplit handles the smallest possible split: 1 existing + 1 new = 2 total.
// splitPoint=1 → old=[min], new=[max], sep=max(old)=min.
func TestSplitLeaf_MinimalSplit(t *testing.T) {
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	page := fillLeaf(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID, []uint64{20})

	got, sep, err := NewBTree(pm).splitLeaf(page, makeRecord(10, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !equalSlices(pageKeys(page), []uint64{10}) {
		t.Errorf("old page keys: got %v, want [10]", pageKeys(page))
	}
	if !equalSlices(pageKeys(got), []uint64{20}) {
		t.Errorf("new page keys: got %v, want [20]", pageKeys(got))
	}
	if sep != 10 {
		t.Errorf("separator: got %d, want 10 (max of left page)", sep)
	}
}

// ---- Invariants ----

// TestSplitLeaf_AllKeysPreserved verifies no record is lost or duplicated across the two pages.
func TestSplitLeaf_AllKeysPreserved(t *testing.T) {
	existing := []uint64{10, 20, 30, 40}
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	page := fillLeaf(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID, existing)

	newKey := uint64(25)
	got, _, err := NewBTree(pm).splitLeaf(page, makeRecord(newKey, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	all := append(pageKeys(page), pageKeys(got)...)
	want := []uint64{10, 20, 25, 30, 40}
	if !equalSlices(all, want) {
		t.Errorf("all keys: got %v, want %v", all, want)
	}
}

// TestSplitLeaf_SortOrderPreserved verifies both pages hold strictly ascending keys.
func TestSplitLeaf_SortOrderPreserved(t *testing.T) {
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	page := fillLeaf(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID, []uint64{10, 20, 30, 40, 50})

	newKey := uint64(35)
	got, _, err := NewBTree(pm).splitLeaf(page, makeRecord(newKey, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lk := pageKeys(page); !isSortedAsc(lk) {
		t.Errorf("old page not sorted: %v", lk)
	}
	if rk := pageKeys(got); !isSortedAsc(rk) {
		t.Errorf("new page not sorted: %v", rk)
	}
}

// TestSplitLeaf_LeftRightPartition verifies every key on the old page is less than every key on the new page.
func TestSplitLeaf_LeftRightPartition(t *testing.T) {
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	page := fillLeaf(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID, []uint64{10, 20, 30, 40, 50})

	newKey := uint64(35)
	got, _, err := NewBTree(pm).splitLeaf(page, makeRecord(newKey, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	left, right := pageKeys(page), pageKeys(got)
	if len(left) == 0 || len(right) == 0 {
		t.Fatal("expected non-empty pages on both sides")
	}
	if left[len(left)-1] >= right[0] {
		t.Errorf("partition violated: max(left)=%d >= min(right)=%d", left[len(left)-1], right[0])
	}
}

// TestSplitLeaf_SeparatorEqualsLastKeyOfOldPage verifies the separator key equals the old (left) page's last record.
// With key<=sep→leftChild routing, sep must equal max(leftPage) so the boundary key still routes correctly.
func TestSplitLeaf_SeparatorEqualsLastKeyOfOldPage(t *testing.T) {
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	page := fillLeaf(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID, []uint64{10, 20, 30, 40})

	newKey := uint64(25)
	_, sep, err := NewBTree(pm).splitLeaf(page, makeRecord(newKey, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	leftKeys := pageKeys(page)
	if len(leftKeys) == 0 {
		t.Fatal("old page has no records after split")
	}
	lastLeft := leftKeys[len(leftKeys)-1]
	if sep != lastLeft {
		t.Errorf("separator=%d, last key of old page=%d: must be equal", sep, lastLeft)
	}
}

// TestSplitLeaf_PageIDsPreserved verifies the old page ID and new page ID are unchanged by the split.
func TestSplitLeaf_PageIDsPreserved(t *testing.T) {
	const oldID uint32 = 7
	const newID uint32 = 42

	pm := newSplitPM(pagemanager.NewLeafPage(newID, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	page := fillLeaf(oldID, pagemanager.InvalidPageID, pagemanager.InvalidPageID, []uint64{10, 20, 30, 40})

	got, _, err := NewBTree(pm).splitLeaf(page, makeRecord(25, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if page.GetPageId() != oldID {
		t.Errorf("old page ID changed: got %d, want %d", page.GetPageId(), oldID)
	}
	if got.GetPageId() != newID {
		t.Errorf("new page ID: got %d, want %d", got.GetPageId(), newID)
	}
}

// TestSplitLeaf_RecordValueBytes verifies record value bytes survive the split intact.
func TestSplitLeaf_RecordValueBytes(t *testing.T) {
	page := pagemanager.NewLeafPage(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID)
	page.InsertRecord(makeRecord(10, []byte("alpha")))
	page.InsertRecord(makeRecord(20, []byte("beta")))
	page.InsertRecord(makeRecord(40, []byte("delta")))
	page.InsertRecord(makeRecord(50, []byte("epsilon")))
	newRec := makeRecord(30, []byte("gamma"))

	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	// all=[10,20,30,40,50], split=2: old=[10,20], new=[30,40,50]
	got, _, err := NewBTree(pm).splitLeaf(page, newRec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	check := func(p *pagemanager.Page, slot int, wantVal string) {
		t.Helper()
		rec, ok := p.GetRecord(slot)
		if !ok {
			t.Errorf("slot %d: record not found", slot)
			return
		}
		if got := string(rec[8:]); got != wantVal {
			t.Errorf("slot %d value: got %q, want %q", slot, got, wantVal)
		}
	}

	check(page, 0, "alpha")
	check(page, 1, "beta")
	check(got, 0, "gamma")
	check(got, 1, "delta")
	check(got, 2, "epsilon")
}

// ---- Sibling pointer correctness ----

// TestSplitLeaf_Siblings_NoOldRightSibling verifies the sibling chain when page had no right neighbour.
// Before: page(1) — rightSib=invalid
// After:  page(1) → newPage(2), newPage(2) — rightSib=invalid
func TestSplitLeaf_Siblings_NoOldRightSibling(t *testing.T) {
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	page := fillLeaf(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID, []uint64{10, 20, 30, 40})

	got, _, err := NewBTree(pm).splitLeaf(page, makeRecord(25, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if page.GetRightSibling() != 2 {
		t.Errorf("page.rightSibling: got %d, want 2", page.GetRightSibling())
	}
	if got.GetLeftSibling() != 1 {
		t.Errorf("newPage.leftSibling: got %d, want 1", got.GetLeftSibling())
	}
	if got.GetRightSibling() != pagemanager.InvalidPageID {
		t.Errorf("newPage.rightSibling: got %d, want InvalidPageID", got.GetRightSibling())
	}
}

// TestSplitLeaf_Siblings_WithOldRightSibling verifies the full four-pointer chain update.
// Before: page(1) → oldRight(3)
// After:  page(1) → newPage(2) → oldRight(3); oldRight.left = newPage(2)
func TestSplitLeaf_Siblings_WithOldRightSibling(t *testing.T) {
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	oldRight := pagemanager.NewLeafPage(3, 1, pagemanager.InvalidPageID)
	pm.pages[3] = oldRight

	page := fillLeaf(1, pagemanager.InvalidPageID, 3, []uint64{10, 20, 30, 40})
	got, _, err := NewBTree(pm).splitLeaf(page, makeRecord(25, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if page.GetRightSibling() != 2 {
		t.Errorf("page.rightSibling: got %d, want 2", page.GetRightSibling())
	}
	if got.GetLeftSibling() != 1 {
		t.Errorf("newPage.leftSibling: got %d, want 1", got.GetLeftSibling())
	}
	if got.GetRightSibling() != 3 {
		t.Errorf("newPage.rightSibling: got %d, want 3", got.GetRightSibling())
	}
	if oldRight.GetLeftSibling() != 2 {
		t.Errorf("oldRight.leftSibling: got %d, want 2", oldRight.GetLeftSibling())
	}
}

// TestSplitLeaf_OldRightSibling_RightPointerUnchanged verifies the old right sibling's
// own right pointer is not touched when it itself has a further right neighbour.
func TestSplitLeaf_OldRightSibling_RightPointerUnchanged(t *testing.T) {
	// chain: page(1) → oldRight(3) → farRight(99)
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	oldRight := pagemanager.NewLeafPage(3, 1, 99)
	pm.pages[3] = oldRight

	page := fillLeaf(1, pagemanager.InvalidPageID, 3, []uint64{10, 20, 30, 40})
	_, _, err := NewBTree(pm).splitLeaf(page, makeRecord(25, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if oldRight.GetRightSibling() != 99 {
		t.Errorf("oldRight.rightSibling should remain 99, got %d", oldRight.GetRightSibling())
	}
}

// TestSplitLeaf_OldRightSibling_WrittenToDisk verifies WritePage is called for the old right sibling.
func TestSplitLeaf_OldRightSibling_WrittenToDisk(t *testing.T) {
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	oldRight := pagemanager.NewLeafPage(3, 1, pagemanager.InvalidPageID)
	pm.pages[3] = oldRight

	page := fillLeaf(1, pagemanager.InvalidPageID, 3, []uint64{10, 20, 30, 40})
	_, _, err := NewBTree(pm).splitLeaf(page, makeRecord(25, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// splitLeaf writes: oldRight (sibling update), page (left), newPage (right) = 3 total.
	// Verify oldRight is written first — that is the only write caused by sibling maintenance.
	if len(pm.written) != 3 || pm.written[0] != oldRight {
		t.Errorf("expected oldRight as first of 3 WritePage calls, got %d writes (first=%v)", len(pm.written), pm.written[0])
	}
}

// TestSplitLeaf_NoWriteWhenNoRightSibling verifies WritePage is NOT called when there is no right sibling.
func TestSplitLeaf_NoWriteWhenNoRightSibling(t *testing.T) {
	pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
	page := fillLeaf(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID, []uint64{10, 20, 30, 40})

	_, _, err := NewBTree(pm).splitLeaf(page, makeRecord(25, []byte("val")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// splitLeaf always writes the two split pages; with no right sibling there should be
	// exactly 2 writes (left page, right page) — no extra write for a non-existent sibling.
	if len(pm.written) != 2 {
		t.Errorf("expected 2 WritePage calls (left and right split pages), got %d", len(pm.written))
	}
}

// ---- All insert positions ----

// TestSplitLeaf_AllInsertPositions iterates over every valid insertPos for a 4-record page
// and verifies record count, sort order, left/right partition, and separator correctness.
func TestSplitLeaf_AllInsertPositions(t *testing.T) {
	existing := []uint64{10, 20, 30, 40}
	// newKeys[i] is the unique key that sorts to insertPos i
	newKeys := []uint64{5, 15, 25, 35, 50}

	for _, newKey := range newKeys {
		t.Run(fmt.Sprintf("key_%d", newKey), func(t *testing.T) {
			pm := newSplitPM(pagemanager.NewLeafPage(2, pagemanager.InvalidPageID, pagemanager.InvalidPageID))
			page := fillLeaf(1, pagemanager.InvalidPageID, pagemanager.InvalidPageID, existing)

			gotPage, sep, err := NewBTree(pm).splitLeaf(page, makeRecord(newKey, []byte("v")))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			left, right := pageKeys(page), pageKeys(gotPage)

			if len(left)+len(right) != 5 {
				t.Errorf("total=%d, want 5 (left=%v right=%v)", len(left)+len(right), left, right)
			}
			if !isSortedAsc(left) {
				t.Errorf("old page not sorted: %v", left)
			}
			if !isSortedAsc(right) {
				t.Errorf("new page not sorted: %v", right)
			}
			if len(left) > 0 && len(right) > 0 && left[len(left)-1] >= right[0] {
				t.Errorf("partition violated: left=%v right=%v", left, right)
			}
			if len(left) > 0 && sep != left[len(left)-1] {
				t.Errorf("separator=%d, want last key of left page=%d", sep, left[len(left)-1])
			}
		})
	}
}

// =====================================
// splitInternal tests
// =====================================

// internalSplitPM is a minimal PageManager for splitInternal tests. AllocatePage returns a
// pre-built page; WritePage succeeds unless a per-page-ID error is injected.
type internalSplitPM struct {
	*mockPM
	allocPage    *pagemanager.Page
	allocErr     error
	writeErrByID map[uint32]error
}

func newInternalSplitPM(allocPage *pagemanager.Page) *internalSplitPM {
	return &internalSplitPM{
		mockPM:       newMockPM(),
		allocPage:    allocPage,
		writeErrByID: make(map[uint32]error),
	}
}

func (m *internalSplitPM) AllocatePage() (*pagemanager.Page, error) {
	if m.allocErr != nil {
		return nil, m.allocErr
	}
	return m.allocPage, nil
}

func (m *internalSplitPM) WritePage(p *pagemanager.Page) error {
	if err, ok := m.writeErrByID[p.GetPageId()]; ok {
		return err
	}
	return nil
}

// makeInternalPageL creates an internal page with a configurable level.
func makeInternalPageL(id uint32, level uint16, seps []uint64, leftChildren []uint32, rightmost uint32) *pagemanager.Page {
	p := pagemanager.NewInternalPage(id, level, rightmost)
	for i, k := range seps {
		p.InsertRecord(EncodeInternalRecord(k, leftChildren[i]))
	}
	return p
}

// internalPageEntries returns (keys, childIDs) for every slot of an internal page.
func internalPageEntries(t *testing.T, p *pagemanager.Page) ([]uint64, []uint32) {
	t.Helper()
	var keys []uint64
	var children []uint32
	for i := 0; i < int(p.GetRowCount()); i++ {
		rec, ok := p.GetRecord(i)
		if !ok {
			t.Fatalf("slot %d: GetRecord returned false", i)
		}
		k, c, err := DecodeInternalRecord(rec)
		if err != nil {
			t.Fatalf("slot %d: DecodeInternalRecord: %v", i, err)
		}
		keys = append(keys, k)
		children = append(children, c)
	}
	return keys, children
}

// equalSlicesU32 reports whether two uint32 slices are identical.
func equalSlicesU32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// newRightPage returns a fresh leaf-typed placeholder page with the given ID.
// splitInternal immediately overwrites it with NewInternalPage, so the type does not matter.
func newRightPage(id uint32) *pagemanager.Page {
	return pagemanager.NewLeafPage(id, pagemanager.InvalidPageID, pagemanager.InvalidPageID)
}

// ---- Structural correctness ----

// TestSplitInternal_NewKeyGoesToRightHalf: insertPos >= splitPoint (right branch).
// Input: [(10,C1),(20,C2),(30,C3)] rm=C4, insert (25,C5). Combined splitPoint=2, insertPos=2.
// Separator=(25,C5). Left:[(10,C1),(20,C2)] rm=C5. Right:[(30,C3)] rm=C4.
func TestSplitInternal_NewKeyGoesToRightHalf(t *testing.T) {
	C1, C2, C3, C4, C5 := uint32(11), uint32(12), uint32(13), uint32(14), uint32(15)
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30}, []uint32{C1, C2, C3}, C4)
	bt := NewBTree(pm)

	right, sep, err := bt.splitInternal(left, 25, C5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sep != 25 {
		t.Errorf("separator key: want 25, got %d", sep)
	}

	lKeys, lCh := internalPageEntries(t, left)
	if !equalSlices(lKeys, []uint64{10, 20}) {
		t.Errorf("left keys: want [10 20], got %v", lKeys)
	}
	if !equalSlicesU32(lCh, []uint32{C1, C2}) {
		t.Errorf("left children: want [%d %d], got %v", C1, C2, lCh)
	}
	if left.GetRightMostChild() != C5 {
		t.Errorf("left rightmost: want C5=%d, got %d", C5, left.GetRightMostChild())
	}
	if left.GetLevel() != 1 {
		t.Errorf("left level: want 1, got %d", left.GetLevel())
	}

	rKeys, rCh := internalPageEntries(t, right)
	if !equalSlices(rKeys, []uint64{30}) {
		t.Errorf("right keys: want [30], got %v", rKeys)
	}
	if !equalSlicesU32(rCh, []uint32{C3}) {
		t.Errorf("right children: want [%d], got %v", C3, rCh)
	}
	if right.GetRightMostChild() != C4 {
		t.Errorf("right rightmost: want C4=%d, got %d", C4, right.GetRightMostChild())
	}
	if right.GetLevel() != 1 {
		t.Errorf("right level: want 1, got %d", right.GetLevel())
	}
}

// TestSplitInternal_NewKeyGoesToLeftHalf: insertPos < splitPoint (left branch).
// Input: [(10,C1),(20,C2),(30,C3)] rm=C4, insert (15,C5). Combined splitPoint=2, insertPos=1.
// Separator=(20,C2). Left:[(10,C1),(15,C5)] rm=C2. Right:[(30,C3)] rm=C4.
func TestSplitInternal_NewKeyGoesToLeftHalf(t *testing.T) {
	C1, C2, C3, C4, C5 := uint32(11), uint32(12), uint32(13), uint32(14), uint32(15)
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30}, []uint32{C1, C2, C3}, C4)
	bt := NewBTree(pm)

	right, sep, err := bt.splitInternal(left, 15, C5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sep != 20 {
		t.Errorf("separator key: want 20, got %d", sep)
	}

	lKeys, lCh := internalPageEntries(t, left)
	if !equalSlices(lKeys, []uint64{10, 15}) {
		t.Errorf("left keys: want [10 15], got %v", lKeys)
	}
	if !equalSlicesU32(lCh, []uint32{C1, C5}) {
		t.Errorf("left children: want [%d %d], got %v", C1, C5, lCh)
	}
	if left.GetRightMostChild() != C2 {
		t.Errorf("left rightmost: want C2=%d (separator child), got %d", C2, left.GetRightMostChild())
	}

	rKeys, rCh := internalPageEntries(t, right)
	if !equalSlices(rKeys, []uint64{30}) {
		t.Errorf("right keys: want [30], got %v", rKeys)
	}
	if !equalSlicesU32(rCh, []uint32{C3}) {
		t.Errorf("right children: want [%d], got %v", C3, rCh)
	}
	if right.GetRightMostChild() != C4 {
		t.Errorf("right rightmost: want C4=%d, got %d", C4, right.GetRightMostChild())
	}
}

// TestSplitInternal_NewKeyIsSmallest: new key sorts to position 0 (left branch, insertPos=0).
// Input: [(10,C1),(20,C2),(30,C3)] rm=C4, insert (5,C5). insertPos=0, splitPoint=2.
// Separator=(20,C2). Left:[(5,C5),(10,C1)] rm=C2. Right:[(30,C3)] rm=C4.
func TestSplitInternal_NewKeyIsSmallest(t *testing.T) {
	C1, C2, C3, C4, C5 := uint32(11), uint32(12), uint32(13), uint32(14), uint32(15)
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30}, []uint32{C1, C2, C3}, C4)
	bt := NewBTree(pm)

	right, sep, err := bt.splitInternal(left, 5, C5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sep != 20 {
		t.Errorf("separator key: want 20, got %d", sep)
	}
	lKeys, _ := internalPageEntries(t, left)
	if !equalSlices(lKeys, []uint64{5, 10}) {
		t.Errorf("left keys: want [5 10], got %v", lKeys)
	}
	if left.GetRightMostChild() != C2 {
		t.Errorf("left rightmost: want C2=%d, got %d", C2, left.GetRightMostChild())
	}

	rKeys, _ := internalPageEntries(t, right)
	if !equalSlices(rKeys, []uint64{30}) {
		t.Errorf("right keys: want [30], got %v", rKeys)
	}
	if right.GetRightMostChild() != C4 {
		t.Errorf("right rightmost: want C4=%d, got %d", C4, right.GetRightMostChild())
	}
}

// TestSplitInternal_NewKeyIsLargest: new key sorts to the last position (right branch).
// Input: [(10,C1),(20,C2),(30,C3)] rm=C4, insert (40,C5). insertPos=3, splitPoint=2.
// Separator=(30,C3). Left:[(10,C1),(20,C2)] rm=C3. Right:[(40,C5)] rm=C4.
func TestSplitInternal_NewKeyIsLargest(t *testing.T) {
	C1, C2, C3, C4, C5 := uint32(11), uint32(12), uint32(13), uint32(14), uint32(15)
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30}, []uint32{C1, C2, C3}, C4)
	bt := NewBTree(pm)

	right, sep, err := bt.splitInternal(left, 40, C5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sep != 30 {
		t.Errorf("separator key: want 30, got %d", sep)
	}
	lKeys, lCh := internalPageEntries(t, left)
	if !equalSlices(lKeys, []uint64{10, 20}) {
		t.Errorf("left keys: want [10 20], got %v", lKeys)
	}
	if !equalSlicesU32(lCh, []uint32{C1, C2}) {
		t.Errorf("left children: got %v", lCh)
	}
	if left.GetRightMostChild() != C3 {
		t.Errorf("left rightmost: want C3=%d, got %d", C3, left.GetRightMostChild())
	}

	rKeys, rCh := internalPageEntries(t, right)
	if !equalSlices(rKeys, []uint64{40}) {
		t.Errorf("right keys: want [40], got %v", rKeys)
	}
	if !equalSlicesU32(rCh, []uint32{C5}) {
		t.Errorf("right children: got %v", rCh)
	}
	if right.GetRightMostChild() != C4 {
		t.Errorf("right rightmost: want C4=%d, got %d", C4, right.GetRightMostChild())
	}
}

// TestSplitInternal_MinimalSplit: smallest viable split — 2 existing records + 1 new = 3 total.
// Input: [(10,C1),(20,C2)] rm=C3, insert (15,C4). splitPoint=1, insertPos=1 (right branch).
// Separator=(15,C4). Left:[(10,C1)] rm=C4. Right:[(20,C2)] rm=C3.
func TestSplitInternal_MinimalSplit(t *testing.T) {
	C1, C2, C3, C4 := uint32(11), uint32(12), uint32(13), uint32(14)
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20}, []uint32{C1, C2}, C3)
	bt := NewBTree(pm)

	right, sep, err := bt.splitInternal(left, 15, C4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sep != 15 {
		t.Errorf("separator key: want 15, got %d", sep)
	}
	lKeys, lCh := internalPageEntries(t, left)
	if !equalSlices(lKeys, []uint64{10}) {
		t.Errorf("left keys: want [10], got %v", lKeys)
	}
	if !equalSlicesU32(lCh, []uint32{C1}) {
		t.Errorf("left children: got %v", lCh)
	}
	if left.GetRightMostChild() != C4 {
		t.Errorf("left rightmost: want C4=%d (separator child), got %d", C4, left.GetRightMostChild())
	}

	rKeys, rCh := internalPageEntries(t, right)
	if !equalSlices(rKeys, []uint64{20}) {
		t.Errorf("right keys: want [20], got %v", rKeys)
	}
	if !equalSlicesU32(rCh, []uint32{C2}) {
		t.Errorf("right children: got %v", rCh)
	}
	if right.GetRightMostChild() != C3 {
		t.Errorf("right rightmost: want C3=%d, got %d", C3, right.GetRightMostChild())
	}
}

// TestSplitInternal_NewKeyBecomesSeparator: new key lands exactly at splitPoint in the combined
// sequence, so it is selected as the separator and never stored in either page.
// Input: [(10,C1),(20,C2),(30,C3),(40,C4)] rm=C5, insert (25,C_new). splitPoint=2, insertPos=2.
// Separator=(25,C_new). Left:[(10,C1),(20,C2)] rm=C_new. Right:[(30,C3),(40,C4)] rm=C5.
func TestSplitInternal_NewKeyBecomesSeparator(t *testing.T) {
	C1, C2, C3, C4, C5, Cn := uint32(11), uint32(12), uint32(13), uint32(14), uint32(15), uint32(16)
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30, 40}, []uint32{C1, C2, C3, C4}, C5)
	bt := NewBTree(pm)

	right, sep, err := bt.splitInternal(left, 25, Cn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sep != 25 {
		t.Errorf("separator key: want 25, got %d", sep)
	}
	lKeys, _ := internalPageEntries(t, left)
	if !equalSlices(lKeys, []uint64{10, 20}) {
		t.Errorf("left keys: want [10 20], got %v", lKeys)
	}
	if left.GetRightMostChild() != Cn {
		t.Errorf("left rightmost: want Cn=%d, got %d", Cn, left.GetRightMostChild())
	}

	rKeys, rCh := internalPageEntries(t, right)
	if !equalSlices(rKeys, []uint64{30, 40}) {
		t.Errorf("right keys: want [30 40], got %v", rKeys)
	}
	if !equalSlicesU32(rCh, []uint32{C3, C4}) {
		t.Errorf("right children: got %v", rCh)
	}
	if right.GetRightMostChild() != C5 {
		t.Errorf("right rightmost: want C5=%d, got %d", C5, right.GetRightMostChild())
	}
}

// ---- Key invariants ----

// TestSplitInternal_SeparatorNotInEitherPage verifies the separator key does not appear in
// either the left or right page records after the split (it is pushed up to the parent).
func TestSplitInternal_SeparatorNotInEitherPage(t *testing.T) {
	C1, C2, C3, C4, C5 := uint32(11), uint32(12), uint32(13), uint32(14), uint32(15)
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30}, []uint32{C1, C2, C3}, C4)

	right, sep, err := NewBTree(pm).splitInternal(left, 25, C5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, k := range pageKeys(left) {
		if k == sep {
			t.Errorf("separator key %d found in left page", sep)
		}
	}
	// pageKeys reads raw key bytes; DecodeInternalRecord is not needed for key comparison.
	for i := 0; i < int(right.GetRowCount()); i++ {
		rec, _ := right.GetRecord(i)
		if RecordKey(rec) == sep {
			t.Errorf("separator key %d found in right page at slot %d", sep, i)
		}
	}
}

// TestSplitInternal_LeftRightmostChildIsSeparatorChild verifies the critical invariant
// introduced by the bug fix: the left page's rightmost child must equal the child pointer
// that was stored in the separator record.
func TestSplitInternal_LeftRightmostChildIsSeparatorChild(t *testing.T) {
	tests := []struct {
		name       string
		seps       []uint64
		children   []uint32
		rightmost  uint32
		newKey     uint64
		newChildID uint32
		wantSep    uint64
		wantLRM    uint32 // expected left rightmost child after split
	}{
		{
			name:      "new key in right half, separator child is the new child",
			seps:      []uint64{10, 20, 30},
			children:  []uint32{11, 12, 13},
			rightmost: 14,
			newKey:    25, newChildID: 99,
			wantSep: 25, wantLRM: 99,
		},
		{
			name:      "new key in left half, separator child is original record's child",
			seps:      []uint64{10, 20, 30},
			children:  []uint32{11, 12, 13},
			rightmost: 14,
			newKey:    15, newChildID: 99,
			wantSep: 20, wantLRM: 12, // separator is (20,C2=12)
		},
		{
			name:      "minimal split, new key becomes separator",
			seps:      []uint64{10, 20},
			children:  []uint32{11, 12},
			rightmost: 13,
			newKey:    15, newChildID: 99,
			wantSep: 15, wantLRM: 99,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm := newInternalSplitPM(newRightPage(2))
			left := makeInternalPageL(1, 1, tt.seps, tt.children, tt.rightmost)

			_, sep, err := NewBTree(pm).splitInternal(left, tt.newKey, tt.newChildID)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if sep != tt.wantSep {
				t.Errorf("separator: want %d, got %d", tt.wantSep, sep)
			}
			if left.GetRightMostChild() != tt.wantLRM {
				t.Errorf("left rightmost: want %d, got %d", tt.wantLRM, left.GetRightMostChild())
			}
		})
	}
}

// TestSplitInternal_RightRightmostChildIsOldRightmost verifies that the right page always
// inherits the old left page's rightmost child pointer.
func TestSplitInternal_RightRightmostChildIsOldRightmost(t *testing.T) {
	const oldRightmost uint32 = 999
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30}, []uint32{11, 12, 13}, oldRightmost)

	right, _, err := NewBTree(pm).splitInternal(left, 25, 77)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if right.GetRightMostChild() != oldRightmost {
		t.Errorf("right rightmost: want %d, got %d", oldRightmost, right.GetRightMostChild())
	}
}

// TestSplitInternal_LevelPreservedInBothPages verifies that neither the left page's level
// is incremented nor the right page gets a different level — the split is horizontal.
func TestSplitInternal_LevelPreservedInBothPages(t *testing.T) {
	const level uint16 = 3
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, level, []uint64{10, 20, 30}, []uint32{11, 12, 13}, 14)

	right, _, err := NewBTree(pm).splitInternal(left, 25, 99)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if left.GetLevel() != level {
		t.Errorf("left level: want %d, got %d", level, left.GetLevel())
	}
	if right.GetLevel() != level {
		t.Errorf("right level: want %d, got %d", level, right.GetLevel())
	}
}

// TestSplitInternal_AllKeysAccountedFor verifies that separator + left page keys + right page
// keys equal exactly the original set of keys plus the new key — no key is lost or duplicated.
func TestSplitInternal_AllKeysAccountedFor(t *testing.T) {
	// 5 existing + 1 new = 6 total.  sep + 3 left + 2 right = 6 keys.
	origKeys := []uint64{10, 20, 30, 40, 50}
	newKey := uint64(35)
	allKeys := []uint64{10, 20, 30, 35, 40, 50}

	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, origKeys, []uint32{11, 12, 13, 14, 15}, 16)

	right, sep, err := NewBTree(pm).splitInternal(left, newKey, 99)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var got []uint64
	got = append(got, pageKeys(left)...)
	got = append(got, sep)
	for i := 0; i < int(right.GetRowCount()); i++ {
		rec, _ := right.GetRecord(i)
		got = append(got, RecordKey(rec))
	}

	// Sort got for comparison (order across left/right may differ from allKeys layout).
	for i := 0; i < len(got)-1; i++ {
		for j := i + 1; j < len(got); j++ {
			if got[j] < got[i] {
				got[i], got[j] = got[j], got[i]
			}
		}
	}

	if !equalSlices(got, allKeys) {
		t.Errorf("keys after split: want %v, got %v", allKeys, got)
	}
}

// TestSplitInternal_LeftKeysAllBelowSeparator verifies that every key remaining in the left
// page is strictly less than the separator.
func TestSplitInternal_LeftKeysAllBelowSeparator(t *testing.T) {
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30, 40, 50}, []uint32{11, 12, 13, 14, 15}, 16)

	_, sep, err := NewBTree(pm).splitInternal(left, 35, 99)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, k := range pageKeys(left) {
		if k >= sep {
			t.Errorf("left key %d >= separator %d", k, sep)
		}
	}
}

// TestSplitInternal_RightKeysAllAboveSeparator verifies that every key in the right page is
// strictly greater than the separator.
func TestSplitInternal_RightKeysAllAboveSeparator(t *testing.T) {
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30, 40, 50}, []uint32{11, 12, 13, 14, 15}, 16)

	right, sep, err := NewBTree(pm).splitInternal(left, 35, 99)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i < int(right.GetRowCount()); i++ {
		rec, _ := right.GetRecord(i)
		k := RecordKey(rec)
		if k <= sep {
			t.Errorf("right key %d <= separator %d", k, sep)
		}
	}
}

// TestSplitInternal_LeftAndRightPagesSorted verifies that both result pages store their
// records in strictly ascending key order.
func TestSplitInternal_LeftAndRightPagesSorted(t *testing.T) {
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30, 40}, []uint32{11, 12, 13, 14}, 15)

	right, _, err := NewBTree(pm).splitInternal(left, 25, 99)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lk := pageKeys(left); !isSortedAsc(lk) {
		t.Errorf("left page not sorted: %v", lk)
	}
	var rKeys []uint64
	for i := 0; i < int(right.GetRowCount()); i++ {
		rec, _ := right.GetRecord(i)
		rKeys = append(rKeys, RecordKey(rec))
	}
	if !isSortedAsc(rKeys) {
		t.Errorf("right page not sorted: %v", rKeys)
	}
}

// ---- Page type and routing ----

// TestSplitInternal_RightPageIsInternalType verifies AllocatePage's placeholder type is
// replaced and the returned page is PageTypeInternal.
func TestSplitInternal_RightPageIsInternalType(t *testing.T) {
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30}, []uint32{11, 12, 13}, 14)

	right, _, err := NewBTree(pm).splitInternal(left, 25, 99)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if right.GetPageType() != pagemanager.PageTypeInternal {
		t.Errorf("right page type: want PageTypeInternal, got %d", right.GetPageType())
	}
}

// TestSplitInternal_SearchRoutingAfterSplit_LeftPage verifies that searchInternal correctly
// routes keys on the left page after the split.
// Left page after split: [(10,C1),(20,C2)] rm=C5.
// Routing: key<=10→C1, 10<key<=20→C2, key>20→C5.
func TestSplitInternal_SearchRoutingAfterSplit_LeftPage(t *testing.T) {
	C1, C2, C3, C4, C5 := uint32(11), uint32(12), uint32(13), uint32(14), uint32(15)
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30}, []uint32{C1, C2, C3}, C4)

	_, _, err := NewBTree(pm).splitInternal(left, 25, C5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// After split: left=[(10,C1),(20,C2)] rm=C5
	routing := []struct {
		key  uint64
		want uint32
		name string
	}{
		{5, C1, "key below first sep"},
		{10, C1, "key equal first sep"},
		{15, C2, "key between seps"},
		{20, C2, "key equal second sep"},
		{21, C5, "key above all seps → rightmost (separator child)"},
		{100, C5, "key far above → rightmost"},
	}
	for _, tt := range routing {
		t.Run(tt.name, func(t *testing.T) {
			got := searchInternal(tt.key, left)
			if got != tt.want {
				t.Errorf("key=%d: want child %d, got %d", tt.key, tt.want, got)
			}
		})
	}
}

// TestSplitInternal_SearchRoutingAfterSplit_RightPage verifies that searchInternal correctly
// routes keys on the right page after the split.
// Right page after split: [(30,C3)] rm=C4.
// Routing: key<=30→C3, key>30→C4.
func TestSplitInternal_SearchRoutingAfterSplit_RightPage(t *testing.T) {
	C1, C2, C3, C4, C5 := uint32(11), uint32(12), uint32(13), uint32(14), uint32(15)
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30}, []uint32{C1, C2, C3}, C4)

	right, _, err := NewBTree(pm).splitInternal(left, 25, C5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// After split: right=[(30,C3)] rm=C4
	routing := []struct {
		key  uint64
		want uint32
		name string
	}{
		{26, C3, "key just above separator → first right-page child"},
		{30, C3, "key equal first right-page sep"},
		{31, C4, "key above right-page sep → rightmost"},
		{999, C4, "key far above → rightmost"},
	}
	for _, tt := range routing {
		t.Run(tt.name, func(t *testing.T) {
			got := searchInternal(tt.key, right)
			if got != tt.want {
				t.Errorf("key=%d: want child %d, got %d", tt.key, tt.want, got)
			}
		})
	}
}

// ---- Larger splits ----

// TestSplitInternal_LargerSplit_EvenTotal: 5 existing + 1 new = 6 total (even), splitPoint=3.
// Insert (35,Cn) at insertPos=3 (right branch, pos=0).
// Separator=(35,Cn). Left:[(10,C1),(20,C2),(30,C3)] rm=Cn. Right:[(40,C4),(50,C5)] rm=C6.
func TestSplitInternal_LargerSplit_EvenTotal(t *testing.T) {
	C1, C2, C3, C4, C5, C6, Cn := uint32(11), uint32(12), uint32(13), uint32(14), uint32(15), uint32(16), uint32(99)
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30, 40, 50}, []uint32{C1, C2, C3, C4, C5}, C6)

	right, sep, err := NewBTree(pm).splitInternal(left, 35, Cn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sep != 35 {
		t.Errorf("separator key: want 35, got %d", sep)
	}
	if left.GetRightMostChild() != Cn {
		t.Errorf("left rightmost: want Cn=%d, got %d", Cn, left.GetRightMostChild())
	}
	lKeys, _ := internalPageEntries(t, left)
	if !equalSlices(lKeys, []uint64{10, 20, 30}) {
		t.Errorf("left keys: want [10 20 30], got %v", lKeys)
	}
	rKeys, rCh := internalPageEntries(t, right)
	if !equalSlices(rKeys, []uint64{40, 50}) {
		t.Errorf("right keys: want [40 50], got %v", rKeys)
	}
	if !equalSlicesU32(rCh, []uint32{C4, C5}) {
		t.Errorf("right children: got %v", rCh)
	}
	if right.GetRightMostChild() != C6 {
		t.Errorf("right rightmost: want C6=%d, got %d", C6, right.GetRightMostChild())
	}
}

// TestSplitInternal_LargerSplit_OddTotal: 4 existing + 1 new = 5 total (odd), splitPoint=2.
// Insert (35,Cn) at insertPos=3 (right branch, pos=1).
// Separator=(30,C3). Left:[(10,C1),(20,C2)] rm=C3. Right:[(35,Cn),(40,C4)] rm=C5.
func TestSplitInternal_LargerSplit_OddTotal(t *testing.T) {
	C1, C2, C3, C4, C5, Cn := uint32(11), uint32(12), uint32(13), uint32(14), uint32(15), uint32(99)
	pm := newInternalSplitPM(newRightPage(2))
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30, 40}, []uint32{C1, C2, C3, C4}, C5)

	right, sep, err := NewBTree(pm).splitInternal(left, 35, Cn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sep != 30 {
		t.Errorf("separator key: want 30, got %d", sep)
	}
	if left.GetRightMostChild() != C3 {
		t.Errorf("left rightmost: want C3=%d, got %d", C3, left.GetRightMostChild())
	}
	lKeys, _ := internalPageEntries(t, left)
	if !equalSlices(lKeys, []uint64{10, 20}) {
		t.Errorf("left keys: want [10 20], got %v", lKeys)
	}
	rKeys, rCh := internalPageEntries(t, right)
	if !equalSlices(rKeys, []uint64{35, 40}) {
		t.Errorf("right keys: want [35 40], got %v", rKeys)
	}
	if !equalSlicesU32(rCh, []uint32{Cn, C4}) {
		t.Errorf("right children: got %v", rCh)
	}
	if right.GetRightMostChild() != C5 {
		t.Errorf("right rightmost: want C5=%d, got %d", C5, right.GetRightMostChild())
	}
}

// TestSplitInternal_AllInsertPositions iterates every valid insertPos for a 4-record page
// and verifies: sorted invariants, sep not in either page, all keys accounted for,
// left rightmost = separator child, right rightmost = old rightmost.
func TestSplitInternal_AllInsertPositions(t *testing.T) {
	const oldRightmost uint32 = 15
	existingKeys := []uint64{10, 20, 30, 40}
	existingChildren := []uint32{11, 12, 13, 14}
	// newKey[i] sorts to insertPos i: 5→0, 15→1, 25→2, 35→3, 50→4.
	newKeys := []uint64{5, 15, 25, 35, 50}
	const newChildID uint32 = 99

	for i, nk := range newKeys {
		t.Run(fmt.Sprintf("insertPos_%d_key_%d", i, nk), func(t *testing.T) {
			pm := newInternalSplitPM(newRightPage(2))
			left := makeInternalPageL(1, 1, existingKeys, existingChildren, oldRightmost)

			right, sep, err := NewBTree(pm).splitInternal(left, nk, newChildID)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			lKeys := pageKeys(left)
			var rKeys []uint64
			for j := 0; j < int(right.GetRowCount()); j++ {
				rec, _ := right.GetRecord(j)
				rKeys = append(rKeys, RecordKey(rec))
			}

			// Total key count: (len(lKeys) + 1 for sep + len(rKeys)) == 5
			total := len(lKeys) + 1 + len(rKeys)
			if total != 5 {
				t.Errorf("key count: want 5 (left=%v sep=%d right=%v), got %d", lKeys, sep, rKeys, total)
			}

			// Sorted order within each page.
			if !isSortedAsc(lKeys) {
				t.Errorf("left page not sorted: %v", lKeys)
			}
			if !isSortedAsc(rKeys) {
				t.Errorf("right page not sorted: %v", rKeys)
			}

			// Partition invariant: all left < sep < all right.
			for _, k := range lKeys {
				if k >= sep {
					t.Errorf("left key %d >= separator %d", k, sep)
				}
			}
			for _, k := range rKeys {
				if k <= sep {
					t.Errorf("right key %d <= separator %d", k, sep)
				}
			}

			// Separator not stored in either page.
			for _, k := range lKeys {
				if k == sep {
					t.Errorf("separator %d found in left page", sep)
				}
			}
			for _, k := range rKeys {
				if k == sep {
					t.Errorf("separator %d found in right page", sep)
				}
			}

			// Right page inherits old rightmost.
			if right.GetRightMostChild() != oldRightmost {
				t.Errorf("right rightmost: want %d, got %d", oldRightmost, right.GetRightMostChild())
			}

			// Left rightmost equals separator's child — verify by decoding sep record from rightHalf[0].
			// We confirm it does NOT equal oldRightmost (which would be the pre-fix bug).
			if left.GetRightMostChild() == oldRightmost {
				t.Errorf("left rightmost still equals old rightmost %d — separator child was not applied", oldRightmost)
			}
		})
	}
}

// ---- Error propagation ----

// TestSplitInternal_AllocatePageError verifies that an AllocatePage failure is returned.
func TestSplitInternal_AllocatePageError(t *testing.T) {
	pm := newInternalSplitPM(nil)
	pm.allocErr = fmt.Errorf("disk full")
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30}, []uint32{11, 12, 13}, 14)

	_, _, err := NewBTree(pm).splitInternal(left, 25, 99)
	if err == nil {
		t.Fatal("expected error from AllocatePage, got nil")
	}
}

// TestSplitInternal_WriteLeftPageError verifies that a WritePage failure on the left page
// is propagated and contains a descriptive message.
func TestSplitInternal_WriteLeftPageError(t *testing.T) {
	pm := newInternalSplitPM(newRightPage(2))
	pm.writeErrByID[1] = fmt.Errorf("write failed on left page")
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30}, []uint32{11, 12, 13}, 14)

	_, _, err := NewBTree(pm).splitInternal(left, 25, 99)
	if err == nil {
		t.Fatal("expected error from WritePage(left), got nil")
	}
}

// TestSplitInternal_WriteRightPageError verifies that a WritePage failure on the right page
// is propagated after the left page has been written successfully.
func TestSplitInternal_WriteRightPageError(t *testing.T) {
	right := newRightPage(2)
	pm := newInternalSplitPM(right)
	pm.writeErrByID[2] = fmt.Errorf("write failed on right page")
	left := makeInternalPageL(1, 1, []uint64{10, 20, 30}, []uint32{11, 12, 13}, 14)

	_, _, err := NewBTree(pm).splitInternal(left, 25, 99)
	if err == nil {
		t.Fatal("expected error from WritePage(right), got nil")
	}
}

// =====================================
// insertIntoParent tests
// =====================================

// parentPM is a full-featured PageManager stub for insertIntoParent tests.
// It tracks writes per page ID, serves pages from a pre-loaded map, and dispenses
// allocated pages from a pool in FIFO order so callers can predict which page ID is returned.
type parentPM struct {
	pages        map[uint32]*pagemanager.Page
	readErr      map[uint32]error
	writeErrByID map[uint32]error
	allocPool    []*pagemanager.Page // returned by AllocatePage in order
	allocIdx     int
	allocErr     error
	rootID       uint32
	writeCount   map[uint32]int // counts WritePage calls per page ID
}

func newParentPM() *parentPM {
	return &parentPM{
		pages:        make(map[uint32]*pagemanager.Page),
		readErr:      make(map[uint32]error),
		writeErrByID: make(map[uint32]error),
		writeCount:   make(map[uint32]int),
	}
}

func (p *parentPM) ReadPage(id uint32) (*pagemanager.Page, error) {
	if err, ok := p.readErr[id]; ok {
		return nil, err
	}
	pg, ok := p.pages[id]
	if !ok {
		return nil, fmt.Errorf("page %d not found", id)
	}
	return pg, nil
}

func (p *parentPM) AllocatePage() (*pagemanager.Page, error) {
	if p.allocErr != nil {
		return nil, p.allocErr
	}
	if p.allocIdx >= len(p.allocPool) {
		return nil, fmt.Errorf("allocPool exhausted at index %d", p.allocIdx)
	}
	pg := p.allocPool[p.allocIdx]
	p.allocIdx++
	return pg, nil
}

func (p *parentPM) WritePage(pg *pagemanager.Page) error {
	if err, ok := p.writeErrByID[pg.GetPageId()]; ok {
		return err
	}
	p.writeCount[pg.GetPageId()]++
	return nil
}

func (p *parentPM) FreePage(_ uint32) error       { return nil }
func (p *parentPM) GetRootPageId() uint32         { return p.rootID }
func (p *parentPM) SetRootPageId(id uint32) error { p.rootID = id; return nil }
func (p *parentPM) Close() error                  { return nil }
func (p *parentPM) Delete() error                 { return nil }

// parentStubPage returns a placeholder page with the given ID.
// insertIntoParent overwrites its contents immediately, so only the ID matters.
func parentStubPage(id uint32) *pagemanager.Page {
	return pagemanager.NewLeafPage(id, pagemanager.InvalidPageID, pagemanager.InvalidPageID)
}

// fillInternalToCapacity inserts internal records starting at startKey into p until no more fit.
// Use large startKey values to keep the low end of the key space free for test separators.
func fillInternalToCapacity(p *pagemanager.Page, startKey uint64) {
	recSize := len(EncodeInternalRecord(0, 0))
	for key := startKey; p.CanAccommodate(recSize); key++ {
		p.InsertRecord(EncodeInternalRecord(key, uint32(key%500+50)))
	}
}

// ---- Empty path: new root creation ----

// TestInsertIntoParent_EmptyPath_LeafSplit_CreatesRootAtLevel1 verifies that splitting a leaf root
// produces a new internal root at level 1.
func TestInsertIntoParent_EmptyPath_LeafSplit_CreatesRootAtLevel1(t *testing.T) {
	pm := newParentPM()
	newRoot := parentStubPage(10)
	pm.allocPool = []*pagemanager.Page{newRoot}

	leftLeaf := leafPage(2, []uint64{10, 20})
	rightLeaf := leafPage(3, []uint64{30, 40})

	if err := NewBTree(pm).insertIntoParent(leftLeaf, 30, rightLeaf, []uint32{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if newRoot.GetPageType() != pagemanager.PageTypeInternal {
		t.Errorf("new root type: want Internal, got %d", newRoot.GetPageType())
	}
	if newRoot.GetLevel() != 1 {
		t.Errorf("new root level: want 1, got %d", newRoot.GetLevel())
	}
}

// TestInsertIntoParent_EmptyPath_InternalSplit_CreatesRootAtParentLevelPlusOne verifies that
// splitting an internal root produces a new root at level = old level + 1.
func TestInsertIntoParent_EmptyPath_InternalSplit_CreatesRootAtParentLevelPlusOne(t *testing.T) {
	pm := newParentPM()
	newRoot := parentStubPage(10)
	pm.allocPool = []*pagemanager.Page{newRoot}

	leftInternal := makeInternalPageL(2, 3, []uint64{10, 20}, []uint32{11, 12}, 13)
	rightInternal := makeInternalPageL(3, 3, []uint64{40, 50}, []uint32{14, 15}, 16)

	if err := NewBTree(pm).insertIntoParent(leftInternal, 35, rightInternal, []uint32{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if newRoot.GetLevel() != 4 {
		t.Errorf("new root level: want 4 (3+1), got %d", newRoot.GetLevel())
	}
}

// TestInsertIntoParent_EmptyPath_NewRootHasCorrectPointers verifies separator key, left child
// pointer, and rightmost child of the newly created root.
func TestInsertIntoParent_EmptyPath_NewRootHasCorrectPointers(t *testing.T) {
	pm := newParentPM()
	newRoot := parentStubPage(10)
	pm.allocPool = []*pagemanager.Page{newRoot}

	leftLeaf := leafPage(2, []uint64{10, 20})
	rightLeaf := leafPage(3, []uint64{30, 40})
	const sep = uint64(30)

	if err := NewBTree(pm).insertIntoParent(leftLeaf, sep, rightLeaf, []uint32{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if newRoot.GetRowCount() != 1 {
		t.Fatalf("new root row count: want 1, got %d", newRoot.GetRowCount())
	}
	rec, ok := newRoot.GetRecord(0)
	if !ok {
		t.Fatal("GetRecord(0) returned false")
	}
	k, child, err := DecodeInternalRecord(rec)
	if err != nil {
		t.Fatalf("DecodeInternalRecord: %v", err)
	}
	if k != sep {
		t.Errorf("root separator key: want %d, got %d", sep, k)
	}
	if child != leftLeaf.GetPageId() {
		t.Errorf("root child pointer: want %d (leftLeaf), got %d", leftLeaf.GetPageId(), child)
	}
	if newRoot.GetRightMostChild() != rightLeaf.GetPageId() {
		t.Errorf("root rightmost: want %d (rightLeaf), got %d", rightLeaf.GetPageId(), newRoot.GetRightMostChild())
	}
}

// TestInsertIntoParent_EmptyPath_SetRootPageIdCalled verifies that the page manager's root ID is
// updated to the new root's page ID.
func TestInsertIntoParent_EmptyPath_SetRootPageIdCalled(t *testing.T) {
	pm := newParentPM()
	pm.allocPool = []*pagemanager.Page{parentStubPage(42)}

	leftLeaf := leafPage(2, []uint64{10})
	rightLeaf := leafPage(3, []uint64{20})

	if err := NewBTree(pm).insertIntoParent(leftLeaf, 20, rightLeaf, []uint32{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pm.rootID != 42 {
		t.Errorf("rootID: want 42, got %d", pm.rootID)
	}
}

// TestInsertIntoParent_EmptyPath_NewRootIsWritten verifies that the new root page is persisted.
func TestInsertIntoParent_EmptyPath_NewRootIsWritten(t *testing.T) {
	pm := newParentPM()
	pm.allocPool = []*pagemanager.Page{parentStubPage(10)}

	leftLeaf := leafPage(2, []uint64{10})
	rightLeaf := leafPage(3, []uint64{20})

	if err := NewBTree(pm).insertIntoParent(leftLeaf, 20, rightLeaf, []uint32{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pm.writeCount[10] == 0 {
		t.Error("expected WritePage to be called for the new root (page 10)")
	}
}

// TestInsertIntoParent_EmptyPath_AllocatePageError verifies that an AllocatePage failure is
// returned when creating the new root.
func TestInsertIntoParent_EmptyPath_AllocatePageError(t *testing.T) {
	pm := newParentPM()
	pm.allocErr = fmt.Errorf("disk full")

	leftLeaf := leafPage(2, []uint64{10})
	rightLeaf := leafPage(3, []uint64{20})

	err := NewBTree(pm).insertIntoParent(leftLeaf, 20, rightLeaf, []uint32{})
	if err == nil {
		t.Fatal("expected error from AllocatePage, got nil")
	}
}

// TestInsertIntoParent_EmptyPath_WriteNewRootError verifies that a WritePage failure for the new
// root is propagated.
func TestInsertIntoParent_EmptyPath_WriteNewRootError(t *testing.T) {
	pm := newParentPM()
	pm.allocPool = []*pagemanager.Page{parentStubPage(10)}
	pm.writeErrByID[10] = fmt.Errorf("write failed")

	leftLeaf := leafPage(2, []uint64{10})
	rightLeaf := leafPage(3, []uint64{20})

	err := NewBTree(pm).insertIntoParent(leftLeaf, 20, rightLeaf, []uint32{})
	if err == nil {
		t.Fatal("expected error from WritePage(newRoot), got nil")
	}
}

// ---- Parent has room ----

// TestInsertIntoParent_ParentHasRoom_SeparatorBeforeAllExisting verifies that a separator smaller
// than every existing key is inserted at slot 0 pointing to leftLeaf.
func TestInsertIntoParent_ParentHasRoom_SeparatorBeforeAllExisting(t *testing.T) {
	pm := newParentPM()
	C1, C2 := uint32(11), uint32(12)
	parent := makeInternalPageL(1, 1, []uint64{100}, []uint32{C1}, C2)
	pm.pages[1] = parent

	leftLeaf := leafPage(2, []uint64{10})
	rightLeaf := leafPage(5, []uint64{50})

	if err := NewBTree(pm).insertIntoParent(leftLeaf, 50, rightLeaf, []uint32{1}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	keys, children := internalPageEntries(t, parent)
	if !equalSlices(keys, []uint64{50, 100}) {
		t.Errorf("parent keys: want [50 100], got %v", keys)
	}
	// The separator record encodes (separatorKey, leftLeaf): keys ≤ separatorKey route to leftLeaf.
	if children[0] != leftLeaf.GetPageId() {
		t.Errorf("child[0]: want %d (leftLeaf), got %d", leftLeaf.GetPageId(), children[0])
	}
	if children[1] != C1 {
		t.Errorf("child[1]: want %d, got %d", C1, children[1])
	}
	if parent.GetRightMostChild() != C2 {
		t.Errorf("rightmost: want %d, got %d", C2, parent.GetRightMostChild())
	}
}

// TestInsertIntoParent_ParentHasRoom_SeparatorAfterAllExisting verifies that a separator larger
// than every existing key is appended as the last slot pointing to leftLeaf.
func TestInsertIntoParent_ParentHasRoom_SeparatorAfterAllExisting(t *testing.T) {
	pm := newParentPM()
	C1, C2 := uint32(11), uint32(12)
	parent := makeInternalPageL(1, 1, []uint64{50}, []uint32{C1}, C2)
	pm.pages[1] = parent

	leftLeaf := leafPage(2, []uint64{10})
	rightLeaf := leafPage(5, []uint64{100})

	if err := NewBTree(pm).insertIntoParent(leftLeaf, 100, rightLeaf, []uint32{1}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	keys, children := internalPageEntries(t, parent)
	if !equalSlices(keys, []uint64{50, 100}) {
		t.Errorf("parent keys: want [50 100], got %v", keys)
	}
	if children[0] != C1 {
		t.Errorf("child[0]: want %d, got %d", C1, children[0])
	}
	// The separator record encodes (separatorKey, leftLeaf): keys ≤ separatorKey route to leftLeaf.
	if children[1] != leftLeaf.GetPageId() {
		t.Errorf("child[1]: want %d (leftLeaf), got %d", leftLeaf.GetPageId(), children[1])
	}
	if parent.GetRightMostChild() != C2 {
		t.Errorf("rightmost: want %d, got %d", C2, parent.GetRightMostChild())
	}
}

// TestInsertIntoParent_ParentHasRoom_SeparatorBetweenExisting verifies correct mid-list insertion.
func TestInsertIntoParent_ParentHasRoom_SeparatorBetweenExisting(t *testing.T) {
	pm := newParentPM()
	C1, C2, C3 := uint32(11), uint32(12), uint32(13)
	parent := makeInternalPageL(1, 1, []uint64{20, 80}, []uint32{C1, C2}, C3)
	pm.pages[1] = parent

	leftLeaf := leafPage(2, []uint64{30})
	rightLeaf := leafPage(5, []uint64{50})

	if err := NewBTree(pm).insertIntoParent(leftLeaf, 50, rightLeaf, []uint32{1}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	keys, children := internalPageEntries(t, parent)
	if !equalSlices(keys, []uint64{20, 50, 80}) {
		t.Errorf("parent keys: want [20 50 80], got %v", keys)
	}
	if children[0] != C1 {
		t.Errorf("child[0]: want %d, got %d", C1, children[0])
	}
	// The separator record encodes (separatorKey, leftLeaf): keys ≤ separatorKey route to leftLeaf.
	if children[1] != leftLeaf.GetPageId() {
		t.Errorf("child[1]: want %d (leftLeaf), got %d", leftLeaf.GetPageId(), children[1])
	}
	if children[2] != C2 {
		t.Errorf("child[2]: want %d, got %d", C2, children[2])
	}
	if parent.GetRightMostChild() != C3 {
		t.Errorf("rightmost: want %d, got %d", C3, parent.GetRightMostChild())
	}
}

// TestInsertIntoParent_ParentHasRoom_ParentIsWritten verifies that the updated parent is persisted.
func TestInsertIntoParent_ParentHasRoom_ParentIsWritten(t *testing.T) {
	pm := newParentPM()
	parent := makeInternalPageL(1, 1, []uint64{100}, []uint32{11}, 12)
	pm.pages[1] = parent

	leftLeaf := leafPage(2, []uint64{10})
	rightLeaf := leafPage(5, []uint64{50})

	if err := NewBTree(pm).insertIntoParent(leftLeaf, 50, rightLeaf, []uint32{1}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pm.writeCount[1] == 0 {
		t.Error("expected WritePage to be called for the parent (page 1)")
	}
}

// TestInsertIntoParent_ParentHasRoom_RootIdUnchanged verifies that SetRootPageId is NOT called
// when inserting into a non-root parent.
func TestInsertIntoParent_ParentHasRoom_RootIdUnchanged(t *testing.T) {
	pm := newParentPM()
	pm.rootID = 99 // sentinel
	parent := makeInternalPageL(1, 1, []uint64{100}, []uint32{11}, 12)
	pm.pages[1] = parent

	leftLeaf := leafPage(2, []uint64{10})
	rightLeaf := leafPage(5, []uint64{50})

	if err := NewBTree(pm).insertIntoParent(leftLeaf, 50, rightLeaf, []uint32{1}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pm.rootID != 99 {
		t.Errorf("rootID changed unexpectedly: want 99, got %d", pm.rootID)
	}
}

// TestInsertIntoParent_ParentHasRoom_ReadPageError verifies that a ReadPage failure for the parent
// is propagated.
func TestInsertIntoParent_ParentHasRoom_ReadPageError(t *testing.T) {
	pm := newParentPM()
	pm.readErr[1] = fmt.Errorf("disk error on page 1")

	leftLeaf := leafPage(2, []uint64{10})
	rightLeaf := leafPage(5, []uint64{50})

	err := NewBTree(pm).insertIntoParent(leftLeaf, 50, rightLeaf, []uint32{1})
	if err == nil {
		t.Fatal("expected error from ReadPage, got nil")
	}
}

// TestInsertIntoParent_ParentHasRoom_WritePageError verifies that a WritePage failure on the
// parent is propagated.
func TestInsertIntoParent_ParentHasRoom_WritePageError(t *testing.T) {
	pm := newParentPM()
	parent := makeInternalPageL(1, 1, []uint64{100}, []uint32{11}, 12)
	pm.pages[1] = parent
	pm.writeErrByID[1] = fmt.Errorf("write failed")

	leftLeaf := leafPage(2, []uint64{10})
	rightLeaf := leafPage(5, []uint64{50})

	err := NewBTree(pm).insertIntoParent(leftLeaf, 50, rightLeaf, []uint32{1})
	if err == nil {
		t.Fatal("expected error from WritePage(parent), got nil")
	}
}

// TestInsertIntoParent_ParentHasRoom_DeepPath_OnlyDirectParentModified verifies that with a
// two-element path only the direct parent (last element) is modified; the grandparent is untouched.
func TestInsertIntoParent_ParentHasRoom_DeepPath_OnlyDirectParentModified(t *testing.T) {
	pm := newParentPM()
	grandparent := makeInternalPageL(1, 2, []uint64{200}, []uint32{2}, 3)
	parent := makeInternalPageL(2, 1, []uint64{100}, []uint32{4}, 5)
	pm.pages[1] = grandparent
	pm.pages[2] = parent

	leftLeaf := leafPage(4, []uint64{10})
	rightLeaf := leafPage(6, []uint64{50})

	if err := NewBTree(pm).insertIntoParent(leftLeaf, 50, rightLeaf, []uint32{1, 2}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Direct parent gains the new separator.
	parentKeys, _ := internalPageEntries(t, parent)
	if !equalSlices(parentKeys, []uint64{50, 100}) {
		t.Errorf("parent keys: want [50 100], got %v", parentKeys)
	}

	// Grandparent is unchanged.
	grandKeys, _ := internalPageEntries(t, grandparent)
	if !equalSlices(grandKeys, []uint64{200}) {
		t.Errorf("grandparent keys changed: got %v", grandKeys)
	}
	if pm.writeCount[1] != 0 {
		t.Errorf("grandparent written %d times, want 0", pm.writeCount[1])
	}
}

// ---- Parent full: cascading split ----

// TestInsertIntoParent_ParentFull_SingleAncestor_NewRootCreated verifies that a full parent is
// split and a new root is created when the path has only one ancestor.
func TestInsertIntoParent_ParentFull_SingleAncestor_NewRootCreated(t *testing.T) {
	pm := newParentPM()
	parent := makeInternalPageL(1, 1, nil, nil, 99)
	fillInternalToCapacity(parent, 1000) // keys 1000+; leaves room for sep < 1000
	pm.pages[1] = parent

	// Two allocations needed: sibling from splitInternal, then new root.
	newSibling := parentStubPage(20)
	newRoot := parentStubPage(21)
	pm.allocPool = []*pagemanager.Page{newSibling, newRoot}

	leftLeaf := leafPage(2, []uint64{10})
	rightLeaf := leafPage(3, []uint64{500})

	if err := NewBTree(pm).insertIntoParent(leftLeaf, 500, rightLeaf, []uint32{1}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pm.rootID != 21 {
		t.Errorf("new root ID: want 21, got %d", pm.rootID)
	}
	if newRoot.GetPageType() != pagemanager.PageTypeInternal {
		t.Errorf("new root type: want Internal, got %d", newRoot.GetPageType())
	}
	// New root is above a level-1 internal, so level must be 2.
	if newRoot.GetLevel() != 2 {
		t.Errorf("new root level: want 2, got %d", newRoot.GetLevel())
	}
	// New root has exactly one separator.
	if newRoot.GetRowCount() != 1 {
		t.Errorf("new root row count: want 1, got %d", newRoot.GetRowCount())
	}
	// New root rightmost = newSibling.
	if newRoot.GetRightMostChild() != newSibling.GetPageId() {
		t.Errorf("new root rightmost: want %d (newSibling), got %d", newSibling.GetPageId(), newRoot.GetRightMostChild())
	}
}

// TestInsertIntoParent_ParentFull_TwoAncestors_GrandparentAbsorbsSplit verifies that when the
// direct parent is full but the grandparent has room, the cascaded separator lands in the
// grandparent and no new root is created.
func TestInsertIntoParent_ParentFull_TwoAncestors_GrandparentAbsorbsSplit(t *testing.T) {
	pm := newParentPM()
	grandparent := makeInternalPageL(1, 2, []uint64{999}, []uint32{2}, 3)
	parent := makeInternalPageL(2, 1, nil, nil, 88)
	fillInternalToCapacity(parent, 1000)
	pm.pages[1] = grandparent
	pm.pages[2] = parent

	// One allocation: new sibling of the split parent.
	newSibling := parentStubPage(20)
	pm.allocPool = []*pagemanager.Page{newSibling}

	leftLeaf := leafPage(4, []uint64{10})
	rightLeaf := leafPage(5, []uint64{500})

	if err := NewBTree(pm).insertIntoParent(leftLeaf, 500, rightLeaf, []uint32{1, 2}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No new root should have been set.
	if pm.rootID != 0 {
		t.Errorf("rootID changed to %d; expected grandparent to absorb the split", pm.rootID)
	}

	// Grandparent now has two separators: the original and the one cascaded up from the split.
	grandKeys, _ := internalPageEntries(t, grandparent)
	if len(grandKeys) != 2 {
		t.Errorf("grandparent separator count: want 2, got %d (keys=%v)", len(grandKeys), grandKeys)
	}

	// Grandparent was written.
	if pm.writeCount[1] == 0 {
		t.Error("expected WritePage to be called for grandparent (page 1)")
	}
}

// TestInsertIntoParent_TwoLevelCascade_BothAncestorsFull_NewRootCreated verifies a two-level
// cascade where both the direct parent and grandparent are full, ultimately creating a new root.
func TestInsertIntoParent_TwoLevelCascade_BothAncestorsFull_NewRootCreated(t *testing.T) {
	pm := newParentPM()
	grandparent := makeInternalPageL(1, 2, nil, nil, 77)
	fillInternalToCapacity(grandparent, 2000)
	parent := makeInternalPageL(2, 1, nil, nil, 88)
	fillInternalToCapacity(parent, 1000)
	pm.pages[1] = grandparent
	pm.pages[2] = parent

	// Three allocations in cascade order:
	//   [0] sibling from splitInternal(parent)
	//   [1] sibling from splitInternal(grandparent)
	//   [2] new root
	pm.allocPool = []*pagemanager.Page{parentStubPage(30), parentStubPage(31), parentStubPage(32)}

	leftLeaf := leafPage(4, []uint64{10})
	rightLeaf := leafPage(5, []uint64{500})

	if err := NewBTree(pm).insertIntoParent(leftLeaf, 500, rightLeaf, []uint32{1, 2}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pm.rootID != 32 {
		t.Errorf("new root ID: want 32, got %d", pm.rootID)
	}
	newRoot := pm.allocPool[2]
	// New root sits above a level-2 grandparent, so level = 3.
	if newRoot.GetLevel() != 3 {
		t.Errorf("new root level: want 3, got %d", newRoot.GetLevel())
	}
	if newRoot.GetRowCount() != 1 {
		t.Errorf("new root row count: want 1, got %d", newRoot.GetRowCount())
	}
}

// TestInsertIntoParent_ParentFull_AllocatePageError verifies that an AllocatePage failure during
// the internal split of a full parent is propagated.
func TestInsertIntoParent_ParentFull_AllocatePageError(t *testing.T) {
	pm := newParentPM()
	parent := makeInternalPageL(1, 1, nil, nil, 99)
	fillInternalToCapacity(parent, 1000)
	pm.pages[1] = parent
	pm.allocErr = fmt.Errorf("disk full")

	leftLeaf := leafPage(2, []uint64{10})
	rightLeaf := leafPage(3, []uint64{500})

	err := NewBTree(pm).insertIntoParent(leftLeaf, 500, rightLeaf, []uint32{1})
	if err == nil {
		t.Fatal("expected error from AllocatePage during parent split, got nil")
	}
}

// =====================================
// Insert end-to-end tests
// =====================================
//
// These tests use a real on-disk PageManager so that page allocation, writes,
// and reads all exercise the actual storage path.  Each test creates its own
// temporary database file that is deleted automatically when the test ends.

// newTempBTree creates a BTree backed by a fresh on-disk PageManager in a
// temporary directory.  The database file is deleted when the test ends.
func newTempBTree(t *testing.T) (*BTree, pagemanager.PageManager) {
	t.Helper()
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { pm.Delete() })
	return NewBTree(pm), pm
}

// mustInsert calls Insert and fatals on any error.
func mustInsert(t *testing.T, bt *BTree, key uint64, fields []Field) {
	t.Helper()
	if err := bt.Insert(key, fields); err != nil {
		t.Fatalf("Insert(key=%d): %v", key, err)
	}
}

// assertFound searches for key and fatals if it is not found or the fields differ.
func assertFound(t *testing.T, bt *BTree, key uint64, want []Field) {
	t.Helper()
	got, found, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Search(%d): %v", key, err)
	}
	if !found {
		t.Fatalf("key %d: expected found=true, got false", key)
	}
	assertFields(t, got, want)
}

// assertMissing searches for key and fatals if it is found.
func assertMissing(t *testing.T, bt *BTree, key uint64) {
	t.Helper()
	_, found, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Search(%d): %v", key, err)
	}
	if found {
		t.Fatalf("key %d: expected found=false, got true", key)
	}
}

// verifyAllKeys checks that every entry in inserted can be found with its correct fields.
func verifyAllKeys(t *testing.T, bt *BTree, inserted map[uint64][]Field) {
	t.Helper()
	for k, fields := range inserted {
		assertFound(t, bt, k, fields)
	}
}

// verifyLeafChain walks the leaf sibling chain and asserts that every key in
// wantKeys appears exactly once, in ascending order, with no holes.
func verifyLeafChain(t *testing.T, bt *BTree, pm pagemanager.PageManager, wantKeys []uint64) {
	t.Helper()

	rootId := pm.GetRootPageId()
	if rootId == pagemanager.InvalidPageID {
		if len(wantKeys) != 0 {
			t.Fatalf("leaf chain: tree is empty but %d keys were expected", len(wantKeys))
		}
		return
	}

	// Walk to the leftmost leaf: always follow the first record's childId in internal pages.
	page, err := pm.ReadPage(rootId)
	if err != nil {
		t.Fatalf("ReadPage(root=%d): %v", rootId, err)
	}
	for page.GetPageType() != pagemanager.PageTypeLeaf {
		var childId uint32
		if rec, ok := page.GetRecord(0); ok {
			_, childId, _ = DecodeInternalRecord(rec)
		} else {
			childId = page.GetRightMostChild()
		}
		page, err = pm.ReadPage(childId)
		if err != nil {
			t.Fatalf("ReadPage(%d) walking to leftmost leaf: %v", childId, err)
		}
	}

	// Traverse siblings, collecting every key.
	var chainKeys []uint64
	for {
		count := int(page.GetRowCount())
		for i := 0; i < count; i++ {
			rec, ok := page.GetRecord(i)
			if !ok {
				t.Fatalf("GetRecord(%d) on leaf page %d failed", i, page.GetPageId())
			}
			k := RecordKey(rec)
			if len(chainKeys) > 0 && k <= chainKeys[len(chainKeys)-1] {
				t.Errorf("leaf chain out of order: key %d <= prev %d (page %d slot %d)",
					k, chainKeys[len(chainKeys)-1], page.GetPageId(), i)
			}
			chainKeys = append(chainKeys, k)
		}
		rightId := page.GetRightSibling()
		if rightId == pagemanager.InvalidPageID {
			break
		}
		page, err = pm.ReadPage(rightId)
		if err != nil {
			t.Fatalf("ReadPage(rightSibling=%d): %v", rightId, err)
		}
	}

	// Compare against a sorted copy of the expected keys.
	sorted := make([]uint64, len(wantKeys))
	copy(sorted, wantKeys)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	if len(chainKeys) != len(sorted) {
		t.Fatalf("leaf chain: got %d keys, want %d", len(chainKeys), len(sorted))
	}
	for i, k := range sorted {
		if chainKeys[i] != k {
			t.Errorf("leaf chain[%d]: got %d, want %d", i, chainKeys[i], k)
		}
	}
}

// repeatedStr returns a string of length n made of repeated character c, used
// to produce large field payloads that force leaf splits after fewer inserts.
func repeatedStr(c byte, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = c
	}
	return string(b)
}

// ---- Basic Insert ----

// TestInsert_EmptyTree_SingleRecord verifies the very first Insert creates a root leaf and finds the record.
func TestInsert_EmptyTree_SingleRecord(t *testing.T) {
	bt, _ := newTempBTree(t)
	want := []Field{strF(1, "hello"), intF(2, 99)}
	mustInsert(t, bt, 42, want)
	assertFound(t, bt, 42, want)
	assertMissing(t, bt, 41)
	assertMissing(t, bt, 43)
}

// TestInsert_EmptyTree_KeyZero verifies key=0 (minimum uint64) is stored and retrieved.
func TestInsert_EmptyTree_KeyZero(t *testing.T) {
	bt, _ := newTempBTree(t)
	want := []Field{intF(1, 0)}
	mustInsert(t, bt, 0, want)
	assertFound(t, bt, 0, want)
	assertMissing(t, bt, 1)
}

// TestInsert_EmptyTree_MaxUint64 verifies the maximum uint64 key is stored and retrieved.
func TestInsert_EmptyTree_MaxUint64(t *testing.T) {
	const maxKey = ^uint64(0)
	bt, _ := newTempBTree(t)
	want := []Field{strF(1, "max")}
	mustInsert(t, bt, maxKey, want)
	assertFound(t, bt, maxKey, want)
	assertMissing(t, bt, maxKey-1)
}

// TestInsert_NoFields stores and retrieves a key-only record with an empty field list.
func TestInsert_NoFields(t *testing.T) {
	bt, _ := newTempBTree(t)
	mustInsert(t, bt, 7, nil)
	got, found, err := bt.Search(7)
	if err != nil || !found {
		t.Fatalf("expected found, got found=%v err=%v", found, err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 fields, got %d", len(got))
	}
}

// TestInsert_NullField stores a record containing a null-typed field.
func TestInsert_NullField(t *testing.T) {
	bt, _ := newTempBTree(t)
	want := []Field{nullF(1)}
	mustInsert(t, bt, 55, want)
	assertFound(t, bt, 55, want)
}

// TestInsert_IntField stores a record with a negative int64 value.
func TestInsert_IntField(t *testing.T) {
	bt, _ := newTempBTree(t)
	want := []Field{intF(1, -9999999)}
	mustInsert(t, bt, 1, want)
	assertFound(t, bt, 1, want)
}

// TestInsert_StringField stores and retrieves a record with a multi-word string value.
func TestInsert_StringField(t *testing.T) {
	bt, _ := newTempBTree(t)
	want := []Field{strF(1, "hello world")}
	mustInsert(t, bt, 100, want)
	assertFound(t, bt, 100, want)
}

// TestInsert_ListField stores a record with a list<string> field and verifies element order.
func TestInsert_ListField(t *testing.T) {
	bt, _ := newTempBTree(t)
	ips := []Value{StringValue{V: "10.0.0.1"}, StringValue{V: "10.0.0.2"}, StringValue{V: "10.0.0.3"}}
	want := []Field{{Tag: 1, Value: ListValue{ElemType: FieldTypeString, Elems: ips}}}
	mustInsert(t, bt, 200, want)
	assertFound(t, bt, 200, want)
}

// TestInsert_AllFieldTypesInOneRecord stores a record containing all four field types.
func TestInsert_AllFieldTypesInOneRecord(t *testing.T) {
	bt, _ := newTempBTree(t)
	ips := []Value{StringValue{V: "192.168.1.1"}}
	want := []Field{
		strF(1, "alice"),
		intF(2, -42),
		nullF(3),
		{Tag: 4, Value: ListValue{ElemType: FieldTypeString, Elems: ips}},
	}
	mustInsert(t, bt, 300, want)
	assertFound(t, bt, 300, want)
}

// TestInsert_MultipleRecords_SinglePage inserts several records that all fit in the root
// leaf without splitting and verifies every key is found.
func TestInsert_MultipleRecords_SinglePage(t *testing.T) {
	bt, _ := newTempBTree(t)
	entries := []struct {
		key  uint64
		name string
		age  int64
	}{
		{1001, "Alice", 28},
		{1002, "Bob", 35},
		{1003, "Carol", 42},
		{1004, "Dave", 31},
		{1005, "Eve", 22},
	}
	for _, e := range entries {
		mustInsert(t, bt, e.key, []Field{strF(1, e.name), intF(2, e.age)})
	}
	for _, e := range entries {
		assertFound(t, bt, e.key, []Field{strF(1, e.name), intF(2, e.age)})
	}
	assertMissing(t, bt, 1000)
	assertMissing(t, bt, 1006)
}

// ---- Ordering ----

// TestInsert_Ascending_SmallSet inserts keys in ascending order and checks all are found
// along with correct leaf-chain ordering.
func TestInsert_Ascending_SmallSet(t *testing.T) {
	bt, pm := newTempBTree(t)
	keys := []uint64{10, 20, 30, 40, 50, 60, 70, 80}
	for _, k := range keys {
		mustInsert(t, bt, k, []Field{intF(1, int64(k)*10)})
	}
	for _, k := range keys {
		assertFound(t, bt, k, []Field{intF(1, int64(k)*10)})
	}
	verifyLeafChain(t, bt, pm, keys)
}

// TestInsert_Descending_SmallSet inserts keys in descending order; the tree must still
// route and store them in sorted order.
func TestInsert_Descending_SmallSet(t *testing.T) {
	bt, pm := newTempBTree(t)
	keys := []uint64{80, 70, 60, 50, 40, 30, 20, 10}
	for _, k := range keys {
		mustInsert(t, bt, k, []Field{intF(1, int64(k))})
	}
	for _, k := range keys {
		assertFound(t, bt, k, []Field{intF(1, int64(k))})
	}
	verifyLeafChain(t, bt, pm, keys)
}

// TestInsert_AlternatingHighLow inserts in an alternating high-low pattern, which causes
// new records to land in non-rightmost leaves once splits occur.
func TestInsert_AlternatingHighLow(t *testing.T) {
	bt, pm := newTempBTree(t)
	// Build a sequence that alternates between the top and bottom of the range.
	lo, hi := uint64(1), uint64(10000)
	var keys []uint64
	for lo < hi {
		keys = append(keys, lo, hi)
		lo++
		hi--
	}
	if lo == hi {
		keys = append(keys, lo)
	}
	for _, k := range keys {
		mustInsert(t, bt, k, []Field{intF(1, int64(k))})
	}
	for _, k := range keys {
		assertFound(t, bt, k, []Field{intF(1, int64(k))})
	}
	verifyLeafChain(t, bt, pm, keys)
}

// TestInsert_SparseKeys inserts keys that are spread very far apart (powers of two).
func TestInsert_SparseKeys(t *testing.T) {
	bt, pm := newTempBTree(t)
	var keys []uint64
	for shift := uint(0); shift < 60; shift++ {
		keys = append(keys, uint64(1)<<shift)
	}
	for _, k := range keys {
		mustInsert(t, bt, k, []Field{strF(1, fmt.Sprintf("2^%d", k))})
	}
	for i, k := range keys {
		assertFound(t, bt, k, []Field{strF(1, fmt.Sprintf("2^%d", k))})
		_ = i
	}
	verifyLeafChain(t, bt, pm, keys)
}

// TestInsert_ConsecutiveKeys inserts 1..300 in order; with small int records this triggers
// multiple leaf splits and tests the full split+propagate path.
func TestInsert_ConsecutiveKeys(t *testing.T) {
	bt, pm := newTempBTree(t)
	const n = 300
	var keys []uint64
	for i := uint64(1); i <= n; i++ {
		keys = append(keys, i)
		mustInsert(t, bt, i, []Field{intF(1, int64(i))})
	}
	for _, k := range keys {
		assertFound(t, bt, k, []Field{intF(1, int64(k))})
	}
	assertMissing(t, bt, 0)
	assertMissing(t, bt, n+1)
	verifyLeafChain(t, bt, pm, keys)
}

// TestInsert_ReverseConsecutiveKeys inserts 300..1 in descending order.
func TestInsert_ReverseConsecutiveKeys(t *testing.T) {
	bt, pm := newTempBTree(t)
	const n = 300
	var keys []uint64
	for i := uint64(n); i >= 1; i-- {
		keys = append(keys, i)
		mustInsert(t, bt, i, []Field{intF(1, int64(i)*-1)})
	}
	for i := uint64(1); i <= n; i++ {
		assertFound(t, bt, i, []Field{intF(1, int64(i)*-1)})
	}
	verifyLeafChain(t, bt, pm, keys)
}

// TestInsert_KeyZeroAndMaxUint64_Together stores both extreme keys in the same tree.
func TestInsert_KeyZeroAndMaxUint64_Together(t *testing.T) {
	const maxKey = ^uint64(0)
	bt, pm := newTempBTree(t)
	keys := []uint64{0, 1, 1000, maxKey - 1, maxKey}
	for _, k := range keys {
		mustInsert(t, bt, k, []Field{intF(1, int64(k))}) //nolint:gosec
	}
	for _, k := range keys {
		assertFound(t, bt, k, []Field{intF(1, int64(k))}) //nolint:gosec
	}
	verifyLeafChain(t, bt, pm, keys)
}

// ---- Update (duplicate key) ----

// TestInsert_Update_SameSize re-inserts the same key with a same-length value.
func TestInsert_Update_SameSize(t *testing.T) {
	bt, _ := newTempBTree(t)
	mustInsert(t, bt, 10, []Field{strF(1, "original")})
	mustInsert(t, bt, 10, []Field{strF(1, "replaced")}) // same length
	assertFound(t, bt, 10, []Field{strF(1, "replaced")})
}

// TestInsert_Update_SmallerValue replaces a record with a shorter string value.
func TestInsert_Update_SmallerValue(t *testing.T) {
	bt, _ := newTempBTree(t)
	mustInsert(t, bt, 20, []Field{strF(1, "a long string value here")})
	mustInsert(t, bt, 20, []Field{strF(1, "short")})
	assertFound(t, bt, 20, []Field{strF(1, "short")})
}

// TestInsert_Update_LargerValue replaces a record with a longer string value.
func TestInsert_Update_LargerValue(t *testing.T) {
	bt, _ := newTempBTree(t)
	mustInsert(t, bt, 30, []Field{strF(1, "x")})
	mustInsert(t, bt, 30, []Field{strF(1, "this is now a much longer replacement value")})
	assertFound(t, bt, 30, []Field{strF(1, "this is now a much longer replacement value")})
}

// TestInsert_Update_ChangeFieldType replaces an int field record with a string field record.
func TestInsert_Update_ChangeFieldType(t *testing.T) {
	bt, _ := newTempBTree(t)
	mustInsert(t, bt, 40, []Field{intF(1, 12345)})
	mustInsert(t, bt, 40, []Field{strF(1, "now a string")})
	assertFound(t, bt, 40, []Field{strF(1, "now a string")})
}

// TestInsert_Update_MultipleRounds updates the same key many times and verifies only
// the last value is retained.
func TestInsert_Update_MultipleRounds(t *testing.T) {
	bt, _ := newTempBTree(t)
	const key = uint64(99)
	for i := 0; i < 20; i++ {
		mustInsert(t, bt, key, []Field{intF(1, int64(i))})
	}
	assertFound(t, bt, key, []Field{intF(1, 19)})
}

// TestInsert_Update_NeighboursUnaffected verifies adjacent keys are not corrupted after an update.
func TestInsert_Update_NeighboursUnaffected(t *testing.T) {
	bt, _ := newTempBTree(t)
	mustInsert(t, bt, 10, []Field{strF(1, "left")})
	mustInsert(t, bt, 20, []Field{strF(1, "middle")})
	mustInsert(t, bt, 30, []Field{strF(1, "right")})

	mustInsert(t, bt, 20, []Field{strF(1, "updated")})

	assertFound(t, bt, 10, []Field{strF(1, "left")})
	assertFound(t, bt, 20, []Field{strF(1, "updated")})
	assertFound(t, bt, 30, []Field{strF(1, "right")})
}

// TestInsert_Update_AfterSplit updates a key that lands in the left half of a split leaf.
// Large payloads force a split after a few inserts (≈20 records with a 180-byte string).
func TestInsert_Update_AfterSplit(t *testing.T) {
	bt, _ := newTempBTree(t)
	bigVal := repeatedStr('x', 180)

	// Insert enough large records to force at least one leaf split.
	for i := uint64(1); i <= 25; i++ {
		mustInsert(t, bt, i, []Field{strF(1, bigVal)})
	}

	// Update a key from the left portion (before the split point).
	newVal := repeatedStr('y', 180)
	mustInsert(t, bt, 5, []Field{strF(1, newVal)})
	assertFound(t, bt, 5, []Field{strF(1, newVal)})

	// All other keys must still be readable.
	for i := uint64(1); i <= 25; i++ {
		if i == 5 {
			continue
		}
		assertFound(t, bt, i, []Field{strF(1, bigVal)})
	}
}

// TestInsert_Update_ThenInsertMore updates a key, then inserts additional keys around it.
func TestInsert_Update_ThenInsertMore(t *testing.T) {
	bt, pm := newTempBTree(t)
	mustInsert(t, bt, 50, []Field{strF(1, "original")})
	mustInsert(t, bt, 50, []Field{strF(1, "updated")})
	mustInsert(t, bt, 40, []Field{strF(1, "before")})
	mustInsert(t, bt, 60, []Field{strF(1, "after")})

	assertFound(t, bt, 40, []Field{strF(1, "before")})
	assertFound(t, bt, 50, []Field{strF(1, "updated")})
	assertFound(t, bt, 60, []Field{strF(1, "after")})
	verifyLeafChain(t, bt, pm, []uint64{40, 50, 60})
}

// ---- Leaf split scenarios ----

// TestInsert_FirstLeafSplit verifies the tree splits its root leaf once the page fills,
// producing a two-level tree where all keys remain searchable.
//
// With 180-byte string payloads each record consumes ≈196 bytes (record+slot).
// A 4096-byte leaf page has 4064 bytes of usable space, fitting floor(4064/196)=20 records.
// The 21st insert triggers the first split.
func TestInsert_FirstLeafSplit(t *testing.T) {
	bt, pm := newTempBTree(t)
	bigVal := repeatedStr('a', 180)
	var keys []uint64
	for i := uint64(1); i <= 25; i++ {
		keys = append(keys, i)
		mustInsert(t, bt, i, []Field{strF(1, bigVal)})
	}
	for _, k := range keys {
		assertFound(t, bt, k, []Field{strF(1, bigVal)})
	}
	verifyLeafChain(t, bt, pm, keys)
}

// TestInsert_MultipleSplits_AscendingOrder inserts enough records to trigger many leaf
// splits (300 records with small int payloads → ≈2 splits; verifies all are found).
func TestInsert_MultipleSplits_AscendingOrder(t *testing.T) {
	bt, pm := newTempBTree(t)
	var keys []uint64
	for i := uint64(1); i <= 300; i++ {
		keys = append(keys, i)
		mustInsert(t, bt, i, []Field{intF(1, int64(i))})
	}
	for _, k := range keys {
		assertFound(t, bt, k, []Field{intF(1, int64(k))})
	}
	verifyLeafChain(t, bt, pm, keys)
}

// TestInsert_MultipleSplits_DescendingOrder inserts 300 records in descending order,
// forcing splits on non-rightmost leaves.
func TestInsert_MultipleSplits_DescendingOrder(t *testing.T) {
	bt, pm := newTempBTree(t)
	var keys []uint64
	for i := uint64(300); i >= 1; i-- {
		keys = append(keys, i)
		mustInsert(t, bt, i, []Field{intF(1, int64(i))})
	}
	for i := uint64(1); i <= 300; i++ {
		assertFound(t, bt, i, []Field{intF(1, int64(i))})
	}
	verifyLeafChain(t, bt, pm, keys)
}

// TestInsert_MultipleSplits_LargePayload uses 180-byte strings to reach many splits
// quickly (≈20 records per leaf) and verifies the full set after all inserts.
func TestInsert_MultipleSplits_LargePayload(t *testing.T) {
	bt, pm := newTempBTree(t)
	bigVal := repeatedStr('z', 180)
	const n = 100
	var keys []uint64
	for i := uint64(1); i <= n; i++ {
		keys = append(keys, i)
		mustInsert(t, bt, i, []Field{strF(1, bigVal)})
	}
	for _, k := range keys {
		assertFound(t, bt, k, []Field{strF(1, bigVal)})
	}
	verifyLeafChain(t, bt, pm, keys)
}

// TestInsert_SplitAtSmallestKey inserts a very small key after the tree has already
// grown so that the new key targets a non-rightmost leaf.
func TestInsert_SplitAtSmallestKey(t *testing.T) {
	bt, pm := newTempBTree(t)
	bigVal := repeatedStr('b', 180)
	// Populate the tree with keys 100..124 to force at least one split.
	var keys []uint64
	for i := uint64(100); i <= 124; i++ {
		keys = append(keys, i)
		mustInsert(t, bt, i, []Field{strF(1, bigVal)})
	}
	// Now insert key=1, which routes to the very first (leftmost) leaf.
	mustInsert(t, bt, 1, []Field{strF(1, bigVal)})
	keys = append(keys, 1)

	for _, k := range keys {
		assertFound(t, bt, k, []Field{strF(1, bigVal)})
	}
	verifyLeafChain(t, bt, pm, keys)
}

// TestInsert_SplitAtLargestKey inserts a very large key after the tree is populated,
// targeting the rightmost leaf and triggering an append-only split.
func TestInsert_SplitAtLargestKey(t *testing.T) {
	const maxKey = ^uint64(0)
	bt, pm := newTempBTree(t)
	bigVal := repeatedStr('c', 180)
	var keys []uint64
	for i := uint64(1); i <= 24; i++ {
		keys = append(keys, i)
		mustInsert(t, bt, i, []Field{strF(1, bigVal)})
	}
	mustInsert(t, bt, maxKey, []Field{strF(1, bigVal)})
	keys = append(keys, maxKey)

	for _, k := range keys {
		assertFound(t, bt, k, []Field{strF(1, bigVal)})
	}
	verifyLeafChain(t, bt, pm, keys)
}

// ---- Real-life scenarios ----

// TestInsert_UserRegistry simulates a user registration system where rows are inserted
// as users sign up (roughly ascending IDs) and some users update their profile.
func TestInsert_UserRegistry(t *testing.T) {
	bt, pm := newTempBTree(t)

	type user struct {
		id    uint64
		name  string
		email string
		age   int64
		role  string
	}

	users := []user{
		{1001, "Alice Chen", "alice@example.com", 28, "user"},
		{1002, "Bob Smith", "bob@example.com", 35, "admin"},
		{1003, "Carol White", "carol@example.com", 42, "user"},
		{1004, "Dave Jones", "dave@example.com", 31, "moderator"},
		{1005, "Eve Brown", "eve@example.com", 22, "user"},
		{1006, "Frank Lee", "frank@example.com", 47, "user"},
		{1007, "Grace Park", "grace@example.com", 19, "user"},
		{1008, "Henry Liu", "henry@example.com", 55, "admin"},
	}

	userFields := func(u user) []Field {
		return []Field{strF(1, u.name), strF(2, u.email), intF(3, u.age), strF(4, u.role)}
	}

	// Register users.
	var keys []uint64
	for _, u := range users {
		keys = append(keys, u.id)
		mustInsert(t, bt, u.id, userFields(u))
	}

	// Verify initial state.
	for _, u := range users {
		assertFound(t, bt, u.id, userFields(u))
	}

	// Eve gets promoted to moderator (update existing record).
	users[4].role = "moderator"
	mustInsert(t, bt, users[4].id, userFields(users[4]))
	assertFound(t, bt, users[4].id, userFields(users[4]))

	// Alice changes her email (update with same-size value).
	users[0].email = "alice2@example.com"
	mustInsert(t, bt, users[0].id, userFields(users[0]))
	assertFound(t, bt, users[0].id, userFields(users[0]))

	// New user joins.
	newUser := user{1009, "Iris Wang", "iris@example.com", 26, "user"}
	keys = append(keys, newUser.id)
	mustInsert(t, bt, newUser.id, userFields(newUser))
	assertFound(t, bt, newUser.id, userFields(newUser))

	// Verify the whole tree is coherent.
	verifyLeafChain(t, bt, pm, keys)
	assertMissing(t, bt, 1000)
	assertMissing(t, bt, 1010)
}

// TestInsert_ECommerceOrders simulates an order-management system inserting orders
// with a mix of ascending (recent) and out-of-order (backfill) IDs.
func TestInsert_ECommerceOrders(t *testing.T) {
	bt, pm := newTempBTree(t)

	type order struct {
		id         uint64
		customerID int64
		totalCents int64
		status     string
	}

	orders := []order{
		{10001, 5001, 4999, "pending"},
		{10002, 5002, 12999, "shipped"},
		{10003, 5001, 799, "delivered"},
		{10004, 5003, 34999, "pending"},
		{9999, 5004, 999, "cancelled"},   // backfill: older order
		{10000, 5004, 4999, "delivered"}, // backfill
		{10005, 5002, 19999, "shipped"},
		{10006, 5001, 5999, "pending"},
	}

	orderFields := func(o order) []Field {
		return []Field{intF(1, o.customerID), intF(2, o.totalCents), strF(3, o.status)}
	}

	var keys []uint64
	for _, o := range orders {
		keys = append(keys, o.id)
		mustInsert(t, bt, o.id, orderFields(o))
	}

	for _, o := range orders {
		assertFound(t, bt, o.id, orderFields(o))
	}

	// Update order 10004: status changed to shipped.
	orders[3].status = "shipped"
	mustInsert(t, bt, orders[3].id, orderFields(orders[3]))
	assertFound(t, bt, orders[3].id, orderFields(orders[3]))

	verifyLeafChain(t, bt, pm, keys)
	assertMissing(t, bt, 9998)
	assertMissing(t, bt, 10007)
}

// TestInsert_TimeSeries simulates a time-series store where sensor readings arrive
// in timestamp order (strictly ascending).  With enough readings, multiple splits occur.
func TestInsert_TimeSeries(t *testing.T) {
	bt, pm := newTempBTree(t)

	const baseTS = uint64(1_700_000_000)
	const interval = uint64(60) // one reading per minute

	type reading struct {
		ts    uint64
		value int64
	}

	var readings []reading
	for i := 0; i < 250; i++ {
		readings = append(readings, reading{baseTS + uint64(i)*interval, int64(i * 100)})
	}

	var keys []uint64
	for _, r := range readings {
		keys = append(keys, r.ts)
		mustInsert(t, bt, r.ts, []Field{intF(1, r.value)})
	}
	for _, r := range readings {
		assertFound(t, bt, r.ts, []Field{intF(1, r.value)})
	}

	// A reading that arrived late (slightly out of order).
	lateTS := baseTS + 30 // between first and second reading
	mustInsert(t, bt, lateTS, []Field{intF(1, 9999)})
	assertFound(t, bt, lateTS, []Field{intF(1, 9999)})
	keys = append(keys, lateTS)

	verifyLeafChain(t, bt, pm, keys)
}

// TestInsert_SensorData simulates multiple sensors writing readings into a single table
// keyed by (sensorID << 32 | sequence), producing a non-sequential insert order.
func TestInsert_SensorData(t *testing.T) {
	bt, pm := newTempBTree(t)

	sensorIDs := []uint64{1, 2, 3, 4}
	const seqPerSensor = 40

	var keys []uint64
	// Insert sensor readings round-robin across all sensors.
	for seq := uint64(0); seq < seqPerSensor; seq++ {
		for _, sid := range sensorIDs {
			k := (sid << 32) | seq
			keys = append(keys, k)
			mustInsert(t, bt, k, []Field{intF(1, int64(seq)), intF(2, int64(sid))})
		}
	}

	// Verify every reading is found with the right sensor ID field.
	for _, sid := range sensorIDs {
		for seq := uint64(0); seq < seqPerSensor; seq++ {
			k := (sid << 32) | seq
			assertFound(t, bt, k, []Field{intF(1, int64(seq)), intF(2, int64(sid))})
		}
	}

	verifyLeafChain(t, bt, pm, keys)
}

// TestInsert_ProductCatalog simulates a product catalog with sparse SKUs (large gaps
// between product IDs) and occasional price updates.
func TestInsert_ProductCatalog(t *testing.T) {
	bt, pm := newTempBTree(t)

	type product struct {
		sku      uint64
		name     string
		price    int64
		category string
		stock    int64
	}

	catalog := []product{
		{1000100, "Wireless Mouse", 2999, "Electronics", 150},
		{1000200, "USB-C Keyboard", 4999, "Electronics", 80},
		{2000050, "HDMI Cable 2m", 899, "Accessories", 300},
		{2000300, "Laptop Stand", 3499, "Accessories", 60},
		{3000010, "Webcam HD", 5999, "Electronics", 45},
		{3000500, "Desk Lamp", 1999, "Office", 200},
		{5000001, "Ergonomic Chair", 59999, "Furniture", 12},
		{5000999, "Standing Desk", 69999, "Furniture", 8},
		{9999999, "Premium Headphones", 24999, "Electronics", 30},
	}

	productFields := func(p product) []Field {
		return []Field{strF(1, p.name), intF(2, p.price), strF(3, p.category), intF(4, p.stock)}
	}

	var keys []uint64
	for _, p := range catalog {
		keys = append(keys, p.sku)
		mustInsert(t, bt, p.sku, productFields(p))
	}

	for _, p := range catalog {
		assertFound(t, bt, p.sku, productFields(p))
	}

	// Price update: Webcam HD goes on sale.
	catalog[4].price = 3999
	mustInsert(t, bt, catalog[4].sku, productFields(catalog[4]))
	assertFound(t, bt, catalog[4].sku, productFields(catalog[4]))

	// Stock update: Ergonomic Chair restocked.
	catalog[6].stock = 50
	mustInsert(t, bt, catalog[6].sku, productFields(catalog[6]))
	assertFound(t, bt, catalog[6].sku, productFields(catalog[6]))

	// Discontinued SKUs must not appear.
	assertMissing(t, bt, 1000150)
	assertMissing(t, bt, 4000000)

	verifyLeafChain(t, bt, pm, keys)
}

// TestInsert_SessionStore simulates a session store where session tokens (large random-ish
// uint64 IDs) are inserted, looked up, and occasionally refreshed.
func TestInsert_SessionStore(t *testing.T) {
	bt, pm := newTempBTree(t)

	// Simulate session token IDs (large, spread-out values).
	sessionIDs := []uint64{
		0xABCDEF0123456789,
		0x0123456789ABCDEF,
		0xFEDCBA9876543210,
		0x1111222233334444,
		0xAAAABBBBCCCCDDDD,
		0xDEADBEEFCAFEBABE,
		0x0000000100000001,
		0xFFFF000000000001,
	}

	for _, sid := range sessionIDs {
		mustInsert(t, bt, sid, []Field{intF(1, int64(sid>>32)), strF(2, "user-agent-string")}) //nolint:gosec
	}
	for _, sid := range sessionIDs {
		assertFound(t, bt, sid, []Field{intF(1, int64(sid>>32)), strF(2, "user-agent-string")}) //nolint:gosec
	}

	// Refresh two sessions (update).
	mustInsert(t, bt, sessionIDs[0], []Field{intF(1, int64(sessionIDs[0]>>32)), strF(2, "refreshed-agent")})  //nolint:gosec
	assertFound(t, bt, sessionIDs[0], []Field{intF(1, int64(sessionIDs[0]>>32)), strF(2, "refreshed-agent")}) //nolint:gosec

	verifyLeafChain(t, bt, pm, sessionIDs)

	// Non-existent sessions must return miss.
	assertMissing(t, bt, 0x9999999999999999)
	assertMissing(t, bt, 0x0)
}

// TestInsert_AuditLog simulates an audit-event log with strictly monotone event IDs and
// string payloads of varying size; exercises many leaf splits.
func TestInsert_AuditLog(t *testing.T) {
	bt, pm := newTempBTree(t)

	eventTypes := []string{"LOGIN", "LOGOUT", "PURCHASE", "ADMIN_ACTION", "PASSWORD_CHANGE",
		"FAILED_LOGIN", "MFA_ENABLED", "API_CALL", "EXPORT", "BACKUP"}

	const numEvents = 200
	var keys []uint64
	for i := uint64(1); i <= numEvents; i++ {
		etype := eventTypes[i%uint64(len(eventTypes))]
		fields := []Field{
			strF(1, etype),
			intF(2, int64(1_700_000_000+i)),
			strF(3, fmt.Sprintf("192.168.%d.%d", i/256, i%256)),
			intF(4, int64(1000+i%50)),
		}
		keys = append(keys, i)
		mustInsert(t, bt, i, fields)
	}

	for i := uint64(1); i <= numEvents; i++ {
		etype := eventTypes[i%uint64(len(eventTypes))]
		assertFound(t, bt, i, []Field{
			strF(1, etype),
			intF(2, int64(1_700_000_000+i)),
			strF(3, fmt.Sprintf("192.168.%d.%d", i/256, i%256)),
			intF(4, int64(1000+i%50)),
		})
	}

	verifyLeafChain(t, bt, pm, keys)
}

// TestInsert_MixedInsertAndUpdate interleaves new inserts with updates to already-inserted
// keys, verifying the tree always reflects the latest value for each key.
func TestInsert_MixedInsertAndUpdate(t *testing.T) {
	bt, pm := newTempBTree(t)

	// Round 1: insert keys 1..20.
	for i := uint64(1); i <= 20; i++ {
		mustInsert(t, bt, i, []Field{intF(1, int64(i)), strF(2, "v1")})
	}

	// Round 2: update every even key; insert keys 21..40.
	for i := uint64(1); i <= 40; i++ {
		if i <= 20 && i%2 == 0 {
			mustInsert(t, bt, i, []Field{intF(1, int64(i)), strF(2, "v2")})
		} else if i > 20 {
			mustInsert(t, bt, i, []Field{intF(1, int64(i)), strF(2, "v1")})
		}
	}

	// Verify: even keys ≤20 have "v2"; all others have "v1".
	for i := uint64(1); i <= 40; i++ {
		version := "v1"
		if i <= 20 && i%2 == 0 {
			version = "v2"
		}
		assertFound(t, bt, i, []Field{intF(1, int64(i)), strF(2, version)})
	}

	var keys []uint64
	for i := uint64(1); i <= 40; i++ {
		keys = append(keys, i)
	}
	verifyLeafChain(t, bt, pm, keys)
}
