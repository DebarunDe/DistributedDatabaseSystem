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
	t.Cleanup(func() { _ = pm.Delete() })
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

// ── redistributeLeaf helpers ───────────────────────────────────────────────────

// mustReadPage re-reads a page from the page manager, fataling on error.
func mustReadPage(t *testing.T, pm pagemanager.PageManager, id uint32) *pagemanager.Page {
	t.Helper()
	p, err := pm.ReadPage(id)
	if err != nil {
		t.Fatalf("ReadPage(%d): %v", id, err)
	}
	return p
}

// redistLeafKeys returns all keys from a leaf page in slot order.
func redistLeafKeys(t *testing.T, page *pagemanager.Page) []uint64 {
	t.Helper()
	keys := make([]uint64, page.GetRowCount())
	for i := range keys {
		rec, ok := page.GetRecord(i)
		if !ok {
			t.Fatalf("GetRecord(%d) failed on page %d", i, page.GetPageId())
		}
		keys[i] = RecordKey(rec)
	}
	return keys
}

// redistParentSepKey scans an internal page for the record whose child pointer
// equals childId, returning the key and true if found.
func redistParentSepKey(t *testing.T, parent *pagemanager.Page, childId uint32) (uint64, bool) {
	t.Helper()
	for i := 0; i < int(parent.GetRowCount()); i++ {
		rec, ok := parent.GetRecord(i)
		if !ok {
			continue
		}
		k, cid, err := DecodeInternalRecord(rec)
		if err != nil {
			t.Fatalf("DecodeInternalRecord(slot %d): %v", i, err)
		}
		if cid == childId {
			return k, true
		}
	}
	return 0, false
}

// setupRedistLeaf builds two adjacent sibling leaf pages and a parent internal
// page, writes all of them to a fresh DB, and returns the BTree and pages.
// leftKeys/rightKeys must be pre-sorted with all leftKeys < all rightKeys.
// When rightIsRightMost is true, rightPage is the parent's rightMostChild and
// has no separator record of its own; otherwise a dummy ceiling leaf is created
// as the rightMostChild so rightPage has its own record in the parent.
func setupRedistLeaf(
	t *testing.T,
	leftKeys, rightKeys []uint64,
	rightIsRightMost bool,
) (bt *BTree, pm pagemanager.PageManager, leftPage, rightPage, parentPage *pagemanager.Page) {
	t.Helper()
	var err error
	pm, err = pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	leftPage, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(left): %v", err)
	}
	rightPage, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(right): %v", err)
	}
	leftId, rightId := leftPage.GetPageId(), rightPage.GetPageId()

	*leftPage = *pagemanager.NewLeafPage(leftId, pagemanager.InvalidPageID, rightId)
	*rightPage = *pagemanager.NewLeafPage(rightId, leftId, pagemanager.InvalidPageID)

	for _, k := range leftKeys {
		if _, ok := leftPage.InsertRecord(makeRecord(k, []byte("v"))); !ok {
			t.Fatalf("InsertRecord(left, key=%d) failed", k)
		}
	}
	for _, k := range rightKeys {
		if _, ok := rightPage.InsertRecord(makeRecord(k, []byte("v"))); !ok {
			t.Fatalf("InsertRecord(right, key=%d) failed", k)
		}
	}

	rightMostId := rightId
	if !rightIsRightMost {
		ceilPage, err := pm.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage(ceil): %v", err)
		}
		ceilId := ceilPage.GetPageId()
		*ceilPage = *pagemanager.NewLeafPage(ceilId, rightId, pagemanager.InvalidPageID)
		if err := pm.WritePage(ceilPage); err != nil {
			t.Fatalf("WritePage(ceil): %v", err)
		}
		rightMostId = ceilId
	}

	parentPage, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(parent): %v", err)
	}
	*parentPage = *pagemanager.NewInternalPage(parentPage.GetPageId(), 1, rightMostId)

	// Records are inserted in ascending key order so the slot array is sorted,
	// satisfying searchInternal's binary search invariant.
	maxLeft := leftKeys[len(leftKeys)-1]
	parentPage.InsertRecord(EncodeInternalRecord(maxLeft, leftId))
	if !rightIsRightMost {
		maxRight := rightKeys[len(rightKeys)-1]
		parentPage.InsertRecord(EncodeInternalRecord(maxRight, rightId))
	}

	for _, p := range []*pagemanager.Page{leftPage, rightPage, parentPage} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}
	bt = NewBTree(pm)
	return
}

// ── TestRedistributeLeaf cases ────────────────────────────────────────────────

// TestRedistributeLeaf_LeftHeavy_RightMostChild: left=[1,2,3], right=[10],
// rightPage is parent's rightMostChild.
// total=4, mid=2 → left=[1,2], right=[3,10]; separator updated 3→2.
func TestRedistributeLeaf_LeftHeavy_RightMostChild(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistLeaf(t,
		[]uint64{1, 2, 3}, []uint64{10}, true)

	if err := bt.redistributeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeLeaf: %v", err)
	}

	leftId, rightId := leftPage.GetPageId(), rightPage.GetPageId()
	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightId)
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	if !equalSlices(redistLeafKeys(t, left), []uint64{1, 2}) {
		t.Errorf("left keys: got %v, want [1 2]", redistLeafKeys(t, left))
	}
	if !equalSlices(redistLeafKeys(t, right), []uint64{3, 10}) {
		t.Errorf("right keys: got %v, want [3 10]", redistLeafKeys(t, right))
	}

	sep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator record for leftPage in parent")
	}
	if sep != 2 {
		t.Errorf("separator key: got %d, want 2", sep)
	}
	if parent.GetRightMostChild() != rightId {
		t.Errorf("rightMostChild changed: got %d, want %d", parent.GetRightMostChild(), rightId)
	}
}

// TestRedistributeLeaf_RightHeavy_RightMostChild: left=[1], right=[10,20,30],
// rightPage is parent's rightMostChild.
// total=4, mid=2 → left=[1,10], right=[20,30]; separator updated 1→10.
func TestRedistributeLeaf_RightHeavy_RightMostChild(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistLeaf(t,
		[]uint64{1}, []uint64{10, 20, 30}, true)

	if err := bt.redistributeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeLeaf: %v", err)
	}

	leftId, rightId := leftPage.GetPageId(), rightPage.GetPageId()
	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightId)
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	if !equalSlices(redistLeafKeys(t, left), []uint64{1, 10}) {
		t.Errorf("left keys: got %v, want [1 10]", redistLeafKeys(t, left))
	}
	if !equalSlices(redistLeafKeys(t, right), []uint64{20, 30}) {
		t.Errorf("right keys: got %v, want [20 30]", redistLeafKeys(t, right))
	}

	sep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator for leftPage in parent")
	}
	if sep != 10 {
		t.Errorf("separator key: got %d, want 10", sep)
	}
	if parent.GetRightMostChild() != rightId {
		t.Errorf("rightMostChild changed: got %d, want %d", parent.GetRightMostChild(), rightId)
	}
}

// TestRedistributeLeaf_LeftHeavy_NotRightMostChild: same distribution as
// LeftHeavy above, but rightPage is not the rightMostChild (has its own record).
// Also verifies rightPage's own separator is left untouched.
func TestRedistributeLeaf_LeftHeavy_NotRightMostChild(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistLeaf(t,
		[]uint64{1, 2, 3}, []uint64{10}, false)

	if err := bt.redistributeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeLeaf: %v", err)
	}

	leftId, rightId := leftPage.GetPageId(), rightPage.GetPageId()
	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightId)
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	if !equalSlices(redistLeafKeys(t, left), []uint64{1, 2}) {
		t.Errorf("left keys: got %v, want [1 2]", redistLeafKeys(t, left))
	}
	if !equalSlices(redistLeafKeys(t, right), []uint64{3, 10}) {
		t.Errorf("right keys: got %v, want [3 10]", redistLeafKeys(t, right))
	}

	sep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator for leftPage in parent")
	}
	if sep != 2 {
		t.Errorf("left separator: got %d, want 2", sep)
	}

	// rightPage's own separator key must not change.
	rightSep, found := redistParentSepKey(t, parent, rightId)
	if !found {
		t.Fatal("no separator for rightPage in parent")
	}
	if rightSep != 10 {
		t.Errorf("right separator: got %d, want 10 (unchanged)", rightSep)
	}
}

// TestRedistributeLeaf_RightHeavy_NotRightMostChild: left=[1], right=[10,20,30],
// rightPage is not the rightMostChild.
func TestRedistributeLeaf_RightHeavy_NotRightMostChild(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistLeaf(t,
		[]uint64{1}, []uint64{10, 20, 30}, false)

	if err := bt.redistributeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeLeaf: %v", err)
	}

	leftId, rightId := leftPage.GetPageId(), rightPage.GetPageId()
	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightId)
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	if !equalSlices(redistLeafKeys(t, left), []uint64{1, 10}) {
		t.Errorf("left keys: got %v, want [1 10]", redistLeafKeys(t, left))
	}
	if !equalSlices(redistLeafKeys(t, right), []uint64{20, 30}) {
		t.Errorf("right keys: got %v, want [20 30]", redistLeafKeys(t, right))
	}

	sep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator for leftPage in parent")
	}
	if sep != 10 {
		t.Errorf("left separator: got %d, want 10", sep)
	}

	rightSep, found := redistParentSepKey(t, parent, rightId)
	if !found {
		t.Fatal("no separator for rightPage in parent")
	}
	if rightSep != 30 {
		t.Errorf("right separator: got %d, want 30 (unchanged)", rightSep)
	}
}

// TestRedistributeLeaf_EqualSizes: both pages have the same record count.
// Distribution is unchanged; separator key is rewritten with same value.
func TestRedistributeLeaf_EqualSizes(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistLeaf(t,
		[]uint64{10, 20}, []uint64{30, 40}, true)

	if err := bt.redistributeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeLeaf: %v", err)
	}

	leftId := leftPage.GetPageId()
	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightPage.GetPageId())
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	if !equalSlices(redistLeafKeys(t, left), []uint64{10, 20}) {
		t.Errorf("left keys: got %v, want [10 20]", redistLeafKeys(t, left))
	}
	if !equalSlices(redistLeafKeys(t, right), []uint64{30, 40}) {
		t.Errorf("right keys: got %v, want [30 40]", redistLeafKeys(t, right))
	}

	sep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator for leftPage in parent")
	}
	if sep != 20 {
		t.Errorf("separator key: got %d, want 20", sep)
	}
}

// TestRedistributeLeaf_OddTotal: total=5, left gets floor(5/2)=2, right gets 3.
func TestRedistributeLeaf_OddTotal(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistLeaf(t,
		[]uint64{5}, []uint64{10, 20, 30, 40}, true)

	if err := bt.redistributeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeLeaf: %v", err)
	}

	leftId := leftPage.GetPageId()
	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightPage.GetPageId())
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	if !equalSlices(redistLeafKeys(t, left), []uint64{5, 10}) {
		t.Errorf("left keys: got %v, want [5 10]", redistLeafKeys(t, left))
	}
	if !equalSlices(redistLeafKeys(t, right), []uint64{20, 30, 40}) {
		t.Errorf("right keys: got %v, want [20 30 40]", redistLeafKeys(t, right))
	}

	sep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator for leftPage in parent")
	}
	if sep != 10 {
		t.Errorf("separator key: got %d, want 10", sep)
	}
}

// TestRedistributeLeaf_TwoRecordsTotal: minimum case — one record per page.
// After redistribution each page still holds one record; separator stays put.
func TestRedistributeLeaf_TwoRecordsTotal(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistLeaf(t,
		[]uint64{1}, []uint64{2}, true)

	if err := bt.redistributeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeLeaf: %v", err)
	}

	leftId := leftPage.GetPageId()
	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightPage.GetPageId())
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	if !equalSlices(redistLeafKeys(t, left), []uint64{1}) {
		t.Errorf("left keys: got %v, want [1]", redistLeafKeys(t, left))
	}
	if !equalSlices(redistLeafKeys(t, right), []uint64{2}) {
		t.Errorf("right keys: got %v, want [2]", redistLeafKeys(t, right))
	}

	sep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator for leftPage in parent")
	}
	if sep != 1 {
		t.Errorf("separator key: got %d, want 1", sep)
	}
}

// TestRedistributeLeaf_SiblingLinksPreserved verifies that left/right sibling
// pointers on both pages are not modified during redistribution.
func TestRedistributeLeaf_SiblingLinksPreserved(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistLeaf(t,
		[]uint64{1, 2, 3}, []uint64{10, 20}, true)

	leftId, rightId := leftPage.GetPageId(), rightPage.GetPageId()

	if err := bt.redistributeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeLeaf: %v", err)
	}

	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightId)

	if left.GetRightSibling() != rightId {
		t.Errorf("left.rightSibling: got %d, want %d", left.GetRightSibling(), rightId)
	}
	if left.GetLeftSibling() != pagemanager.InvalidPageID {
		t.Errorf("left.leftSibling: got %d, want InvalidPageID", left.GetLeftSibling())
	}
	if right.GetLeftSibling() != leftId {
		t.Errorf("right.leftSibling: got %d, want %d", right.GetLeftSibling(), leftId)
	}
	if right.GetRightSibling() != pagemanager.InvalidPageID {
		t.Errorf("right.rightSibling: got %d, want InvalidPageID", right.GetRightSibling())
	}
}

// TestRedistributeLeaf_RecordsRemainSorted verifies that both leaf pages have
// their keys in ascending order after redistribution.
func TestRedistributeLeaf_RecordsRemainSorted(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistLeaf(t,
		[]uint64{2, 4, 6, 8}, []uint64{10}, true)

	if err := bt.redistributeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeLeaf: %v", err)
	}

	for _, id := range []uint32{leftPage.GetPageId(), rightPage.GetPageId()} {
		page := mustReadPage(t, pm, id)
		keys := redistLeafKeys(t, page)
		for i := 1; i < len(keys); i++ {
			if keys[i] <= keys[i-1] {
				t.Errorf("page %d: keys out of order at index %d: %v", id, i, keys)
			}
		}
	}
}

// TestRedistributeLeaf_RecordValuesIntact verifies full record payloads (not
// just keys) survive redistribution without corruption.
func TestRedistributeLeaf_RecordValuesIntact(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })
	bt := NewBTree(pm)

	leftPage, _ := pm.AllocatePage()
	rightPage, _ := pm.AllocatePage()
	leftId, rightId := leftPage.GetPageId(), rightPage.GetPageId()
	*leftPage = *pagemanager.NewLeafPage(leftId, pagemanager.InvalidPageID, rightId)
	*rightPage = *pagemanager.NewLeafPage(rightId, leftId, pagemanager.InvalidPageID)

	type entry struct {
		key uint64
		val string
	}
	leftEntries := []entry{{1, "alpha"}, {2, "beta"}, {3, "gamma"}}
	rightEntries := []entry{{20, "delta"}}

	for _, e := range leftEntries {
		leftPage.InsertRecord(makeRecord(e.key, []byte(e.val)))
	}
	for _, e := range rightEntries {
		rightPage.InsertRecord(makeRecord(e.key, []byte(e.val)))
	}

	parentPage, _ := pm.AllocatePage()
	*parentPage = *pagemanager.NewInternalPage(parentPage.GetPageId(), 1, rightId)
	parentPage.InsertRecord(EncodeInternalRecord(3, leftId))
	if err := pm.WritePage(leftPage); err != nil {
		t.Fatalf("WritePage(leftPage): %v", err)
	}
	if err := pm.WritePage(rightPage); err != nil {
		t.Fatalf("WritePage(rightPage): %v", err)
	}
	if err := pm.WritePage(parentPage); err != nil {
		t.Fatalf("WritePage(parentPage): %v", err)
	}

	if err := bt.redistributeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeLeaf: %v", err)
	}

	// total=4, mid=2 → left=[{1,"alpha"},{2,"beta"}], right=[{3,"gamma"},{20,"delta"}]
	wantVals := map[uint64]string{1: "alpha", 2: "beta", 3: "gamma", 20: "delta"}
	for _, id := range []uint32{leftId, rightId} {
		page := mustReadPage(t, pm, id)
		for i := 0; i < int(page.GetRowCount()); i++ {
			rec, ok := page.GetRecord(i)
			if !ok {
				t.Fatalf("page %d slot %d: GetRecord failed", id, i)
			}
			k := RecordKey(rec)
			if got, want := string(rec[8:]), wantVals[k]; got != want {
				t.Errorf("key %d value: got %q, want %q", k, got, want)
			}
		}
	}
}

// TestRedistributeLeaf_PagesWrittenToDisk verifies all three pages are
// durably written by re-reading them from the page manager.
func TestRedistributeLeaf_PagesWrittenToDisk(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistLeaf(t,
		[]uint64{1, 2, 3}, []uint64{10}, true)

	leftId := leftPage.GetPageId()
	rightId := rightPage.GetPageId()
	parentId := parentPage.GetPageId()

	if err := bt.redistributeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeLeaf: %v", err)
	}

	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightId)
	parent := mustReadPage(t, pm, parentId)

	if left.GetRowCount() != 2 {
		t.Errorf("left row count: got %d, want 2", left.GetRowCount())
	}
	if right.GetRowCount() != 2 {
		t.Errorf("right row count: got %d, want 2", right.GetRowCount())
	}
	if parent.GetRowCount() != 1 {
		t.Errorf("parent row count: got %d, want 1", parent.GetRowCount())
	}
}

// TestRedistributeLeaf_MultipleChildren verifies that when the parent has
// several children, only leftPage's separator is updated and all others are
// left untouched.
func TestRedistributeLeaf_MultipleChildren(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })
	bt := NewBTree(pm)

	alloc := func() *pagemanager.Page {
		p, err := pm.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage: %v", err)
		}
		return p
	}

	// Four leaf pages: A (left, heavy), B (right), C (unchanged sibling), D (rightMostChild).
	a, b, c, d := alloc(), alloc(), alloc(), alloc()
	aId, bId, cId, dId := a.GetPageId(), b.GetPageId(), c.GetPageId(), d.GetPageId()

	*a = *pagemanager.NewLeafPage(aId, pagemanager.InvalidPageID, bId)
	*b = *pagemanager.NewLeafPage(bId, aId, cId)
	*c = *pagemanager.NewLeafPage(cId, bId, pagemanager.InvalidPageID)
	*d = *pagemanager.NewLeafPage(dId, pagemanager.InvalidPageID, pagemanager.InvalidPageID)

	for _, k := range []uint64{1, 2, 3, 4} {
		a.InsertRecord(makeRecord(k, []byte("v")))
	}
	b.InsertRecord(makeRecord(50, []byte("v")))
	c.InsertRecord(makeRecord(100, []byte("v")))

	// Parent: (4,aId), (50,bId), (100,cId), rightMostChild=dId
	parent := alloc()
	*parent = *pagemanager.NewInternalPage(parent.GetPageId(), 1, dId)
	parent.InsertRecord(EncodeInternalRecord(4, aId))
	parent.InsertRecord(EncodeInternalRecord(50, bId))
	parent.InsertRecord(EncodeInternalRecord(100, cId))

	for _, p := range []*pagemanager.Page{a, b, c, d, parent} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}

	if err := bt.redistributeLeaf(a, b, parent); err != nil {
		t.Fatalf("redistributeLeaf: %v", err)
	}

	// total=5, mid=2 → A=[1,2], B=[3,4,50]
	pa := mustReadPage(t, pm, parent.GetPageId())
	aPage := mustReadPage(t, pm, aId)
	bPage := mustReadPage(t, pm, bId)

	if !equalSlices(redistLeafKeys(t, aPage), []uint64{1, 2}) {
		t.Errorf("A keys: got %v, want [1 2]", redistLeafKeys(t, aPage))
	}
	if !equalSlices(redistLeafKeys(t, bPage), []uint64{3, 4, 50}) {
		t.Errorf("B keys: got %v, want [3 4 50]", redistLeafKeys(t, bPage))
	}

	aSep, found := redistParentSepKey(t, pa, aId)
	if !found {
		t.Fatal("no separator for A in parent")
	}
	if aSep != 2 {
		t.Errorf("A separator: got %d, want 2", aSep)
	}

	bSep, found := redistParentSepKey(t, pa, bId)
	if !found {
		t.Fatal("no separator for B in parent")
	}
	if bSep != 50 {
		t.Errorf("B separator: got %d, want 50 (unchanged)", bSep)
	}

	cSep, found := redistParentSepKey(t, pa, cId)
	if !found {
		t.Fatal("no separator for C in parent")
	}
	if cSep != 100 {
		t.Errorf("C separator: got %d, want 100 (unchanged)", cSep)
	}

	if pa.GetRightMostChild() != dId {
		t.Errorf("rightMostChild: got %d, want %d", pa.GetRightMostChild(), dId)
	}
}

// ── redistributeInternal helpers ─────────────────────────────────────────────

// redistInternalKeys returns the keys from an internal page's slot array in slot order.
func redistInternalKeys(t *testing.T, page *pagemanager.Page) []uint64 {
	t.Helper()
	keys := make([]uint64, page.GetRowCount())
	for i := range keys {
		rec, ok := page.GetRecord(i)
		if !ok {
			t.Fatalf("GetRecord(%d) failed on page %d", i, page.GetPageId())
		}
		k, _, err := DecodeInternalRecord(rec)
		if err != nil {
			t.Fatalf("DecodeInternalRecord slot %d on page %d: %v", i, page.GetPageId(), err)
		}
		keys[i] = k
	}
	return keys
}

// redistInternalChildIds returns the child IDs from an internal page's slot array in slot order.
func redistInternalChildIds(t *testing.T, page *pagemanager.Page) []uint32 {
	t.Helper()
	ids := make([]uint32, page.GetRowCount())
	for i := range ids {
		rec, ok := page.GetRecord(i)
		if !ok {
			t.Fatalf("GetRecord(%d) failed on page %d", i, page.GetPageId())
		}
		_, cid, err := DecodeInternalRecord(rec)
		if err != nil {
			t.Fatalf("DecodeInternalRecord slot %d on page %d: %v", i, page.GetPageId(), err)
		}
		ids[i] = cid
	}
	return ids
}

// setupRedistInternal builds two sibling internal pages and a parent, writes them to a
// fresh DB, and returns the BTree and pages.
//
// leftSlots/rightSlots are (key, childId) pairs for each page's slot array (pre-sorted,
// ascending). leftRmc/rightRmc are the RightMostChild values. parentSepKey is the
// separator that was previously pushed up to the parent for leftPage; it must satisfy
// max(leftSlots keys) < parentSepKey < min(rightSlots keys) for a consistent tree.
//
// When rightIsRightMost=true, rightPage is the parent's rightMostChild (no own record).
// When false, a ceiling internal page is created as the rightMostChild and rightPage
// has its own record in the parent keyed by max(rightSlots keys).
func setupRedistInternal(
	t *testing.T,
	leftSlots [][2]uint64, leftRmc uint32,
	rightSlots [][2]uint64, rightRmc uint32,
	parentSepKey uint64,
	rightIsRightMost bool,
) (bt *BTree, pm pagemanager.PageManager, leftPage, rightPage, parentPage *pagemanager.Page) {
	t.Helper()
	var err error
	pm, err = pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	leftPage, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(left): %v", err)
	}
	rightPage, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(right): %v", err)
	}
	leftId, rightId := leftPage.GetPageId(), rightPage.GetPageId()

	*leftPage = *pagemanager.NewInternalPage(leftId, 1, leftRmc)
	*rightPage = *pagemanager.NewInternalPage(rightId, 1, rightRmc)

	for _, s := range leftSlots {
		if _, ok := leftPage.InsertRecord(EncodeInternalRecord(s[0], uint32(s[1]))); !ok {
			t.Fatalf("InsertRecord(left, key=%d) failed", s[0])
		}
	}
	for _, s := range rightSlots {
		if _, ok := rightPage.InsertRecord(EncodeInternalRecord(s[0], uint32(s[1]))); !ok {
			t.Fatalf("InsertRecord(right, key=%d) failed", s[0])
		}
	}

	rightMostId := rightId
	if !rightIsRightMost {
		ceilPage, err := pm.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage(ceil): %v", err)
		}
		ceilId := ceilPage.GetPageId()
		*ceilPage = *pagemanager.NewInternalPage(ceilId, 1, pagemanager.InvalidPageID)
		if err := pm.WritePage(ceilPage); err != nil {
			t.Fatalf("WritePage(ceil): %v", err)
		}
		rightMostId = ceilId
	}

	parentPage, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(parent): %v", err)
	}
	*parentPage = *pagemanager.NewInternalPage(parentPage.GetPageId(), 2, rightMostId)

	// Insert parentSepKey for leftPage; records must be inserted in ascending key order.
	parentPage.InsertRecord(EncodeInternalRecord(parentSepKey, leftId))
	if !rightIsRightMost {
		// rightPage has its own record keyed by max(rightSlots).
		var maxRight uint64
		for _, s := range rightSlots {
			if s[0] > maxRight {
				maxRight = s[0]
			}
		}
		parentPage.InsertRecord(EncodeInternalRecord(maxRight, rightId))
	}

	for _, p := range []*pagemanager.Page{leftPage, rightPage, parentPage} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}
	bt = NewBTree(pm)
	return
}

// ── TestRedistributeInternal cases ───────────────────────────────────────────

// TestRedistributeInternal_LeftHeavy_RightMostChild: leftPage has 4 slots, rightPage 1.
// Extended sequence: [(10,101),(20,102),(30,103),(40,104),(50,105),(100,106)], rmc=107.
// total=6, mid=3 → left=[(10,101),(20,102),(30,103)], boundary=(40,104), right=[(50,105),(100,106)].
func TestRedistributeInternal_LeftHeavy_RightMostChild(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistInternal(t,
		[][2]uint64{{10, 101}, {20, 102}, {30, 103}, {40, 104}}, 105,
		[][2]uint64{{100, 106}}, 107,
		50, true)

	if err := bt.redistributeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeInternal: %v", err)
	}

	leftId, rightId := leftPage.GetPageId(), rightPage.GetPageId()
	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightId)
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	if !equalSlices(redistInternalKeys(t, left), []uint64{10, 20, 30}) {
		t.Errorf("left keys: got %v, want [10 20 30]", redistInternalKeys(t, left))
	}
	if !equalSlicesU32(redistInternalChildIds(t, left), []uint32{101, 102, 103}) {
		t.Errorf("left child IDs: got %v, want [101 102 103]", redistInternalChildIds(t, left))
	}
	if left.GetRightMostChild() != 104 {
		t.Errorf("left.rmc: got %d, want 104", left.GetRightMostChild())
	}

	if !equalSlices(redistInternalKeys(t, right), []uint64{50, 100}) {
		t.Errorf("right keys: got %v, want [50 100]", redistInternalKeys(t, right))
	}
	if !equalSlicesU32(redistInternalChildIds(t, right), []uint32{105, 106}) {
		t.Errorf("right child IDs: got %v, want [105 106]", redistInternalChildIds(t, right))
	}
	if right.GetRightMostChild() != 107 {
		t.Errorf("right.rmc: got %d, want 107 (unchanged)", right.GetRightMostChild())
	}

	sep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator record for leftPage in parent")
	}
	if sep != 40 {
		t.Errorf("parent separator: got %d, want 40", sep)
	}
	if parent.GetRightMostChild() != rightId {
		t.Errorf("parent.rightMostChild: got %d, want %d", parent.GetRightMostChild(), rightId)
	}
}

// TestRedistributeInternal_RightHeavy_RightMostChild: leftPage has 1 slot, rightPage 4.
// Extended: [(10,101),(20,102),(50,103),(100,104),(150,105),(200,106)], rmc=107.
// total=6, mid=3 → left=[(10,101),(20,102),(50,103)], boundary=(100,104), right=[(150,105),(200,106)].
func TestRedistributeInternal_RightHeavy_RightMostChild(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistInternal(t,
		[][2]uint64{{10, 101}}, 102,
		[][2]uint64{{50, 103}, {100, 104}, {150, 105}, {200, 106}}, 107,
		20, true)

	if err := bt.redistributeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeInternal: %v", err)
	}

	leftId, rightId := leftPage.GetPageId(), rightPage.GetPageId()
	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightId)
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	if !equalSlices(redistInternalKeys(t, left), []uint64{10, 20, 50}) {
		t.Errorf("left keys: got %v, want [10 20 50]", redistInternalKeys(t, left))
	}
	if !equalSlicesU32(redistInternalChildIds(t, left), []uint32{101, 102, 103}) {
		t.Errorf("left child IDs: got %v, want [101 102 103]", redistInternalChildIds(t, left))
	}
	if left.GetRightMostChild() != 104 {
		t.Errorf("left.rmc: got %d, want 104", left.GetRightMostChild())
	}

	if !equalSlices(redistInternalKeys(t, right), []uint64{150, 200}) {
		t.Errorf("right keys: got %v, want [150 200]", redistInternalKeys(t, right))
	}
	if !equalSlicesU32(redistInternalChildIds(t, right), []uint32{105, 106}) {
		t.Errorf("right child IDs: got %v, want [105 106]", redistInternalChildIds(t, right))
	}
	if right.GetRightMostChild() != 107 {
		t.Errorf("right.rmc: got %d, want 107 (unchanged)", right.GetRightMostChild())
	}

	sep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator for leftPage in parent")
	}
	if sep != 100 {
		t.Errorf("parent separator: got %d, want 100", sep)
	}
	if parent.GetRightMostChild() != rightId {
		t.Errorf("parent.rightMostChild: got %d, want %d", parent.GetRightMostChild(), rightId)
	}
}

// TestRedistributeInternal_LeftHeavy_NotRightMostChild: same as LeftHeavy above but
// rightPage has its own record in the parent. Verifies rightPage's separator is untouched.
func TestRedistributeInternal_LeftHeavy_NotRightMostChild(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistInternal(t,
		[][2]uint64{{10, 101}, {20, 102}, {30, 103}, {40, 104}}, 105,
		[][2]uint64{{100, 106}}, 107,
		50, false)

	if err := bt.redistributeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeInternal: %v", err)
	}

	leftId, rightId := leftPage.GetPageId(), rightPage.GetPageId()
	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightId)
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	if !equalSlices(redistInternalKeys(t, left), []uint64{10, 20, 30}) {
		t.Errorf("left keys: got %v, want [10 20 30]", redistInternalKeys(t, left))
	}
	if left.GetRightMostChild() != 104 {
		t.Errorf("left.rmc: got %d, want 104", left.GetRightMostChild())
	}
	if !equalSlices(redistInternalKeys(t, right), []uint64{50, 100}) {
		t.Errorf("right keys: got %v, want [50 100]", redistInternalKeys(t, right))
	}

	leftSep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator for leftPage in parent")
	}
	if leftSep != 40 {
		t.Errorf("left separator: got %d, want 40", leftSep)
	}

	rightSep, found := redistParentSepKey(t, parent, rightId)
	if !found {
		t.Fatal("no separator for rightPage in parent")
	}
	if rightSep != 100 {
		t.Errorf("right separator: got %d, want 100 (unchanged)", rightSep)
	}
}

// TestRedistributeInternal_RightHeavy_NotRightMostChild: right has more slots and its
// own parent record. Verifies rightPage's separator key is not changed.
func TestRedistributeInternal_RightHeavy_NotRightMostChild(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistInternal(t,
		[][2]uint64{{10, 101}}, 102,
		[][2]uint64{{50, 103}, {100, 104}, {150, 105}, {200, 106}}, 107,
		20, false)

	if err := bt.redistributeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeInternal: %v", err)
	}

	leftId, rightId := leftPage.GetPageId(), rightPage.GetPageId()
	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightId)
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	if !equalSlices(redistInternalKeys(t, left), []uint64{10, 20, 50}) {
		t.Errorf("left keys: got %v, want [10 20 50]", redistInternalKeys(t, left))
	}
	if left.GetRightMostChild() != 104 {
		t.Errorf("left.rmc: got %d, want 104", left.GetRightMostChild())
	}
	if !equalSlices(redistInternalKeys(t, right), []uint64{150, 200}) {
		t.Errorf("right keys: got %v, want [150 200]", redistInternalKeys(t, right))
	}

	leftSep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator for leftPage in parent")
	}
	if leftSep != 100 {
		t.Errorf("left separator: got %d, want 100", leftSep)
	}

	rightSep, found := redistParentSepKey(t, parent, rightId)
	if !found {
		t.Fatal("no separator for rightPage in parent")
	}
	if rightSep != 200 {
		t.Errorf("right separator: got %d, want 200 (unchanged)", rightSep)
	}
}

// TestRedistributeInternal_EqualSizes: both pages have the same slot count.
// Extended: [(10,101),(20,102),(40,103),(60,104),(80,105)], rmc=106.
// total=5, mid=2 → left=[(10,101),(20,102)], boundary=(40,103), right=[(60,104),(80,105)].
// Distribution is unchanged; separator stays at 40.
func TestRedistributeInternal_EqualSizes(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistInternal(t,
		[][2]uint64{{10, 101}, {20, 102}}, 103,
		[][2]uint64{{60, 104}, {80, 105}}, 106,
		40, true)

	if err := bt.redistributeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeInternal: %v", err)
	}

	leftId := leftPage.GetPageId()
	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightPage.GetPageId())
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	if !equalSlices(redistInternalKeys(t, left), []uint64{10, 20}) {
		t.Errorf("left keys: got %v, want [10 20]", redistInternalKeys(t, left))
	}
	if left.GetRightMostChild() != 103 {
		t.Errorf("left.rmc: got %d, want 103 (unchanged)", left.GetRightMostChild())
	}
	if !equalSlices(redistInternalKeys(t, right), []uint64{60, 80}) {
		t.Errorf("right keys: got %v, want [60 80]", redistInternalKeys(t, right))
	}
	if right.GetRightMostChild() != 106 {
		t.Errorf("right.rmc: got %d, want 106 (unchanged)", right.GetRightMostChild())
	}

	sep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator for leftPage in parent")
	}
	if sep != 40 {
		t.Errorf("parent separator: got %d, want 40 (unchanged)", sep)
	}
}

// TestRedistributeInternal_OddTotal: 1+3=4 slots + bridge = 5 total; left gets 2, right gets 2.
// Extended: [(10,101),(20,102),(50,103),(100,104),(150,105)], rmc=106.
// total=5, mid=2 → left=[(10,101),(20,102)], boundary=(50,103), right=[(100,104),(150,105)].
func TestRedistributeInternal_OddTotal(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistInternal(t,
		[][2]uint64{{10, 101}}, 102,
		[][2]uint64{{50, 103}, {100, 104}, {150, 105}}, 106,
		20, true)

	if err := bt.redistributeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeInternal: %v", err)
	}

	leftId := leftPage.GetPageId()
	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightPage.GetPageId())
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	if !equalSlices(redistInternalKeys(t, left), []uint64{10, 20}) {
		t.Errorf("left keys: got %v, want [10 20]", redistInternalKeys(t, left))
	}
	if left.GetRightMostChild() != 103 {
		t.Errorf("left.rmc: got %d, want 103", left.GetRightMostChild())
	}
	if !equalSlices(redistInternalKeys(t, right), []uint64{100, 150}) {
		t.Errorf("right keys: got %v, want [100 150]", redistInternalKeys(t, right))
	}
	if right.GetRightMostChild() != 106 {
		t.Errorf("right.rmc: got %d, want 106 (unchanged)", right.GetRightMostChild())
	}

	sep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator for leftPage in parent")
	}
	if sep != 50 {
		t.Errorf("parent separator: got %d, want 50", sep)
	}
}

// TestRedistributeInternal_MinimumCase: one slot per page — minimum viable redistribution.
// Extended: [(10,101),(20,102),(50,103)], rmc=104.
// total=3, mid=1 → left=[(10,101)], boundary=(20,102), right=[(50,103)].
func TestRedistributeInternal_MinimumCase(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistInternal(t,
		[][2]uint64{{10, 101}}, 102,
		[][2]uint64{{50, 103}}, 104,
		20, true)

	if err := bt.redistributeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeInternal: %v", err)
	}

	leftId := leftPage.GetPageId()
	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightPage.GetPageId())
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	if !equalSlices(redistInternalKeys(t, left), []uint64{10}) {
		t.Errorf("left keys: got %v, want [10]", redistInternalKeys(t, left))
	}
	if left.GetRightMostChild() != 102 {
		t.Errorf("left.rmc: got %d, want 102 (unchanged for minimum case)", left.GetRightMostChild())
	}
	if !equalSlices(redistInternalKeys(t, right), []uint64{50}) {
		t.Errorf("right keys: got %v, want [50]", redistInternalKeys(t, right))
	}
	if right.GetRightMostChild() != 104 {
		t.Errorf("right.rmc: got %d, want 104 (unchanged)", right.GetRightMostChild())
	}

	sep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator for leftPage in parent")
	}
	if sep != 20 {
		t.Errorf("parent separator: got %d, want 20 (unchanged for minimum case)", sep)
	}
}

// TestRedistributeInternal_LeftRmcUpdated verifies that leftPage.RightMostChild is
// correctly changed to the boundary record's child pointer after redistribution.
// Extended: [(10,101),(20,102),(30,103),(40,104),(80,105)], rmc=106.
// total=5, mid=2 → left=[(10,101),(20,102)], boundary=(30,103), right=[(40,104),(80,105)].
// leftPage.rmc was 104, becomes 103.
func TestRedistributeInternal_LeftRmcUpdated(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistInternal(t,
		[][2]uint64{{10, 101}, {20, 102}, {30, 103}}, 104,
		[][2]uint64{{80, 105}}, 106,
		40, true)

	rmcBefore := leftPage.GetRightMostChild() // 104
	if rmcBefore != 104 {
		t.Fatalf("precondition: left.rmc should be 104, got %d", rmcBefore)
	}

	if err := bt.redistributeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeInternal: %v", err)
	}

	left := mustReadPage(t, pm, leftPage.GetPageId())
	if left.GetRightMostChild() != 103 {
		t.Errorf("left.rmc after redistribution: got %d, want 103 (was 104)", left.GetRightMostChild())
	}
}

// TestRedistributeInternal_RightRmcUnchanged verifies that rightPage.RightMostChild
// is never modified during redistribution.
func TestRedistributeInternal_RightRmcUnchanged(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistInternal(t,
		[][2]uint64{{10, 101}, {20, 102}, {30, 103}, {40, 104}}, 105,
		[][2]uint64{{100, 106}}, 107,
		50, true)

	rmcBefore := rightPage.GetRightMostChild() // 107

	if err := bt.redistributeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeInternal: %v", err)
	}

	right := mustReadPage(t, pm, rightPage.GetPageId())
	if right.GetRightMostChild() != rmcBefore {
		t.Errorf("right.rmc changed: got %d, want %d (should be unchanged)", right.GetRightMostChild(), rmcBefore)
	}
}

// TestRedistributeInternal_ChildPointersIntact verifies that every (key, childId) pair
// in both pages is exactly correct after redistribution — no pointer corruption.
func TestRedistributeInternal_ChildPointersIntact(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistInternal(t,
		[][2]uint64{{10, 101}, {20, 102}, {30, 103}, {40, 104}}, 105,
		[][2]uint64{{100, 106}}, 107,
		50, true)

	if err := bt.redistributeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeInternal: %v", err)
	}

	left := mustReadPage(t, pm, leftPage.GetPageId())
	right := mustReadPage(t, pm, rightPage.GetPageId())

	// left: [(10,101),(20,102),(30,103)], rmc=104
	wantLeftKeys := []uint64{10, 20, 30}
	wantLeftCids := []uint32{101, 102, 103}
	if !equalSlices(redistInternalKeys(t, left), wantLeftKeys) {
		t.Errorf("left keys: got %v, want %v", redistInternalKeys(t, left), wantLeftKeys)
	}
	if !equalSlicesU32(redistInternalChildIds(t, left), wantLeftCids) {
		t.Errorf("left child IDs: got %v, want %v", redistInternalChildIds(t, left), wantLeftCids)
	}
	if left.GetRightMostChild() != 104 {
		t.Errorf("left.rmc: got %d, want 104", left.GetRightMostChild())
	}

	// right: [(50,105),(100,106)], rmc=107
	wantRightKeys := []uint64{50, 100}
	wantRightCids := []uint32{105, 106}
	if !equalSlices(redistInternalKeys(t, right), wantRightKeys) {
		t.Errorf("right keys: got %v, want %v", redistInternalKeys(t, right), wantRightKeys)
	}
	if !equalSlicesU32(redistInternalChildIds(t, right), wantRightCids) {
		t.Errorf("right child IDs: got %v, want %v", redistInternalChildIds(t, right), wantRightCids)
	}
	if right.GetRightMostChild() != 107 {
		t.Errorf("right.rmc: got %d, want 107", right.GetRightMostChild())
	}
}

// TestRedistributeInternal_PagesWrittenToDisk verifies that all three pages are
// durably written by re-reading them from the page manager after redistribution.
func TestRedistributeInternal_PagesWrittenToDisk(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistInternal(t,
		[][2]uint64{{10, 101}, {20, 102}, {30, 103}, {40, 104}}, 105,
		[][2]uint64{{100, 106}}, 107,
		50, true)

	leftId, rightId, parentId := leftPage.GetPageId(), rightPage.GetPageId(), parentPage.GetPageId()

	if err := bt.redistributeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeInternal: %v", err)
	}

	left := mustReadPage(t, pm, leftId)
	right := mustReadPage(t, pm, rightId)
	parent := mustReadPage(t, pm, parentId)

	if left.GetRowCount() != 3 {
		t.Errorf("left row count after disk re-read: got %d, want 3", left.GetRowCount())
	}
	if right.GetRowCount() != 2 {
		t.Errorf("right row count after disk re-read: got %d, want 2", right.GetRowCount())
	}
	if parent.GetRowCount() != 1 {
		t.Errorf("parent row count after disk re-read: got %d, want 1", parent.GetRowCount())
	}
	if left.GetRightMostChild() != 104 {
		t.Errorf("left.rmc after disk re-read: got %d, want 104", left.GetRightMostChild())
	}
	sep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator for leftPage after disk re-read")
	}
	if sep != 40 {
		t.Errorf("parent separator after disk re-read: got %d, want 40", sep)
	}
}

// TestRedistributeInternal_RecordsRemainSorted verifies that both pages have strictly
// ascending key order in their slot arrays after redistribution.
func TestRedistributeInternal_RecordsRemainSorted(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupRedistInternal(t,
		[][2]uint64{{10, 101}, {20, 102}}, 103,
		[][2]uint64{{60, 104}, {120, 105}, {180, 106}, {240, 107}}, 108,
		40, true)

	if err := bt.redistributeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeInternal: %v", err)
	}

	for _, id := range []uint32{leftPage.GetPageId(), rightPage.GetPageId()} {
		page := mustReadPage(t, pm, id)
		keys := redistInternalKeys(t, page)
		for i := 1; i < len(keys); i++ {
			if keys[i] <= keys[i-1] {
				t.Errorf("page %d: keys out of order at index %d: %v", id, i, keys)
			}
		}
	}
}

// TestRedistributeInternal_ParentSeparatorCorrect verifies the parent separator key
// reflects the exact key of the promoted boundary entry (not an adjacent key).
func TestRedistributeInternal_ParentSeparatorCorrect(t *testing.T) {
	// Extended: [(5,101),(15,102),(25,103),(35,104),(45,105),(55,106),(65,107)], rmc=108.
	// total=7, mid=3 → boundary=(35,104), new sep=35.
	bt, pm, leftPage, rightPage, parentPage := setupRedistInternal(t,
		[][2]uint64{{5, 101}, {15, 102}, {25, 103}}, 104,
		[][2]uint64{{45, 105}, {55, 106}, {65, 107}}, 108,
		35, true)

	if err := bt.redistributeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("redistributeInternal: %v", err)
	}

	parent := mustReadPage(t, pm, parentPage.GetPageId())
	sep, found := redistParentSepKey(t, parent, leftPage.GetPageId())
	if !found {
		t.Fatal("no separator for leftPage in parent")
	}
	if sep != 35 {
		t.Errorf("parent separator: got %d, want 35 (the promoted bridge key)", sep)
	}
	// Verify the bridge child (104) is now leftPage's rightmost child.
	left := mustReadPage(t, pm, leftPage.GetPageId())
	if left.GetRightMostChild() != 104 {
		t.Errorf("left.rmc: got %d, want 104 (the promoted bridge child)", left.GetRightMostChild())
	}
}

// TestRedistributeInternal_MultipleChildren verifies that when the parent has
// multiple children, only leftPage's separator is updated and all others are untouched.
func TestRedistributeInternal_MultipleChildren(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })
	bt := NewBTree(pm)

	alloc := func() *pagemanager.Page {
		p, err := pm.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage: %v", err)
		}
		return p
	}

	// Four internal pages: A (left, heavy), B (right, light), C (unchanged sibling), D (rightMostChild).
	a, b, c, d := alloc(), alloc(), alloc(), alloc()
	aId, bId, cId, dId := a.GetPageId(), b.GetPageId(), c.GetPageId(), d.GetPageId()

	// A: [(10,101),(20,102),(30,103)], rmc=104; parentSep=40
	*a = *pagemanager.NewInternalPage(aId, 1, 104)
	a.InsertRecord(EncodeInternalRecord(10, 101))
	a.InsertRecord(EncodeInternalRecord(20, 102))
	a.InsertRecord(EncodeInternalRecord(30, 103))

	// B: [(80,105)], rmc=106; parent record key=100
	*b = *pagemanager.NewInternalPage(bId, 1, 106)
	b.InsertRecord(EncodeInternalRecord(80, 105))

	// C: [(200,107)], rmc=108; parent record key=200
	*c = *pagemanager.NewInternalPage(cId, 1, 108)
	c.InsertRecord(EncodeInternalRecord(200, 107))

	// D: empty internal, rightMostChild of parent
	*d = *pagemanager.NewInternalPage(dId, 1, pagemanager.InvalidPageID)

	// Parent: [(40,aId),(100,bId),(200,cId)], rightMostChild=dId
	parent := alloc()
	*parent = *pagemanager.NewInternalPage(parent.GetPageId(), 2, dId)
	parent.InsertRecord(EncodeInternalRecord(40, aId))
	parent.InsertRecord(EncodeInternalRecord(100, bId))
	parent.InsertRecord(EncodeInternalRecord(200, cId))

	for _, p := range []*pagemanager.Page{a, b, c, d, parent} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}

	// Redistribute A (heavy) and B (light).
	// Extended: [(10,101),(20,102),(30,103),(40,104),(80,105)], rmc=106.
	// total=5, mid=2 → A=[(10,101),(20,102)], boundary=(30,103)→sep=30,Armc=103; B=[(40,104),(80,105)].
	if err := bt.redistributeInternal(a, b, parent); err != nil {
		t.Fatalf("redistributeInternal: %v", err)
	}

	pa := mustReadPage(t, pm, parent.GetPageId())
	aPage := mustReadPage(t, pm, aId)
	bPage := mustReadPage(t, pm, bId)

	if !equalSlices(redistInternalKeys(t, aPage), []uint64{10, 20}) {
		t.Errorf("A keys: got %v, want [10 20]", redistInternalKeys(t, aPage))
	}
	if aPage.GetRightMostChild() != 103 {
		t.Errorf("A.rmc: got %d, want 103", aPage.GetRightMostChild())
	}
	if !equalSlices(redistInternalKeys(t, bPage), []uint64{40, 80}) {
		t.Errorf("B keys: got %v, want [40 80]", redistInternalKeys(t, bPage))
	}
	if bPage.GetRightMostChild() != 106 {
		t.Errorf("B.rmc: got %d, want 106 (unchanged)", bPage.GetRightMostChild())
	}

	aSep, found := redistParentSepKey(t, pa, aId)
	if !found {
		t.Fatal("no separator for A in parent")
	}
	if aSep != 30 {
		t.Errorf("A separator: got %d, want 30", aSep)
	}

	bSep, found := redistParentSepKey(t, pa, bId)
	if !found {
		t.Fatal("no separator for B in parent")
	}
	if bSep != 100 {
		t.Errorf("B separator: got %d, want 100 (unchanged)", bSep)
	}

	cSep, found := redistParentSepKey(t, pa, cId)
	if !found {
		t.Fatal("no separator for C in parent")
	}
	if cSep != 200 {
		t.Errorf("C separator: got %d, want 200 (unchanged)", cSep)
	}

	if pa.GetRightMostChild() != dId {
		t.Errorf("parent.rightMostChild: got %d, want %d (unchanged)", pa.GetRightMostChild(), dId)
	}
}

// ── TestMergeLeaf cases ───────────────────────────────────────────────────────

// setupMergeLeaf is like setupRedistLeaf but also wires rightPage.rightSibling
// to ceilPage when !rightIsRightMost, giving a fully correct sibling chain.
func setupMergeLeaf(
	t *testing.T,
	leftKeys, rightKeys []uint64,
	rightIsRightMost bool,
) (bt *BTree, pm pagemanager.PageManager, leftPage, rightPage, parentPage *pagemanager.Page) {
	t.Helper()
	var err error
	pm, err = pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	leftPage, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(left): %v", err)
	}
	rightPage, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(right): %v", err)
	}
	leftId, rightId := leftPage.GetPageId(), rightPage.GetPageId()

	ceilId := pagemanager.InvalidPageID
	rightMostId := rightId
	if !rightIsRightMost {
		ceilPage, cerr := pm.AllocatePage()
		if cerr != nil {
			t.Fatalf("AllocatePage(ceil): %v", cerr)
		}
		ceilId = ceilPage.GetPageId()
		*ceilPage = *pagemanager.NewLeafPage(ceilId, rightId, pagemanager.InvalidPageID)
		if err := pm.WritePage(ceilPage); err != nil {
			t.Fatalf("WritePage(ceil): %v", err)
		}
		rightMostId = ceilId
	}

	// Wire full sibling chain: left ↔ right ↔ ceil (if present).
	*leftPage = *pagemanager.NewLeafPage(leftId, pagemanager.InvalidPageID, rightId)
	*rightPage = *pagemanager.NewLeafPage(rightId, leftId, ceilId)

	for _, k := range leftKeys {
		if _, ok := leftPage.InsertRecord(makeRecord(k, []byte("v"))); !ok {
			t.Fatalf("InsertRecord(left, key=%d) failed", k)
		}
	}
	for _, k := range rightKeys {
		if _, ok := rightPage.InsertRecord(makeRecord(k, []byte("v"))); !ok {
			t.Fatalf("InsertRecord(right, key=%d) failed", k)
		}
	}

	parentPage, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(parent): %v", err)
	}
	*parentPage = *pagemanager.NewInternalPage(parentPage.GetPageId(), 1, rightMostId)

	maxLeft := leftKeys[len(leftKeys)-1]
	parentPage.InsertRecord(EncodeInternalRecord(maxLeft, leftId))
	if !rightIsRightMost {
		maxRight := rightKeys[len(rightKeys)-1]
		parentPage.InsertRecord(EncodeInternalRecord(maxRight, rightId))
	}

	for _, p := range []*pagemanager.Page{leftPage, rightPage, parentPage} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}

	bt = NewBTree(pm)
	return
}

// TestMergeLeaf_RightMostChild_RecordsMergedInOrder: rightPage is parent's
// rightMostChild. After merge leftPage must contain all records from both pages
// in ascending key order.
func TestMergeLeaf_RightMostChild_RecordsMergedInOrder(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeLeaf(t,
		[]uint64{1, 2, 3}, []uint64{10, 20}, true)

	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf: %v", err)
	}

	left := mustReadPage(t, pm, leftPage.GetPageId())
	if !equalSlices(redistLeafKeys(t, left), []uint64{1, 2, 3, 10, 20}) {
		t.Errorf("leftPage keys: got %v, want [1 2 3 10 20]", redistLeafKeys(t, left))
	}
}

// TestMergeLeaf_NotRightMostChild_RecordsMergedInOrder: rightPage has its own
// slot entry in the parent. After merge leftPage must hold all records in order.
func TestMergeLeaf_NotRightMostChild_RecordsMergedInOrder(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeLeaf(t,
		[]uint64{1, 2, 3}, []uint64{10, 20}, false)

	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf: %v", err)
	}

	left := mustReadPage(t, pm, leftPage.GetPageId())
	if !equalSlices(redistLeafKeys(t, left), []uint64{1, 2, 3, 10, 20}) {
		t.Errorf("leftPage keys: got %v, want [1 2 3 10 20]", redistLeafKeys(t, left))
	}
}

// TestMergeLeaf_RightMostChild_ParentRMCBecomesLeft: after the merge the
// parent's rightMostChild must point to leftPage, not the freed rightPage.
func TestMergeLeaf_RightMostChild_ParentRMCBecomesLeft(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeLeaf(t,
		[]uint64{5, 10}, []uint64{15, 25}, true)

	leftId := leftPage.GetPageId()

	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf: %v", err)
	}

	parent := mustReadPage(t, pm, parentPage.GetPageId())
	if parent.GetRightMostChild() != leftId {
		t.Errorf("parent.rightMostChild: got %d, want %d (leftPage)", parent.GetRightMostChild(), leftId)
	}
	// leftPage's old slot entry must be gone – it is now the rightMostChild.
	if _, found := redistParentSepKey(t, parent, leftId); found {
		t.Error("leftPage should have no slot entry in parent after becoming rightMostChild")
	}
}

// TestMergeLeaf_RightMostChild_ParentBecomesEmpty: when the parent had exactly
// one slot entry (k_L, leftPage) and rightPage was the RMC, the parent must end
// up with zero slot entries.
func TestMergeLeaf_RightMostChild_ParentBecomesEmpty(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeLeaf(t,
		[]uint64{5}, []uint64{50}, true)

	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf: %v", err)
	}

	parent := mustReadPage(t, pm, parentPage.GetPageId())
	if parent.GetRowCount() != 0 {
		t.Errorf("parent slot count: got %d, want 0", parent.GetRowCount())
	}
	if parent.GetRightMostChild() != leftPage.GetPageId() {
		t.Errorf("parent.rightMostChild: got %d, want %d (leftPage)", parent.GetRightMostChild(), leftPage.GetPageId())
	}
}

// TestMergeLeaf_NotRightMostChild_LeftSepRaisedToRightSep: when rightPage is not
// the RMC, leftPage's separator in the parent must be raised from max(leftPage)
// to max(rightPage) after the merge.
func TestMergeLeaf_NotRightMostChild_LeftSepRaisedToRightSep(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeLeaf(t,
		[]uint64{10, 20}, []uint64{30, 40}, false)

	leftId := leftPage.GetPageId()

	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf: %v", err)
	}

	parent := mustReadPage(t, pm, parentPage.GetPageId())

	sep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator for leftPage in parent after merge")
	}
	if sep != 40 {
		t.Errorf("leftPage separator: got %d, want 40 (old max of rightPage)", sep)
	}
}

// TestMergeLeaf_NotRightMostChild_RightPageGoneFromParent: rightPage's slot
// entry must be removed from the parent.
func TestMergeLeaf_NotRightMostChild_RightPageGoneFromParent(t *testing.T) {
	bt, _, leftPage, rightPage, parentPage := setupMergeLeaf(t,
		[]uint64{1, 2}, []uint64{10, 20}, false)

	_ = bt.mergeLeaf(leftPage, rightPage, parentPage)

	if _, found := redistParentSepKey(t, parentPage, rightPage.GetPageId()); found {
		t.Error("rightPage should have no slot entry in parent after merge")
	}
}

// TestMergeLeaf_NotRightMostChild_ParentSlotCount: parent had two slot entries
// before the merge; it should have exactly one afterwards.
func TestMergeLeaf_NotRightMostChild_ParentSlotCount(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeLeaf(t,
		[]uint64{1, 2, 3}, []uint64{10, 20}, false)

	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf: %v", err)
	}

	parent := mustReadPage(t, pm, parentPage.GetPageId())
	if parent.GetRowCount() != 1 {
		t.Errorf("parent slot count: got %d, want 1", parent.GetRowCount())
	}
}

// TestMergeLeaf_NotRightMostChild_RMCUnchanged: the parent's rightMostChild
// must remain the ceilPage (unchanged) when rightPage is not the RMC.
func TestMergeLeaf_NotRightMostChild_RMCUnchanged(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeLeaf(t,
		[]uint64{1, 2}, []uint64{10, 20}, false)

	// ceilPage is the current rightMostChild.
	ceilId := parentPage.GetRightMostChild()

	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf: %v", err)
	}

	parent := mustReadPage(t, pm, parentPage.GetPageId())
	if parent.GetRightMostChild() != ceilId {
		t.Errorf("parent.rightMostChild: got %d, want %d (ceilPage, unchanged)", parent.GetRightMostChild(), ceilId)
	}
}

// TestMergeLeaf_SiblingChain_WithRightSibling: when rightPage has a right
// sibling (ceilPage), after merge leftPage must point to ceilPage and ceilPage
// must point back to leftPage.
func TestMergeLeaf_SiblingChain_WithRightSibling(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeLeaf(t,
		[]uint64{1, 2}, []uint64{10, 20}, false)

	leftId := leftPage.GetPageId()
	ceilId := rightPage.GetRightSibling()
	if ceilId == pagemanager.InvalidPageID {
		t.Fatal("test setup: rightPage.rightSibling should be ceilPage")
	}

	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf: %v", err)
	}

	left := mustReadPage(t, pm, leftId)
	ceil := mustReadPage(t, pm, ceilId)

	if left.GetRightSibling() != ceilId {
		t.Errorf("leftPage.rightSibling: got %d, want %d (ceilPage)", left.GetRightSibling(), ceilId)
	}
	if ceil.GetLeftSibling() != leftId {
		t.Errorf("ceilPage.leftSibling: got %d, want %d (leftPage)", ceil.GetLeftSibling(), leftId)
	}
}

// TestMergeLeaf_SiblingChain_NoRightSibling: when rightPage is the rightmost
// leaf, leftPage must have no right sibling after the merge.
func TestMergeLeaf_SiblingChain_NoRightSibling(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeLeaf(t,
		[]uint64{1, 2}, []uint64{10, 20}, true)

	leftId := leftPage.GetPageId()

	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf: %v", err)
	}

	left := mustReadPage(t, pm, leftId)
	if left.GetRightSibling() != pagemanager.InvalidPageID {
		t.Errorf("leftPage.rightSibling: got %d, want InvalidPageID", left.GetRightSibling())
	}
}

// TestMergeLeaf_LeftSiblingUnchanged: leftPage's left sibling must not be
// modified by the merge.
func TestMergeLeaf_LeftSiblingUnchanged(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeLeaf(t,
		[]uint64{1, 2, 3}, []uint64{10, 20}, true)

	leftId := leftPage.GetPageId()
	wantLeft := leftPage.GetLeftSibling()

	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf: %v", err)
	}

	left := mustReadPage(t, pm, leftId)
	if left.GetLeftSibling() != wantLeft {
		t.Errorf("leftPage.leftSibling: got %d, want %d (unchanged)", left.GetLeftSibling(), wantLeft)
	}
}

// TestMergeLeaf_SingleRecordEachPage_RightMost: merge when each page has one
// record, rightPage is RMC.
func TestMergeLeaf_SingleRecordEachPage_RightMost(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeLeaf(t,
		[]uint64{5}, []uint64{15}, true)

	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf: %v", err)
	}

	left := mustReadPage(t, pm, leftPage.GetPageId())
	if !equalSlices(redistLeafKeys(t, left), []uint64{5, 15}) {
		t.Errorf("leftPage keys: got %v, want [5 15]", redistLeafKeys(t, left))
	}
}

// TestMergeLeaf_ManyRecords_Sorted: merge pages with several records each;
// result must be strictly ascending.
func TestMergeLeaf_ManyRecords_Sorted(t *testing.T) {
	leftKeys := []uint64{1, 2, 3, 4, 5}
	rightKeys := []uint64{10, 20, 30, 40, 50}
	bt, pm, leftPage, rightPage, parentPage := setupMergeLeaf(t, leftKeys, rightKeys, true)

	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf: %v", err)
	}

	left := mustReadPage(t, pm, leftPage.GetPageId())
	keys := redistLeafKeys(t, left)
	want := []uint64{1, 2, 3, 4, 5, 10, 20, 30, 40, 50}
	if !equalSlices(keys, want) {
		t.Errorf("leftPage keys: got %v, want %v", keys, want)
	}
	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			t.Errorf("keys not sorted at position %d: %v", i, keys)
			break
		}
	}
}

// TestMergeLeaf_ParentHasFourChildren_NotRightMost: parent has four child
// slots [(10,A),(30,left),(50,right),(70,B)] with RMC=C.  After merging right
// into left the parent should be [(10,A),(50,left),(70,B)] with RMC=C.
func TestMergeLeaf_ParentHasFourChildren_NotRightMost(t *testing.T) {
	var err error
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	alloc := func() *pagemanager.Page {
		p, e := pm.AllocatePage()
		if e != nil {
			t.Fatalf("AllocatePage: %v", e)
		}
		return p
	}
	write := func(p *pagemanager.Page) {
		if e := pm.WritePage(p); e != nil {
			t.Fatalf("WritePage: %v", e)
		}
	}

	pageA := alloc()
	leftPage := alloc()
	rightPage := alloc()
	pageB := alloc()
	pageC := alloc()

	aId := pageA.GetPageId()
	leftId := leftPage.GetPageId()
	rightId := rightPage.GetPageId()
	bId := pageB.GetPageId()
	cId := pageC.GetPageId()

	*pageA = *pagemanager.NewLeafPage(aId, pagemanager.InvalidPageID, leftId)
	*leftPage = *pagemanager.NewLeafPage(leftId, aId, rightId)
	*rightPage = *pagemanager.NewLeafPage(rightId, leftId, bId)
	*pageB = *pagemanager.NewLeafPage(bId, rightId, cId)
	*pageC = *pagemanager.NewLeafPage(cId, bId, pagemanager.InvalidPageID)

	for _, k := range []uint64{5, 10} {
		pageA.InsertRecord(makeRecord(k, []byte("v")))
	}
	for _, k := range []uint64{20, 30} {
		leftPage.InsertRecord(makeRecord(k, []byte("v")))
	}
	for _, k := range []uint64{40, 50} {
		rightPage.InsertRecord(makeRecord(k, []byte("v")))
	}
	for _, k := range []uint64{60, 70} {
		pageB.InsertRecord(makeRecord(k, []byte("v")))
	}

	// Parent: [(10,A),(30,left),(50,right),(70,B)], RMC=C
	parentPage := alloc()
	*parentPage = *pagemanager.NewInternalPage(parentPage.GetPageId(), 1, cId)
	for _, rec := range []struct {
		k  uint64
		id uint32
	}{{10, aId}, {30, leftId}, {50, rightId}, {70, bId}} {
		parentPage.InsertRecord(EncodeInternalRecord(rec.k, rec.id))
	}

	for _, p := range []*pagemanager.Page{pageA, leftPage, rightPage, pageB, pageC, parentPage} {
		write(p)
	}

	bt := NewBTree(pm)
	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf: %v", err)
	}

	left := mustReadPage(t, pm, leftId)
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	// leftPage holds records from both pages.
	if !equalSlices(redistLeafKeys(t, left), []uint64{20, 30, 40, 50}) {
		t.Errorf("leftPage keys: got %v, want [20 30 40 50]", redistLeafKeys(t, left))
	}

	// Parent: [(10,A),(50,left),(70,B)] with RMC=C.
	if parent.GetRowCount() != 3 {
		t.Errorf("parent slot count: got %d, want 3", parent.GetRowCount())
	}
	if aSep, found := redistParentSepKey(t, parent, aId); !found || aSep != 10 {
		t.Errorf("A separator: got (%d, %v), want (10, true)", aSep, found)
	}
	if leftSep, found := redistParentSepKey(t, parent, leftId); !found || leftSep != 50 {
		t.Errorf("leftPage separator: got (%d, %v), want (50, true)", leftSep, found)
	}
	if _, found := redistParentSepKey(t, parent, rightId); found {
		t.Error("rightPage should have no separator in parent after merge")
	}
	if bSep, found := redistParentSepKey(t, parent, bId); !found || bSep != 70 {
		t.Errorf("B separator: got (%d, %v), want (70, true)", bSep, found)
	}
	if parent.GetRightMostChild() != cId {
		t.Errorf("parent.rightMostChild: got %d, want %d (C)", parent.GetRightMostChild(), cId)
	}

	// Sibling chain after merge: left → B → C.
	leftRe := mustReadPage(t, pm, leftId)
	bRe := mustReadPage(t, pm, bId)
	if leftRe.GetRightSibling() != bId {
		t.Errorf("leftPage.rightSibling: got %d, want %d (B)", leftRe.GetRightSibling(), bId)
	}
	if bRe.GetLeftSibling() != leftId {
		t.Errorf("B.leftSibling: got %d, want %d (leftPage)", bRe.GetLeftSibling(), leftId)
	}
}

// TestMergeLeaf_ReturnsNoError: basic sanity – mergeLeaf must not return an
// error for well-formed input.
func TestMergeLeaf_ReturnsNoError(t *testing.T) {
	bt, _, leftPage, rightPage, parentPage := setupMergeLeaf(t,
		[]uint64{1, 2}, []uint64{10}, true)

	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf unexpectedly returned error: %v", err)
	}
}

// setupMergeLeafEncoded is like setupMergeLeaf but inserts records using
// EncodeLeafRecord so they are compatible with bt.Search / DecodeLeafRecord.
func setupMergeLeafEncoded(
	t *testing.T,
	leftKeys, rightKeys []uint64,
	rightIsRightMost bool,
) (bt *BTree, pm pagemanager.PageManager, leftPage, rightPage, parentPage *pagemanager.Page) {
	t.Helper()
	var err error
	pm, err = pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	leftPage, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(left): %v", err)
	}
	rightPage, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(right): %v", err)
	}
	leftId, rightId := leftPage.GetPageId(), rightPage.GetPageId()

	ceilId := pagemanager.InvalidPageID
	rightMostId := rightId
	if !rightIsRightMost {
		ceilPage, cerr := pm.AllocatePage()
		if cerr != nil {
			t.Fatalf("AllocatePage(ceil): %v", cerr)
		}
		ceilId = ceilPage.GetPageId()
		*ceilPage = *pagemanager.NewLeafPage(ceilId, rightId, pagemanager.InvalidPageID)
		if err := pm.WritePage(ceilPage); err != nil {
			t.Fatalf("WritePage(ceil): %v", err)
		}
		rightMostId = ceilId
	}

	*leftPage = *pagemanager.NewLeafPage(leftId, pagemanager.InvalidPageID, rightId)
	*rightPage = *pagemanager.NewLeafPage(rightId, leftId, ceilId)

	fields := []Field{strF(1, "val")}
	for _, k := range leftKeys {
		rec := encodeLeafRec(t, k, fields)
		if _, ok := leftPage.InsertRecord(rec); !ok {
			t.Fatalf("InsertRecord(left, key=%d) failed", k)
		}
	}
	for _, k := range rightKeys {
		rec := encodeLeafRec(t, k, fields)
		if _, ok := rightPage.InsertRecord(rec); !ok {
			t.Fatalf("InsertRecord(right, key=%d) failed", k)
		}
	}

	parentPage, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(parent): %v", err)
	}
	*parentPage = *pagemanager.NewInternalPage(parentPage.GetPageId(), 1, rightMostId)

	maxLeft := leftKeys[len(leftKeys)-1]
	parentPage.InsertRecord(EncodeInternalRecord(maxLeft, leftId))
	if !rightIsRightMost {
		maxRight := rightKeys[len(rightKeys)-1]
		parentPage.InsertRecord(EncodeInternalRecord(maxRight, rightId))
	}

	for _, p := range []*pagemanager.Page{leftPage, rightPage, parentPage} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}

	bt = NewBTree(pm)
	return
}

// TestMergeLeaf_SearchConsistency_RightMost: after merging rightPage into
// leftPage (rightPage was RMC), a tree-level Search must find every key that
// was in either page.
func TestMergeLeaf_SearchConsistency_RightMost(t *testing.T) {
	leftKeys := []uint64{10, 20, 30}
	rightKeys := []uint64{40, 50}

	bt, pm, leftPage, rightPage, parentPage := setupMergeLeafEncoded(t, leftKeys, rightKeys, true)
	if err := pm.SetRootPageId(parentPage.GetPageId()); err != nil {
		t.Fatalf("SetRootPageId: %v", err)
	}

	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf: %v", err)
	}

	for _, k := range append(leftKeys, rightKeys...) {
		_, found, err := bt.Search(k)
		if err != nil {
			t.Errorf("Search(%d): unexpected error: %v", k, err)
		}
		if !found {
			t.Errorf("Search(%d): key not found after merge", k)
		}
	}
}

// TestMergeLeaf_SearchConsistency_NotRightMost: after merging rightPage into
// leftPage (rightPage was not RMC), a tree-level Search must find every key
// that was in either page and must also find keys in the ceiling page.
func TestMergeLeaf_SearchConsistency_NotRightMost(t *testing.T) {
	leftKeys := []uint64{10, 20}
	rightKeys := []uint64{30, 40}

	bt, pm, leftPage, rightPage, parentPage := setupMergeLeafEncoded(t, leftKeys, rightKeys, false)

	// Insert a record into ceilPage so we can verify it is still reachable.
	ceilId := parentPage.GetRightMostChild()
	ceilPage := mustReadPage(t, pm, ceilId)
	ceilKey := uint64(100)
	rec := encodeLeafRec(t, ceilKey, []Field{strF(1, "val")})
	if _, ok := ceilPage.InsertRecord(rec); !ok {
		t.Fatal("InsertRecord(ceil) failed")
	}
	if err := pm.WritePage(ceilPage); err != nil {
		t.Fatalf("WritePage(ceil): %v", err)
	}

	if err := pm.SetRootPageId(parentPage.GetPageId()); err != nil {
		t.Fatalf("SetRootPageId: %v", err)
	}

	if err := bt.mergeLeaf(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeLeaf: %v", err)
	}

	for _, k := range append(append(leftKeys, rightKeys...), ceilKey) {
		_, found, err := bt.Search(k)
		if err != nil {
			t.Errorf("Search(%d): unexpected error: %v", k, err)
		}
		if !found {
			t.Errorf("Search(%d): key not found after merge", k)
		}
	}
}

// ── mergeInternal helpers & tests ────────────────────────────────────────────

// setupMergeInternal builds two sibling internal pages and a parent, writes them
// to a fresh DB, and returns the BTree and pages.
//
// leftSlots/rightSlots are (key, childId) pairs (pre-sorted ascending).
// leftRmc/rightRmc are the RightMostChild values for each page.
// parentSepKey is the separator previously pushed to the parent; it must satisfy
// max(leftSlots keys) < parentSepKey < min(rightSlots keys).
//
// When rightIsRightMost=true, rightPage is the parent's rightMostChild.
// When false, a ceiling internal page is the parent's rightMostChild and
// rightPage has its own record keyed by max(rightSlots).
func setupMergeInternal(
	t *testing.T,
	leftSlots [][2]uint64, leftRmc uint32,
	rightSlots [][2]uint64, rightRmc uint32,
	parentSepKey uint64,
	rightIsRightMost bool,
) (bt *BTree, pm pagemanager.PageManager, leftPage, rightPage, parentPage *pagemanager.Page) {
	t.Helper()
	var err error
	pm, err = pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	leftPage, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(left): %v", err)
	}
	rightPage, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(right): %v", err)
	}
	leftId, rightId := leftPage.GetPageId(), rightPage.GetPageId()

	*leftPage = *pagemanager.NewInternalPage(leftId, 1, leftRmc)
	*rightPage = *pagemanager.NewInternalPage(rightId, 1, rightRmc)

	for _, s := range leftSlots {
		if _, ok := leftPage.InsertRecord(EncodeInternalRecord(s[0], uint32(s[1]))); !ok {
			t.Fatalf("InsertRecord(left, key=%d) failed", s[0])
		}
	}
	for _, s := range rightSlots {
		if _, ok := rightPage.InsertRecord(EncodeInternalRecord(s[0], uint32(s[1]))); !ok {
			t.Fatalf("InsertRecord(right, key=%d) failed", s[0])
		}
	}

	rightMostId := rightId
	if !rightIsRightMost {
		ceilPage, cerr := pm.AllocatePage()
		if cerr != nil {
			t.Fatalf("AllocatePage(ceil): %v", cerr)
		}
		ceilId := ceilPage.GetPageId()
		*ceilPage = *pagemanager.NewInternalPage(ceilId, 1, pagemanager.InvalidPageID)
		if err := pm.WritePage(ceilPage); err != nil {
			t.Fatalf("WritePage(ceil): %v", err)
		}
		rightMostId = ceilId
	}

	parentPage, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(parent): %v", err)
	}
	*parentPage = *pagemanager.NewInternalPage(parentPage.GetPageId(), 2, rightMostId)

	parentPage.InsertRecord(EncodeInternalRecord(parentSepKey, leftId))
	if !rightIsRightMost {
		var maxRight uint64
		for _, s := range rightSlots {
			if s[0] > maxRight {
				maxRight = s[0]
			}
		}
		parentPage.InsertRecord(EncodeInternalRecord(maxRight, rightId))
	}

	for _, p := range []*pagemanager.Page{leftPage, rightPage, parentPage} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}
	bt = NewBTree(pm)
	return
}

// TestMergeInternal_RightMostChild_SlotsMergedInOrder: rightPage is the parent's
// rightMostChild. After merge leftPage must contain all slots (including the bridge
// entry keyed by parentSepKey) in ascending key order.
func TestMergeInternal_RightMostChild_SlotsMergedInOrder(t *testing.T) {
	// left=[(10,101),(20,102)] rmc=103, sep=30, right=[(40,104),(50,105)] rmc=106
	// merged keys: [10,20,30,40,50]
	bt, pm, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{10, 101}, {20, 102}}, 103,
		[][2]uint64{{40, 104}, {50, 105}}, 106,
		30, true)

	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeInternal: %v", err)
	}

	left := mustReadPage(t, pm, leftPage.GetPageId())
	if !equalSlices(redistInternalKeys(t, left), []uint64{10, 20, 30, 40, 50}) {
		t.Errorf("merged keys: got %v, want [10 20 30 40 50]", redistInternalKeys(t, left))
	}
}

// TestMergeInternal_RightMostChild_ChildPointersIntact: all child IDs in the merged
// slot array must preserve the original pointers, including the bridge's child (leftRmc).
func TestMergeInternal_RightMostChild_ChildPointersIntact(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{10, 101}, {20, 102}}, 103,
		[][2]uint64{{40, 104}, {50, 105}}, 106,
		30, true)

	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeInternal: %v", err)
	}

	left := mustReadPage(t, pm, leftPage.GetPageId())
	// bridge slot child = leftRmc = 103; rightPage slots = (40,104),(50,105)
	if !equalSlicesU32(redistInternalChildIds(t, left), []uint32{101, 102, 103, 104, 105}) {
		t.Errorf("merged child IDs: got %v, want [101 102 103 104 105]", redistInternalChildIds(t, left))
	}
}

// TestMergeInternal_RightMostChild_RmcTransferred: after merge leftPage's
// RightMostChild must equal rightPage's original RightMostChild, not leftPage's old one.
func TestMergeInternal_RightMostChild_RmcTransferred(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{10, 101}, {20, 102}}, 103,
		[][2]uint64{{40, 104}, {50, 105}}, 106,
		30, true)

	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeInternal: %v", err)
	}

	left := mustReadPage(t, pm, leftPage.GetPageId())
	if left.GetRightMostChild() != 106 {
		t.Errorf("leftPage.rmc: got %d, want 106 (rightPage's original rmc)", left.GetRightMostChild())
	}
}

// TestMergeInternal_RightMostChild_ParentRMCBecomesLeft: the parent's
// rightMostChild must point to leftPage, not the freed rightPage.
func TestMergeInternal_RightMostChild_ParentRMCBecomesLeft(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{5, 101}}, 102,
		[][2]uint64{{30, 104}}, 105,
		15, true)

	leftId := leftPage.GetPageId()

	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeInternal: %v", err)
	}

	parent := mustReadPage(t, pm, parentPage.GetPageId())
	if parent.GetRightMostChild() != leftId {
		t.Errorf("parent.rmc: got %d, want %d (leftPage)", parent.GetRightMostChild(), leftId)
	}
	// leftPage's old slot entry is gone – it is now the rightMostChild.
	if _, found := redistParentSepKey(t, parent, leftId); found {
		t.Error("leftPage should have no slot entry in parent after becoming rightMostChild")
	}
}

// TestMergeInternal_RightMostChild_ParentBecomesEmpty: when the parent had exactly
// one slot entry (parentSepKey, leftPage) and rightPage was the RMC, the parent must
// end up with zero slot entries after the merge.
func TestMergeInternal_RightMostChild_ParentBecomesEmpty(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{10, 101}}, 102,
		[][2]uint64{{30, 104}}, 105,
		20, true)

	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeInternal: %v", err)
	}

	parent := mustReadPage(t, pm, parentPage.GetPageId())
	if parent.GetRowCount() != 0 {
		t.Errorf("parent slot count: got %d, want 0", parent.GetRowCount())
	}
	if parent.GetRightMostChild() != leftPage.GetPageId() {
		t.Errorf("parent.rmc: got %d, want %d (leftPage)", parent.GetRightMostChild(), leftPage.GetPageId())
	}
}

// TestMergeInternal_RightMostChild_BridgeKeyCorrect: the bridge slot inserted into
// the merged page must carry key=parentSepKey and child=leftPage's old RMC.
func TestMergeInternal_RightMostChild_BridgeKeyCorrect(t *testing.T) {
	const parentSep = uint64(30)
	const leftRmcVal = uint32(103)

	bt, pm, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{10, 101}, {20, 102}}, leftRmcVal,
		[][2]uint64{{40, 104}}, 105,
		parentSep, true)

	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeInternal: %v", err)
	}

	left := mustReadPage(t, pm, leftPage.GetPageId())
	bridgeFound := false
	for i := 0; i < int(left.GetRowCount()); i++ {
		rec, ok := left.GetRecord(i)
		if !ok {
			continue
		}
		k, cid, err := DecodeInternalRecord(rec)
		if err != nil {
			t.Fatalf("DecodeInternalRecord: %v", err)
		}
		if k == parentSep && cid == leftRmcVal {
			bridgeFound = true
			break
		}
	}
	if !bridgeFound {
		t.Errorf("bridge record (key=%d, child=%d) not found in merged page", parentSep, leftRmcVal)
	}
}

// TestMergeInternal_NotRightMostChild_SlotsMergedInOrder: rightPage has its own
// slot in the parent. After merge leftPage must hold all slots in ascending order.
func TestMergeInternal_NotRightMostChild_SlotsMergedInOrder(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{10, 101}, {20, 102}}, 103,
		[][2]uint64{{40, 104}, {50, 105}}, 106,
		30, false)

	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeInternal: %v", err)
	}

	left := mustReadPage(t, pm, leftPage.GetPageId())
	if !equalSlices(redistInternalKeys(t, left), []uint64{10, 20, 30, 40, 50}) {
		t.Errorf("merged keys: got %v, want [10 20 30 40 50]", redistInternalKeys(t, left))
	}
}

// TestMergeInternal_NotRightMostChild_RmcTransferred: leftPage's RMC must be
// updated to rightPage's original RMC even when rightPage is not the parent's RMC.
func TestMergeInternal_NotRightMostChild_RmcTransferred(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{10, 101}, {20, 102}}, 103,
		[][2]uint64{{40, 104}, {50, 105}}, 106,
		30, false)

	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeInternal: %v", err)
	}

	left := mustReadPage(t, pm, leftPage.GetPageId())
	if left.GetRightMostChild() != 106 {
		t.Errorf("leftPage.rmc: got %d, want 106", left.GetRightMostChild())
	}
}

// TestMergeInternal_NotRightMostChild_LeftSepRaisedToRightSep: leftPage's separator
// in the parent must be raised from parentSepKey to max(rightSlots) after the merge.
func TestMergeInternal_NotRightMostChild_LeftSepRaisedToRightSep(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{10, 101}, {20, 102}}, 103,
		[][2]uint64{{40, 104}, {50, 105}}, 106,
		30, false)

	leftId := leftPage.GetPageId()

	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeInternal: %v", err)
	}

	parent := mustReadPage(t, pm, parentPage.GetPageId())
	sep, found := redistParentSepKey(t, parent, leftId)
	if !found {
		t.Fatal("no separator for leftPage in parent after merge")
	}
	if sep != 50 {
		t.Errorf("leftPage separator: got %d, want 50 (old max of rightPage)", sep)
	}
}

// TestMergeInternal_NotRightMostChild_RightPageGoneFromParent: rightPage's slot
// entry must be removed from the parent after merge.
func TestMergeInternal_NotRightMostChild_RightPageGoneFromParent(t *testing.T) {
	bt, _, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{10, 101}}, 102,
		[][2]uint64{{40, 104}}, 105,
		25, false)

	_ = bt.mergeInternal(leftPage, rightPage, parentPage)

	if _, found := redistParentSepKey(t, parentPage, rightPage.GetPageId()); found {
		t.Error("rightPage should have no slot entry in parent after merge")
	}
}

// TestMergeInternal_NotRightMostChild_ParentSlotCount: parent had two slot entries
// before the merge; it must have exactly one afterwards.
func TestMergeInternal_NotRightMostChild_ParentSlotCount(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{10, 101}, {20, 102}}, 103,
		[][2]uint64{{40, 104}, {50, 105}}, 106,
		30, false)

	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeInternal: %v", err)
	}

	parent := mustReadPage(t, pm, parentPage.GetPageId())
	if parent.GetRowCount() != 1 {
		t.Errorf("parent slot count: got %d, want 1", parent.GetRowCount())
	}
}

// TestMergeInternal_NotRightMostChild_ParentRMCUnchanged: the parent's
// rightMostChild must remain the ceiling page, not rightPage or leftPage.
func TestMergeInternal_NotRightMostChild_ParentRMCUnchanged(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{10, 101}}, 102,
		[][2]uint64{{40, 104}}, 105,
		25, false)

	ceilId := parentPage.GetRightMostChild()

	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeInternal: %v", err)
	}

	parent := mustReadPage(t, pm, parentPage.GetPageId())
	if parent.GetRightMostChild() != ceilId {
		t.Errorf("parent.rmc: got %d, want %d (ceilPage)", parent.GetRightMostChild(), ceilId)
	}
}

// TestMergeInternal_PagesWrittenToDisk: re-read leftPage and parentPage from the
// page manager after merge to verify the durable state matches expectations.
func TestMergeInternal_PagesWrittenToDisk(t *testing.T) {
	// left=[(10,101),(20,102)] rmc=103, sep=30, right=[(40,104)] rmc=105
	// merged: slots=[(10,101),(20,102),(30,103),(40,104)], rmc=105; parent empty, rmc=leftId
	bt, pm, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{10, 101}, {20, 102}}, 103,
		[][2]uint64{{40, 104}}, 105,
		30, true)

	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeInternal: %v", err)
	}

	left := mustReadPage(t, pm, leftPage.GetPageId())
	parent := mustReadPage(t, pm, parentPage.GetPageId())

	if left.GetRowCount() != 4 {
		t.Errorf("leftPage slot count from disk: got %d, want 4", left.GetRowCount())
	}
	if left.GetRightMostChild() != 105 {
		t.Errorf("leftPage.rmc from disk: got %d, want 105", left.GetRightMostChild())
	}
	if parent.GetRowCount() != 0 {
		t.Errorf("parent slot count from disk: got %d, want 0", parent.GetRowCount())
	}
	if parent.GetRightMostChild() != leftPage.GetPageId() {
		t.Errorf("parent.rmc from disk: got %d, want leftPage (%d)", parent.GetRightMostChild(), leftPage.GetPageId())
	}
}

// TestMergeInternal_SingleSlotEach_RightMost: minimal case – one slot per page.
// Merged page must have 3 slots (left + bridge + right) with the correct RMC.
func TestMergeInternal_SingleSlotEach_RightMost(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{10, 101}}, 102,
		[][2]uint64{{30, 104}}, 105,
		20, true)

	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeInternal: %v", err)
	}

	left := mustReadPage(t, pm, leftPage.GetPageId())
	if !equalSlices(redistInternalKeys(t, left), []uint64{10, 20, 30}) {
		t.Errorf("merged keys: got %v, want [10 20 30]", redistInternalKeys(t, left))
	}
	if !equalSlicesU32(redistInternalChildIds(t, left), []uint32{101, 102, 104}) {
		t.Errorf("merged child IDs: got %v, want [101 102 104]", redistInternalChildIds(t, left))
	}
	if left.GetRightMostChild() != 105 {
		t.Errorf("leftPage.rmc: got %d, want 105", left.GetRightMostChild())
	}
}

// TestMergeInternal_ManySlots_RightMost: merging pages with several slots each
// verifies that key ordering is maintained across the full combined sequence.
func TestMergeInternal_ManySlots_RightMost(t *testing.T) {
	bt, pm, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{5, 11}, {10, 12}, {15, 13}, {20, 14}}, 15,
		[][2]uint64{{35, 21}, {40, 22}, {45, 23}}, 24,
		28, true)

	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeInternal: %v", err)
	}

	left := mustReadPage(t, pm, leftPage.GetPageId())
	wantKeys := []uint64{5, 10, 15, 20, 28, 35, 40, 45}
	if !equalSlices(redistInternalKeys(t, left), wantKeys) {
		t.Errorf("merged keys: got %v, want %v", redistInternalKeys(t, left), wantKeys)
	}
	if left.GetRightMostChild() != 24 {
		t.Errorf("leftPage.rmc: got %d, want 24", left.GetRightMostChild())
	}
	// sanity-check test invariant
	if !sort.SliceIsSorted(wantKeys, func(i, j int) bool { return wantKeys[i] < wantKeys[j] }) {
		t.Error("test invariant: wantKeys must be sorted")
	}
}

// TestMergeInternal_MultipleChildren_NotRightMost: parent has four children
// (floor, left, right, ceil). After merging left and right only the relevant
// parent entries change; the floor entry and the parent RMC are untouched.
func TestMergeInternal_MultipleChildren_NotRightMost(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	floorPage, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(floor): %v", err)
	}
	leftPage, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(left): %v", err)
	}
	rightPage, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(right): %v", err)
	}
	ceilPage, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(ceil): %v", err)
	}
	parentPage, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(parent): %v", err)
	}

	floorId := floorPage.GetPageId()
	leftId := leftPage.GetPageId()
	rightId := rightPage.GetPageId()
	ceilId := ceilPage.GetPageId()

	*floorPage = *pagemanager.NewInternalPage(floorId, 1, 501)
	*leftPage = *pagemanager.NewInternalPage(leftId, 1, 503)
	*rightPage = *pagemanager.NewInternalPage(rightId, 1, 505)
	*ceilPage = *pagemanager.NewInternalPage(ceilId, 1, 507)

	floorPage.InsertRecord(EncodeInternalRecord(10, 501))
	leftPage.InsertRecord(EncodeInternalRecord(30, 502))
	rightPage.InsertRecord(EncodeInternalRecord(60, 504))
	ceilPage.InsertRecord(EncodeInternalRecord(90, 506))

	// parent: [(10,floorId),(40,leftId),(70,rightId)], RMC=ceilId
	// parentSepKey for leftPage = 40; rightPage's sep = 70
	*parentPage = *pagemanager.NewInternalPage(parentPage.GetPageId(), 2, ceilId)
	parentPage.InsertRecord(EncodeInternalRecord(10, floorId))
	parentPage.InsertRecord(EncodeInternalRecord(40, leftId))
	parentPage.InsertRecord(EncodeInternalRecord(70, rightId))

	for _, p := range []*pagemanager.Page{floorPage, leftPage, rightPage, ceilPage, parentPage} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}

	bt := NewBTree(pm)
	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Fatalf("mergeInternal: %v", err)
	}

	parent := mustReadPage(t, pm, parentPage.GetPageId())

	// Parent must have 2 slots: (10,floorId) and (70,leftId). RMC=ceilId.
	if parent.GetRowCount() != 2 {
		t.Errorf("parent slot count: got %d, want 2", parent.GetRowCount())
	}
	if parent.GetRightMostChild() != ceilId {
		t.Errorf("parent.rmc: got %d, want %d (ceilPage)", parent.GetRightMostChild(), ceilId)
	}

	sepFloor, foundFloor := redistParentSepKey(t, parent, floorId)
	if !foundFloor {
		t.Error("floorPage separator missing from parent after merge")
	} else if sepFloor != 10 {
		t.Errorf("floor separator: got %d, want 10", sepFloor)
	}

	sepLeft, foundLeft := redistParentSepKey(t, parent, leftId)
	if !foundLeft {
		t.Error("leftPage separator missing from parent after merge")
	} else if sepLeft != 70 {
		t.Errorf("left separator after merge: got %d, want 70 (raised from 40 to rightPage's old key)", sepLeft)
	}

	if _, found := redistParentSepKey(t, parent, rightId); found {
		t.Error("rightPage separator should be gone from parent after merge")
	}
}

// TestMergeInternal_ReturnsNoError: mergeInternal must not return an error for a
// well-formed pair of sibling internal pages.
func TestMergeInternal_ReturnsNoError(t *testing.T) {
	bt, _, leftPage, rightPage, parentPage := setupMergeInternal(t,
		[][2]uint64{{5, 11}}, 12,
		[][2]uint64{{25, 13}}, 14,
		15, true)

	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMergeInternal_NoParentSep_ReturnsError: mergeInternal must return an error
// when the parent has no slot entry pointing to leftPage.
func TestMergeInternal_NoParentSep_ReturnsError(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	leftPage, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(left): %v", err)
	}
	rightPage, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(right): %v", err)
	}
	parentPage, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(parent): %v", err)
	}

	*leftPage = *pagemanager.NewInternalPage(leftPage.GetPageId(), 1, 999)
	*rightPage = *pagemanager.NewInternalPage(rightPage.GetPageId(), 1, 1000)
	// Parent has no slot entry pointing to leftPage.
	*parentPage = *pagemanager.NewInternalPage(parentPage.GetPageId(), 2, rightPage.GetPageId())

	bt := NewBTree(pm)
	if err := bt.mergeInternal(leftPage, rightPage, parentPage); err == nil {
		t.Error("expected error when no parent separator found for leftPage, got nil")
	}
}

// ── TestHandleUnderflow cases ─────────────────────────────────────────────────
//
// Free-space arithmetic (PageSize=4096, leaf/internal header=32 bytes → usable=4064):
//   Large leaf record : makeRecord(k, 200-byte value) = 208 bytes data + 4 slot = 212 bytes
//   Small leaf record : makeRecord(k, []byte("v"))    =   9 bytes data + 4 slot =  13 bytes
//   Internal record   : EncodeInternalRecord(k, cid)  =  12 bytes data + 4 slot =  16 bytes
//
//   Dense leaf   (10 large records)  : 10×212 = 2120 → free = 4064-2120 = 1944 < 2048 ✓
//   Sparse leaf  (3 small records)   :  3×13  =   39 → free = 4064-39   = 4025 > 2048 ✓
//   Dense internal (130 records)     : 130×16 = 2080 → free = 4064-2080 = 1984 < 2048 ✓
//   Sparse internal (3 records)      :  3×16  =   48 → free = 4064-48   = 4016 > 2048 ✓

// huLargeLeafRecord makes a leaf record with a 200-byte value.
func huLargeLeafRecord(key uint64) []byte { return makeRecord(key, make([]byte, 200)) }

// huDensifyInternal inserts 130 internal records so GetFreeSpace() drops below
// minPageFreeSpace (4064 − 130×16 = 1984 < 2048).
func huDensifyInternal(t *testing.T, page *pagemanager.Page) {
	t.Helper()
	for i := 0; i < 130; i++ {
		if _, ok := page.InsertRecord(EncodeInternalRecord(uint64(5000+i), uint32(9000+i))); !ok {
			t.Fatalf("huDensifyInternal: InsertRecord failed at i=%d", i)
		}
	}
}

// huLeafFreeSpace returns the free space of a freshly read leaf page.
func huLeafFreeSpace(t *testing.T, pm pagemanager.PageManager, id uint32) uint16 {
	t.Helper()
	return mustReadPage(t, pm, id).GetFreeSpace()
}

// ── Group 1: no-op scenarios (page NOT underflowing) ─────────────────────────

// TestHandleUnderflow_NoOp_DenseLeaf: a leaf page that is more than half full
// (free < minPageFreeSpace) must not be modified by handleUnderflow.
func TestHandleUnderflow_NoOp_DenseLeaf(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	page, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	*page = *pagemanager.NewLeafPage(page.GetPageId(), pagemanager.InvalidPageID, pagemanager.InvalidPageID)
	for i := 0; i < 10; i++ {
		page.InsertRecord(huLargeLeafRecord(uint64(i + 1)))
	}
	if err := pm.WritePage(page); err != nil {
		t.Fatalf("WritePage: %v", err)
	}
	freeBefore := page.GetFreeSpace()
	if freeBefore >= minPageFreeSpace {
		t.Fatalf("precondition: expected dense page (free < %d), got free=%d", minPageFreeSpace, freeBefore)
	}

	bt := NewBTree(pm)
	if err := bt.handleUnderflow(page, []uint32{}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	got := huLeafFreeSpace(t, pm, page.GetPageId())
	if got != freeBefore {
		t.Errorf("dense leaf was modified: free changed from %d to %d", freeBefore, got)
	}
}

// TestHandleUnderflow_NoOp_DenseInternal: same invariant for an internal page.
func TestHandleUnderflow_NoOp_DenseInternal(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	page, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	*page = *pagemanager.NewInternalPage(page.GetPageId(), 1, pagemanager.InvalidPageID)
	huDensifyInternal(t, page)
	if err := pm.WritePage(page); err != nil {
		t.Fatalf("WritePage: %v", err)
	}
	freeBefore := page.GetFreeSpace()
	if freeBefore >= minPageFreeSpace {
		t.Fatalf("precondition: expected dense page (free < %d), got free=%d", minPageFreeSpace, freeBefore)
	}

	bt := NewBTree(pm)
	if err := bt.handleUnderflow(page, []uint32{}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	got := mustReadPage(t, pm, page.GetPageId()).GetFreeSpace()
	if got != freeBefore {
		t.Errorf("dense internal was modified: free changed from %d to %d", freeBefore, got)
	}
}

// TestHandleUnderflow_NoOp_ExactThreshold: free == minPageFreeSpace is NOT
// strictly greater, so no underflow handling should fire.
func TestHandleUnderflow_NoOp_ExactThreshold(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	page, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	*page = *pagemanager.NewLeafPage(page.GetPageId(), pagemanager.InvalidPageID, pagemanager.InvalidPageID)
	// Insert records until free == minPageFreeSpace exactly.
	// Each large record consumes 212 bytes. Starting free = 4064.
	// We need free = 2048, so we consume 4064-2048 = 2016 bytes = 9 records × 212 = 1908 bytes consumed,
	// leaving 4064-1908=2156 free. Use 9 records of exact needed size instead:
	// recordSize + 4 = (4064 - minPageFreeSpace) / 9... simpler: insert until free just hits 2048.
	// Actually, use a record size s.t. one record drives free from above 2048 to exactly 2048.
	// 4064 - N*(dataLen+4) = 2048 → N*(dataLen+4) = 2016. Use N=1, dataLen+4=2016, dataLen=2012.
	rec := makeRecord(1, make([]byte, 2004)) // 8 key + 2004 value = 2012 data + 4 slot = 2016 total
	if _, ok := page.InsertRecord(rec); !ok {
		t.Skip("record too large to insert; threshold test skipped")
	}
	if err := pm.WritePage(page); err != nil {
		t.Fatalf("WritePage: %v", err)
	}
	free := page.GetFreeSpace()
	if free != minPageFreeSpace {
		t.Skipf("could not hit exact threshold (got free=%d, want %d); skipping", free, minPageFreeSpace)
	}

	bt := NewBTree(pm)
	if err := bt.handleUnderflow(page, []uint32{}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	got := huLeafFreeSpace(t, pm, page.GetPageId())
	if got != free {
		t.Errorf("page at exact threshold was modified: free changed from %d to %d", free, got)
	}
}

// ── Group 2: root underflow (empty path) ─────────────────────────────────────

// TestHandleUnderflow_Root_SparseLeaf_NoOp: even if a leaf page is underflowing,
// when path is empty (page is the root) handleUnderflow must return nil without touching anything.
func TestHandleUnderflow_Root_SparseLeaf_NoOp(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	page, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	*page = *pagemanager.NewLeafPage(page.GetPageId(), pagemanager.InvalidPageID, pagemanager.InvalidPageID)
	// 3 small records → free > minPageFreeSpace (underflowing).
	for _, k := range []uint64{10, 20, 30} {
		page.InsertRecord(makeRecord(k, []byte("v")))
	}
	if err := pm.WritePage(page); err != nil {
		t.Fatalf("WritePage: %v", err)
	}

	bt := NewBTree(pm)
	if err := bt.handleUnderflow(page, []uint32{}); err != nil {
		t.Fatalf("handleUnderflow on root: %v", err)
	}

	// Row count must be unchanged.
	after := mustReadPage(t, pm, page.GetPageId())
	if after.GetRowCount() != 3 {
		t.Errorf("root page was modified: row count changed to %d", after.GetRowCount())
	}
}

// TestHandleUnderflow_Root_SparseInternal_NoOp: same check for an internal root.
func TestHandleUnderflow_Root_SparseInternal_NoOp(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	page, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	*page = *pagemanager.NewInternalPage(page.GetPageId(), 1, 999)
	page.InsertRecord(EncodeInternalRecord(50, 998))
	if err := pm.WritePage(page); err != nil {
		t.Fatalf("WritePage: %v", err)
	}

	bt := NewBTree(pm)
	if err := bt.handleUnderflow(page, []uint32{}); err != nil {
		t.Fatalf("handleUnderflow on root: %v", err)
	}

	after := mustReadPage(t, pm, page.GetPageId())
	if after.GetRowCount() != 1 {
		t.Errorf("root page was modified: row count changed to %d", after.GetRowCount())
	}
}

// ── Group 3: leaf redistribute (dense sibling) ───────────────────────────────

// setupHULeafRedistRight builds:
//
//	parent: [(maxPageKey, pageId)], rmc=siblingId
//	page  : 3 small records with keys pageKeys (sparse, underflowing)
//	sibling: 10 large records with keys 1000,1010,...,1090 (dense)
//
// In this layout page is NOT the rightmost child, so handleUnderflow picks
// sibling as the right sibling and calls redistributeLeaf(page, sibling, parent).
func setupHULeafRedistRight(t *testing.T, pageKeys []uint64) (bt *BTree, pm pagemanager.PageManager, page, sibling, parent *pagemanager.Page) {
	t.Helper()
	var err error
	pm, err = pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	page, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(page): %v", err)
	}
	sibling, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(sibling): %v", err)
	}
	pageId, sibId := page.GetPageId(), sibling.GetPageId()

	*page = *pagemanager.NewLeafPage(pageId, pagemanager.InvalidPageID, sibId)
	*sibling = *pagemanager.NewLeafPage(sibId, pageId, pagemanager.InvalidPageID)

	for _, k := range pageKeys {
		if _, ok := page.InsertRecord(makeRecord(k, []byte("v"))); !ok {
			t.Fatalf("InsertRecord(page, key=%d) failed", k)
		}
	}
	for i := 0; i < 10; i++ {
		if _, ok := sibling.InsertRecord(huLargeLeafRecord(uint64(1000 + i*10))); !ok {
			t.Fatalf("InsertRecord(dense sibling) failed at i=%d", i)
		}
	}

	parent, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(parent): %v", err)
	}
	*parent = *pagemanager.NewInternalPage(parent.GetPageId(), 1, sibId)
	maxPage := pageKeys[len(pageKeys)-1]
	parent.InsertRecord(EncodeInternalRecord(maxPage, pageId))

	for _, p := range []*pagemanager.Page{page, sibling, parent} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}
	bt = NewBTree(pm)
	return
}

// setupHULeafRedistLeft builds:
//
//	parent: [(maxSiblingKey, siblingId)], rmc=pageId
//	sibling: 10 large records (dense, keys 1000,1010,...,1090)
//	page   : 3 small records (sparse, keys pageKeys) — is the rightmost child
//
// handleUnderflow identifies sibling as the left sibling and calls
// redistributeLeaf(sibling, page, parent).
func setupHULeafRedistLeft(t *testing.T, pageKeys []uint64) (bt *BTree, pm pagemanager.PageManager, page, sibling, parent *pagemanager.Page) {
	t.Helper()
	var err error
	pm, err = pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	page, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(page): %v", err)
	}
	sibling, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(sibling): %v", err)
	}
	pageId, sibId := page.GetPageId(), sibling.GetPageId()

	*sibling = *pagemanager.NewLeafPage(sibId, pagemanager.InvalidPageID, pageId)
	*page = *pagemanager.NewLeafPage(pageId, sibId, pagemanager.InvalidPageID)

	for i := 0; i < 10; i++ {
		if _, ok := sibling.InsertRecord(huLargeLeafRecord(uint64(1000 + i*10))); !ok {
			t.Fatalf("InsertRecord(dense sibling) failed at i=%d", i)
		}
	}
	for _, k := range pageKeys {
		if _, ok := page.InsertRecord(makeRecord(k, []byte("v"))); !ok {
			t.Fatalf("InsertRecord(page, key=%d) failed", k)
		}
	}

	parent, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(parent): %v", err)
	}
	*parent = *pagemanager.NewInternalPage(parent.GetPageId(), 1, pageId)
	// sibling's max key is 1090
	parent.InsertRecord(EncodeInternalRecord(1090, sibId))

	for _, p := range []*pagemanager.Page{page, sibling, parent} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}
	bt = NewBTree(pm)
	return
}

// TestHandleUnderflow_Leaf_RightSibling_Dense_Redistributes verifies that when
// the right sibling is dense, handleUnderflow calls redistributeLeaf and both
// pages end up with records.
func TestHandleUnderflow_Leaf_RightSibling_Dense_Redistributes(t *testing.T) {
	bt, pm, page, sibling, parent := setupHULeafRedistRight(t, []uint64{1, 2, 3})
	pageId, sibId := page.GetPageId(), sibling.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parent.GetPageId()}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	p := mustReadPage(t, pm, pageId)
	s := mustReadPage(t, pm, sibId)

	if p.GetRowCount() == 0 {
		t.Error("page has no records after redistribute")
	}
	if s.GetRowCount() == 0 {
		t.Error("sibling has no records after redistribute")
	}
	// Combined total must be preserved (3 + 10 = 13).
	if int(p.GetRowCount())+int(s.GetRowCount()) != 13 {
		t.Errorf("total record count changed: got %d, want 13", int(p.GetRowCount())+int(s.GetRowCount()))
	}
}

// TestHandleUnderflow_Leaf_LeftSibling_Dense_Redistributes: same with the
// underflowing page as the rightmost child (left sibling scenario).
func TestHandleUnderflow_Leaf_LeftSibling_Dense_Redistributes(t *testing.T) {
	bt, pm, page, sibling, parent := setupHULeafRedistLeft(t, []uint64{2000, 2001, 2002})
	pageId, sibId := page.GetPageId(), sibling.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parent.GetPageId()}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	p := mustReadPage(t, pm, pageId)
	s := mustReadPage(t, pm, sibId)

	if p.GetRowCount() == 0 {
		t.Error("page has no records after redistribute")
	}
	if s.GetRowCount() == 0 {
		t.Error("sibling has no records after redistribute")
	}
	if int(p.GetRowCount())+int(s.GetRowCount()) != 13 {
		t.Errorf("total record count changed: got %d, want 13", int(p.GetRowCount())+int(s.GetRowCount()))
	}
}

// TestHandleUnderflow_Leaf_Redistribute_RecordsEvenlySplit: the 13 combined
// records must be split floor(13/2)=6 left, 7 right.
func TestHandleUnderflow_Leaf_Redistribute_RecordsEvenlySplit(t *testing.T) {
	bt, pm, page, sibling, parent := setupHULeafRedistRight(t, []uint64{1, 2, 3})
	pageId, sibId := page.GetPageId(), sibling.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parent.GetPageId()}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	// Re-read from disk: handleUnderflow allocates its own page pointer for the
	// sibling internally, so the caller's sibling variable is stale.
	p := mustReadPage(t, pm, pageId)
	s := mustReadPage(t, pm, sibId)

	// redistributeLeaf splits at mid = 13/2 = 6.
	if p.GetRowCount() != 6 {
		t.Errorf("page row count: got %d, want 6", p.GetRowCount())
	}
	if s.GetRowCount() != 7 {
		t.Errorf("sibling row count: got %d, want 7", s.GetRowCount())
	}
}

// TestHandleUnderflow_Leaf_Redistribute_KeysRemainSorted: both pages must have
// strictly ascending key order after redistribution.
func TestHandleUnderflow_Leaf_Redistribute_KeysRemainSorted(t *testing.T) {
	bt, pm, page, sibling, parent := setupHULeafRedistRight(t, []uint64{1, 2, 3})

	if err := bt.handleUnderflow(page, []uint32{parent.GetPageId()}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	for _, id := range []uint32{page.GetPageId(), sibling.GetPageId()} {
		p := mustReadPage(t, pm, id)
		keys := redistLeafKeys(t, p)
		for i := 1; i < len(keys); i++ {
			if keys[i] <= keys[i-1] {
				t.Errorf("page %d: keys out of order at index %d: %v", id, i, keys)
			}
		}
	}
}

// TestHandleUnderflow_Leaf_Redistribute_AllOriginalKeysPresent: no key must be
// lost during redistribution.
func TestHandleUnderflow_Leaf_Redistribute_AllOriginalKeysPresent(t *testing.T) {
	bt, pm, page, sibling, parent := setupHULeafRedistRight(t, []uint64{1, 2, 3})

	// Collect the original keys from the sibling before the call.
	wantKeys := []uint64{1, 2, 3}
	for i := 0; i < 10; i++ {
		wantKeys = append(wantKeys, uint64(1000+i*10))
	}
	sort.Slice(wantKeys, func(i, j int) bool { return wantKeys[i] < wantKeys[j] })

	if err := bt.handleUnderflow(page, []uint32{parent.GetPageId()}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	var gotKeys []uint64
	for _, id := range []uint32{page.GetPageId(), sibling.GetPageId()} {
		p := mustReadPage(t, pm, id)
		gotKeys = append(gotKeys, redistLeafKeys(t, p)...)
	}
	sort.Slice(gotKeys, func(i, j int) bool { return gotKeys[i] < gotKeys[j] })

	if !equalSlices(gotKeys, wantKeys) {
		t.Errorf("keys mismatch after redistribute:\n  got  %v\n  want %v", gotKeys, wantKeys)
	}
}

// TestHandleUnderflow_Leaf_Redistribute_ParentSeparatorUpdated: the parent's
// separator for the left page must be updated to max(leftPage) after redistribution.
func TestHandleUnderflow_Leaf_Redistribute_ParentSeparatorUpdated(t *testing.T) {
	bt, pm, page, _, parent := setupHULeafRedistRight(t, []uint64{1, 2, 3})
	pageId := page.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parent.GetPageId()}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	parentAfter := mustReadPage(t, pm, parent.GetPageId())
	pageAfter := mustReadPage(t, pm, pageId)

	sep, found := redistParentSepKey(t, parentAfter, pageId)
	if !found {
		t.Fatal("parent no longer has a separator for the page")
	}
	// Separator must equal the max key now stored in the left page.
	lastKey := redistLeafKeys(t, pageAfter)
	maxLeft := lastKey[len(lastKey)-1]
	if sep != maxLeft {
		t.Errorf("parent separator: got %d, want %d (max of left page)", sep, maxLeft)
	}
}

// TestHandleUnderflow_Leaf_Redistribute_PagesWrittenToDisk: re-reading the pages
// after the call must reflect the redistributed state.
func TestHandleUnderflow_Leaf_Redistribute_PagesWrittenToDisk(t *testing.T) {
	bt, pm, page, sibling, parent := setupHULeafRedistRight(t, []uint64{1, 2, 3})
	pageId, sibId, parentId := page.GetPageId(), sibling.GetPageId(), parent.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parentId}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	p := mustReadPage(t, pm, pageId)
	s := mustReadPage(t, pm, sibId)
	par := mustReadPage(t, pm, parentId)

	if int(p.GetRowCount())+int(s.GetRowCount()) != 13 {
		t.Errorf("total row count after re-read: got %d, want 13", int(p.GetRowCount())+int(s.GetRowCount()))
	}
	_, found := redistParentSepKey(t, par, pageId)
	if !found {
		t.Error("parent separator missing after re-read")
	}
}

// ── Group 4: leaf merge (sparse sibling) ─────────────────────────────────────

// setupHULeafMergeRight builds:
//
//	parent: [(maxPageKey, pageId)], rmc=siblingId
//	page  : sparse (3 small records, keys pageKeys)
//	sibling: sparse (2 small records, keys 500, 600) — rightmost child of parent
//
// handleUnderflow picks sibling as the right sibling and calls
// mergeLeaf(page, sibling, parent).
func setupHULeafMergeRight(t *testing.T, pageKeys []uint64) (bt *BTree, pm pagemanager.PageManager, page, sibling, parent *pagemanager.Page) {
	t.Helper()
	var err error
	pm, err = pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	page, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(page): %v", err)
	}
	sibling, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(sibling): %v", err)
	}
	pageId, sibId := page.GetPageId(), sibling.GetPageId()

	*page = *pagemanager.NewLeafPage(pageId, pagemanager.InvalidPageID, sibId)
	*sibling = *pagemanager.NewLeafPage(sibId, pageId, pagemanager.InvalidPageID)

	for _, k := range pageKeys {
		page.InsertRecord(makeRecord(k, []byte("v")))
	}
	sibling.InsertRecord(makeRecord(500, []byte("v")))
	sibling.InsertRecord(makeRecord(600, []byte("v")))

	parent, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(parent): %v", err)
	}
	*parent = *pagemanager.NewInternalPage(parent.GetPageId(), 1, sibId)
	maxPage := pageKeys[len(pageKeys)-1]
	parent.InsertRecord(EncodeInternalRecord(maxPage, pageId))

	for _, p := range []*pagemanager.Page{page, sibling, parent} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}
	bt = NewBTree(pm)
	return
}

// setupHULeafMergeLeft builds:
//
//	parent: [(maxSiblingKey, siblingId)], rmc=pageId
//	sibling: sparse (keys 1, 2)
//	page   : sparse (keys pageKeys) — is the rightmost child
//
// handleUnderflow identifies sibling as the left sibling and calls
// mergeLeaf(sibling, page, parent).
func setupHULeafMergeLeft(t *testing.T, pageKeys []uint64) (bt *BTree, pm pagemanager.PageManager, page, sibling, parent *pagemanager.Page) {
	t.Helper()
	var err error
	pm, err = pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	page, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(page): %v", err)
	}
	sibling, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(sibling): %v", err)
	}
	pageId, sibId := page.GetPageId(), sibling.GetPageId()

	*sibling = *pagemanager.NewLeafPage(sibId, pagemanager.InvalidPageID, pageId)
	*page = *pagemanager.NewLeafPage(pageId, sibId, pagemanager.InvalidPageID)

	sibling.InsertRecord(makeRecord(1, []byte("v")))
	sibling.InsertRecord(makeRecord(2, []byte("v")))
	for _, k := range pageKeys {
		page.InsertRecord(makeRecord(k, []byte("v")))
	}

	parent, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(parent): %v", err)
	}
	*parent = *pagemanager.NewInternalPage(parent.GetPageId(), 1, pageId)
	parent.InsertRecord(EncodeInternalRecord(2, sibId))

	for _, p := range []*pagemanager.Page{page, sibling, parent} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}
	bt = NewBTree(pm)
	return
}

// TestHandleUnderflow_Leaf_RightSibling_Sparse_Merges: when both page and its
// right sibling are sparse, handleUnderflow merges them. The surviving left page
// must hold all records and the parent must have one fewer entry.
func TestHandleUnderflow_Leaf_RightSibling_Sparse_Merges(t *testing.T) {
	bt, pm, page, sibling, parent := setupHULeafMergeRight(t, []uint64{10, 20, 30})
	pageId, sibId := page.GetPageId(), sibling.GetPageId()
	parentId := parent.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parentId}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	p := mustReadPage(t, pm, pageId)
	par := mustReadPage(t, pm, parentId)

	// All 5 records (3 + 2) must be in the surviving page.
	if p.GetRowCount() != 5 {
		t.Errorf("merged page row count: got %d, want 5", p.GetRowCount())
	}
	// Parent's only slot (pointing to page) must have been removed;
	// parent now has pageId as rightMostChild and zero slot entries.
	if par.GetRowCount() != 0 {
		t.Errorf("parent row count after merge: got %d, want 0", par.GetRowCount())
	}
	if par.GetRightMostChild() != pageId {
		t.Errorf("parent rmc after merge: got %d, want %d", par.GetRightMostChild(), pageId)
	}
	// sibId must have been freed (now at head of free list).
	_ = sibId // just checking via the page manager's meta is sufficient
}

// TestHandleUnderflow_Leaf_LeftSibling_Sparse_Merges: page is the rightmost child,
// sibling is to the left. mergeLeaf(sibling, page, parent) must run; the sibling
// (left) absorbs page's records and page is freed.
func TestHandleUnderflow_Leaf_LeftSibling_Sparse_Merges(t *testing.T) {
	bt, pm, page, sibling, parent := setupHULeafMergeLeft(t, []uint64{100, 200, 300})
	sibId := sibling.GetPageId()
	parentId := parent.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parentId}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	sib := mustReadPage(t, pm, sibId)
	par := mustReadPage(t, pm, parentId)

	// sibling absorbs page's 3 records plus its own 2 → 5 total.
	if sib.GetRowCount() != 5 {
		t.Errorf("merged sibling row count: got %d, want 5", sib.GetRowCount())
	}
	if par.GetRowCount() != 0 {
		t.Errorf("parent row count after merge: got %d, want 0", par.GetRowCount())
	}
	if par.GetRightMostChild() != sibId {
		t.Errorf("parent rmc after merge: got %d, want %d", par.GetRightMostChild(), sibId)
	}
}

// TestHandleUnderflow_Leaf_Merge_AllRecordsPreserved: no record must be lost
// when merging two sparse leaf pages.
func TestHandleUnderflow_Leaf_Merge_AllRecordsPreserved(t *testing.T) {
	bt, pm, page, _, parent := setupHULeafMergeRight(t, []uint64{10, 20, 30})
	pageId := page.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parent.GetPageId()}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	p := mustReadPage(t, pm, pageId)
	got := redistLeafKeys(t, p)
	want := []uint64{10, 20, 30, 500, 600}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if !equalSlices(got, want) {
		t.Errorf("merged page keys: got %v, want %v", got, want)
	}
}

// TestHandleUnderflow_Leaf_Merge_KeysRemainSorted: the merged page must have
// its records in ascending key order.
func TestHandleUnderflow_Leaf_Merge_KeysRemainSorted(t *testing.T) {
	bt, pm, page, _, parent := setupHULeafMergeRight(t, []uint64{10, 20, 30})
	pageId := page.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parent.GetPageId()}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	p := mustReadPage(t, pm, pageId)
	keys := redistLeafKeys(t, p)
	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			t.Errorf("merged page: keys out of order at index %d: %v", i, keys)
		}
	}
}

// TestHandleUnderflow_Leaf_Merge_SiblingChainUpdated: when merging and the freed
// right sibling had its own right neighbour, that neighbour's leftSibling pointer
// must be updated to point to the surviving left page.
func TestHandleUnderflow_Leaf_Merge_SiblingChainUpdated(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	page, _ := pm.AllocatePage()
	sibling, _ := pm.AllocatePage()
	beyond, _ := pm.AllocatePage()
	pageId, sibId, beyondId := page.GetPageId(), sibling.GetPageId(), beyond.GetPageId()

	// Leaf chain: page ↔ sibling ↔ beyond.
	*page = *pagemanager.NewLeafPage(pageId, pagemanager.InvalidPageID, sibId)
	*sibling = *pagemanager.NewLeafPage(sibId, pageId, beyondId)
	*beyond = *pagemanager.NewLeafPage(beyondId, sibId, pagemanager.InvalidPageID)

	page.InsertRecord(makeRecord(10, []byte("v")))
	sibling.InsertRecord(makeRecord(100, []byte("v")))
	beyond.InsertRecord(makeRecord(500, []byte("v")))

	parent, _ := pm.AllocatePage()
	parentId := parent.GetPageId()
	*parent = *pagemanager.NewInternalPage(parentId, 1, beyondId)
	parent.InsertRecord(EncodeInternalRecord(10, pageId))
	parent.InsertRecord(EncodeInternalRecord(100, sibId))

	for _, p := range []*pagemanager.Page{page, sibling, beyond, parent} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}

	bt := NewBTree(pm)
	if err := bt.handleUnderflow(page, []uint32{parentId}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	// After merging page (left) and sibling (right):
	// page.rightSibling must be beyondId.
	pageAfter := mustReadPage(t, pm, pageId)
	if pageAfter.GetRightSibling() != beyondId {
		t.Errorf("page.rightSibling: got %d, want %d", pageAfter.GetRightSibling(), beyondId)
	}
	// beyond.leftSibling must be pageId.
	beyondAfter := mustReadPage(t, pm, beyondId)
	if beyondAfter.GetLeftSibling() != pageId {
		t.Errorf("beyond.leftSibling: got %d, want %d", beyondAfter.GetLeftSibling(), pageId)
	}
}

// TestHandleUnderflow_Leaf_Merge_PagesWrittenToDisk: state must be durable after merge.
func TestHandleUnderflow_Leaf_Merge_PagesWrittenToDisk(t *testing.T) {
	bt, pm, page, _, parent := setupHULeafMergeRight(t, []uint64{10, 20, 30})
	pageId, parentId := page.GetPageId(), parent.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parentId}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	p := mustReadPage(t, pm, pageId)
	par := mustReadPage(t, pm, parentId)

	if p.GetRowCount() != 5 {
		t.Errorf("merged page row count after re-read: got %d, want 5", p.GetRowCount())
	}
	if par.GetRowCount() != 0 {
		t.Errorf("parent row count after re-read: got %d, want 0", par.GetRowCount())
	}
}

// ── Group 5: internal page redistribute (dense sibling) ──────────────────────

// setupHUInternalRedistRight builds:
//
//	parent: [(parentSep, pageId)], rmc=siblingId
//	page  : sparse internal (3 records, keys 10,20,30; rmc=99)
//	sibling: dense internal (130 records; rmc=8999)
//
// handleUnderflow picks sibling as the right sibling and calls
// redistributeInternal(page, sibling, parent).
func setupHUInternalRedistRight(t *testing.T) (bt *BTree, pm pagemanager.PageManager, page, sibling, parent *pagemanager.Page) {
	t.Helper()
	var err error
	pm, err = pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	page, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(page): %v", err)
	}
	sibling, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(sibling): %v", err)
	}
	pageId, sibId := page.GetPageId(), sibling.GetPageId()

	*page = *pagemanager.NewInternalPage(pageId, 1, 99)
	page.InsertRecord(EncodeInternalRecord(10, 97))
	page.InsertRecord(EncodeInternalRecord(20, 98))
	page.InsertRecord(EncodeInternalRecord(30, 99))

	*sibling = *pagemanager.NewInternalPage(sibId, 1, 8999)
	huDensifyInternal(t, sibling)

	parent, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(parent): %v", err)
	}
	*parent = *pagemanager.NewInternalPage(parent.GetPageId(), 2, sibId)
	// parentSep key must be > all keys in page and < all keys in sibling.
	parent.InsertRecord(EncodeInternalRecord(40, pageId))

	for _, p := range []*pagemanager.Page{page, sibling, parent} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}
	bt = NewBTree(pm)
	return
}

// setupHUInternalRedistLeft: page is rightmost child; sibling is dense to the left.
func setupHUInternalRedistLeft(t *testing.T) (bt *BTree, pm pagemanager.PageManager, page, sibling, parent *pagemanager.Page) {
	t.Helper()
	var err error
	pm, err = pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	page, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(page): %v", err)
	}
	sibling, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(sibling): %v", err)
	}
	pageId, sibId := page.GetPageId(), sibling.GetPageId()

	*sibling = *pagemanager.NewInternalPage(sibId, 1, 8999)
	huDensifyInternal(t, sibling)

	*page = *pagemanager.NewInternalPage(pageId, 1, 6099)
	page.InsertRecord(EncodeInternalRecord(6000, 6097))
	page.InsertRecord(EncodeInternalRecord(6010, 6098))
	page.InsertRecord(EncodeInternalRecord(6020, 6099))

	parent, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(parent): %v", err)
	}
	*parent = *pagemanager.NewInternalPage(parent.GetPageId(), 2, pageId)
	// sibling's max key from huDensifyInternal is 5000+129 = 5129.
	parent.InsertRecord(EncodeInternalRecord(5129, sibId))

	for _, p := range []*pagemanager.Page{page, sibling, parent} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}
	bt = NewBTree(pm)
	return
}

// TestHandleUnderflow_Internal_RightSibling_Dense_Redistributes: when the right
// sibling is dense, both internal pages must remain with records after handleUnderflow.
func TestHandleUnderflow_Internal_RightSibling_Dense_Redistributes(t *testing.T) {
	bt, pm, page, sibling, parent := setupHUInternalRedistRight(t)
	pageId, sibId := page.GetPageId(), sibling.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parent.GetPageId()}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	p := mustReadPage(t, pm, pageId)
	s := mustReadPage(t, pm, sibId)

	if p.GetRowCount() == 0 {
		t.Error("page has no records after redistribute")
	}
	if s.GetRowCount() == 0 {
		t.Error("sibling has no records after redistribute")
	}
	// 3 page slots + bridge(1) + 130 sibling slots = 134 total in allRecords;
	// mid=67 → page gets 67 slots, sibling gets 66 slots (mid+1..133).
	totalWant := 3 + 130
	if int(p.GetRowCount())+int(s.GetRowCount()) != totalWant {
		t.Errorf("total slot count changed: got %d, want %d", int(p.GetRowCount())+int(s.GetRowCount()), totalWant)
	}
}

// TestHandleUnderflow_Internal_LeftSibling_Dense_Redistributes: page is the
// rightmost child; dense left sibling triggers redistribute.
func TestHandleUnderflow_Internal_LeftSibling_Dense_Redistributes(t *testing.T) {
	bt, pm, page, sibling, parent := setupHUInternalRedistLeft(t)
	pageId, sibId := page.GetPageId(), sibling.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parent.GetPageId()}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	p := mustReadPage(t, pm, pageId)
	s := mustReadPage(t, pm, sibId)

	if p.GetRowCount() == 0 {
		t.Error("page has no records after redistribute")
	}
	if s.GetRowCount() == 0 {
		t.Error("sibling has no records after redistribute")
	}
	totalWant := 130 + 3
	if int(p.GetRowCount())+int(s.GetRowCount()) != totalWant {
		t.Errorf("total slot count: got %d, want %d", int(p.GetRowCount())+int(s.GetRowCount()), totalWant)
	}
}

// TestHandleUnderflow_Internal_Redistribute_ParentSeparatorUpdated: after
// redistributeInternal the parent separator for the left page must be the
// promoted boundary key — strictly greater than max(leftPage slots) and
// strictly less than min(rightPage slots).
func TestHandleUnderflow_Internal_Redistribute_ParentSeparatorUpdated(t *testing.T) {
	bt, pm, page, sibling, parent := setupHUInternalRedistRight(t)
	pageId, sibId := page.GetPageId(), sibling.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parent.GetPageId()}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	parentAfter := mustReadPage(t, pm, parent.GetPageId())
	pageAfter := mustReadPage(t, pm, pageId)
	sibAfter := mustReadPage(t, pm, sibId)

	sep, found := redistParentSepKey(t, parentAfter, pageId)
	if !found {
		t.Fatal("parent has no separator for page after redistribute")
	}

	// For internal pages the promoted key sits between the two halves:
	//   max(leftPage slots) < sep <= min(rightPage slots).
	pageKeys := redistInternalKeys(t, pageAfter)
	sibKeys := redistInternalKeys(t, sibAfter)
	maxLeft := pageKeys[len(pageKeys)-1]
	minRight := sibKeys[0]

	if sep <= maxLeft {
		t.Errorf("separator %d is not > max(left)=%d", sep, maxLeft)
	}
	if sep > minRight {
		t.Errorf("separator %d is not <= min(right)=%d", sep, minRight)
	}
}

// ── Group 6: internal page merge (sparse sibling) ────────────────────────────

// setupHUInternalMergeRight builds:
//
//	parent: [(40, pageId)], rmc=siblingId
//	page  : sparse (3 records, keys 10,20,30; rmc=99)
//	sibling: sparse (2 records, keys 200,300; rmc=399)
func setupHUInternalMergeRight(t *testing.T) (bt *BTree, pm pagemanager.PageManager, page, sibling, parent *pagemanager.Page) {
	t.Helper()
	var err error
	pm, err = pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	page, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(page): %v", err)
	}
	sibling, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(sibling): %v", err)
	}
	pageId, sibId := page.GetPageId(), sibling.GetPageId()

	*page = *pagemanager.NewInternalPage(pageId, 1, 99)
	page.InsertRecord(EncodeInternalRecord(10, 97))
	page.InsertRecord(EncodeInternalRecord(20, 98))
	page.InsertRecord(EncodeInternalRecord(30, 99))

	*sibling = *pagemanager.NewInternalPage(sibId, 1, 399)
	sibling.InsertRecord(EncodeInternalRecord(200, 397))
	sibling.InsertRecord(EncodeInternalRecord(300, 398))

	parent, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(parent): %v", err)
	}
	*parent = *pagemanager.NewInternalPage(parent.GetPageId(), 2, sibId)
	parent.InsertRecord(EncodeInternalRecord(40, pageId))

	for _, p := range []*pagemanager.Page{page, sibling, parent} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}
	bt = NewBTree(pm)
	return
}

// setupHUInternalMergeLeft: page is rightmost child; sparse sibling is to the left.
func setupHUInternalMergeLeft(t *testing.T) (bt *BTree, pm pagemanager.PageManager, page, sibling, parent *pagemanager.Page) {
	t.Helper()
	var err error
	pm, err = pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	page, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(page): %v", err)
	}
	sibling, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(sibling): %v", err)
	}
	pageId, sibId := page.GetPageId(), sibling.GetPageId()

	*sibling = *pagemanager.NewInternalPage(sibId, 1, 99)
	sibling.InsertRecord(EncodeInternalRecord(10, 97))
	sibling.InsertRecord(EncodeInternalRecord(20, 98))

	*page = *pagemanager.NewInternalPage(pageId, 1, 399)
	page.InsertRecord(EncodeInternalRecord(200, 397))
	page.InsertRecord(EncodeInternalRecord(300, 398))
	page.InsertRecord(EncodeInternalRecord(350, 399))

	parent, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(parent): %v", err)
	}
	*parent = *pagemanager.NewInternalPage(parent.GetPageId(), 2, pageId)
	parent.InsertRecord(EncodeInternalRecord(30, sibId))

	for _, p := range []*pagemanager.Page{page, sibling, parent} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}
	bt = NewBTree(pm)
	return
}

// TestHandleUnderflow_Internal_RightSibling_Sparse_Merges: both internal pages
// sparse → mergeInternal called → left (page) absorbs sibling records + bridge.
func TestHandleUnderflow_Internal_RightSibling_Sparse_Merges(t *testing.T) {
	bt, pm, page, sibling, parent := setupHUInternalMergeRight(t)
	pageId, sibId := page.GetPageId(), sibling.GetPageId()
	parentId := parent.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parentId}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	p := mustReadPage(t, pm, pageId)
	par := mustReadPage(t, pm, parentId)

	// page had 3 slots; bridge adds 1; sibling had 2 → 3+1+2 = 6 slots in merged page.
	if p.GetRowCount() != 6 {
		t.Errorf("merged page row count: got %d, want 6", p.GetRowCount())
	}
	// Parent's only slot (pageId) removed; rmc becomes pageId.
	if par.GetRowCount() != 0 {
		t.Errorf("parent row count after merge: got %d, want 0", par.GetRowCount())
	}
	if par.GetRightMostChild() != pageId {
		t.Errorf("parent rmc: got %d, want %d", par.GetRightMostChild(), pageId)
	}
	// Merged page must adopt sibling's rightmost child.
	if p.GetRightMostChild() != 399 {
		t.Errorf("merged page rmc: got %d, want 399", p.GetRightMostChild())
	}
	_ = sibId
}

// TestHandleUnderflow_Internal_LeftSibling_Sparse_Merges: page is rightmost
// child; sparse left sibling → mergeInternal(sibling, page, parent).
// sibling absorbs page's records + bridge; page is freed.
func TestHandleUnderflow_Internal_LeftSibling_Sparse_Merges(t *testing.T) {
	bt, pm, page, sibling, parent := setupHUInternalMergeLeft(t)
	sibId := sibling.GetPageId()
	parentId := parent.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parentId}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	sib := mustReadPage(t, pm, sibId)
	par := mustReadPage(t, pm, parentId)

	// sibling had 2 slots; bridge adds 1; page had 3 → 2+1+3 = 6 slots.
	if sib.GetRowCount() != 6 {
		t.Errorf("merged sibling row count: got %d, want 6", sib.GetRowCount())
	}
	if par.GetRowCount() != 0 {
		t.Errorf("parent row count after merge: got %d, want 0", par.GetRowCount())
	}
	if par.GetRightMostChild() != sibId {
		t.Errorf("parent rmc: got %d, want %d", par.GetRightMostChild(), sibId)
	}
	// Merged sibling must adopt page's rightmost child.
	if sib.GetRightMostChild() != 399 {
		t.Errorf("merged sibling rmc: got %d, want 399", sib.GetRightMostChild())
	}
}

// ── TestHandleUnderflow parent-page handling (root collapse & recursive) ──────
//
// These tests exercise the new logic that runs after a merge:
//   (a) root collapse: when the root ends up with 0 entries the surviving merged
//       child becomes the new root and the old root is freed.
//   (b) recursive parent underflow: when a non-root parent underflows after a
//       merge, handleUnderflow is called recursively up the tree.

// setupRootCollapseLeaf creates a minimal 2-level tree:
//
//	root  : [(maxLeft, leftId)], rmc=rightId — registered as the PM root
//	left  : sparse leaf (keys 1, 2, 3)
//	right : sparse leaf (keys 100, 200) — the rightmost child
//
// When pageIsRightmost=false, the returned "page" pointer is the LEFT leaf
// (not rightmost child); the right leaf is the sibling.
// When pageIsRightmost=true, the returned "page" pointer is the RIGHT leaf
// (rightmost child); the left leaf is the sibling.
func setupRootCollapseLeaf(
	t *testing.T,
	pageIsRightmost bool,
) (bt *BTree, pm pagemanager.PageManager, page, sibling, root *pagemanager.Page) {
	t.Helper()
	var err error
	pm, err = pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	leftLeaf, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(left): %v", err)
	}
	rightLeaf, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(right): %v", err)
	}
	leftId, rightId := leftLeaf.GetPageId(), rightLeaf.GetPageId()

	*leftLeaf = *pagemanager.NewLeafPage(leftId, pagemanager.InvalidPageID, rightId)
	*rightLeaf = *pagemanager.NewLeafPage(rightId, leftId, pagemanager.InvalidPageID)
	for _, k := range []uint64{1, 2, 3} {
		leftLeaf.InsertRecord(makeRecord(k, []byte("v")))
	}
	for _, k := range []uint64{100, 200} {
		rightLeaf.InsertRecord(makeRecord(k, []byte("v")))
	}

	root, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(root): %v", err)
	}
	rootId := root.GetPageId()
	*root = *pagemanager.NewInternalPage(rootId, 1, rightId)
	root.InsertRecord(EncodeInternalRecord(3, leftId))

	for _, p := range []*pagemanager.Page{leftLeaf, rightLeaf, root} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}
	if err := pm.SetRootPageId(rootId); err != nil {
		t.Fatalf("SetRootPageId: %v", err)
	}

	bt = NewBTree(pm)
	if pageIsRightmost {
		return bt, pm, rightLeaf, leftLeaf, root
	}
	return bt, pm, leftLeaf, rightLeaf, root
}

// TestHandleUnderflow_RootCollapse_Leaf_RightSibling: the underflowing page is
// the left (non-rightmost) child; after merging with the right sibling, the
// merged left page becomes the new root.
func TestHandleUnderflow_RootCollapse_Leaf_RightSibling(t *testing.T) {
	bt, pm, page, _, root := setupRootCollapseLeaf(t, false)
	leftId := page.GetPageId()
	rootId := root.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{rootId}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	// Surviving page (left leaf) must become the root.
	if pm.GetRootPageId() != leftId {
		t.Errorf("root ID: got %d, want %d (left leaf)", pm.GetRootPageId(), leftId)
	}
	// Merged page must hold all 5 records.
	newRoot := mustReadPage(t, pm, leftId)
	if newRoot.GetRowCount() != 5 {
		t.Errorf("new root row count: got %d, want 5", newRoot.GetRowCount())
	}
}

// TestHandleUnderflow_RootCollapse_Leaf_LeftSibling: the underflowing page is
// the right (rightmost) child; after merging into the left sibling, the LEFT
// page becomes the new root — not the freed right page.
// This specifically exercises the survivingPageId fix.
func TestHandleUnderflow_RootCollapse_Leaf_LeftSibling(t *testing.T) {
	bt, pm, page, sibling, root := setupRootCollapseLeaf(t, true)
	leftId := sibling.GetPageId() // sibling is the left leaf here
	rootId := root.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{rootId}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	// The LEFT leaf (sibling) survives the merge and must be the new root.
	if pm.GetRootPageId() != leftId {
		t.Errorf("root ID: got %d, want %d (left/sibling leaf)", pm.GetRootPageId(), leftId)
	}
	newRoot := mustReadPage(t, pm, leftId)
	if newRoot.GetRowCount() != 5 {
		t.Errorf("new root row count: got %d, want 5", newRoot.GetRowCount())
	}
}

// TestHandleUnderflow_RootCollapse_Leaf_AllRecordsPreserved: regardless of
// which sibling direction triggered the collapse, no record should be lost.
func TestHandleUnderflow_RootCollapse_Leaf_AllRecordsPreserved(t *testing.T) {
	for _, rightmost := range []bool{false, true} {
		rightmost := rightmost
		t.Run(fmt.Sprintf("pageIsRightmost=%v", rightmost), func(t *testing.T) {
			bt, pm, page, _, root := setupRootCollapseLeaf(t, rightmost)

			if err := bt.handleUnderflow(page, []uint32{root.GetPageId()}); err != nil {
				t.Fatalf("handleUnderflow: %v", err)
			}

			newRoot := mustReadPage(t, pm, pm.GetRootPageId())
			got := redistLeafKeys(t, newRoot)
			sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
			want := []uint64{1, 2, 3, 100, 200}
			if !equalSlices(got, want) {
				t.Errorf("new root keys: got %v, want %v", got, want)
			}
		})
	}
}

// TestHandleUnderflow_RootCollapse_Leaf_OldRootFreed: after a root collapse the
// old root page must be on the free list (i.e. AllocatePage reuses it next).
func TestHandleUnderflow_RootCollapse_Leaf_OldRootFreed(t *testing.T) {
	bt, pm, page, _, root := setupRootCollapseLeaf(t, false)
	rootId := root.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{rootId}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	// The next AllocatePage call must reuse the freed root page.
	reused, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage after collapse: %v", err)
	}
	if reused.GetPageId() != rootId {
		t.Errorf("freed root not reused: got page %d, want %d", reused.GetPageId(), rootId)
	}
}

// TestHandleUnderflow_RootCollapse_Internal: two sparse internal pages whose
// parent is the root; merging them collapses the root.
func TestHandleUnderflow_RootCollapse_Internal(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	leftInt, _ := pm.AllocatePage()
	rightInt, _ := pm.AllocatePage()
	root, _ := pm.AllocatePage()
	leftId, rightId, rootId := leftInt.GetPageId(), rightInt.GetPageId(), root.GetPageId()

	*leftInt = *pagemanager.NewInternalPage(leftId, 1, 901)
	leftInt.InsertRecord(EncodeInternalRecord(10, 900))
	leftInt.InsertRecord(EncodeInternalRecord(20, 901))

	*rightInt = *pagemanager.NewInternalPage(rightId, 1, 951)
	rightInt.InsertRecord(EncodeInternalRecord(200, 950))
	rightInt.InsertRecord(EncodeInternalRecord(300, 951))

	*root = *pagemanager.NewInternalPage(rootId, 2, rightId)
	root.InsertRecord(EncodeInternalRecord(50, leftId))

	for _, p := range []*pagemanager.Page{leftInt, rightInt, root} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}
	if err := pm.SetRootPageId(rootId); err != nil {
		t.Fatalf("SetRootPageId: %v", err)
	}

	bt := NewBTree(pm)
	// rightInt is the rightmost child → left sibling = leftInt → isLeftSibling=true
	// mergeInternal(leftInt, rightInt, root) → leftInt survives
	// survivingPageId = siblingPage.GetPageId() = leftId
	if err := bt.handleUnderflow(rightInt, []uint32{rootId}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	if pm.GetRootPageId() != leftId {
		t.Errorf("root ID: got %d, want %d (leftInt)", pm.GetRootPageId(), leftId)
	}
	// merged leftInt: 2 own slots + bridge(1) + 2 rightInt slots = 5 slots total
	newRoot := mustReadPage(t, pm, leftId)
	if newRoot.GetRowCount() != 5 {
		t.Errorf("merged root row count: got %d, want 5", newRoot.GetRowCount())
	}
}

// TestHandleUnderflow_RootCollapse_Internal_RightSibling: same as above but
// the underflowing page is the leftInt (non-rightmost); rightInt is the sibling.
func TestHandleUnderflow_RootCollapse_Internal_RightSibling(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	leftInt, _ := pm.AllocatePage()
	rightInt, _ := pm.AllocatePage()
	root, _ := pm.AllocatePage()
	leftId, rightId, rootId := leftInt.GetPageId(), rightInt.GetPageId(), root.GetPageId()

	*leftInt = *pagemanager.NewInternalPage(leftId, 1, 901)
	leftInt.InsertRecord(EncodeInternalRecord(10, 900))

	*rightInt = *pagemanager.NewInternalPage(rightId, 1, 951)
	rightInt.InsertRecord(EncodeInternalRecord(200, 950))
	rightInt.InsertRecord(EncodeInternalRecord(300, 951))

	*root = *pagemanager.NewInternalPage(rootId, 2, rightId)
	root.InsertRecord(EncodeInternalRecord(50, leftId))

	for _, p := range []*pagemanager.Page{leftInt, rightInt, root} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}
	if err := pm.SetRootPageId(rootId); err != nil {
		t.Fatalf("SetRootPageId: %v", err)
	}

	bt := NewBTree(pm)
	// leftInt is NOT rightmost → right sibling = rightInt → isLeftSibling=false
	// mergeInternal(leftInt, rightInt, root) → leftInt survives
	// survivingPageId = page.GetPageId() = leftId
	if err := bt.handleUnderflow(leftInt, []uint32{rootId}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	if pm.GetRootPageId() != leftId {
		t.Errorf("root ID: got %d, want %d (leftInt)", pm.GetRootPageId(), leftId)
	}
	newRoot := mustReadPage(t, pm, leftId)
	// leftInt: 1 own slot + bridge(1) + 2 rightInt slots = 4 slots total
	if newRoot.GetRowCount() != 4 {
		t.Errorf("merged root row count: got %d, want 4", newRoot.GetRowCount())
	}
}

// TestHandleUnderflow_RootNotCollapsed_WhenRootHasEntries: if the root still
// has entries after the merge, it must NOT be replaced as root.
func TestHandleUnderflow_RootNotCollapsed_WhenRootHasEntries(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	// Root has 3 leaf children; merging two of them leaves 1 slot + rmc in root.
	leftLeaf, _ := pm.AllocatePage()
	midLeaf, _ := pm.AllocatePage()
	rightLeaf, _ := pm.AllocatePage()
	root, _ := pm.AllocatePage()
	leftId, midId, rightId, rootId :=
		leftLeaf.GetPageId(), midLeaf.GetPageId(), rightLeaf.GetPageId(), root.GetPageId()

	*leftLeaf = *pagemanager.NewLeafPage(leftId, pagemanager.InvalidPageID, midId)
	*midLeaf = *pagemanager.NewLeafPage(midId, leftId, rightId)
	*rightLeaf = *pagemanager.NewLeafPage(rightId, midId, pagemanager.InvalidPageID)
	for _, k := range []uint64{1, 2, 3} {
		leftLeaf.InsertRecord(makeRecord(k, []byte("v")))
	}
	for _, k := range []uint64{100, 200} {
		midLeaf.InsertRecord(makeRecord(k, []byte("v")))
	}
	for _, k := range []uint64{500, 600} {
		rightLeaf.InsertRecord(makeRecord(k, []byte("v")))
	}

	*root = *pagemanager.NewInternalPage(rootId, 1, rightId)
	root.InsertRecord(EncodeInternalRecord(3, leftId))
	root.InsertRecord(EncodeInternalRecord(200, midId))

	for _, p := range []*pagemanager.Page{leftLeaf, midLeaf, rightLeaf, root} {
		pm.WritePage(p)
	}
	if err := pm.SetRootPageId(rootId); err != nil {
		t.Fatalf("SetRootPageId: %v", err)
	}

	bt := NewBTree(pm)
	// Merge leftLeaf (not rightmost) with midLeaf (right sibling).
	// Root loses the leftLeaf entry but still has (200, midId) + rmc=rightId.
	if err := bt.handleUnderflow(leftLeaf, []uint32{rootId}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	// Root must remain unchanged as the root of the tree.
	if pm.GetRootPageId() != rootId {
		t.Errorf("root ID changed: got %d, want %d", pm.GetRootPageId(), rootId)
	}
	rootAfter := mustReadPage(t, pm, rootId)
	if rootAfter.GetRowCount() != 1 {
		t.Errorf("root row count: got %d, want 1", rootAfter.GetRowCount())
	}
}

// TestHandleUnderflow_RecursiveParentUnderflow: a leaf merge cascades upward —
// the non-root parent underflows, triggering a recursive handleUnderflow call
// that ultimately merges the parent with its sibling and collapses the root.
//
// Tree layout:
//
//	root  (level 2): [(300, parentId)], rmc=sibParentId   ← registered root
//	parent (level 1): [(3, leftLeafId)], rmc=rightLeafId
//	sibParent (level 1): [(500, 497)], rmc=498
//	leftLeaf : keys 1,2,3 (sparse)
//	rightLeaf: keys 100,200 (sparse)
func TestHandleUnderflow_RecursiveParentUnderflow(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	leftLeaf, _ := pm.AllocatePage()
	rightLeaf, _ := pm.AllocatePage()
	parent, _ := pm.AllocatePage()
	sibParent, _ := pm.AllocatePage()
	root, _ := pm.AllocatePage()

	leftId, rightId := leftLeaf.GetPageId(), rightLeaf.GetPageId()
	parentId, sibId, rootId := parent.GetPageId(), sibParent.GetPageId(), root.GetPageId()

	*leftLeaf = *pagemanager.NewLeafPage(leftId, pagemanager.InvalidPageID, rightId)
	*rightLeaf = *pagemanager.NewLeafPage(rightId, leftId, pagemanager.InvalidPageID)
	for _, k := range []uint64{1, 2, 3} {
		leftLeaf.InsertRecord(makeRecord(k, []byte("v")))
	}
	for _, k := range []uint64{100, 200} {
		rightLeaf.InsertRecord(makeRecord(k, []byte("v")))
	}

	*parent = *pagemanager.NewInternalPage(parentId, 1, rightId)
	parent.InsertRecord(EncodeInternalRecord(3, leftId))

	*sibParent = *pagemanager.NewInternalPage(sibId, 1, 498)
	sibParent.InsertRecord(EncodeInternalRecord(500, 497))

	*root = *pagemanager.NewInternalPage(rootId, 2, sibId)
	root.InsertRecord(EncodeInternalRecord(300, parentId))

	for _, p := range []*pagemanager.Page{leftLeaf, rightLeaf, parent, sibParent, root} {
		if err := pm.WritePage(p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}
	if err := pm.SetRootPageId(rootId); err != nil {
		t.Fatalf("SetRootPageId: %v", err)
	}

	bt := NewBTree(pm)
	// handleUnderflow(leftLeaf, [rootId, parentId]):
	//   step 1 — leaf merge: leftLeaf absorbs rightLeaf, parent loses entry → 0 slots
	//   step 2 — recursive: handleUnderflow(parent, [rootId])
	//              parent merges with sibParent → root loses entry → 0 slots
	//   step 3 — root collapse: new root = parent (page, isLeftSibling=false in recursive)
	if err := bt.handleUnderflow(leftLeaf, []uint32{rootId, parentId}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	// parent must now be the root of the tree.
	if pm.GetRootPageId() != parentId {
		t.Errorf("root ID after recursive collapse: got %d, want %d (parent)", pm.GetRootPageId(), parentId)
	}

	// parent (new root) must contain the bridge + sibParent records.
	// After leaf merge: parent has 0 slots, rmc=leftId.
	// After mergeInternal(parent, sibParent, root):
	//   bridge = (300, leftId); sibParent slots = [(500, 497)]; sibParent rmc = 498
	//   parent gets: [(300, leftId), (500, 497)], rmc=498
	newRoot := mustReadPage(t, pm, parentId)
	if newRoot.GetRowCount() != 2 {
		t.Errorf("new root (parent) row count: got %d, want 2", newRoot.GetRowCount())
	}
	if newRoot.GetRightMostChild() != 498 {
		t.Errorf("new root rmc: got %d, want 498", newRoot.GetRightMostChild())
	}

	// Leaf records must still be reachable via the new root's first child.
	rootKeys := redistInternalKeys(t, newRoot)
	if len(rootKeys) < 1 || rootKeys[0] != 300 {
		t.Errorf("new root slot[0] key: got %v, want [300, ...]", rootKeys)
	}
}

// TestHandleUnderflow_MergeError_PropagatesFromLeaf: if mergeLeaf returns an
// error (simulated by passing a nil parentPage write scenario), handleUnderflow
// must propagate it rather than silently continuing.
// We test this indirectly by verifying that a malformed parent (no slot for
// right page) causes mergeLeaf to fail and the error bubbles up.
func TestHandleUnderflow_MergeError_PropagatesFromLeaf(t *testing.T) {
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	leftLeaf, _ := pm.AllocatePage()
	rightLeaf, _ := pm.AllocatePage()
	parent, _ := pm.AllocatePage()
	leftId, rightId := leftLeaf.GetPageId(), rightLeaf.GetPageId()
	parentId := parent.GetPageId()

	*leftLeaf = *pagemanager.NewLeafPage(leftId, pagemanager.InvalidPageID, rightId)
	*rightLeaf = *pagemanager.NewLeafPage(rightId, leftId, pagemanager.InvalidPageID)
	leftLeaf.InsertRecord(makeRecord(1, []byte("v")))
	rightLeaf.InsertRecord(makeRecord(100, []byte("v")))

	// Parent has no slot for rightLeaf (neither slot nor rmc points to it),
	// so mergeLeaf will return an error ("rightPage not found in parent").
	// We set rmc=leftId so rightLeaf appears to be a slot-based child.
	*parent = *pagemanager.NewInternalPage(parentId, 1, leftId)
	// leftLeaf is the rmc; no slot for rightLeaf anywhere in parent.
	// We call handleUnderflow on rightLeaf with parent as its parent; the
	// sibling lookup will find leftId via GetRecord(-1)... actually parent rmc ==
	// rightLeaf? No. Let's set parent rmc=rightId so rightLeaf appears rightmost:
	// then sibling = GetRecord(last slot).child which doesn't exist (0 rows).
	// Easier: make rightLeaf the non-rightmost child with no matching slot.
	*parent = *pagemanager.NewInternalPage(parentId, 1, rightId)
	// parent has 0 slots, rmc=rightId. leftLeaf is a phantom child not in parent.
	// handleUnderflow on rightLeaf: rmc==rightId → isLeftSibling=true →
	// GetRecord(-1) → last slot of 0-row page → that returns (nil, false) →
	// DecodInternalRecord will fail → handleUnderflow returns an error.
	for _, p := range []*pagemanager.Page{leftLeaf, rightLeaf, parent} {
		pm.WritePage(p)
	}

	bt := NewBTree(pm)
	if err := bt.handleUnderflow(rightLeaf, []uint32{parentId}); err == nil {
		t.Error("expected error for malformed parent, got nil")
	}
}

// TestHandleUnderflow_Internal_Merge_AllRecordsPreserved: every key present in
// either page plus the bridging parent separator must appear in the merged page.
func TestHandleUnderflow_Internal_Merge_AllRecordsPreserved(t *testing.T) {
	bt, pm, page, _, parent := setupHUInternalMergeRight(t)
	pageId := page.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parent.GetPageId()}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	p := mustReadPage(t, pm, pageId)
	got := redistInternalKeys(t, p)
	// page keys: 10,20,30; bridge key: 40 (parent sep); sibling keys: 200,300.
	want := []uint64{10, 20, 30, 40, 200, 300}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if !equalSlices(got, want) {
		t.Errorf("merged internal keys: got %v, want %v", got, want)
	}
}

// TestHandleUnderflow_Internal_Merge_PagesWrittenToDisk: re-read the merged page
// from disk and verify the row count and rmc survive the flush.
func TestHandleUnderflow_Internal_Merge_PagesWrittenToDisk(t *testing.T) {
	bt, pm, page, _, parent := setupHUInternalMergeRight(t)
	pageId, parentId := page.GetPageId(), parent.GetPageId()

	if err := bt.handleUnderflow(page, []uint32{parentId}); err != nil {
		t.Fatalf("handleUnderflow: %v", err)
	}

	p := mustReadPage(t, pm, pageId)
	par := mustReadPage(t, pm, parentId)

	if p.GetRowCount() != 6 {
		t.Errorf("merged page row count after re-read: got %d, want 6", p.GetRowCount())
	}
	if p.GetRightMostChild() != 399 {
		t.Errorf("merged page rmc after re-read: got %d, want 399", p.GetRightMostChild())
	}
	if par.GetRowCount() != 0 {
		t.Errorf("parent row count after re-read: got %d, want 0", par.GetRowCount())
	}
}

// =====================================
// Delete tests
// =====================================

// mustDelete calls Delete and fatals on any error.
func mustDelete(t *testing.T, bt *BTree, key uint64) {
	t.Helper()
	if err := bt.Delete(key); err != nil {
		t.Fatalf("Delete(key=%d): %v", key, err)
	}
}

// assertRootIsLeaf fatals if the root page is not a leaf.
func assertRootIsLeaf(t *testing.T, pm pagemanager.PageManager) {
	t.Helper()
	rootId := pm.GetRootPageId()
	if rootId == pagemanager.InvalidPageID {
		t.Fatal("assertRootIsLeaf: tree is empty, expected a leaf root")
	}
	rootPage, err := pm.ReadPage(rootId)
	if err != nil {
		t.Fatalf("ReadPage(root=%d): %v", rootId, err)
	}
	if rootPage.GetPageType() != pagemanager.PageTypeLeaf {
		t.Errorf("expected root to be a leaf page, got page type %d", rootPage.GetPageType())
	}
}

// assertTreeEmpty fatals if the root page ID is not InvalidPageID.
func assertTreeEmpty(t *testing.T, pm pagemanager.PageManager) {
	t.Helper()
	if rootId := pm.GetRootPageId(); rootId != pagemanager.InvalidPageID {
		t.Errorf("expected empty tree (rootId=InvalidPageID), got rootId=%d", rootId)
	}
}

// ---- Basic Delete: empty tree and missing keys ----

// TestDelete_EmptyTree_NoOp verifies Delete on an empty tree returns nil without panics.
func TestDelete_EmptyTree_NoOp(t *testing.T) {
	bt, pm := newTempBTree(t)
	mustDelete(t, bt, 42)
	assertTreeEmpty(t, pm)
}

// TestDelete_MissingKey_NoOp verifies deleting a non-existent key leaves the tree unchanged.
func TestDelete_MissingKey_NoOp(t *testing.T) {
	bt, _ := newTempBTree(t)
	mustInsert(t, bt, 10, []Field{intF(1, 10)})
	mustInsert(t, bt, 20, []Field{intF(1, 20)})
	mustDelete(t, bt, 15) // 15 is not in the tree
	assertFound(t, bt, 10, []Field{intF(1, 10)})
	assertFound(t, bt, 20, []Field{intF(1, 20)})
}

// TestDelete_MissingKey_BelowAll_NoOp verifies that a key below every stored key is a no-op.
func TestDelete_MissingKey_BelowAll_NoOp(t *testing.T) {
	bt, _ := newTempBTree(t)
	for _, k := range []uint64{10, 20, 30} {
		mustInsert(t, bt, k, []Field{intF(1, int64(k))})
	}
	mustDelete(t, bt, 1) // 1 < min stored key
	for _, k := range []uint64{10, 20, 30} {
		assertFound(t, bt, k, []Field{intF(1, int64(k))})
	}
}

// TestDelete_MissingKey_AboveAll_NoOp verifies that a key above every stored key is a no-op.
func TestDelete_MissingKey_AboveAll_NoOp(t *testing.T) {
	bt, _ := newTempBTree(t)
	for _, k := range []uint64{10, 20, 30} {
		mustInsert(t, bt, k, []Field{intF(1, int64(k))})
	}
	mustDelete(t, bt, 999) // 999 > max stored key
	for _, k := range []uint64{10, 20, 30} {
		assertFound(t, bt, k, []Field{intF(1, int64(k))})
	}
}

// ---- Basic Delete: single-record root leaf ----

// TestDelete_SingleRecord_TreeBecomesEmpty deletes the only record and verifies the tree is empty.
func TestDelete_SingleRecord_TreeBecomesEmpty(t *testing.T) {
	bt, pm := newTempBTree(t)
	mustInsert(t, bt, 99, []Field{strF(1, "only-record")})
	mustDelete(t, bt, 99)
	assertTreeEmpty(t, pm)
	assertMissing(t, bt, 99)
}

// TestDelete_SingleRecord_RepeatedDeleteIsNoOp verifies a second deletion of the same key is a no-op.
func TestDelete_SingleRecord_RepeatedDeleteIsNoOp(t *testing.T) {
	bt, pm := newTempBTree(t)
	mustInsert(t, bt, 55, []Field{intF(1, 55)})
	mustDelete(t, bt, 55) // first delete: tree empties
	mustDelete(t, bt, 55) // second delete: no-op on empty tree
	assertTreeEmpty(t, pm)
}

// ---- Basic Delete: multi-record root leaf (single-level tree) ----

// TestDelete_RootLeaf_FirstKey removes the lowest key from a leaf root.
func TestDelete_RootLeaf_FirstKey(t *testing.T) {
	bt, pm := newTempBTree(t)
	keys := []uint64{10, 20, 30, 40, 50}
	for _, k := range keys {
		mustInsert(t, bt, k, []Field{intF(1, int64(k))})
	}
	mustDelete(t, bt, 10)
	assertMissing(t, bt, 10)
	remaining := []uint64{20, 30, 40, 50}
	for _, k := range remaining {
		assertFound(t, bt, k, []Field{intF(1, int64(k))})
	}
	verifyLeafChain(t, bt, pm, remaining)
}

// TestDelete_RootLeaf_LastKey removes the highest key from a leaf root.
func TestDelete_RootLeaf_LastKey(t *testing.T) {
	bt, pm := newTempBTree(t)
	keys := []uint64{10, 20, 30, 40, 50}
	for _, k := range keys {
		mustInsert(t, bt, k, []Field{intF(1, int64(k))})
	}
	mustDelete(t, bt, 50)
	assertMissing(t, bt, 50)
	remaining := []uint64{10, 20, 30, 40}
	for _, k := range remaining {
		assertFound(t, bt, k, []Field{intF(1, int64(k))})
	}
	verifyLeafChain(t, bt, pm, remaining)
}

// TestDelete_RootLeaf_MiddleKey removes a middle key and verifies neighbours are unchanged.
func TestDelete_RootLeaf_MiddleKey(t *testing.T) {
	bt, pm := newTempBTree(t)
	keys := []uint64{10, 20, 30, 40, 50}
	for _, k := range keys {
		mustInsert(t, bt, k, []Field{intF(1, int64(k))})
	}
	mustDelete(t, bt, 30)
	assertMissing(t, bt, 30)
	remaining := []uint64{10, 20, 40, 50}
	for _, k := range remaining {
		assertFound(t, bt, k, []Field{intF(1, int64(k))})
	}
	verifyLeafChain(t, bt, pm, remaining)
}

// TestDelete_RootLeaf_AllRecords_TreeEmpty deletes every record one by one and verifies the tree empties.
func TestDelete_RootLeaf_AllRecords_TreeEmpty(t *testing.T) {
	bt, pm := newTempBTree(t)
	keys := []uint64{5, 15, 25, 35, 45}
	for _, k := range keys {
		mustInsert(t, bt, k, []Field{intF(1, int64(k))})
	}
	for _, k := range keys {
		mustDelete(t, bt, k)
	}
	assertTreeEmpty(t, pm)
	for _, k := range keys {
		assertMissing(t, bt, k)
	}
}

// TestDelete_Idempotent_AfterRootLeafDelete deletes the same key twice; the second must be a no-op.
func TestDelete_Idempotent_AfterRootLeafDelete(t *testing.T) {
	bt, _ := newTempBTree(t)
	mustInsert(t, bt, 10, []Field{intF(1, 1)})
	mustInsert(t, bt, 20, []Field{intF(1, 2)})
	mustDelete(t, bt, 10)
	mustDelete(t, bt, 10) // second delete: no-op
	assertMissing(t, bt, 10)
	assertFound(t, bt, 20, []Field{intF(1, 2)})
}

// TestDelete_ThenReinsert verifies a deleted key can be reinserted with a new value.
func TestDelete_ThenReinsert(t *testing.T) {
	bt, pm := newTempBTree(t)
	mustInsert(t, bt, 42, []Field{intF(1, 1)})
	mustDelete(t, bt, 42)
	assertMissing(t, bt, 42)
	mustInsert(t, bt, 42, []Field{intF(1, 2)})
	assertFound(t, bt, 42, []Field{intF(1, 2)})
	verifyLeafChain(t, bt, pm, []uint64{42})
}

// ---- Edge keys ----

// TestDelete_KeyZero removes key=0 (minimum uint64) and verifies adjacent keys are unaffected.
func TestDelete_KeyZero(t *testing.T) {
	bt, pm := newTempBTree(t)
	mustInsert(t, bt, 0, []Field{intF(1, 0)})
	mustInsert(t, bt, 1, []Field{intF(1, 1)})
	mustInsert(t, bt, 2, []Field{intF(1, 2)})
	mustDelete(t, bt, 0)
	assertMissing(t, bt, 0)
	assertFound(t, bt, 1, []Field{intF(1, 1)})
	assertFound(t, bt, 2, []Field{intF(1, 2)})
	verifyLeafChain(t, bt, pm, []uint64{1, 2})
}

// TestDelete_MaxUint64 removes the maximum uint64 key and verifies the adjacent key is unaffected.
func TestDelete_MaxUint64(t *testing.T) {
	const maxKey = ^uint64(0)
	bt, pm := newTempBTree(t)
	mustInsert(t, bt, maxKey-1, []Field{intF(1, 1)})
	mustInsert(t, bt, maxKey, []Field{intF(1, 2)})
	mustDelete(t, bt, maxKey)
	assertMissing(t, bt, maxKey)
	assertFound(t, bt, maxKey-1, []Field{intF(1, 1)})
	verifyLeafChain(t, bt, pm, []uint64{maxKey - 1})
}

// TestDelete_KeyZeroAndMaxTogether inserts and then removes both extreme keys, leaving nothing.
func TestDelete_KeyZeroAndMaxTogether(t *testing.T) {
	const maxKey = ^uint64(0)
	bt, pm := newTempBTree(t)
	mustInsert(t, bt, 0, []Field{intF(1, 0)})
	mustInsert(t, bt, maxKey, []Field{intF(1, -1)}) //nolint:gosec
	mustDelete(t, bt, 0)
	mustDelete(t, bt, maxKey)
	assertTreeEmpty(t, pm)
}

// ---- Multi-level tree: key visibility after deletion ----

// TestDelete_MultiLevel_DeletedKeyNotFound verifies a key is not found after deletion from a
// multi-level tree built with small records (300 inserts → several leaf splits).
func TestDelete_MultiLevel_DeletedKeyNotFound(t *testing.T) {
	bt, pm := newTempBTree(t)
	const n = 300
	for i := uint64(1); i <= n; i++ {
		mustInsert(t, bt, i, []Field{intF(1, int64(i))})
	}
	mustDelete(t, bt, 150)
	assertMissing(t, bt, 150)
	assertFound(t, bt, 149, []Field{intF(1, 149)})
	assertFound(t, bt, 151, []Field{intF(1, 151)})
	remaining := make([]uint64, 0, n-1)
	for i := uint64(1); i <= n; i++ {
		if i != 150 {
			remaining = append(remaining, i)
		}
	}
	verifyLeafChain(t, bt, pm, remaining)
}

// TestDelete_MultiLevel_FirstKey deletes the smallest key from a multi-level tree.
func TestDelete_MultiLevel_FirstKey(t *testing.T) {
	bt, pm := newTempBTree(t)
	const n = 300
	for i := uint64(1); i <= n; i++ {
		mustInsert(t, bt, i, []Field{intF(1, int64(i))})
	}
	mustDelete(t, bt, 1)
	assertMissing(t, bt, 1)
	assertFound(t, bt, 2, []Field{intF(1, 2)})
	remaining := make([]uint64, 0, n-1)
	for i := uint64(2); i <= n; i++ {
		remaining = append(remaining, i)
	}
	verifyLeafChain(t, bt, pm, remaining)
}

// TestDelete_MultiLevel_LastKey deletes the largest key from a multi-level tree.
func TestDelete_MultiLevel_LastKey(t *testing.T) {
	bt, pm := newTempBTree(t)
	const n = 300
	for i := uint64(1); i <= n; i++ {
		mustInsert(t, bt, i, []Field{intF(1, int64(i))})
	}
	mustDelete(t, bt, n)
	assertMissing(t, bt, n)
	assertFound(t, bt, n-1, []Field{intF(1, int64(n-1))})
	remaining := make([]uint64, 0, n-1)
	for i := uint64(1); i < n; i++ {
		remaining = append(remaining, i)
	}
	verifyLeafChain(t, bt, pm, remaining)
}

// ---- Underflow handling: redistribution ----
//
// With 180-byte string payloads each record is ~196 bytes (8-byte key + encoding + 4-byte slot).
// A 4096-byte leaf has 4064 usable bytes, fitting at most 20 records.
// The 21st insert triggers the first split:
//
//	leaf_L = keys 1..10  (10 records, 2104 bytes free → above minPageFreeSpace=2048 → underflowing)
//	leaf_R = keys 11..21 (11 records, 1908 bytes free → below minPageFreeSpace=2048 → dense)
//
// Deleting a key from leaf_L pushes it to 9 records (2300 bytes free), triggering redistribution
// with the dense right sibling.

// TestDelete_TriggerRedistribute_AllKeysAccessible deletes a left-leaf key to trigger
// redistribution and verifies every remaining key is still accessible.
func TestDelete_TriggerRedistribute_AllKeysAccessible(t *testing.T) {
	bt, pm := newTempBTree(t)
	bigVal := repeatedStr('a', 180)
	for i := uint64(1); i <= 21; i++ {
		mustInsert(t, bt, i, []Field{strF(1, bigVal)})
	}
	mustDelete(t, bt, 1)
	assertMissing(t, bt, 1)
	remaining := make([]uint64, 20)
	for i := range remaining {
		remaining[i] = uint64(i + 2)
	}
	for _, k := range remaining {
		assertFound(t, bt, k, []Field{strF(1, bigVal)})
	}
	verifyLeafChain(t, bt, pm, remaining)
}

// TestDelete_TriggerRedistribute_LeafChainOrdered verifies the sibling chain is strictly sorted
// after a redistribution triggered by deleting a key from the left leaf.
func TestDelete_TriggerRedistribute_LeafChainOrdered(t *testing.T) {
	bt, pm := newTempBTree(t)
	bigVal := repeatedStr('b', 180)
	for i := uint64(1); i <= 21; i++ {
		mustInsert(t, bt, i, []Field{strF(1, bigVal)})
	}
	mustDelete(t, bt, 5)
	remaining := make([]uint64, 0, 20)
	for i := uint64(1); i <= 21; i++ {
		if i != 5 {
			remaining = append(remaining, i)
		}
	}
	verifyLeafChain(t, bt, pm, remaining)
}

// TestDelete_TriggerRedistribute_MultipleKeys deletes several left-leaf keys in succession,
// triggering redistribution on each, and verifies all remaining keys are found.
func TestDelete_TriggerRedistribute_MultipleKeys(t *testing.T) {
	bt, pm := newTempBTree(t)
	bigVal := repeatedStr('c', 180)
	for i := uint64(1); i <= 21; i++ {
		mustInsert(t, bt, i, []Field{strF(1, bigVal)})
	}
	mustDelete(t, bt, 1)
	mustDelete(t, bt, 3)
	assertMissing(t, bt, 1)
	assertMissing(t, bt, 3)
	remaining := make([]uint64, 0, 19)
	for i := uint64(1); i <= 21; i++ {
		if i != 1 && i != 3 {
			remaining = append(remaining, i)
		}
	}
	for _, k := range remaining {
		assertFound(t, bt, k, []Field{strF(1, bigVal)})
	}
	verifyLeafChain(t, bt, pm, remaining)
}

// ---- Underflow handling: merge and tree collapse ----

// TestDelete_TriggerMerge_TreeCollapsesToLeaf builds a two-level tree, performs deletions that
// first trigger redistribution and then trigger a merge, and verifies the tree collapses to a
// single leaf root.
//
// Deletion sequence:
//  1. Delete key 1  → leaf_L underflows → redistribute → leaf_L=[2..11], leaf_R=[12..21]
//  2. Delete key 2  → leaf_L underflows → right sibling is now also sparse → merge
//     → surviving leaf holds keys 3..21 and becomes the new root leaf
func TestDelete_TriggerMerge_TreeCollapsesToLeaf(t *testing.T) {
	bt, pm := newTempBTree(t)
	bigVal := repeatedStr('d', 180)
	for i := uint64(1); i <= 21; i++ {
		mustInsert(t, bt, i, []Field{strF(1, bigVal)})
	}
	mustDelete(t, bt, 1) // redistribution
	mustDelete(t, bt, 2) // merge + collapse

	assertRootIsLeaf(t, pm)

	remaining := make([]uint64, 0, 19)
	for i := uint64(3); i <= 21; i++ {
		remaining = append(remaining, i)
	}
	for _, k := range remaining {
		assertFound(t, bt, k, []Field{strF(1, bigVal)})
	}
	assertMissing(t, bt, 1)
	assertMissing(t, bt, 2)
	verifyLeafChain(t, bt, pm, remaining)
}

// TestDelete_TriggerMerge_SiblingChainUpdated verifies the leaf sibling chain has no orphaned
// pages after a merge: the freed right leaf must be bypassed in the doubly-linked list.
func TestDelete_TriggerMerge_SiblingChainUpdated(t *testing.T) {
	bt, pm := newTempBTree(t)
	bigVal := repeatedStr('e', 180)
	for i := uint64(1); i <= 21; i++ {
		mustInsert(t, bt, i, []Field{strF(1, bigVal)})
	}
	mustDelete(t, bt, 1) // redistribution
	mustDelete(t, bt, 2) // merge

	remaining := make([]uint64, 0, 19)
	for i := uint64(3); i <= 21; i++ {
		remaining = append(remaining, i)
	}
	verifyLeafChain(t, bt, pm, remaining)
}

// TestDelete_TriggerMerge_AllOriginalLeftLeafKeys deletes all original left-leaf keys in
// succession and verifies only the right-leaf keys remain in the tree.
func TestDelete_TriggerMerge_AllOriginalLeftLeafKeys(t *testing.T) {
	bt, pm := newTempBTree(t)
	bigVal := repeatedStr('f', 180)
	// Insert 1..21: split → leaf_L=[1..10], leaf_R=[11..21]
	for i := uint64(1); i <= 21; i++ {
		mustInsert(t, bt, i, []Field{strF(1, bigVal)})
	}
	// Deleting the 10 lowest keys (which start in the left leaf) forces redistribution then merge.
	for k := uint64(1); k <= 10; k++ {
		mustDelete(t, bt, k)
	}
	for i := uint64(11); i <= 21; i++ {
		assertFound(t, bt, i, []Field{strF(1, bigVal)})
	}
	for i := uint64(1); i <= 10; i++ {
		assertMissing(t, bt, i)
	}
	remaining := make([]uint64, 11)
	for i := range remaining {
		remaining[i] = uint64(i + 11)
	}
	verifyLeafChain(t, bt, pm, remaining)
}

// ---- Sequential delete-all ----

// TestDelete_AllKeys_AscendingOrder inserts N records and deletes them in ascending key order,
// verifying the tree is empty at the end.
func TestDelete_AllKeys_AscendingOrder(t *testing.T) {
	bt, pm := newTempBTree(t)
	const n = 100
	for i := uint64(1); i <= n; i++ {
		mustInsert(t, bt, i, []Field{intF(1, int64(i))})
	}
	for i := uint64(1); i <= n; i++ {
		mustDelete(t, bt, i)
		assertMissing(t, bt, i)
	}
	assertTreeEmpty(t, pm)
}

// TestDelete_AllKeys_DescendingOrder inserts N records and deletes them in descending key order.
func TestDelete_AllKeys_DescendingOrder(t *testing.T) {
	bt, pm := newTempBTree(t)
	const n = 100
	for i := uint64(1); i <= n; i++ {
		mustInsert(t, bt, i, []Field{intF(1, int64(i))})
	}
	for i := uint64(n); i >= 1; i-- {
		mustDelete(t, bt, i)
		assertMissing(t, bt, i)
	}
	assertTreeEmpty(t, pm)
}

// TestDelete_AllKeys_LeafChainTrackedPerStep verifies the leaf chain is correct after every
// single deletion when deleting all keys in ascending order.
func TestDelete_AllKeys_LeafChainTrackedPerStep(t *testing.T) {
	bt, pm := newTempBTree(t)
	const n = 50
	keys := make([]uint64, n)
	for i := range keys {
		keys[i] = uint64(i + 1)
		mustInsert(t, bt, keys[i], []Field{intF(1, int64(keys[i]))})
	}
	for i, k := range keys {
		mustDelete(t, bt, k)
		remaining := keys[i+1:]
		if len(remaining) == 0 {
			assertTreeEmpty(t, pm)
		} else {
			for _, rk := range remaining {
				assertFound(t, bt, rk, []Field{intF(1, int64(rk))})
			}
			verifyLeafChain(t, bt, pm, remaining)
		}
	}
}

// TestDelete_AllKeys_ConsecutiveLarge inserts records with large payloads (each ≈196 bytes,
// enough to force multiple splits) and then deletes them all, verifying the tree empties.
func TestDelete_AllKeys_ConsecutiveLarge(t *testing.T) {
	bt, pm := newTempBTree(t)
	bigVal := repeatedStr('g', 180)
	const n = 50
	for i := uint64(1); i <= n; i++ {
		mustInsert(t, bt, i, []Field{strF(1, bigVal)})
	}
	for i := uint64(1); i <= n; i++ {
		mustDelete(t, bt, i)
	}
	assertTreeEmpty(t, pm)
}

// ---- Mixed patterns ----

// TestDelete_HalfKeys_OtherHalfPreserved inserts 200 keys, deletes every odd key, and verifies
// only the even keys remain in the correct order.
func TestDelete_HalfKeys_OtherHalfPreserved(t *testing.T) {
	bt, pm := newTempBTree(t)
	const n = 200
	for i := uint64(1); i <= n; i++ {
		mustInsert(t, bt, i, []Field{intF(1, int64(i))})
	}
	var remaining []uint64
	for i := uint64(1); i <= n; i++ {
		if i%2 == 0 {
			remaining = append(remaining, i)
		} else {
			mustDelete(t, bt, i)
		}
	}
	for _, k := range remaining {
		assertFound(t, bt, k, []Field{intF(1, int64(k))})
	}
	for i := uint64(1); i <= n; i++ {
		if i%2 != 0 {
			assertMissing(t, bt, i)
		}
	}
	verifyLeafChain(t, bt, pm, remaining)
}

// TestDelete_EveryThirdKey builds a 300-key tree and deletes every third key, verifying
// the remaining keys and the leaf chain are correct.
func TestDelete_EveryThirdKey(t *testing.T) {
	bt, pm := newTempBTree(t)
	const n = 300
	for i := uint64(1); i <= n; i++ {
		mustInsert(t, bt, i, []Field{intF(1, int64(i))})
	}
	var deleted, remaining []uint64
	for i := uint64(1); i <= n; i++ {
		if i%3 == 0 {
			deleted = append(deleted, i)
			mustDelete(t, bt, i)
		} else {
			remaining = append(remaining, i)
		}
	}
	for _, k := range deleted {
		assertMissing(t, bt, k)
	}
	for _, k := range remaining {
		assertFound(t, bt, k, []Field{intF(1, int64(k))})
	}
	verifyLeafChain(t, bt, pm, remaining)
}

// TestDelete_InterleavedInsertDelete interleaves insertions and deletions across two phases
// and verifies the tree is consistent throughout.
func TestDelete_InterleavedInsertDelete(t *testing.T) {
	bt, pm := newTempBTree(t)

	// Phase 1: insert keys 1..50
	for i := uint64(1); i <= 50; i++ {
		mustInsert(t, bt, i, []Field{intF(1, int64(i))})
	}
	// Phase 2: delete every other key, insert new keys 51..100
	for i := uint64(1); i <= 50; i += 2 {
		mustDelete(t, bt, i)
	}
	for i := uint64(51); i <= 100; i++ {
		mustInsert(t, bt, i, []Field{intF(1, int64(i))})
	}

	live := make(map[uint64]bool)
	for i := uint64(2); i <= 50; i += 2 {
		live[i] = true
	}
	for i := uint64(51); i <= 100; i++ {
		live[i] = true
	}

	var liveKeys []uint64
	for k := range live {
		liveKeys = append(liveKeys, k)
		assertFound(t, bt, k, []Field{intF(1, int64(k))})
	}
	for i := uint64(1); i <= 100; i++ {
		if !live[i] {
			assertMissing(t, bt, i)
		}
	}
	verifyLeafChain(t, bt, pm, liveKeys)
}

// TestDelete_ThenInsertSameRange deletes a contiguous range of keys and then reinserts the
// same keys with different values, verifying the final state is fully consistent.
func TestDelete_ThenInsertSameRange(t *testing.T) {
	bt, pm := newTempBTree(t)
	const n = 200
	for i := uint64(1); i <= n; i++ {
		mustInsert(t, bt, i, []Field{intF(1, int64(i))})
	}
	// Delete keys 50..150
	for i := uint64(50); i <= 150; i++ {
		mustDelete(t, bt, i)
	}
	// Reinsert keys 50..150 with negated values
	for i := uint64(50); i <= 150; i++ {
		mustInsert(t, bt, i, []Field{intF(1, int64(i)*-1)}) //nolint:gosec
	}
	for i := uint64(1); i < 50; i++ {
		assertFound(t, bt, i, []Field{intF(1, int64(i))})
	}
	for i := uint64(50); i <= 150; i++ {
		assertFound(t, bt, i, []Field{intF(1, int64(i)*-1)}) //nolint:gosec
	}
	for i := uint64(151); i <= n; i++ {
		assertFound(t, bt, i, []Field{intF(1, int64(i))})
	}
	fullKeys := make([]uint64, n)
	for i := range fullKeys {
		fullKeys[i] = uint64(i + 1)
	}
	verifyLeafChain(t, bt, pm, fullKeys)
}
