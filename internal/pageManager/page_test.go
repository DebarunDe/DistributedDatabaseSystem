package pagemanager

import (
	"bytes"
	"testing"
)

// ============================================================
// Helpers
// ============================================================

// newTestPage creates a leaf page for use in record/slot tests.
// Leaf pages support InsertRecord, DeleteRecord, and updateRecord;
// meta pages panic on those operations so they cannot be used as a
// general-purpose test fixture.
func newTestPage(t *testing.T, id uint32) *Page {
	t.Helper()
	return NewPage(PageTypeLeaf, id)
}

// ============================================================
// Constants
// ============================================================

func TestConstants_PageSize(t *testing.T) {
	if PageSize != 4096 {
		t.Errorf("PageSize = %d, want 4096", PageSize)
	}
}

func TestConstants_HeaderSizes(t *testing.T) {
	if CommonHeaderSize != 24 {
		t.Errorf("CommonHeaderSize = %d, want 24", CommonHeaderSize)
	}
	if LeafHeaderSize != 32 {
		t.Errorf("LeafHeaderSize = %d, want 32", LeafHeaderSize)
	}
	if InternalHeaderSize != 32 {
		t.Errorf("InternalHeaderSize = %d, want 32", InternalHeaderSize)
	}
}

func TestConstants_InvalidPageID(t *testing.T) {
	if InvalidPageID != 0xFFFFFFFF {
		t.Errorf("InvalidPageID = %#x, want 0xFFFFFFFF", InvalidPageID)
	}
}

func TestConstants_PageTypeValues(t *testing.T) {
	if PageTypeMeta != 0 {
		t.Errorf("PageTypeMeta = %d, want 0", PageTypeMeta)
	}
	if PageTypeLeaf != 1 {
		t.Errorf("PageTypeLeaf = %d, want 1", PageTypeLeaf)
	}
	if PageTypeInternal != 2 {
		t.Errorf("PageTypeInternal = %d, want 2", PageTypeInternal)
	}
	if PageTypeOverflow != 3 {
		t.Errorf("PageTypeOverflow = %d, want 3", PageTypeOverflow)
	}
}

func TestConstants_FieldOffsets(t *testing.T) {
	if OffsetPageType != 0 {
		t.Errorf("OffsetPageType = %d, want 0", OffsetPageType)
	}
	if OffsetPageID != 1 {
		t.Errorf("OffsetPageID = %d, want 1", OffsetPageID)
	}
	if OffsetFreeSpaceStart != 5 {
		t.Errorf("OffsetFreeSpaceStart = %d, want 5", OffsetFreeSpaceStart)
	}
	if OffsetFreeSpaceEnd != 7 {
		t.Errorf("OffsetFreeSpaceEnd = %d, want 7", OffsetFreeSpaceEnd)
	}
	if OffsetRowCount != 9 {
		t.Errorf("OffsetRowCount = %d, want 9", OffsetRowCount)
	}
	if OffsetPageLSN != 11 {
		t.Errorf("OffsetPageLSN = %d, want 11", OffsetPageLSN)
	}
}

func TestConstants_LeafOffsets(t *testing.T) {
	if OffsetLeftSibling != CommonHeaderSize {
		t.Errorf("OffsetLeftSibling = %d, want %d", OffsetLeftSibling, CommonHeaderSize)
	}
	if OffsetRightSibling != CommonHeaderSize+4 {
		t.Errorf("OffsetRightSibling = %d, want %d", OffsetRightSibling, CommonHeaderSize+4)
	}
}

func TestConstants_InternalOffsets(t *testing.T) {
	if OffsetRightmostChild != CommonHeaderSize {
		t.Errorf("OffsetRightmostChild = %d, want %d", OffsetRightmostChild, CommonHeaderSize)
	}
	if OffsetLevel != CommonHeaderSize+4 {
		t.Errorf("OffsetLevel = %d, want %d", OffsetLevel, CommonHeaderSize+4)
	}
}

// ============================================================
// NewPage initialisation
// ============================================================

func TestNewPage_MetaInitialState(t *testing.T) {
	p := NewPage(PageTypeMeta, 0)
	assertPageInit(t, p, PageTypeMeta, 0, CommonHeaderSize)
}

func TestNewPage_LeafInitialState(t *testing.T) {
	p := NewPage(PageTypeLeaf, 42)
	assertPageInit(t, p, PageTypeLeaf, 42, LeafHeaderSize)
}

func TestNewPage_InternalInitialState(t *testing.T) {
	p := NewPage(PageTypeInternal, 7)
	assertPageInit(t, p, PageTypeInternal, 7, InternalHeaderSize)
}

func TestNewPage_OverflowInitialState(t *testing.T) {
	p := NewPage(PageTypeOverflow, 99)
	assertPageInit(t, p, PageTypeOverflow, 99, CommonHeaderSize)
}

func assertPageInit(t *testing.T, p *Page, wantType uint8, wantID uint32, wantFSSStart int) {
	t.Helper()
	if got := p.GetPageType(); got != wantType {
		t.Errorf("GetPageType() = %d, want %d", got, wantType)
	}
	if got := p.GetPageId(); got != wantID {
		t.Errorf("GetPageId() = %d, want %d", got, wantID)
	}
	if got := p.GetFreeSpaceStart(); got != uint16(wantFSSStart) {
		t.Errorf("GetFreeSpaceStart() = %d, want %d", got, wantFSSStart)
	}
	if got := p.GetFreeSpaceEnd(); got != PageSize {
		t.Errorf("GetFreeSpaceEnd() = %d, want %d (PageSize)", got, PageSize)
	}
	if got := p.GetRowCount(); got != 0 {
		t.Errorf("GetRowCount() = %d, want 0", got)
	}
	if got := p.GetPageLSN(); got != 0 {
		t.Errorf("GetPageLSN() = %d, want 0", got)
	}
}

func TestNewPage_ZeroesData(t *testing.T) {
	p := NewPage(PageTypeMeta, 1)
	// Header bytes that are not written by NewPage should be zero (reserved)
	for i := OffsetReserved; i < CommonHeaderSize; i++ {
		if p.Data[i] != 0 {
			t.Errorf("reserved byte %d = %d, want 0", i, p.Data[i])
		}
	}
}

// ============================================================
// Common header accessor round-trips
// ============================================================

func TestPageType_RoundTrip(t *testing.T) {
	p := &Page{}
	for _, pt := range []uint8{PageTypeMeta, PageTypeLeaf, PageTypeInternal, PageTypeOverflow} {
		p.setPageType(pt)
		if got := p.GetPageType(); got != pt {
			t.Errorf("setPageType(%d) → GetPageType() = %d", pt, got)
		}
	}
}

func TestPageID_RoundTrip(t *testing.T) {
	p := &Page{}
	cases := []uint32{0, 1, 255, 65535, 0xFFFFFFFE, InvalidPageID}
	for _, id := range cases {
		p.setPageId(id)
		if got := p.GetPageId(); got != id {
			t.Errorf("setPageId(%d) → GetPageId() = %d", id, got)
		}
	}
}

func TestFreeSpaceStart_RoundTrip(t *testing.T) {
	p := &Page{}
	for _, v := range []uint16{0, 24, 32, 4000, 4096} {
		p.setFreeSpaceStart(v)
		if got := p.GetFreeSpaceStart(); got != v {
			t.Errorf("setFreeSpaceStart(%d) → GetFreeSpaceStart() = %d", v, got)
		}
	}
}

func TestFreeSpaceEnd_RoundTrip(t *testing.T) {
	p := &Page{}
	for _, v := range []uint16{0, 100, 2000, 4096} {
		p.setFreeSpaceEnd(v)
		if got := p.GetFreeSpaceEnd(); got != v {
			t.Errorf("setFreeSpaceEnd(%d) → GetFreeSpaceEnd() = %d", v, got)
		}
	}
}

func TestRowCount_RoundTrip(t *testing.T) {
	p := &Page{}
	for _, v := range []uint16{0, 1, 100, 0xFFFF} {
		p.setRowCount(v)
		if got := p.GetRowCount(); got != v {
			t.Errorf("setRowCount(%d) → GetRowCount() = %d", v, got)
		}
	}
}

func TestPageLSN_RoundTrip(t *testing.T) {
	p := &Page{}
	cases := []uint64{0, 1, 0xDEADBEEFCAFEBABE, 0xFFFFFFFFFFFFFFFF}
	for _, lsn := range cases {
		p.setPageLSN(lsn)
		if got := p.GetPageLSN(); got != lsn {
			t.Errorf("setPageLSN(%d) → GetPageLSN() = %d", lsn, got)
		}
	}
}

// ============================================================
// Leaf-specific header round-trips
// ============================================================

func TestLeftSibling_RoundTrip(t *testing.T) {
	p := NewPage(PageTypeLeaf, 1)
	for _, id := range []uint32{0, 1, 100, InvalidPageID} {
		p.setLeftSibling(id)
		if got := p.GetLeftSibling(); got != id {
			t.Errorf("setLeftSibling(%d) → GetLeftSibling() = %d", id, got)
		}
	}
}

func TestRightSibling_RoundTrip(t *testing.T) {
	p := NewPage(PageTypeLeaf, 1)
	for _, id := range []uint32{0, 1, 100, InvalidPageID} {
		p.setRightSibling(id)
		if got := p.GetRightSibling(); got != id {
			t.Errorf("setRightSibling(%d) → GetRightSibling() = %d", id, got)
		}
	}
}

func TestLeftAndRightSibling_Independent(t *testing.T) {
	p := NewPage(PageTypeLeaf, 1)
	p.setLeftSibling(10)
	p.setRightSibling(20)

	if got := p.GetLeftSibling(); got != 10 {
		t.Errorf("GetLeftSibling() = %d, want 10", got)
	}
	if got := p.GetRightSibling(); got != 20 {
		t.Errorf("GetRightSibling() = %d, want 20", got)
	}
}

func TestSiblings_IndependentOfPageID(t *testing.T) {
	p := NewPage(PageTypeLeaf, 5)
	p.setLeftSibling(111)
	p.setRightSibling(222)

	if got := p.GetPageId(); got != 5 {
		t.Errorf("GetPageId() = %d after setting siblings, want 5", got)
	}
}

// ============================================================
// Internal-specific header round-trips
// ============================================================

func TestRightMostChild_RoundTrip(t *testing.T) {
	p := NewPage(PageTypeInternal, 1)
	for _, id := range []uint32{0, 1, 100, InvalidPageID} {
		p.setRightMostChild(id)
		if got := p.GetRightMostChild(); got != id {
			t.Errorf("setRightMostChild(%d) → GetRightMostChild() = %d", id, got)
		}
	}
}

func TestLevel_RoundTrip(t *testing.T) {
	p := NewPage(PageTypeInternal, 1)
	for _, level := range []uint16{0, 1, 5, 100, 0xFFFF} {
		p.setLevel(level)
		if got := p.GetLevel(); got != level {
			t.Errorf("setLevel(%d) → GetLevel() = %d", level, got)
		}
	}
}

func TestRightMostChildAndLevel_Independent(t *testing.T) {
	p := NewPage(PageTypeInternal, 1)
	p.setRightMostChild(999)
	p.setLevel(3)

	if got := p.GetRightMostChild(); got != 999 {
		t.Errorf("GetRightMostChild() = %d, want 999", got)
	}
	if got := p.GetLevel(); got != 3 {
		t.Errorf("GetLevel() = %d, want 3", got)
	}
}

// ============================================================
// headerSize
// ============================================================

func TestHeaderSize_ByPageType(t *testing.T) {
	cases := []struct {
		pageType uint8
		want     int
	}{
		{PageTypeLeaf, LeafHeaderSize},
		{PageTypeInternal, InternalHeaderSize},
		{PageTypeMeta, CommonHeaderSize},
		{PageTypeOverflow, CommonHeaderSize},
	}
	for _, tc := range cases {
		p := NewPage(tc.pageType, 1)
		if got := p.headerSize(); got != tc.want {
			t.Errorf("pageType %d: headerSize() = %d, want %d", tc.pageType, got, tc.want)
		}
	}
}

// ============================================================
// Low-level binary encoding
// ============================================================

func TestLittleEndianEncoding_Uint16(t *testing.T) {
	p := &Page{}
	writeUint16(p, 0, 0x0102)
	if p.Data[0] != 0x02 || p.Data[1] != 0x01 {
		t.Errorf("uint16 little-endian: got [%x %x], want [02 01]", p.Data[0], p.Data[1])
	}
	if got := readUint16(p, 0); got != 0x0102 {
		t.Errorf("readUint16 = %#x, want 0x0102", got)
	}
}

func TestLittleEndianEncoding_Uint32(t *testing.T) {
	p := &Page{}
	writeUint32(p, 0, 0x01020304)
	if p.Data[0] != 0x04 || p.Data[1] != 0x03 || p.Data[2] != 0x02 || p.Data[3] != 0x01 {
		t.Errorf("uint32 little-endian: got %v, want [04 03 02 01]", p.Data[:4])
	}
	if got := readUint32(p, 0); got != 0x01020304 {
		t.Errorf("readUint32 = %#x, want 0x01020304", got)
	}
}

func TestLittleEndianEncoding_Uint64(t *testing.T) {
	p := &Page{}
	val := uint64(0x0102030405060708)
	writeUint64(p, 0, val)
	if got := readUint64(p, 0); got != val {
		t.Errorf("readUint64 = %#x, want %#x", got, val)
	}
}

// ============================================================
// Slot directory
// ============================================================

func TestSlotOffset_UsedFlag(t *testing.T) {
	p := newTestPage(t, 1)

	p.setSlotOffset(0, 1000, true)
	off, used := p.GetSlotOffset(0)
	if !used {
		t.Error("Expected slot to be used")
	}
	if off != 1000 {
		t.Errorf("GetSlotOffset() = %d, want 1000", off)
	}
}

func TestSlotOffset_UnusedFlag(t *testing.T) {
	p := newTestPage(t, 1)

	p.setSlotOffset(0, 500, false)
	off, used := p.GetSlotOffset(0)
	if used {
		t.Error("Expected slot to be unused")
	}
	if off != 500 {
		t.Errorf("GetSlotOffset() = %d, want 500", off)
	}
}

func TestSlotOffset_MaxValue(t *testing.T) {
	p := newTestPage(t, 1)
	// Maximum 15-bit offset (0x7FFF)
	p.setSlotOffset(0, 0x7FFF, true)
	off, used := p.GetSlotOffset(0)
	if !used {
		t.Error("Expected used flag")
	}
	if off != 0x7FFF {
		t.Errorf("GetSlotOffset() = %#x, want 0x7FFF", off)
	}
}

func TestSlotLength_NormalFlag(t *testing.T) {
	p := newTestPage(t, 1)
	p.setSlotLength(0, 128, false)
	length, overflow := p.GetSlotLength(0)
	if overflow {
		t.Error("Expected no overflow")
	}
	if length != 128 {
		t.Errorf("GetSlotLength() = %d, want 128", length)
	}
}

func TestSlotLength_OverflowFlag(t *testing.T) {
	p := newTestPage(t, 1)
	p.setSlotLength(0, 256, true)
	length, overflow := p.GetSlotLength(0)
	if !overflow {
		t.Error("Expected overflow flag")
	}
	if length != 256 {
		t.Errorf("GetSlotLength() = %d, want 256", length)
	}
}

func TestSlotDirectory_MultipleSlots(t *testing.T) {
	p := newTestPage(t, 1)

	p.setSlotOffset(0, 100, true)
	p.setSlotOffset(1, 200, true)
	p.setSlotOffset(2, 300, false)

	off0, used0 := p.GetSlotOffset(0)
	off1, used1 := p.GetSlotOffset(1)
	off2, used2 := p.GetSlotOffset(2)

	if off0 != 100 || !used0 {
		t.Errorf("Slot 0: offset=%d used=%v, want offset=100 used=true", off0, used0)
	}
	if off1 != 200 || !used1 {
		t.Errorf("Slot 1: offset=%d used=%v, want offset=200 used=true", off1, used1)
	}
	if off2 != 300 || used2 {
		t.Errorf("Slot 2: offset=%d used=%v, want offset=300 used=false", off2, used2)
	}
}

func TestSlotDirectory_SlotsDoNotInterfere(t *testing.T) {
	p := newTestPage(t, 1)

	// Write different offsets and lengths to adjacent slots
	p.setSlotOffset(0, 100, true)
	p.setSlotLength(0, 50, false)
	p.setSlotOffset(1, 200, true)
	p.setSlotLength(1, 60, true)

	off0, used0 := p.GetSlotOffset(0)
	len0, ovf0 := p.GetSlotLength(0)
	off1, used1 := p.GetSlotOffset(1)
	len1, ovf1 := p.GetSlotLength(1)

	if off0 != 100 || !used0 || len0 != 50 || ovf0 {
		t.Errorf("Slot 0 corrupted: off=%d used=%v len=%d ovf=%v", off0, used0, len0, ovf0)
	}
	if off1 != 200 || !used1 || len1 != 60 || !ovf1 {
		t.Errorf("Slot 1 corrupted: off=%d used=%v len=%d ovf=%v", off1, used1, len1, ovf1)
	}
}

// ============================================================
// GetRecord
// ============================================================

func TestGetRecord_UnusedSlotReturnsFalse(t *testing.T) {
	p := newTestPage(t, 1)
	data, ok := p.GetRecord(0)
	if ok {
		t.Error("Expected GetRecord to return false for unused slot")
	}
	if data != nil {
		t.Error("Expected GetRecord to return nil data for unused slot")
	}
}

func TestGetRecord_ReturnsCorrectData(t *testing.T) {
	p := newTestPage(t, 1)
	record := []byte("hello, world")
	idx, ok := p.InsertRecord(record)
	if !ok {
		t.Fatal("InsertRecord failed unexpectedly")
	}
	got, ok := p.GetRecord(idx)
	if !ok {
		t.Fatal("GetRecord returned false for valid slot")
	}
	if !bytes.Equal(got, record) {
		t.Errorf("GetRecord = %v, want %v", got, record)
	}
}

func TestGetRecord_ReturnsSliceIntoPageData(t *testing.T) {
	// The returned slice should reference Data, not a copy
	p := newTestPage(t, 1)
	p.InsertRecord([]byte("abc"))
	got, _ := p.GetRecord(0)
	if len(got) != 3 {
		t.Fatalf("Expected length 3, got %d", len(got))
	}
	// Modifying the returned slice should be visible in the page data
	got[0] = 'X'
	got2, _ := p.GetRecord(0)
	if got2[0] != 'X' {
		t.Error("GetRecord should return a slice into page data, not a copy")
	}
}

// ============================================================
// InsertRecord
// ============================================================

func TestInsertRecord_BasicInsert(t *testing.T) {
	p := newTestPage(t, 1)
	record := []byte("test data")

	idx, ok := p.InsertRecord(record)
	if !ok {
		t.Fatal("InsertRecord returned false for a page with space")
	}
	if idx != 0 {
		t.Errorf("InsertRecord returned slot index %d, want 0", idx)
	}
}

func TestInsertRecord_SequentialSlotIndices(t *testing.T) {
	p := newTestPage(t, 1)
	for i := 0; i < 5; i++ {
		idx, ok := p.InsertRecord([]byte{byte(i)})
		if !ok {
			t.Fatalf("InsertRecord[%d] failed", i)
		}
		if idx != i {
			t.Errorf("InsertRecord[%d] returned index %d, want %d", i, idx, i)
		}
	}
}

func TestInsertRecord_IncrementsRowCount(t *testing.T) {
	p := newTestPage(t, 1)
	for i := 1; i <= 5; i++ {
		p.InsertRecord([]byte{byte(i)})
		if got := p.GetRowCount(); got != uint16(i) {
			t.Errorf("After %d inserts, GetRowCount() = %d, want %d", i, got, i)
		}
	}
}

func TestInsertRecord_DecreasesFreeSpaceEnd(t *testing.T) {
	p := newTestPage(t, 1)
	endBefore := p.GetFreeSpaceEnd()
	p.InsertRecord([]byte("record"))
	endAfter := p.GetFreeSpaceEnd()
	if endAfter >= endBefore {
		t.Errorf("FreeSpaceEnd should decrease: before=%d after=%d", endBefore, endAfter)
	}
}

func TestInsertRecord_IncreasesFreeSpaceStart(t *testing.T) {
	p := newTestPage(t, 1)
	startBefore := p.GetFreeSpaceStart()
	p.InsertRecord([]byte("record"))
	startAfter := p.GetFreeSpaceStart()
	if startAfter <= startBefore {
		t.Errorf("FreeSpaceStart should increase: before=%d after=%d", startBefore, startAfter)
	}
}

func TestInsertRecord_MultipleRecordsPreservedData(t *testing.T) {
	p := newTestPage(t, 1)
	records := [][]byte{
		[]byte("first"),
		[]byte("second record"),
		[]byte("third"),
	}
	for _, r := range records {
		p.InsertRecord(r)
	}
	for i, want := range records {
		got, ok := p.GetRecord(i)
		if !ok {
			t.Fatalf("GetRecord(%d) returned false", i)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("GetRecord(%d) = %q, want %q", i, got, want)
		}
	}
}

func TestInsertRecord_EmptyRecord(t *testing.T) {
	p := newTestPage(t, 1)
	idx, ok := p.InsertRecord([]byte{})
	if !ok {
		t.Fatal("InsertRecord with empty record should succeed")
	}
	got, ok := p.GetRecord(idx)
	if !ok {
		t.Fatal("GetRecord for empty record slot should return true")
	}
	if len(got) != 0 {
		t.Errorf("Expected empty record, got len=%d", len(got))
	}
}

func TestInsertRecord_ReturnsFalseWhenFull(t *testing.T) {
	p := newTestPage(t, 1)
	bigRecord := make([]byte, 100)
	var sawFailure bool
	for i := 0; i < 200; i++ {
		_, ok := p.InsertRecord(bigRecord)
		if !ok {
			sawFailure = true
			break
		}
	}
	if !sawFailure {
		t.Error("Expected InsertRecord to return false when page is full")
	}
}

func TestInsertRecord_LargeRecord(t *testing.T) {
	p := newTestPage(t, 1)
	// A record nearly as large as the page
	large := make([]byte, 4000)
	for i := range large {
		large[i] = byte(i % 256)
	}
	idx, ok := p.InsertRecord(large)
	if !ok {
		t.Fatal("InsertRecord failed for large record")
	}
	got, ok := p.GetRecord(idx)
	if !ok {
		t.Fatal("GetRecord failed for large record")
	}
	if !bytes.Equal(got, large) {
		t.Error("Large record data mismatch")
	}
}

// ============================================================
// InsertRecord on extended-header page types
// ============================================================

func TestInsertRecord_LeafPage(t *testing.T) {
	p := NewPage(PageTypeLeaf, 10)
	record := []byte("leaf record data")

	idx, ok := p.InsertRecord(record)
	if !ok {
		t.Fatal("InsertRecord failed on leaf page")
	}
	got, ok := p.GetRecord(idx)
	if !ok {
		t.Fatal("GetRecord failed on leaf page")
	}
	if !bytes.Equal(got, record) {
		t.Errorf("GetRecord on leaf = %q, want %q", got, record)
	}
}

func TestInsertRecord_InternalPage(t *testing.T) {
	p := NewPage(PageTypeInternal, 11)
	record := []byte("internal record data")

	idx, ok := p.InsertRecord(record)
	if !ok {
		t.Fatal("InsertRecord failed on internal page")
	}
	got, ok := p.GetRecord(idx)
	if !ok {
		t.Fatal("GetRecord failed on internal page")
	}
	if !bytes.Equal(got, record) {
		t.Errorf("GetRecord on internal = %q, want %q", got, record)
	}
}

func TestInsertRecord_LeafPageDoesNotCorruptSiblings(t *testing.T) {
	p := NewPage(PageTypeLeaf, 1)
	p.setLeftSibling(10)
	p.setRightSibling(20)

	p.InsertRecord([]byte("data"))

	if got := p.GetLeftSibling(); got != 10 {
		t.Errorf("GetLeftSibling() = %d after insert, want 10", got)
	}
	if got := p.GetRightSibling(); got != 20 {
		t.Errorf("GetRightSibling() = %d after insert, want 20", got)
	}
}

func TestInsertRecord_InternalPageDoesNotCorruptChild(t *testing.T) {
	p := NewPage(PageTypeInternal, 1)
	p.setRightMostChild(50)
	p.setLevel(2)

	p.InsertRecord([]byte("data"))

	if got := p.GetRightMostChild(); got != 50 {
		t.Errorf("GetRightMostChild() = %d after insert, want 50", got)
	}
	if got := p.GetLevel(); got != 2 {
		t.Errorf("GetLevel() = %d after insert, want 2", got)
	}
}

// ============================================================
// InsertRecordAt
// ============================================================

func TestInsertRecordAt_AtEnd_EquivalentToAppend(t *testing.T) {
	p := newTestPage(t, 1)
	p.InsertRecord([]byte("a"))
	p.InsertRecord([]byte("b"))

	ok := p.InsertRecordAt(2, []byte("c"))
	if !ok {
		t.Fatal("InsertRecordAt at end returned false")
	}
	got, ok := p.GetRecord(2)
	if !ok || !bytes.Equal(got, []byte("c")) {
		t.Errorf("GetRecord(2) = %q ok=%v, want %q true", got, ok, "c")
	}
}

func TestInsertRecordAt_AtBeginning_ShiftsAllSlots(t *testing.T) {
	p := newTestPage(t, 1)
	p.InsertRecord([]byte("first"))
	p.InsertRecord([]byte("second"))

	ok := p.InsertRecordAt(0, []byte("zeroth"))
	if !ok {
		t.Fatal("InsertRecordAt at index 0 returned false")
	}

	want := [][]byte{[]byte("zeroth"), []byte("first"), []byte("second")}
	for i, w := range want {
		got, ok := p.GetRecord(i)
		if !ok {
			t.Fatalf("GetRecord(%d) returned false after insert at 0", i)
		}
		if !bytes.Equal(got, w) {
			t.Errorf("GetRecord(%d) = %q, want %q", i, got, w)
		}
	}
}

func TestInsertRecordAt_AtMiddle_ShiftsHigherSlots(t *testing.T) {
	p := newTestPage(t, 1)
	p.InsertRecord([]byte("a"))
	p.InsertRecord([]byte("c"))
	p.InsertRecord([]byte("d"))

	ok := p.InsertRecordAt(1, []byte("b"))
	if !ok {
		t.Fatal("InsertRecordAt at middle returned false")
	}

	want := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	for i, w := range want {
		got, ok := p.GetRecord(i)
		if !ok {
			t.Fatalf("GetRecord(%d) returned false", i)
		}
		if !bytes.Equal(got, w) {
			t.Errorf("GetRecord(%d) = %q, want %q", i, got, w)
		}
	}
}

func TestInsertRecordAt_IntoEmptyPage_AtIndexZero(t *testing.T) {
	p := newTestPage(t, 1)

	ok := p.InsertRecordAt(0, []byte("only"))
	if !ok {
		t.Fatal("InsertRecordAt(0) on empty page returned false")
	}
	if got := p.GetRowCount(); got != 1 {
		t.Errorf("RowCount = %d, want 1", got)
	}
	got, ok := p.GetRecord(0)
	if !ok || !bytes.Equal(got, []byte("only")) {
		t.Errorf("GetRecord(0) = %q ok=%v, want %q true", got, ok, "only")
	}
}

func TestInsertRecordAt_IncrementsRowCount(t *testing.T) {
	p := newTestPage(t, 1)
	p.InsertRecord([]byte("x"))
	p.InsertRecord([]byte("z"))

	before := p.GetRowCount()
	p.InsertRecordAt(1, []byte("y"))
	if got := p.GetRowCount(); got != before+1 {
		t.Errorf("RowCount = %d, want %d", got, before+1)
	}
}

func TestInsertRecordAt_IncreasesFreeSpaceStart(t *testing.T) {
	p := newTestPage(t, 1)
	p.InsertRecord([]byte("a"))
	before := p.GetFreeSpaceStart()

	p.InsertRecordAt(0, []byte("b"))

	if got := p.GetFreeSpaceStart(); got <= before {
		t.Errorf("FreeSpaceStart should increase: before=%d after=%d", before, got)
	}
}

func TestInsertRecordAt_DecreasesFreeSpaceEnd(t *testing.T) {
	p := newTestPage(t, 1)
	p.InsertRecord([]byte("a"))
	before := p.GetFreeSpaceEnd()

	p.InsertRecordAt(0, []byte("b"))

	if got := p.GetFreeSpaceEnd(); got >= before {
		t.Errorf("FreeSpaceEnd should decrease: before=%d after=%d", before, got)
	}
}

func TestInsertRecordAt_ReturnsFalseWhenFull(t *testing.T) {
	p := newTestPage(t, 1)
	filler := make([]byte, 100)
	for p.CanAccommodate(len(filler)) {
		p.InsertRecord(filler)
	}
	// Use same size so we're certain it won't fit (leftover gap < 104 bytes).
	if p.InsertRecordAt(0, filler) {
		t.Error("InsertRecordAt should return false when page cannot fit the record")
	}
}

func TestInsertRecordAt_PanicsOnNegativeIndex(t *testing.T) {
	p := newTestPage(t, 1)
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for negative slot index")
		}
	}()
	p.InsertRecordAt(-1, []byte("x"))
}

func TestInsertRecordAt_PanicsOnIndexBeyondRowCount(t *testing.T) {
	p := newTestPage(t, 1)
	p.InsertRecord([]byte("a"))
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for slotIndex > rowCount")
		}
	}()
	p.InsertRecordAt(2, []byte("x"))
}

func TestInsertRecordAt_PanicsOnMetaPage(t *testing.T) {
	p := NewMetaPage()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when inserting into meta page")
		}
	}()
	p.InsertRecordAt(0, []byte("x"))
}

func TestInsertRecordAt_PanicsOnOverflowPage(t *testing.T) {
	p := NewPage(PageTypeOverflow, 1)
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when inserting into overflow page")
		}
	}()
	p.InsertRecordAt(0, []byte("x"))
}

func TestInsertRecordAt_DoesNotCorruptLeafSiblings(t *testing.T) {
	p := NewLeafPage(1, 10, 20)
	p.InsertRecord([]byte("a"))
	p.InsertRecord([]byte("c"))

	p.InsertRecordAt(1, []byte("b"))

	if got := p.GetLeftSibling(); got != 10 {
		t.Errorf("GetLeftSibling() = %d after InsertRecordAt, want 10", got)
	}
	if got := p.GetRightSibling(); got != 20 {
		t.Errorf("GetRightSibling() = %d after InsertRecordAt, want 20", got)
	}
}

func TestInsertRecordAt_MultipleInsertsPreserveOrder(t *testing.T) {
	p := newTestPage(t, 1)
	// Build sorted sequence by always inserting at the right position
	p.InsertRecord([]byte("b"))
	p.InsertRecordAt(0, []byte("a")) // insert before "b"
	p.InsertRecordAt(2, []byte("d")) // append after "b"
	p.InsertRecordAt(2, []byte("c")) // insert before "d"

	want := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	if got := p.GetRowCount(); int(got) != len(want) {
		t.Fatalf("RowCount = %d, want %d", got, len(want))
	}
	for i, w := range want {
		got, ok := p.GetRecord(i)
		if !ok {
			t.Fatalf("GetRecord(%d) returned false", i)
		}
		if !bytes.Equal(got, w) {
			t.Errorf("GetRecord(%d) = %q, want %q", i, got, w)
		}
	}
}

func TestInsertRecordAt_FreeSpaceStartEqualsHeaderPlusSlots(t *testing.T) {
	p := newTestPage(t, 1)
	p.InsertRecord([]byte("x"))
	p.InsertRecord([]byte("z"))

	p.InsertRecordAt(1, []byte("y"))

	rowCount := int(p.GetRowCount())
	expected := uint16(p.headerSize() + rowCount*4)
	if got := p.GetFreeSpaceStart(); got != expected {
		t.Errorf("FreeSpaceStart = %d, want %d (headerSize + rowCount*4)", got, expected)
	}
}

// ============================================================
// DeleteRecord
// ============================================================

func TestDeleteRecord_MarksSlotUnused(t *testing.T) {
	p := newTestPage(t, 1)
	idx, _ := p.InsertRecord([]byte("data"))

	p.DeleteRecord(idx)

	_, used := p.GetSlotOffset(idx)
	if used {
		t.Error("Slot should be marked unused after DeleteRecord")
	}
}

func TestDeleteRecord_GetRecordReturnsFalse(t *testing.T) {
	p := newTestPage(t, 1)
	idx, _ := p.InsertRecord([]byte("data"))

	p.DeleteRecord(idx)

	data, ok := p.GetRecord(idx)
	if ok {
		t.Error("GetRecord should return false after delete")
	}
	if data != nil {
		t.Error("GetRecord should return nil data after delete")
	}
}

func TestDeleteRecord_ClearsSlotLength(t *testing.T) {
	p := newTestPage(t, 1)
	idx, _ := p.InsertRecord([]byte("data"))

	p.DeleteRecord(idx)

	length, _ := p.GetSlotLength(idx)
	if length != 0 {
		t.Errorf("Slot length = %d after delete, want 0", length)
	}
}

func TestDeleteRecord_NeighboursUnaffected(t *testing.T) {
	p := newTestPage(t, 1)
	p.InsertRecord([]byte("record 0"))
	p.InsertRecord([]byte("record 1"))
	p.InsertRecord([]byte("record 2"))

	p.DeleteRecord(1)

	got0, ok0 := p.GetRecord(0)
	got2, ok2 := p.GetRecord(2)

	if !ok0 || !bytes.Equal(got0, []byte("record 0")) {
		t.Errorf("Record 0 affected by deleting record 1: ok=%v data=%q", ok0, got0)
	}
	if !ok2 || !bytes.Equal(got2, []byte("record 2")) {
		t.Errorf("Record 2 affected by deleting record 1: ok=%v data=%q", ok2, got2)
	}
}

// ============================================================
// updateRecord
// ============================================================

func TestUpdateRecord_SameSize(t *testing.T) {
	p := newTestPage(t, 1)
	idx, _ := p.InsertRecord([]byte("hello"))

	p.updateRecord(idx, []byte("world"), false)

	got, ok := p.GetRecord(idx)
	if !ok {
		t.Fatal("GetRecord returned false after update")
	}
	if !bytes.Equal(got, []byte("world")) {
		t.Errorf("GetRecord = %q, want %q", got, "world")
	}
}

func TestUpdateRecord_SmallerRecord(t *testing.T) {
	p := newTestPage(t, 1)
	idx, _ := p.InsertRecord([]byte("hello world"))

	p.updateRecord(idx, []byte("hi"), false)

	got, ok := p.GetRecord(idx)
	if !ok {
		t.Fatal("GetRecord returned false after shrink update")
	}
	if !bytes.Equal(got, []byte("hi")) {
		t.Errorf("GetRecord = %q, want %q", got, "hi")
	}
}

func TestUpdateRecord_LargerRecord(t *testing.T) {
	p := newTestPage(t, 1)
	idx, _ := p.InsertRecord([]byte("hi"))

	p.updateRecord(idx, []byte("hello world"), false)

	got, ok := p.GetRecord(idx)
	if !ok {
		t.Fatal("GetRecord returned false after grow update")
	}
	if !bytes.Equal(got, []byte("hello world")) {
		t.Errorf("GetRecord = %q, want %q", got, "hello world")
	}
}

func TestUpdateRecord_PanicsOnNonExistentSlot(t *testing.T) {
	p := newTestPage(t, 1)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic when updating non-existent slot")
		}
	}()

	p.updateRecord(0, []byte("data"), false)
}

func TestUpdateRecord_OverflowFlagSet(t *testing.T) {
	p := newTestPage(t, 1)
	idx, _ := p.InsertRecord([]byte("data"))
	p.updateRecord(idx, []byte("new"), true)

	_, overflow := p.GetSlotLength(idx)
	if !overflow {
		t.Error("Expected overflow flag to be set after updateRecord with overflow=true")
	}
}

func TestUpdateRecord_GrowFailsWhenInsufficientContiguousSpace(t *testing.T) {
	// Fill the page so there is almost no free gap left, then try to grow
	// a record beyond the contiguous free space. The dead bytes from the old
	// record are NOT contiguous with the free gap, so updateRecord must return
	// false instead of blindly writing and corrupting the slot directory.
	p := newTestPage(t, 1)

	// Pack the page until there is room for exactly one more small record.
	filler := make([]byte, 100)
	for p.CanAccommodate(len(filler)) {
		p.InsertRecord(filler)
	}

	// Insert a small record that fits in whatever gap remains.
	small := make([]byte, int(p.GetFreeSpace())-4) // -4 for the slot entry
	idx, ok := p.InsertRecord(small)
	if !ok {
		t.Fatal("setup: InsertRecord for small record failed")
	}

	// Try to grow it beyond the now-empty contiguous free gap.
	// The old record's bytes are dead after DeleteRecord but are NOT part of
	// the free gap, so this must fail.
	big := make([]byte, len(small)+10)
	if p.updateRecord(idx, big, false) {
		t.Error("updateRecord should return false when contiguous free space is insufficient")
	}

	// The original record must be intact — no corruption.
	got, ok := p.GetRecord(idx)
	if !ok {
		t.Fatal("GetRecord returned false after failed update; slot was corrupted")
	}
	if len(got) != len(small) {
		t.Errorf("record length changed after failed update: got %d, want %d", len(got), len(small))
	}
}

// ============================================================
// GetFreeSpace
// ============================================================

func TestGetFreeSpace_InitialValues(t *testing.T) {
	cases := []struct {
		pageType uint8
		want     uint16
	}{
		{PageTypeMeta, PageSize - CommonHeaderSize},
		{PageTypeLeaf, PageSize - LeafHeaderSize},
		{PageTypeInternal, PageSize - InternalHeaderSize},
		{PageTypeOverflow, PageSize - CommonHeaderSize},
	}
	for _, tc := range cases {
		p := NewPage(tc.pageType, 1)
		if got := p.GetFreeSpace(); got != tc.want {
			t.Errorf("pageType %d: GetFreeSpace() = %d, want %d", tc.pageType, got, tc.want)
		}
	}
}

func TestGetFreeSpace_DecreasesAfterInsert(t *testing.T) {
	p := newTestPage(t, 1)
	before := p.GetFreeSpace()
	p.InsertRecord([]byte("test"))
	after := p.GetFreeSpace()
	if after >= before {
		t.Errorf("FreeSpace should decrease: before=%d after=%d", before, after)
	}
}

func TestGetFreeSpace_ZeroWhenStartEqualsEnd(t *testing.T) {
	p := &Page{}
	p.setFreeSpaceStart(100)
	p.setFreeSpaceEnd(100)
	if got := p.GetFreeSpace(); got != 0 {
		t.Errorf("GetFreeSpace() = %d, want 0 when start==end", got)
	}
}

func TestGetFreeSpace_ZeroWhenStartExceedsEnd(t *testing.T) {
	p := &Page{}
	p.setFreeSpaceStart(200)
	p.setFreeSpaceEnd(100)
	if got := p.GetFreeSpace(); got != 0 {
		t.Errorf("GetFreeSpace() = %d, want 0 when start>end", got)
	}
}

// ============================================================
// IsFull
// ============================================================

func TestIsFull_NewPageIsNotFull(t *testing.T) {
	p := newTestPage(t, 1)
	if p.IsFull() {
		t.Error("New page should not be full")
	}
}

func TestIsFull_TrueWhenNoFreeSpace(t *testing.T) {
	p := &Page{}
	p.setFreeSpaceStart(100)
	p.setFreeSpaceEnd(100)
	if !p.IsFull() {
		t.Error("IsFull should return true when FreeSpaceStart == FreeSpaceEnd")
	}
}

// ============================================================
// CanAccommodate
// ============================================================

func TestCanAccommodate_TrueForSmallRecord(t *testing.T) {
	p := newTestPage(t, 1)
	if !p.CanAccommodate(10) {
		t.Error("New page should accommodate 10 bytes")
	}
}

func TestCanAccommodate_FalseForOversizedRecord(t *testing.T) {
	p := newTestPage(t, 1)
	if p.CanAccommodate(PageSize + 1) {
		t.Error("Should not accommodate record larger than the whole page")
	}
}

func TestCanAccommodate_FalseAfterFilling(t *testing.T) {
	p := newTestPage(t, 1)
	record := make([]byte, 100)
	for p.CanAccommodate(len(record)) {
		_, ok := p.InsertRecord(record)
		if !ok {
			break
		}
	}
	if p.CanAccommodate(len(record)) {
		t.Error("Should not accommodate more records after page is full")
	}
}

func TestCanAccommodate_ZeroBytes(t *testing.T) {
	p := newTestPage(t, 1)
	// Zero-byte record still needs a slot entry (4 bytes)
	if !p.CanAccommodate(0) {
		t.Error("New page should accommodate a zero-byte record")
	}
}

// ============================================================
// CompactPage
// ============================================================

func TestCompactPage_NoDeletedRecords_DataIntact(t *testing.T) {
	p := newTestPage(t, 1)
	records := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
	}
	for _, r := range records {
		p.InsertRecord(r)
	}
	rowsBefore := p.GetRowCount()

	p.CompactPage()

	if got := p.GetRowCount(); got != rowsBefore {
		t.Errorf("RowCount changed after compact with no deletes: %d → %d", rowsBefore, got)
	}
	for i, want := range records {
		got, ok := p.GetRecord(i)
		if !ok {
			t.Fatalf("GetRecord(%d) returned false after compaction", i)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Record[%d] = %q after compact, want %q", i, got, want)
		}
	}
}

func TestCompactPage_ReclaimsDeletedSpace(t *testing.T) {
	p := newTestPage(t, 1)
	for i := 0; i < 5; i++ {
		p.InsertRecord(make([]byte, 100))
	}
	p.DeleteRecord(1)
	p.DeleteRecord(3)

	freeBefore := p.GetFreeSpace()
	p.CompactPage()
	freeAfter := p.GetFreeSpace()

	if freeAfter <= freeBefore {
		t.Errorf("CompactPage should increase free space: before=%d after=%d", freeBefore, freeAfter)
	}
}

func TestCompactPage_ReducesRowCount(t *testing.T) {
	p := newTestPage(t, 1)
	for i := 0; i < 4; i++ {
		p.InsertRecord([]byte{byte(i)})
	}
	p.DeleteRecord(0)
	p.DeleteRecord(2)

	p.CompactPage()

	if got := p.GetRowCount(); got != 2 {
		t.Errorf("RowCount after compact = %d, want 2", got)
	}
}

func TestCompactPage_PreservesKeptRecordData(t *testing.T) {
	p := newTestPage(t, 1)
	records := [][]byte{
		[]byte("keep-A"),
		[]byte("delete-B"),
		[]byte("keep-C"),
		[]byte("delete-D"),
		[]byte("keep-E"),
	}
	for _, r := range records {
		p.InsertRecord(r)
	}
	p.DeleteRecord(1)
	p.DeleteRecord(3)

	p.CompactPage()

	want := [][]byte{
		[]byte("keep-A"),
		[]byte("keep-C"),
		[]byte("keep-E"),
	}
	if got := p.GetRowCount(); int(got) != len(want) {
		t.Fatalf("RowCount = %d, want %d", got, len(want))
	}
	for i, w := range want {
		got, ok := p.GetRecord(i)
		if !ok {
			t.Fatalf("GetRecord(%d) returned false after compaction", i)
		}
		if !bytes.Equal(got, w) {
			t.Errorf("Record[%d] = %q, want %q", i, got, w)
		}
	}
}

func TestCompactPage_AllDeleted_EmptyPage(t *testing.T) {
	p := newTestPage(t, 1)
	for i := 0; i < 3; i++ {
		p.InsertRecord([]byte{byte(i)})
	}
	for i := 0; i < 3; i++ {
		p.DeleteRecord(i)
	}

	p.CompactPage()

	if got := p.GetRowCount(); got != 0 {
		t.Errorf("RowCount = %d after deleting all, want 0", got)
	}
	expectedFreeSpace := uint16(PageSize - p.headerSize())
	if got := p.GetFreeSpace(); got != expectedFreeSpace {
		t.Errorf("FreeSpace = %d after full compact, want %d", got, expectedFreeSpace)
	}
}

func TestCompactPage_FreeSpaceStartResetToHeaderSize(t *testing.T) {
	p := newTestPage(t, 1)
	for i := 0; i < 3; i++ {
		p.InsertRecord([]byte("record"))
	}
	p.DeleteRecord(0)
	p.DeleteRecord(1)

	p.CompactPage()

	// After compaction 1 record remains → freeSpaceStart = headerSize + 1*4
	expectedStart := uint16(p.headerSize() + 1*4)
	if got := p.GetFreeSpaceStart(); got != expectedStart {
		t.Errorf("FreeSpaceStart after compact = %d, want %d", got, expectedStart)
	}
}

func TestCompactPage_AllowsFurtherInserts(t *testing.T) {
	p := newTestPage(t, 1)
	record := make([]byte, 50)

	insertCount := 0
	for p.CanAccommodate(len(record) + 4) {
		_, ok := p.InsertRecord(record)
		if !ok {
			break
		}
		insertCount++
	}

	// Delete half the records to fragment space
	for i := 0; i < insertCount/2; i++ {
		p.DeleteRecord(i)
	}

	p.CompactPage()

	_, ok := p.InsertRecord(record)
	if !ok {
		t.Error("Should be able to insert a record after compaction freed space")
	}
}

// ============================================================
// Field isolation: headers do not overlap each other
// ============================================================

func TestLeafHeaderFields_DoNotOverlapPageLSN(t *testing.T) {
	p := NewPage(PageTypeLeaf, 5)
	p.setPageLSN(0xDEADBEEF12345678)
	p.setLeftSibling(111)
	p.setRightSibling(222)

	if got := p.GetPageLSN(); got != 0xDEADBEEF12345678 {
		t.Errorf("GetPageLSN() = %#x after setting siblings, want 0xDEADBEEF12345678", got)
	}
	if got := p.GetLeftSibling(); got != 111 {
		t.Errorf("GetLeftSibling() = %d, want 111", got)
	}
	if got := p.GetRightSibling(); got != 222 {
		t.Errorf("GetRightSibling() = %d, want 222", got)
	}
}

func TestInternalHeaderFields_DoNotOverlapPageLSN(t *testing.T) {
	p := NewPage(PageTypeInternal, 5)
	p.setPageLSN(0xCAFEBABEDEADC0DE)
	p.setRightMostChild(999)
	p.setLevel(4)

	if got := p.GetPageLSN(); got != 0xCAFEBABEDEADC0DE {
		t.Errorf("GetPageLSN() = %#x after setting internal fields, want 0xCAFEBABEDEADC0DE", got)
	}
	if got := p.GetRightMostChild(); got != 999 {
		t.Errorf("GetRightMostChild() = %d, want 999", got)
	}
	if got := p.GetLevel(); got != 4 {
		t.Errorf("GetLevel() = %d, want 4", got)
	}
}

func TestCommonHeaderFields_DoNotOverlapEachOther(t *testing.T) {
	p := &Page{}
	p.setPageType(PageTypeLeaf)
	p.setPageId(0xABCD1234)
	p.setFreeSpaceStart(100)
	p.setFreeSpaceEnd(3000)
	p.setRowCount(7)
	p.setPageLSN(0x1122334455667788)

	if got := p.GetPageType(); got != PageTypeLeaf {
		t.Errorf("GetPageType() = %d", got)
	}
	if got := p.GetPageId(); got != 0xABCD1234 {
		t.Errorf("GetPageId() = %#x", got)
	}
	if got := p.GetFreeSpaceStart(); got != 100 {
		t.Errorf("GetFreeSpaceStart() = %d", got)
	}
	if got := p.GetFreeSpaceEnd(); got != 3000 {
		t.Errorf("GetFreeSpaceEnd() = %d", got)
	}
	if got := p.GetRowCount(); got != 7 {
		t.Errorf("GetRowCount() = %d", got)
	}
	if got := p.GetPageLSN(); got != 0x1122334455667788 {
		t.Errorf("GetPageLSN() = %#x", got)
	}
}

// ============================================================
// Scenario-specific regression tests
// ============================================================

// Scenario 1: leaf page with three records — sibling pointers must survive all inserts.
func TestLeafPage_ThreeInserts_SiblingsIntact(t *testing.T) {
	p := NewPage(PageTypeLeaf, 1)
	p.setLeftSibling(111)
	p.setRightSibling(222)

	records := [][]byte{
		[]byte("record one"),
		[]byte("record two"),
		[]byte("record three"),
	}
	for i, r := range records {
		idx, ok := p.InsertRecord(r)
		if !ok {
			t.Fatalf("InsertRecord[%d] failed", i)
		}
		got, ok := p.GetRecord(idx)
		if !ok || !bytes.Equal(got, r) {
			t.Fatalf("GetRecord[%d] wrong after insert: ok=%v data=%q", i, ok, got)
		}
		// Check siblings after every insert, not just the last.
		if left := p.GetLeftSibling(); left != 111 {
			t.Errorf("after insert %d: GetLeftSibling() = %d, want 111", i, left)
		}
		if right := p.GetRightSibling(); right != 222 {
			t.Errorf("after insert %d: GetRightSibling() = %d, want 222", i, right)
		}
	}
}

// Scenario 2: fill the page one record at a time; CanAccommodate must flip to false
// at exactly the same step that InsertRecord starts returning false.
func TestCanAccommodate_BoundaryMatchesInsertRecord(t *testing.T) {
	p := newTestPage(t, 1)
	record := make([]byte, 50)
	recordCost := len(record) + 4 // slot entry (4 bytes) + data

	for {
		canFit := p.CanAccommodate(recordCost)
		_, inserted := p.InsertRecord(record)

		if canFit != inserted {
			t.Errorf("CanAccommodate(%d)=%v but InsertRecord returned ok=%v — they disagree at rowCount=%d",
				recordCost, canFit, inserted, p.GetRowCount())
		}
		if !inserted {
			break
		}
	}

	// One final check: after the page is full both should be false.
	if p.CanAccommodate(recordCost) {
		t.Error("CanAccommodate should be false after page is full")
	}
}

// ============================================================
// NewLeafPage constructor
// ============================================================

func TestNewLeafPage_PageType(t *testing.T) {
	p := NewLeafPage(5, 1, 2)
	if got := p.GetPageType(); got != PageTypeLeaf {
		t.Errorf("GetPageType() = %d, want %d", got, PageTypeLeaf)
	}
}

func TestNewLeafPage_PageID(t *testing.T) {
	p := NewLeafPage(42, 0, 0)
	if got := p.GetPageId(); got != 42 {
		t.Errorf("GetPageId() = %d, want 42", got)
	}
}

func TestNewLeafPage_LeftSibling(t *testing.T) {
	p := NewLeafPage(1, 99, 0)
	if got := p.GetLeftSibling(); got != 99 {
		t.Errorf("GetLeftSibling() = %d, want 99", got)
	}
}

func TestNewLeafPage_RightSibling(t *testing.T) {
	p := NewLeafPage(1, 0, 77)
	if got := p.GetRightSibling(); got != 77 {
		t.Errorf("GetRightSibling() = %d, want 77", got)
	}
}

func TestNewLeafPage_InvalidSiblings(t *testing.T) {
	p := NewLeafPage(3, InvalidPageID, InvalidPageID)
	if got := p.GetLeftSibling(); got != InvalidPageID {
		t.Errorf("GetLeftSibling() = %#x, want InvalidPageID", got)
	}
	if got := p.GetRightSibling(); got != InvalidPageID {
		t.Errorf("GetRightSibling() = %#x, want InvalidPageID", got)
	}
}

func TestNewLeafPage_CommonHeaderDefaults(t *testing.T) {
	p := NewLeafPage(7, 10, 20)
	assertPageInit(t, p, PageTypeLeaf, 7, LeafHeaderSize)
}

func TestNewLeafPage_SiblingsIndependentOfEachOther(t *testing.T) {
	p := NewLeafPage(1, 100, 200)
	if got := p.GetLeftSibling(); got != 100 {
		t.Errorf("GetLeftSibling() = %d, want 100", got)
	}
	if got := p.GetRightSibling(); got != 200 {
		t.Errorf("GetRightSibling() = %d, want 200", got)
	}
}

// ============================================================
// NewInternalPage constructor
// ============================================================

func TestNewInternalPage_PageType(t *testing.T) {
	p := NewInternalPage(3, 1, 5)
	if got := p.GetPageType(); got != PageTypeInternal {
		t.Errorf("GetPageType() = %d, want %d", got, PageTypeInternal)
	}
}

func TestNewInternalPage_PageID(t *testing.T) {
	p := NewInternalPage(88, 0, 0)
	if got := p.GetPageId(); got != 88 {
		t.Errorf("GetPageId() = %d, want 88", got)
	}
}

func TestNewInternalPage_Level(t *testing.T) {
	p := NewInternalPage(1, 3, 0)
	if got := p.GetLevel(); got != 3 {
		t.Errorf("GetLevel() = %d, want 3", got)
	}
}

func TestNewInternalPage_RightmostChild(t *testing.T) {
	p := NewInternalPage(1, 0, 42)
	if got := p.GetRightMostChild(); got != 42 {
		t.Errorf("GetRightMostChild() = %d, want 42", got)
	}
}

func TestNewInternalPage_InvalidRightmostChild(t *testing.T) {
	p := NewInternalPage(1, 0, InvalidPageID)
	if got := p.GetRightMostChild(); got != InvalidPageID {
		t.Errorf("GetRightMostChild() = %#x, want InvalidPageID", got)
	}
}

func TestNewInternalPage_CommonHeaderDefaults(t *testing.T) {
	p := NewInternalPage(9, 2, 50)
	assertPageInit(t, p, PageTypeInternal, 9, InternalHeaderSize)
}

func TestNewInternalPage_LevelAndChildIndependent(t *testing.T) {
	p := NewInternalPage(1, 5, 123)
	if got := p.GetLevel(); got != 5 {
		t.Errorf("GetLevel() = %d, want 5", got)
	}
	if got := p.GetRightMostChild(); got != 123 {
		t.Errorf("GetRightMostChild() = %d, want 123", got)
	}
}

// ============================================================
// NewMetaPage constructor
// ============================================================

func TestNewMetaPage_PageType(t *testing.T) {
	p := NewMetaPage()
	if got := p.GetPageType(); got != PageTypeMeta {
		t.Errorf("GetPageType() = %d, want %d", got, PageTypeMeta)
	}
}

func TestNewMetaPage_PageID(t *testing.T) {
	p := NewMetaPage()
	if got := p.GetPageId(); got != 0 {
		t.Errorf("GetPageId() = %d, want 0", got)
	}
}

func TestNewMetaPage_MagicNumber(t *testing.T) {
	p := NewMetaPage()
	const wantMagic uint32 = 0x54455354
	if got := p.GetMetaPageMagicNumber(); got != wantMagic {
		t.Errorf("GetMetaPageMagicNumber() = %#x, want %#x", got, wantMagic)
	}
}

func TestNewMetaPage_Version(t *testing.T) {
	p := NewMetaPage()
	if got := p.GetMetaPageVersion(); got != 1 {
		t.Errorf("GetMetaPageVersion() = %d, want 1", got)
	}
}

func TestNewMetaPage_PageCount(t *testing.T) {
	p := NewMetaPage()
	if got := p.GetMetaPageCount(); got != 1 {
		t.Errorf("GetMetaPageCount() = %d, want 1", got)
	}
}

func TestNewMetaPage_FreeList(t *testing.T) {
	p := NewMetaPage()
	if got := p.GetMetaFreeList(); got != InvalidPageID {
		t.Errorf("GetMetaFreeList() = %#x, want InvalidPageID", got)
	}
}

func TestNewMetaPage_RootPage(t *testing.T) {
	p := NewMetaPage()
	if got := p.GetMetaRootPage(); got != InvalidPageID {
		t.Errorf("GetMetaRootPage() = %#x, want InvalidPageID", got)
	}
}

func TestNewMetaPage_Checkpoint(t *testing.T) {
	p := NewMetaPage()
	if got := p.GetMetaCheckpoint(); got != 0 {
		t.Errorf("GetMetaCheckpoint() = %d, want 0", got)
	}
}

func TestNewMetaPage_CommonHeaderDefaults(t *testing.T) {
	p := NewMetaPage()
	assertPageInit(t, p, PageTypeMeta, 0, CommonHeaderSize)
}

// ============================================================
// Meta-page accessor round-trips
// ============================================================

func TestMetaVersion_RoundTrip(t *testing.T) {
	p := NewMetaPage()
	for _, v := range []uint16{0, 1, 2, 0xFFFF} {
		p.setMetaPageVersion(v)
		if got := p.GetMetaPageVersion(); got != v {
			t.Errorf("setMetaPageVersion(%d) → GetMetaPageVersion() = %d", v, got)
		}
	}
}

func TestMetaPageCount_RoundTrip(t *testing.T) {
	p := NewMetaPage()
	for _, v := range []uint32{0, 1, 100, 0xFFFFFFFE, InvalidPageID} {
		p.setMetaPageCount(v)
		if got := p.GetMetaPageCount(); got != v {
			t.Errorf("setMetaPageCount(%d) → GetMetaPageCount() = %d", v, got)
		}
	}
}

func TestMetaFreeList_RoundTrip(t *testing.T) {
	p := NewMetaPage()
	for _, v := range []uint32{0, 1, 50, InvalidPageID} {
		p.setMetaFreeList(v)
		if got := p.GetMetaFreeList(); got != v {
			t.Errorf("setMetaFreeList(%d) → GetMetaFreeList() = %d", v, got)
		}
	}
}

func TestMetaRootPage_RoundTrip(t *testing.T) {
	p := NewMetaPage()
	for _, v := range []uint32{0, 1, 99, InvalidPageID} {
		p.setMetaRootPage(v)
		if got := p.GetMetaRootPage(); got != v {
			t.Errorf("setMetaRootPage(%d) → GetMetaRootPage() = %d", v, got)
		}
	}
}

func TestMetaCheckpoint_RoundTrip(t *testing.T) {
	p := NewMetaPage()
	for _, lsn := range []uint64{0, 1, 0xDEADBEEFCAFEBABE, 0xFFFFFFFFFFFFFFFF} {
		p.setMetaCheckpoint(lsn)
		if got := p.GetMetaCheckpoint(); got != lsn {
			t.Errorf("setMetaCheckpoint(%#x) → GetMetaCheckpoint() = %#x", lsn, got)
		}
	}
}

func TestMetaFields_AllIndependent(t *testing.T) {
	p := NewMetaPage()
	p.setMetaPageVersion(7)
	p.setMetaPageCount(333)
	p.setMetaFreeList(10)
	p.setMetaRootPage(20)
	p.setMetaCheckpoint(0xABCDEF0123456789)

	if got := p.GetMetaPageVersion(); got != 7 {
		t.Errorf("GetMetaPageVersion() = %d, want 7", got)
	}
	if got := p.GetMetaPageCount(); got != 333 {
		t.Errorf("GetMetaPageCount() = %d, want 333", got)
	}
	if got := p.GetMetaFreeList(); got != 10 {
		t.Errorf("GetMetaFreeList() = %d, want 10", got)
	}
	if got := p.GetMetaRootPage(); got != 20 {
		t.Errorf("GetMetaRootPage() = %d, want 20", got)
	}
	if got := p.GetMetaCheckpoint(); got != 0xABCDEF0123456789 {
		t.Errorf("GetMetaCheckpoint() = %#x, want 0xABCDEF0123456789", got)
	}
}

func TestMetaFields_DoNotCorruptMagic(t *testing.T) {
	p := NewMetaPage()
	p.setMetaPageVersion(2)
	p.setMetaPageCount(500)
	p.setMetaFreeList(3)
	p.setMetaRootPage(4)
	p.setMetaCheckpoint(12345)

	const wantMagic uint32 = 0x54455354
	if got := p.GetMetaPageMagicNumber(); got != wantMagic {
		t.Errorf("magic corrupted after field writes: got %#x, want %#x", got, wantMagic)
	}
}

func TestMetaFields_DoNotCorruptCommonHeader(t *testing.T) {
	p := NewMetaPage()
	p.setPageLSN(0xFEEDFACEDEADBEEF)
	p.setMetaPageCount(999)
	p.setMetaRootPage(7)

	if got := p.GetPageLSN(); got != 0xFEEDFACEDEADBEEF {
		t.Errorf("GetPageLSN() corrupted by meta writes: %#x", got)
	}
	if got := p.GetPageType(); got != PageTypeMeta {
		t.Errorf("GetPageType() corrupted: %d", got)
	}
}

// ============================================================
// Scenario-specific regression tests
// ============================================================

// Scenario 3: insert → delete → insert → compact; all live records readable
// and the slot directory is contiguous (no gaps) after compaction.
func TestCompactPage_InsertDeleteInsert_SlotDirectoryContiguous(t *testing.T) {
	p := newTestPage(t, 1)

	// Phase 1: insert four records.
	origRecords := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
		[]byte("date"),
	}
	for _, r := range origRecords {
		_, ok := p.InsertRecord(r)
		if !ok {
			t.Fatal("initial InsertRecord failed")
		}
	}

	// Phase 2: delete slots 1 and 2 to create holes.
	p.DeleteRecord(1)
	p.DeleteRecord(2)

	// Phase 3: insert a new record into the fragmented page (before compaction).
	newRecord := []byte("elderberry")
	_, ok := p.InsertRecord(newRecord)
	if !ok {
		t.Fatal("InsertRecord after delete failed")
	}

	// Phase 4: compact.
	p.CompactPage()

	// Expected survivors in order: apple, date, elderberry.
	want := [][]byte{
		[]byte("apple"),
		[]byte("date"),
		[]byte("elderberry"),
	}
	rowCount := int(p.GetRowCount())
	if rowCount != len(want) {
		t.Fatalf("RowCount = %d after compact, want %d", rowCount, len(want))
	}
	for i, w := range want {
		got, ok := p.GetRecord(i)
		if !ok {
			t.Fatalf("GetRecord(%d) returned false after compact", i)
		}
		if !bytes.Equal(got, w) {
			t.Errorf("Record[%d] = %q, want %q", i, got, w)
		}
	}

	// Slot directory must be contiguous: slots 0..rowCount-1 all used,
	// and freeSpaceStart must equal headerSize + rowCount*4.
	for i := 0; i < rowCount; i++ {
		_, used := p.GetSlotOffset(i)
		if !used {
			t.Errorf("Slot %d is unused — slot directory has a gap after compaction", i)
		}
	}
	expectedStart := uint16(p.headerSize() + rowCount*4)
	if got := p.GetFreeSpaceStart(); got != expectedStart {
		t.Errorf("FreeSpaceStart = %d after compact, want %d (headerSize + rowCount*4)", got, expectedStart)
	}
}
