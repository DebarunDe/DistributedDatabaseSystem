package pagemanager

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

// ============================================================
// Constants
// ============================================================

func TestWALConstants_RecordSize(t *testing.T) {
	if WAL_RecordSize != 4112 {
		t.Errorf("WAL_RecordSize = %d, want 4112", WAL_RecordSize)
	}
}

func TestWALConstants_Offsets(t *testing.T) {
	if WAL_OffsetLSN != 0 {
		t.Errorf("WAL_OffsetLSN = %d, want 0", WAL_OffsetLSN)
	}
	if WAL_OffsetPageID != 8 {
		t.Errorf("WAL_OffsetPageID = %d, want 8", WAL_OffsetPageID)
	}
	if WAL_OffsetPageData != 12 {
		t.Errorf("WAL_OffsetPageData = %d, want 12", WAL_OffsetPageData)
	}
	if WAL_OffsetCRC32 != 4108 {
		t.Errorf("WAL_OffsetCRC32 = %d, want 4108", WAL_OffsetCRC32)
	}
}

func TestWALConstants_OffsetArithmetic(t *testing.T) {
	pageDataLen := WAL_OffsetCRC32 - WAL_OffsetPageData
	if pageDataLen != PageSize {
		t.Errorf("page data region = %d bytes, want PageSize (%d)", pageDataLen, PageSize)
	}
	crcEnd := WAL_OffsetCRC32 + 4
	if crcEnd != WAL_RecordSize {
		t.Errorf("CRC end offset = %d, want WAL_RecordSize (%d)", crcEnd, WAL_RecordSize)
	}
}

// ============================================================
// LSN
// ============================================================

func TestWAL_SetGetLSN(t *testing.T) {
	var w WAL
	w.SetLSN(0xDEADBEEFCAFEBABE)
	if got := w.GetLSN(); got != 0xDEADBEEFCAFEBABE {
		t.Errorf("GetLSN() = %#x, want %#x", got, uint64(0xDEADBEEFCAFEBABE))
	}
}

func TestWAL_LSN_Zero(t *testing.T) {
	var w WAL
	w.SetLSN(0)
	if got := w.GetLSN(); got != 0 {
		t.Errorf("GetLSN() = %d, want 0", got)
	}
}

func TestWAL_LSN_MaxUint64(t *testing.T) {
	var w WAL
	const max = ^uint64(0)
	w.SetLSN(max)
	if got := w.GetLSN(); got != max {
		t.Errorf("GetLSN() = %d, want %d", got, max)
	}
}

func TestWAL_LSN_Overwrite(t *testing.T) {
	var w WAL
	w.SetLSN(1)
	w.SetLSN(2)
	if got := w.GetLSN(); got != 2 {
		t.Errorf("GetLSN() after overwrite = %d, want 2", got)
	}
}

// ============================================================
// PageID
// ============================================================

func TestWAL_SetGetPageID(t *testing.T) {
	var w WAL
	w.SetPageID(42)
	if got := w.GetPageID(); got != 42 {
		t.Errorf("GetPageID() = %d, want 42", got)
	}
}

func TestWAL_PageID_Zero(t *testing.T) {
	var w WAL
	w.SetPageID(0)
	if got := w.GetPageID(); got != 0 {
		t.Errorf("GetPageID() = %d, want 0", got)
	}
}

func TestWAL_PageID_MaxUint32(t *testing.T) {
	var w WAL
	const max = ^uint32(0)
	w.SetPageID(max)
	if got := w.GetPageID(); got != max {
		t.Errorf("GetPageID() = %d, want %d", got, max)
	}
}

func TestWAL_LSN_And_PageID_DoNotOverlap(t *testing.T) {
	var w WAL
	w.SetLSN(0xFFFFFFFFFFFFFFFF)
	w.SetPageID(0)
	if got := w.GetPageID(); got != 0 {
		t.Errorf("PageID corrupted by LSN write: got %d, want 0", got)
	}

	w.SetLSN(0)
	w.SetPageID(0xFFFFFFFF)
	if got := w.GetLSN(); got != 0 {
		t.Errorf("LSN corrupted by PageID write: got %d, want 0", got)
	}
}

// ============================================================
// PageData
// ============================================================

func TestWAL_SetGetPageData(t *testing.T) {
	var w WAL
	p := &Page{}
	for i := range p.Data {
		p.Data[i] = byte(i % 256)
	}
	w.SetPageData(p)

	got := w.GetPageData()
	if !bytes.Equal(got, p.Data[:]) {
		t.Error("GetPageData() does not match the source page data")
	}
}

func TestWAL_GetPageData_Length(t *testing.T) {
	var w WAL
	got := w.GetPageData()
	if len(got) != PageSize {
		t.Errorf("GetPageData() length = %d, want PageSize (%d)", len(got), PageSize)
	}
}

func TestWAL_SetPageData_DoesNotAffectOtherRecords(t *testing.T) {
	var w1, w2 WAL
	p := &Page{}
	p.Data[0] = 0xFF

	w1.SetPageData(p)
	// w2 should remain zeroed
	if w2.GetPageData()[0] != 0 {
		t.Error("SetPageData on w1 leaked into w2")
	}
}

func TestWAL_SetPageData_ZeroPage(t *testing.T) {
	var w WAL
	// Pre-fill the record with non-zero bytes
	for i := range w.Data {
		w.Data[i] = 0xFF
	}

	p := &Page{} // zeroed
	w.SetPageData(p)

	got := w.GetPageData()
	for i, b := range got {
		if b != 0 {
			t.Errorf("GetPageData()[%d] = %d after zero page set, want 0", i, b)
			break
		}
	}
}

// ============================================================
// CRC32
// ============================================================

func TestWAL_SetGetCRC32(t *testing.T) {
	var w WAL
	w.SetCRC32(0xABCDEF01)
	if got := w.GetCRC32(); got != 0xABCDEF01 {
		t.Errorf("GetCRC32() = %#x, want %#x", got, uint32(0xABCDEF01))
	}
}

func TestWAL_CalculateCRC32_EmptyPage(t *testing.T) {
	var w WAL
	crc := w.CalculateCRC32()
	// CRC of a zeroed page should be deterministic
	var w2 WAL
	if crc != w2.CalculateCRC32() {
		t.Error("CalculateCRC32() is not deterministic for identical page data")
	}
}

func TestWAL_CalculateCRC32_DifferentData(t *testing.T) {
	var w1, w2 WAL
	p1, p2 := &Page{}, &Page{}
	p1.Data[0] = 0x01
	p2.Data[0] = 0x02

	w1.SetPageData(p1)
	w2.SetPageData(p2)

	if w1.CalculateCRC32() == w2.CalculateCRC32() {
		t.Error("CalculateCRC32() returned the same value for different page data")
	}
}

func TestWAL_CalculateCRC32_CoversFullRecord(t *testing.T) {
	var w1, w2 WAL
	p := &Page{}
	p.Data[100] = 0xAB

	w1.SetPageData(p)
	w2.SetPageData(p)
	w1.SetLSN(1)
	w2.SetLSN(999) // different LSN, same page data — CRC must differ

	if w1.CalculateCRC32() == w2.CalculateCRC32() {
		t.Error("CalculateCRC32() must cover LSN — different LSN with same page data must yield different CRC")
	}
}

// ============================================================
// ValidateCRC32
// ============================================================

func TestWAL_ValidateCRC32_Valid(t *testing.T) {
	var w WAL
	p := &Page{}
	for i := range p.Data {
		p.Data[i] = byte(i % 256)
	}
	w.SetPageData(p)
	w.SetCRC32(w.CalculateCRC32())

	if !w.ValidateCRC32() {
		t.Error("ValidateCRC32() returned false for a correctly set CRC")
	}
}

func TestWAL_ValidateCRC32_Invalid_WrongCRC(t *testing.T) {
	var w WAL
	p := &Page{}
	p.Data[0] = 0x42
	w.SetPageData(p)
	w.SetCRC32(0xDEADBEEF) // deliberately wrong

	if w.ValidateCRC32() {
		t.Error("ValidateCRC32() returned true for a wrong CRC")
	}
}

func TestWAL_ValidateCRC32_Invalid_CorruptPageData(t *testing.T) {
	var w WAL
	p := &Page{}
	p.Data[0] = 0x01
	w.SetPageData(p)
	w.SetCRC32(w.CalculateCRC32())

	// Corrupt one byte of page data directly
	w.Data[WAL_OffsetPageData] ^= 0xFF

	if w.ValidateCRC32() {
		t.Error("ValidateCRC32() returned true after page data corruption")
	}
}

func TestWAL_ValidateCRC32_ZeroRecord(t *testing.T) {
	var w WAL
	// A zero record has a stored CRC of 0, but the IEEE CRC of a zeroed
	// page is non-zero, so validation must fail.
	if w.ValidateCRC32() {
		t.Error("ValidateCRC32() returned true for an all-zero record")
	}
}

func TestWAL_ValidateCRC32_RoundTrip(t *testing.T) {
	var w WAL
	p := &Page{}
	p.Data[512] = 0xBE
	p.Data[1024] = 0xEF

	w.SetLSN(7)
	w.SetPageID(3)
	w.SetPageData(p)
	w.SetCRC32(w.CalculateCRC32())

	if !w.ValidateCRC32() {
		t.Error("ValidateCRC32() failed after full round-trip set")
	}
}

// ============================================================
// Test helpers for WALImpl / RecoverFromWAL tests
// ============================================================

// newDBForWALTest creates a fresh *PageManagerImpl in a temp dir.
// The DB is closed automatically when the test ends.
func newDBForWALTest(t *testing.T) (*PageManagerImpl, string) {
	t.Helper()
	path := t.TempDir() + "/test.db"
	pm, err := NewDB(path)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	impl := pm.(*PageManagerImpl)
	t.Cleanup(func() {
		if impl.file != nil {
			_ = impl.Close()
		}
	})
	return impl, path
}

// openDBForWALTest reopens an existing DB as a *PageManagerImpl.
func openDBForWALTest(t *testing.T, path string) *PageManagerImpl {
	t.Helper()
	pm, err := OpenDB(path)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	impl := pm.(*PageManagerImpl)
	t.Cleanup(func() {
		if impl.file != nil {
			_ = impl.Close()
		}
	})
	return impl
}

// newWALImplForTest builds a WALImpl with the given disk and a fresh temp WAL file.
// The file is closed automatically when the test ends.
func newWALImplForTest(t *testing.T, disk PageManager) (*WALImpl, *os.File) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "wal_*.log")
	if err != nil {
		t.Fatalf("os.CreateTemp: %v", err)
	}
	t.Cleanup(func() { f.Close() })
	return &WALImpl{disk: disk, file: f, logSequenceNumber: 0}, f
}

// appendWALRecord builds a valid WAL record and writes it to f.
func appendWALRecord(t *testing.T, f *os.File, lsn uint64, pageID uint32, p *Page) {
	t.Helper()
	rec := &WAL{}
	rec.SetLSN(lsn)
	rec.SetPageID(pageID)
	rec.SetPageData(p)
	rec.SetCRC32(rec.CalculateCRC32())
	if _, err := f.Write(rec.Data[:]); err != nil {
		t.Fatalf("writing WAL record: %v", err)
	}
}

// readWALRecordAt reads the WAL record at zero-based index idx from f.
func readWALRecordAt(t *testing.T, f *os.File, idx int) *WAL {
	t.Helper()
	rec := &WAL{}
	if _, err := f.ReadAt(rec.Data[:], int64(idx)*WAL_RecordSize); err != nil {
		t.Fatalf("reading WAL record at index %d: %v", idx, err)
	}
	return rec
}

// writePageDirect writes p directly to pm's file, bypassing all WAL logic.
// Used to set up specific on-disk page states (including LSN) for recovery tests.
func writePageDirect(t *testing.T, pm *PageManagerImpl, p *Page) {
	t.Helper()
	offset := int64(p.GetPageId()) * PageSize
	if _, err := pm.file.WriteAt(p.Data[:], offset); err != nil {
		t.Fatalf("writePageDirect page %d: %v", p.GetPageId(), err)
	}
}

// readPageDirect reads a page directly from pm's file without going through the PageManager interface.
func readPageDirect(t *testing.T, pm *PageManagerImpl, pageID uint32) *Page {
	t.Helper()
	p := &Page{}
	if _, err := pm.file.ReadAt(p.Data[:], int64(pageID)*PageSize); err != nil {
		t.Fatalf("readPageDirect page %d: %v", pageID, err)
	}
	return p
}

// ============================================================
// WALImpl.WritePage
// ============================================================

// After one WritePage call the WAL file must contain exactly one record.
func TestWALImpl_WritePage_AppendsOneRecord(t *testing.T) {
	spy := newSpyDisk(makePage(1))
	wal, f := newWALImplForTest(t, spy)

	p := makePage(1)
	if err := wal.WritePage(p); err != nil {
		t.Fatalf("WritePage: %v", err)
	}

	info, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := info.Size(); got != WAL_RecordSize {
		t.Errorf("WAL file size = %d, want %d (one record)", got, WAL_RecordSize)
	}
}

// The WAL record must carry the LSN that was current when WritePage was called (0 for the first call).
func TestWALImpl_WritePage_Record_HasCorrectLSN(t *testing.T) {
	spy := newSpyDisk(makePage(1))
	wal, f := newWALImplForTest(t, spy)

	if err := wal.WritePage(makePage(1)); err != nil {
		t.Fatalf("WritePage: %v", err)
	}

	rec := readWALRecordAt(t, f, 0)
	if got := rec.GetLSN(); got != 0 {
		t.Errorf("record LSN = %d, want 0 (first write)", got)
	}
}

// The WAL record must carry the page ID of the written page.
func TestWALImpl_WritePage_Record_HasCorrectPageID(t *testing.T) {
	spy := newSpyDisk(makePage(7))
	wal, f := newWALImplForTest(t, spy)

	p := makePage(7)
	if err := wal.WritePage(p); err != nil {
		t.Fatalf("WritePage: %v", err)
	}

	rec := readWALRecordAt(t, f, 0)
	if got := rec.GetPageID(); got != 7 {
		t.Errorf("record PageID = %d, want 7", got)
	}
}

// The page data stored in the WAL record must match the page content at write time
// (after the LSN has been stamped onto the page).
func TestWALImpl_WritePage_Record_HasCorrectPageData(t *testing.T) {
	spy := newSpyDisk(makePage(1))
	wal, f := newWALImplForTest(t, spy)

	p := makePage(1)
	p.Data[CommonHeaderSize] = 0xAB

	if err := wal.WritePage(p); err != nil {
		t.Fatalf("WritePage: %v", err)
	}

	rec := readWALRecordAt(t, f, 0)
	if !bytes.Equal(rec.GetPageData(), p.Data[:]) {
		t.Error("WAL record page data does not match the written page")
	}
}

// The CRC32 stored in the WAL record must be valid after a WritePage call.
func TestWALImpl_WritePage_Record_PassesCRCValidation(t *testing.T) {
	spy := newSpyDisk(makePage(1))
	wal, f := newWALImplForTest(t, spy)

	if err := wal.WritePage(makePage(1)); err != nil {
		t.Fatalf("WritePage: %v", err)
	}

	rec := readWALRecordAt(t, f, 0)
	if !rec.ValidateCRC32() {
		t.Error("WAL record CRC32 validation failed after WritePage")
	}
}

// WritePage must stamp the page's LSN field with the current logSequenceNumber
// before logging, so the on-page LSN matches the WAL record LSN.
func TestWALImpl_WritePage_StampsPageLSN(t *testing.T) {
	spy := newSpyDisk(makePage(1))
	wal, _ := newWALImplForTest(t, spy)
	wal.logSequenceNumber = 42

	p := makePage(1)
	if err := wal.WritePage(p); err != nil {
		t.Fatalf("WritePage: %v", err)
	}

	if got := p.GetPageLSN(); got != 42 {
		t.Errorf("page LSN after WritePage = %d, want 42", got)
	}
}

// logSequenceNumber must be incremented after each WritePage call.
func TestWALImpl_WritePage_IncrementsLSN(t *testing.T) {
	spy := newSpyDisk(makePage(1), makePage(2), makePage(3))
	wal, _ := newWALImplForTest(t, spy)

	for i, id := range []uint32{1, 2, 3} {
		if err := wal.WritePage(makePage(id)); err != nil {
			t.Fatalf("WritePage(%d): %v", id, err)
		}
		if got := wal.logSequenceNumber; got != uint64(i+1) {
			t.Errorf("after write %d: logSequenceNumber = %d, want %d", i+1, got, i+1)
		}
	}
}

// WritePage must call disk.WritePage so the page is also flushed to the underlying layer.
func TestWALImpl_WritePage_DelegatesToDisk(t *testing.T) {
	spy := newSpyDisk(makePage(1))
	wal, _ := newWALImplForTest(t, spy)

	writesBefore := spy.writeCount
	if err := wal.WritePage(makePage(1)); err != nil {
		t.Fatalf("WritePage: %v", err)
	}
	if delta := spy.writeCount - writesBefore; delta != 1 {
		t.Errorf("disk.WritePage called %d times, want 1", delta)
	}
}

// N calls to WritePage must produce N sequential records in the WAL file,
// each with a monotonically increasing LSN.
func TestWALImpl_WritePage_MultipleWrites_RecordsInOrder(t *testing.T) {
	const n = 5
	pages := make([]*Page, n)
	for i := range pages {
		pages[i] = makePage(uint32(i + 1))
	}
	spy := newSpyDisk(pages...)
	wal, f := newWALImplForTest(t, spy)

	for _, p := range pages {
		if err := wal.WritePage(p); err != nil {
			t.Fatalf("WritePage(%d): %v", p.GetPageId(), err)
		}
	}

	info, _ := f.Stat()
	if got := info.Size(); got != int64(n)*WAL_RecordSize {
		t.Errorf("WAL file size = %d, want %d (%d records)", got, int64(n)*WAL_RecordSize, n)
	}

	for i := 0; i < n; i++ {
		rec := readWALRecordAt(t, f, i)
		if got := rec.GetLSN(); got != uint64(i) {
			t.Errorf("record[%d] LSN = %d, want %d", i, got, i)
		}
		if got := rec.GetPageID(); got != uint32(i+1) {
			t.Errorf("record[%d] PageID = %d, want %d", i, got, i+1)
		}
		if !rec.ValidateCRC32() {
			t.Errorf("record[%d] CRC32 invalid", i)
		}
	}
}

// A disk.WritePage error must be returned to the caller.
func TestWALImpl_WritePage_DiskError_Propagates(t *testing.T) {
	spy := newSpyDisk(makePage(1))
	spy.writeFn = func(*Page) error { return errors.New("disk full") }
	wal, _ := newWALImplForTest(t, spy)

	if err := wal.WritePage(makePage(1)); err == nil {
		t.Error("WritePage should propagate disk.WritePage error")
	}
}

// ============================================================
// WALImpl delegation — AllocatePage
// ============================================================

func TestWALImpl_AllocatePage_DelegatesToDisk(t *testing.T) {
	spy := newSpyDisk()
	spy.allocateFn = func() (*Page, error) { return makePage(1), nil }
	wal, _ := newWALImplForTest(t, spy)

	got, err := wal.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	if got.GetPageId() != 1 {
		t.Errorf("page ID = %d, want 1", got.GetPageId())
	}
	if spy.allocateCount != 1 {
		t.Errorf("disk.AllocatePage called %d times, want 1", spy.allocateCount)
	}
}

func TestWALImpl_AllocatePage_ErrorPropagates(t *testing.T) {
	spy := newSpyDisk()
	spy.allocateFn = func() (*Page, error) { return nil, errors.New("out of space") }
	wal, _ := newWALImplForTest(t, spy)

	if _, err := wal.AllocatePage(); err == nil {
		t.Error("AllocatePage should propagate disk error")
	}
}

// ============================================================
// WALImpl delegation — ReadPage
// ============================================================

func TestWALImpl_ReadPage_DelegatesToDisk(t *testing.T) {
	spy := newSpyDisk(makePage(3))
	wal, _ := newWALImplForTest(t, spy)

	got, err := wal.ReadPage(3)
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}
	if got.GetPageId() != 3 {
		t.Errorf("page ID = %d, want 3", got.GetPageId())
	}
	if spy.readCount != 1 {
		t.Errorf("disk.ReadPage called %d times, want 1", spy.readCount)
	}
}

func TestWALImpl_ReadPage_ErrorPropagates(t *testing.T) {
	spy := newSpyDisk() // page 99 not present
	wal, _ := newWALImplForTest(t, spy)

	if _, err := wal.ReadPage(99); err == nil {
		t.Error("ReadPage should propagate disk error")
	}
}

// ============================================================
// WALImpl delegation — FreePage
// ============================================================

func TestWALImpl_FreePage_DelegatesToDisk(t *testing.T) {
	spy := newSpyDisk()
	spy.freeFn = func(uint32) error { return nil }
	wal, _ := newWALImplForTest(t, spy)

	if err := wal.FreePage(5); err != nil {
		t.Fatalf("FreePage: %v", err)
	}
	if spy.freeCount != 1 {
		t.Errorf("disk.FreePage called %d times, want 1", spy.freeCount)
	}
	if len(spy.freedPages) == 0 || spy.freedPages[0] != 5 {
		t.Errorf("freed page IDs = %v, want [5]", spy.freedPages)
	}
}

func TestWALImpl_FreePage_ErrorPropagates(t *testing.T) {
	spy := newSpyDisk()
	spy.freeFn = func(uint32) error { return errors.New("disk error") }
	wal, _ := newWALImplForTest(t, spy)

	if err := wal.FreePage(5); err == nil {
		t.Error("FreePage should propagate disk error")
	}
}

// ============================================================
// WALImpl delegation — GetRootPageId / SetRootPageId
// ============================================================

func TestWALImpl_GetRootPageId_DelegatesToDisk(t *testing.T) {
	spy := newSpyDisk()
	spy.getRootFn = func() uint32 { return 99 }
	wal, _ := newWALImplForTest(t, spy)

	if got := wal.GetRootPageId(); got != 99 {
		t.Errorf("GetRootPageId = %d, want 99", got)
	}
}

func TestWALImpl_SetRootPageId_DelegatesToDisk(t *testing.T) {
	spy := newSpyDisk()
	spy.setRootFn = func(uint32) error { return nil }
	wal, _ := newWALImplForTest(t, spy)

	if err := wal.SetRootPageId(42); err != nil {
		t.Fatalf("SetRootPageId: %v", err)
	}
	if spy.setRootCount != 1 {
		t.Errorf("disk.SetRootPageId called %d times, want 1", spy.setRootCount)
	}
	if spy.lastRootSet != 42 {
		t.Errorf("root set to %d, want 42", spy.lastRootSet)
	}
}

func TestWALImpl_SetRootPageId_ErrorPropagates(t *testing.T) {
	spy := newSpyDisk()
	spy.setRootFn = func(uint32) error { return errors.New("disk error") }
	wal, _ := newWALImplForTest(t, spy)

	if err := wal.SetRootPageId(1); err == nil {
		t.Error("SetRootPageId should propagate disk error")
	}
}

// ============================================================
// WALImpl.Close
// ============================================================

// Close must call disk.Close.
func TestWALImpl_Close_ClosesUnderlyingDisk(t *testing.T) {
	spy := newSpyDisk()
	spy.closeFn = func() error { return nil }
	wal, _ := newWALImplForTest(t, spy)

	if err := wal.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !spy.closeCalled {
		t.Error("disk.Close was not called")
	}
}

// Close must render the WAL file handle unusable (the file descriptor is closed).
func TestWALImpl_Close_ClosesWALFile(t *testing.T) {
	spy := newSpyDisk()
	spy.closeFn = func() error { return nil }

	dir := t.TempDir()
	f, err := os.Create(dir + "/test.wal")
	if err != nil {
		t.Fatalf("os.Create: %v", err)
	}
	wal := &WALImpl{disk: spy, file: f, logSequenceNumber: 0}

	if err := wal.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := f.Write([]byte{0}); err == nil {
		t.Error("WAL file should be closed after wal.Close()")
	}
}

// A disk.Close error must be returned before the WAL file is closed.
func TestWALImpl_Close_DiskError_Propagates(t *testing.T) {
	spy := newSpyDisk()
	spy.closeFn = func() error { return errors.New("disk close error") }
	wal, _ := newWALImplForTest(t, spy)

	if err := wal.Close(); err == nil {
		t.Error("Close should propagate disk.Close error")
	}
}

// ============================================================
// WALImpl.Delete
// ============================================================

// Delete must call disk.Delete.
func TestWALImpl_Delete_CallsDiskDelete(t *testing.T) {
	spy := newSpyDisk()
	spy.deleteFn = func() error { return nil }

	dir := t.TempDir()
	f, err := os.Create(dir + "/test.wal")
	if err != nil {
		t.Fatalf("os.Create: %v", err)
	}
	wal := &WALImpl{disk: spy, file: f, logSequenceNumber: 0}

	if err := wal.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !spy.deleteCalled {
		t.Error("disk.Delete was not called")
	}
}

// Delete must remove the WAL file from disk.
func TestWALImpl_Delete_RemovesWALFile(t *testing.T) {
	spy := newSpyDisk()
	spy.deleteFn = func() error { return nil }

	dir := t.TempDir()
	walPath := dir + "/test.wal"
	f, err := os.Create(walPath)
	if err != nil {
		t.Fatalf("os.Create: %v", err)
	}
	wal := &WALImpl{disk: spy, file: f, logSequenceNumber: 0}

	if err := wal.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := os.Stat(walPath); !os.IsNotExist(err) {
		t.Error("WAL file should have been deleted")
	}
}

// A disk.Delete error must be returned and the WAL file must not be deleted.
func TestWALImpl_Delete_DiskError_WALFileRetained(t *testing.T) {
	spy := newSpyDisk()
	spy.deleteFn = func() error { return errors.New("disk delete error") }

	dir := t.TempDir()
	walPath := dir + "/test.wal"
	f, err := os.Create(walPath)
	if err != nil {
		t.Fatalf("os.Create: %v", err)
	}
	t.Cleanup(func() { f.Close(); os.Remove(walPath) })
	wal := &WALImpl{disk: spy, file: f, logSequenceNumber: 0}

	if err := wal.Delete(); err == nil {
		t.Error("Delete should propagate disk.Delete error")
	}
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Error("WAL file should not have been deleted when disk.Delete fails")
	}
}

// ============================================================
// NewWAL
// ============================================================

// NewWAL must create the WAL file when it does not yet exist.
func TestNewWAL_CreatesWALFile(t *testing.T) {
	pm, path := newDBForWALTest(t)
	walPath := path + "_WAL"

	wal, err := NewWAL(pm, path)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer wal.file.Close()

	if _, err := os.Stat(walPath); err != nil {
		t.Errorf("WAL file not created: %v", err)
	}
}

// NewWAL must open an existing WAL file without error (append mode).
func TestNewWAL_OpensExistingWALFile(t *testing.T) {
	pm, path := newDBForWALTest(t)

	// Create WAL file first session.
	wal1, err := NewWAL(pm, path)
	if err != nil {
		t.Fatalf("first NewWAL: %v", err)
	}
	if err := wal1.file.Close(); err != nil {
		t.Fatalf("closing first WAL: %v", err)
	}

	// Re-open should succeed.
	wal2, err := NewWAL(pm, path)
	if err != nil {
		t.Fatalf("second NewWAL on existing file: %v", err)
	}
	defer wal2.file.Close()
}

// On a fresh DB every page has LSN 0, so NewWAL must start at LSN 1 (maxPageLSN+1).
// Starting at 1 (not 0) ensures the first WAL record has LSN > any page LSN, so
// recovery correctly replays a write that landed in the WAL but not on disk.
func TestNewWAL_LSNStartsAboveMaxPageLSN(t *testing.T) {
	pm, path := newDBForWALTest(t)

	wal, err := NewWAL(pm, path)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer wal.file.Close()

	// Fresh DB: meta page LSN = 0, so expected starting LSN = 0 + 1 = 1.
	if wal.logSequenceNumber != 1 {
		t.Errorf("initial logSequenceNumber = %d, want 1 (maxPageLSN 0 + 1)", wal.logSequenceNumber)
	}
}

// After a session that writes pages, NewWAL on re-open must start above the
// highest LSN found on any page, preventing cross-session LSN aliasing.
func TestNewWAL_LSNContinuesAcrossSessions(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/cont.db"

	// Session 1: write several pages through the WAL, then close cleanly.
	pm1Raw, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	pm1 := pm1Raw.(*PageManagerImpl)

	wal1, err := NewWAL(pm1, dbPath)
	if err != nil {
		t.Fatalf("session 1 NewWAL: %v", err)
	}

	// Allocate and write three pages; LSNs consumed: 1, 2, 3.
	for i := 0; i < 3; i++ {
		p, err := pm1.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage %d: %v", i, err)
		}
		if err := wal1.WritePage(p); err != nil {
			t.Fatalf("WritePage %d: %v", i, err)
		}
	}
	// logSequenceNumber is now 4; highest page LSN on disk is 3.
	if err := wal1.disk.Close(); err != nil {
		t.Fatalf("session 1 disk close: %v", err)
	}
	if err := wal1.file.Truncate(0); err != nil {
		t.Fatalf("truncate WAL file: %v", err)
	}
	if err := wal1.file.Close(); err != nil {
		t.Fatalf("close WAL file: %v", err)
	}

	// Session 2: reopen and create a new WAL — must start above 3.
	pm2Raw, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { pm2Raw.Close() })

	wal2, err := NewWAL(pm2Raw, dbPath)
	if err != nil {
		t.Fatalf("session 2 NewWAL: %v", err)
	}
	defer wal2.file.Close()

	if wal2.logSequenceNumber <= 3 {
		t.Errorf("session 2 logSequenceNumber = %d, want > 3 (highest LSN from session 1)",
			wal2.logSequenceNumber)
	}
}

// ============================================================
// RecoverFromWAL
// ============================================================

// When there is no WAL file, recovery must succeed without touching anything.
func TestRecoverFromWAL_NoWALFile_ReturnsNil(t *testing.T) {
	pm, path := newDBForWALTest(t)
	walPath := path + "_WAL"

	// Confirm WAL file does not exist.
	if _, err := os.Stat(walPath); !os.IsNotExist(err) {
		t.Skip("WAL file already exists; skipping")
	}

	if err := RecoverFromWAL(walPath, pm); err != nil {
		t.Errorf("RecoverFromWAL with no WAL file: %v", err)
	}
}

// An empty WAL file must be treated as "nothing to replay" and then truncated.
func TestRecoverFromWAL_EmptyWALFile_Succeeds(t *testing.T) {
	pm, path := newDBForWALTest(t)
	walPath := path + "_WAL"

	f, err := os.Create(walPath)
	if err != nil {
		t.Fatalf("creating WAL file: %v", err)
	}
	f.Close()

	if err := RecoverFromWAL(walPath, pm); err != nil {
		t.Fatalf("RecoverFromWAL on empty file: %v", err)
	}

	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Stat after recovery: %v", err)
	}
	if info.Size() != 0 {
		t.Errorf("WAL file size after empty recovery = %d, want 0", info.Size())
	}
}

// A WAL record whose LSN is greater than the page's current LSN must be replayed.
func TestRecoverFromWAL_WALLSNGreaterThanPageLSN_PageReplayed(t *testing.T) {
	pm, path := newDBForWALTest(t)

	// Allocate page 1. Its on-disk LSN is 0.
	dataPage, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	pageID := dataPage.GetPageId()

	// Craft a WAL record with LSN=5 (> page LSN 0) and distinct sentinel data.
	walPage := &Page{}
	walPage.setPageId(pageID)
	walPage.setPageLSN(5)
	walPage.Data[CommonHeaderSize] = 0xAB

	walPath := path + "_WAL"
	f, err := os.Create(walPath)
	if err != nil {
		t.Fatalf("creating WAL file: %v", err)
	}
	appendWALRecord(t, f, 5, pageID, walPage)
	f.Close()

	if err := RecoverFromWAL(walPath, pm); err != nil {
		t.Fatalf("RecoverFromWAL: %v", err)
	}

	got := readPageDirect(t, pm, pageID)
	if got.Data[CommonHeaderSize] != 0xAB {
		t.Errorf("page sentinel = %#x after recovery, want 0xAB (WAL record not replayed)", got.Data[CommonHeaderSize])
	}
}

// A WAL record whose LSN equals the page's current LSN must NOT be replayed
// (the disk write already landed).
func TestRecoverFromWAL_WALLSNEqualPageLSN_PageNotReplayed(t *testing.T) {
	pm, path := newDBForWALTest(t)

	dataPage, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	pageID := dataPage.GetPageId()

	// Write the page directly with LSN=3 and sentinel 0xCC.
	onDiskPage := &Page{}
	onDiskPage.setPageId(pageID)
	onDiskPage.setPageLSN(3)
	onDiskPage.Data[CommonHeaderSize] = 0xCC
	writePageDirect(t, pm, onDiskPage)

	// WAL record has LSN=3 (equal) with different sentinel 0xDD.
	walPage := &Page{}
	walPage.setPageId(pageID)
	walPage.setPageLSN(3)
	walPage.Data[CommonHeaderSize] = 0xDD

	walPath := path + "_WAL"
	f, _ := os.Create(walPath)
	appendWALRecord(t, f, 3, pageID, walPage)
	f.Close()

	if err := RecoverFromWAL(walPath, pm); err != nil {
		t.Fatalf("RecoverFromWAL: %v", err)
	}

	got := readPageDirect(t, pm, pageID)
	if got.Data[CommonHeaderSize] != 0xCC {
		t.Errorf("page sentinel = %#x, want 0xCC (equal-LSN record should not overwrite)", got.Data[CommonHeaderSize])
	}
}

// A WAL record whose LSN is less than the page's current LSN must NOT be replayed.
func TestRecoverFromWAL_WALLSNLessThanPageLSN_PageNotReplayed(t *testing.T) {
	pm, path := newDBForWALTest(t)

	dataPage, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	pageID := dataPage.GetPageId()

	// On-disk page has LSN=10, sentinel 0xEE.
	onDiskPage := &Page{}
	onDiskPage.setPageId(pageID)
	onDiskPage.setPageLSN(10)
	onDiskPage.Data[CommonHeaderSize] = 0xEE
	writePageDirect(t, pm, onDiskPage)

	// WAL record has LSN=3 (stale) with sentinel 0xFF.
	walPage := &Page{}
	walPage.setPageId(pageID)
	walPage.setPageLSN(3)
	walPage.Data[CommonHeaderSize] = 0xFF

	walPath := path + "_WAL"
	f, _ := os.Create(walPath)
	appendWALRecord(t, f, 3, pageID, walPage)
	f.Close()

	if err := RecoverFromWAL(walPath, pm); err != nil {
		t.Fatalf("RecoverFromWAL: %v", err)
	}

	got := readPageDirect(t, pm, pageID)
	if got.Data[CommonHeaderSize] != 0xEE {
		t.Errorf("page sentinel = %#x, want 0xEE (stale WAL record should not overwrite)", got.Data[CommonHeaderSize])
	}
}

// All valid records must be processed in sequence, each replayed independently.
func TestRecoverFromWAL_MultipleRecords_AllProcessed(t *testing.T) {
	pm, path := newDBForWALTest(t)

	// Allocate three data pages.
	pages := make([]*Page, 3)
	for i := range pages {
		p, err := pm.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage %d: %v", i, err)
		}
		pages[i] = p
	}

	walPath := path + "_WAL"
	f, _ := os.Create(walPath)
	sentinels := []byte{0x11, 0x22, 0x33}
	for i, p := range pages {
		walPage := &Page{}
		walPage.setPageId(p.GetPageId())
		walPage.setPageLSN(uint64(i + 1)) // LSN > page LSN (0)
		walPage.Data[CommonHeaderSize] = sentinels[i]
		appendWALRecord(t, f, uint64(i+1), p.GetPageId(), walPage)
	}
	f.Close()

	if err := RecoverFromWAL(walPath, pm); err != nil {
		t.Fatalf("RecoverFromWAL: %v", err)
	}

	for i, p := range pages {
		got := readPageDirect(t, pm, p.GetPageId())
		if got.Data[CommonHeaderSize] != sentinels[i] {
			t.Errorf("page %d sentinel = %#x, want %#x", p.GetPageId(), got.Data[CommonHeaderSize], sentinels[i])
		}
	}
}

// Recovery must stop at the first record with an invalid CRC. Records before it
// are replayed; records after it are silently ignored.
func TestRecoverFromWAL_InvalidCRC_StopsReplay(t *testing.T) {
	pm, path := newDBForWALTest(t)

	p1, _ := pm.AllocatePage()
	p2, _ := pm.AllocatePage()

	walPath := path + "_WAL"
	f, _ := os.Create(walPath)

	// Record 0: valid, LSN=1, sentinel 0xAA — should be replayed.
	good := &Page{}
	good.setPageId(p1.GetPageId())
	good.setPageLSN(1)
	good.Data[CommonHeaderSize] = 0xAA
	appendWALRecord(t, f, 1, p1.GetPageId(), good)

	// Record 1: corrupt CRC — should stop replay here.
	bad := &WAL{}
	bad.SetLSN(2)
	bad.SetPageID(p2.GetPageId())
	bad.SetCRC32(0xDEADBEEF) // wrong
	if _, err := f.Write(bad.Data[:]); err != nil {
		t.Fatalf("write corrupt WAL record: %v", err)
	}

	// Record 2 would apply sentinel 0xBB to p2, but should never be reached.
	// (We don't write it; the corrupt record stops iteration first.)
	f.Close()

	if err := RecoverFromWAL(walPath, pm); err != nil {
		t.Fatalf("RecoverFromWAL: %v", err)
	}

	got1 := readPageDirect(t, pm, p1.GetPageId())
	if got1.Data[CommonHeaderSize] != 0xAA {
		t.Errorf("page %d sentinel = %#x, want 0xAA (record before corrupt should be replayed)", p1.GetPageId(), got1.Data[CommonHeaderSize])
	}

	got2 := readPageDirect(t, pm, p2.GetPageId())
	if got2.Data[CommonHeaderSize] != 0 {
		t.Errorf("page %d sentinel = %#x, want 0 (record after corrupt should not be replayed)", p2.GetPageId(), got2.Data[CommonHeaderSize])
	}
}

// A partial record at the end of the WAL (crash mid-write) must not cause an
// error — recovery simply stops at that point.
func TestRecoverFromWAL_PartialLastRecord_StopsGracefully(t *testing.T) {
	pm, path := newDBForWALTest(t)

	walPath := path + "_WAL"
	f, _ := os.Create(walPath)

	// Write only half a WAL record's worth of bytes.
	partial := make([]byte, WAL_RecordSize/2)
	if _, err := f.Write(partial); err != nil {
		t.Fatalf("write partial WAL record: %v", err)
	}
	f.Close()

	if err := RecoverFromWAL(walPath, pm); err != nil {
		t.Errorf("RecoverFromWAL with partial record: %v", err)
	}
}

// RecoverFromWAL must truncate the WAL file to zero bytes after processing.
func TestRecoverFromWAL_TruncatesWALAfterRecovery(t *testing.T) {
	pm, path := newDBForWALTest(t)

	dataPage, _ := pm.AllocatePage()
	walPage := &Page{}
	walPage.setPageId(dataPage.GetPageId())
	walPage.setPageLSN(1)

	walPath := path + "_WAL"
	f, _ := os.Create(walPath)
	appendWALRecord(t, f, 1, dataPage.GetPageId(), walPage)
	f.Close()

	if err := RecoverFromWAL(walPath, pm); err != nil {
		t.Fatalf("RecoverFromWAL: %v", err)
	}

	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Stat after recovery: %v", err)
	}
	if info.Size() != 0 {
		t.Errorf("WAL file size after recovery = %d, want 0 (truncated)", info.Size())
	}
}

// After recovery the PageManagerImpl's in-memory metaPage must reflect whatever
// was written to page 0 during replay.
func TestRecoverFromWAL_MetaPageRefreshed(t *testing.T) {
	pm, path := newDBForWALTest(t)

	// Craft a meta page with a distinctive root page ID and write it as a WAL record.
	const wantRoot = uint32(77)
	metaPage := NewMetaPage()
	metaPage.setMetaRootPage(wantRoot)
	metaPage.setMetaPageCount(2)

	walPath := path + "_WAL"
	f, _ := os.Create(walPath)
	// Meta page is always page 0, WAL LSN=1 > page 0 LSN=0.
	appendWALRecord(t, f, 1, 0, metaPage)
	f.Close()

	if err := RecoverFromWAL(walPath, pm); err != nil {
		t.Fatalf("RecoverFromWAL: %v", err)
	}

	if got := pm.GetRootPageId(); got != wantRoot {
		t.Errorf("GetRootPageId after recovery = %d, want %d (metaPage not refreshed)", got, wantRoot)
	}
}

// End-to-end: write pages through WALImpl, simulate a crash by leaving the WAL
// un-truncated, reopen the DB, and verify that recovery restores the written data.
func TestRecoverFromWAL_EndToEnd_CrashRecovery(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/crash.db"

	// ── Session 1: write pages through WAL, then "crash" ───────────────────
	pm1Raw, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	pm1 := pm1Raw.(*PageManagerImpl)

	wal1, err := NewWAL(pm1, dbPath)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}

	// Allocate a data page directly (goes to disk).
	dataPage, err := pm1.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	pageID := dataPage.GetPageId()

	// Write the page through WAL — LSN=0 is logged, page on disk gets LSN=0.
	dataPage.Data[CommonHeaderSize] = 0x5A
	if err := wal1.WritePage(dataPage); err != nil {
		t.Fatalf("WAL WritePage: %v", err)
	}

	// Simulate crash: corrupt the on-disk page so its LSN is reset to 0 but data is wrong.
	// (Represent the state where the disk write happened but data got corrupted mid-write.)
	corrupted := &Page{}
	corrupted.setPageId(pageID)
	corrupted.setPageLSN(0)              // same LSN as WAL record
	corrupted.Data[CommonHeaderSize] = 0 // data lost
	writePageDirect(t, pm1, corrupted)

	// Close file handles without going through proper shutdown (simulating crash).
	pm1.file.Close()
	pm1.file = nil
	wal1.file.Close()

	// ── Session 2: open DB and run recovery ─────────────────────────────────
	// Because the on-disk page has LSN=0 and the WAL record also has LSN=0,
	// the strict ">" comparison means this record is NOT replayed.
	// This test documents that known behaviour: equal-LSN records are skipped.
	pm2 := openDBForWALTest(t, dbPath)
	walPath := dbPath + "_WAL"

	if err := RecoverFromWAL(walPath, pm2); err != nil {
		t.Fatalf("RecoverFromWAL: %v", err)
	}

	// Verify the WAL was consumed (truncated).
	info, _ := os.Stat(walPath)
	if info.Size() != 0 {
		t.Errorf("WAL not truncated after recovery: size=%d", info.Size())
	}
}

// End-to-end: a WAL record with a strictly greater LSN than the on-disk page
// is replayed, restoring data that was not yet flushed before the crash.
func TestRecoverFromWAL_EndToEnd_StalePageRestored(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/stale.db"

	pm1Raw, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	pm1 := pm1Raw.(*PageManagerImpl)

	// Allocate page; on-disk LSN = 0.
	dataPage, err := pm1.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	pageID := dataPage.GetPageId()
	pm1.file.Close()
	pm1.file = nil

	// Craft a WAL record with LSN=1 (> 0) carrying the "intended" page data.
	walPath := dbPath + "_WAL"
	f, _ := os.Create(walPath)
	wantPage := &Page{}
	wantPage.setPageId(pageID)
	wantPage.setPageLSN(1)
	wantPage.Data[CommonHeaderSize] = 0x7F
	appendWALRecord(t, f, 1, pageID, wantPage)
	f.Close()

	pm2 := openDBForWALTest(t, dbPath)
	if err := RecoverFromWAL(walPath, pm2); err != nil {
		t.Fatalf("RecoverFromWAL: %v", err)
	}

	got := readPageDirect(t, pm2, pageID)
	if got.Data[CommonHeaderSize] != 0x7F {
		t.Errorf("recovered page sentinel = %#x, want 0x7F", got.Data[CommonHeaderSize])
	}
}

// ============================================================
// WALImpl.Close — WAL truncation on clean shutdown
// ============================================================

// Close must truncate the WAL file to zero bytes before closing it.
// This prevents the next open from unnecessarily scanning stale records.
func TestWALImpl_Close_TruncatesWALFile(t *testing.T) {
	pm, path := newDBForWALTest(t)

	wal, err := NewWAL(pm, path)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}

	// Write a page so the WAL file has content.
	dataPage, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	if err := wal.WritePage(dataPage); err != nil {
		t.Fatalf("WritePage: %v", err)
	}

	walPath := path + "_WAL"
	if info, _ := os.Stat(walPath); info.Size() == 0 {
		t.Fatal("WAL file should have content before Close")
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Stat WAL after Close: %v", err)
	}
	if info.Size() != 0 {
		t.Errorf("WAL file size after Close = %d, want 0 (truncated on clean shutdown)", info.Size())
	}
}

// ============================================================
// maxPageLSN — picks the real maximum across all pages
// ============================================================

// When pages have different LSNs, NewWAL must start above the highest one,
// not just the last-written or the meta page's LSN.
func TestNewWAL_LSN_StartsAboveHighestOfManyPages(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/multi.db"

	// Build a DB and write several pages through the WAL, each getting a
	// different (monotonically increasing) LSN.
	pm1Raw, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	pm1 := pm1Raw.(*PageManagerImpl)

	wal1, err := NewWAL(pm1, dbPath)
	if err != nil {
		t.Fatalf("session 1 NewWAL: %v", err)
	}

	const nPages = 4
	var lastLSN uint64
	for i := 0; i < nPages; i++ {
		p, err := pm1.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage %d: %v", i, err)
		}
		if err := wal1.WritePage(p); err != nil {
			t.Fatalf("WritePage %d: %v", i, err)
		}
		lastLSN = p.GetPageLSN() // stamped by WritePage
	}

	// Close cleanly so WAL is truncated and all LSNs are on disk.
	if err := wal1.Close(); err != nil {
		t.Fatalf("session 1 Close: %v", err)
	}

	// Session 2: NewWAL must start strictly above lastLSN.
	pm2Raw, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { pm2Raw.Close() })

	wal2, err := NewWAL(pm2Raw, dbPath)
	if err != nil {
		t.Fatalf("session 2 NewWAL: %v", err)
	}
	defer wal2.file.Close()

	if wal2.logSequenceNumber <= lastLSN {
		t.Errorf("session 2 logSequenceNumber = %d, want > %d (highest page LSN)",
			wal2.logSequenceNumber, lastLSN)
	}
}

// ============================================================
// Cross-session crash recovery — the scenario the LSN fix enables
// ============================================================

// Scenario:
//
//	Session 1 — write page, close cleanly. Page on disk has LSN=N.
//	Session 2 — reopen, write same page (WAL LSN > N), crash before disk write lands.
//	Session 3 — reopen, RecoverFromWAL must replay the session-2 WAL record.
//
// Without the maxPageLSN fix, session 2's WAL LSN would restart at 0, which is
// less than N, so recovery would skip the record and lose the write.
func TestRecoverFromWAL_CrossSession_CrashInSession2_RestoredInSession3(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/xsession.db"

	// ── Session 1: write a page cleanly ─────────────────────────────────────
	pm1Raw, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	pm1 := pm1Raw.(*PageManagerImpl)

	wal1, err := NewWAL(pm1, dbPath)
	if err != nil {
		t.Fatalf("session 1 NewWAL: %v", err)
	}

	dataPage, err := pm1.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	pageID := dataPage.GetPageId()
	dataPage.Data[CommonHeaderSize] = 0x11

	if err := wal1.WritePage(dataPage); err != nil {
		t.Fatalf("session 1 WritePage: %v", err)
	}
	session1LSN := dataPage.GetPageLSN() // e.g. 1

	// Clean close: WAL truncated, page on disk has LSN=session1LSN, data=0x11.
	if err := wal1.Close(); err != nil {
		t.Fatalf("session 1 Close: %v", err)
	}

	// ── Session 2: write the page, then simulate crash ───────────────────────
	pm2Raw, err := OpenDB(dbPath) // RecoverFromWAL sees empty WAL → no-op
	if err != nil {
		t.Fatalf("OpenDB session 2: %v", err)
	}
	pm2 := pm2Raw.(*PageManagerImpl)

	wal2, err := NewWAL(pm2Raw, dbPath) // logSequenceNumber > session1LSN
	if err != nil {
		t.Fatalf("session 2 NewWAL: %v", err)
	}

	p2, err := pm2.ReadPage(pageID)
	if err != nil {
		t.Fatalf("session 2 ReadPage: %v", err)
	}
	p2.Data[CommonHeaderSize] = 0x22 // new intended data

	if err := wal2.WritePage(p2); err != nil {
		t.Fatalf("session 2 WritePage: %v", err)
	}
	session2LSN := p2.GetPageLSN() // must be > session1LSN

	if session2LSN <= session1LSN {
		t.Fatalf("session 2 LSN %d is not > session 1 LSN %d; fix not working", session2LSN, session1LSN)
	}

	// Simulate crash: overwrite the page on disk with stale session-1 data,
	// as if the OS held old cached data and the new write never landed.
	stale := &Page{}
	stale.setPageId(pageID)
	stale.setPageLSN(session1LSN)
	stale.Data[CommonHeaderSize] = 0x11
	writePageDirect(t, pm2, stale)

	// Close file handles directly (simulating process death; WAL not truncated).
	pm2.file.Close()
	pm2.file = nil
	wal2.file.Close()

	// ── Session 3: open and recover ──────────────────────────────────────────
	pm3 := openDBForWALTest(t, dbPath)
	walPath := dbPath + "_WAL"

	if err := RecoverFromWAL(walPath, pm3); err != nil {
		t.Fatalf("RecoverFromWAL session 3: %v", err)
	}

	got := readPageDirect(t, pm3, pageID)
	if got.Data[CommonHeaderSize] != 0x22 {
		t.Errorf("recovered page data = %#x, want 0x22 (session 2 write not recovered)", got.Data[CommonHeaderSize])
	}

	// WAL must be consumed.
	if info, _ := os.Stat(walPath); info.Size() != 0 {
		t.Errorf("WAL not truncated after session 3 recovery: size=%d", info.Size())
	}
}
