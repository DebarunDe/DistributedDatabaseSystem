package pagemanager

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// ============================================================
// Helpers
// ============================================================

func newTempPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.db")
}

func mustNewDB(t *testing.T, path string) PageManager {
	t.Helper()
	pm, err := NewDB(path)
	if err != nil {
		t.Fatalf("NewDB(%q): %v", path, err)
	}
	return pm
}

func mustClose(t *testing.T, pm PageManager) {
	t.Helper()
	if err := pm.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}

func mustAllocate(t *testing.T, pm PageManager) *Page {
	t.Helper()
	p, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage(): %v", err)
	}
	return p
}

func mustOpenDB(t *testing.T, path string) PageManager {
	t.Helper()
	pm, err := OpenDB(path)
	if err != nil {
		t.Fatalf("OpenDB(%q): %v", path, err)
	}
	return pm
}

// metaOf type-asserts to *PageManagerImpl so tests can inspect metaPage directly.
func metaOf(t *testing.T, pm PageManager) *Page {
	t.Helper()
	return pm.(*PageManagerImpl).metaPage
}

// ============================================================
// NewDB + Close
// ============================================================

// After NewDB + Close the file must exist and be exactly PageSize bytes.
func TestNewDB_Close_FileSize(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	mustClose(t, pm)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat after Close: %v", err)
	}
	if info.Size() != PageSize {
		t.Errorf("file size = %d, want %d (PageSize)", info.Size(), PageSize)
	}
}

// After NewDB + Close the magic number at the correct offset must be valid.
func TestNewDB_Close_MagicNumber(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	mustClose(t, pm)

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	defer f.Close()

	var buf [PageSize]byte
	if _, err := f.ReadAt(buf[:], 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	got := binary.LittleEndian.Uint32(buf[OffsetMetaMagic:])
	if got != MagicNumber {
		t.Errorf("magic = %#x, want %#x", got, MagicNumber)
	}
}

// NewDB on a path that already has a file must return an error.
func TestNewDB_ErrorIfFileExists(t *testing.T) {
	path := newTempPath(t)

	pm := mustNewDB(t, path)
	mustClose(t, pm)

	_, err := NewDB(path)
	if err == nil {
		t.Error("NewDB on existing file should return error")
	}
}

// NewDB initial meta page state: page count 1, free list InvalidPageID, root InvalidPageID.
func TestNewDB_InitialMetaState(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	meta := metaOf(t, pm)

	if got := meta.GetMetaPageMagicNumber(); got != MagicNumber {
		t.Errorf("magic = %#x, want %#x", got, MagicNumber)
	}
	if got := meta.GetMetaPageVersion(); got != 1 {
		t.Errorf("version = %d, want 1", got)
	}
	if got := meta.GetMetaPageCount(); got != 1 {
		t.Errorf("page count = %d, want 1", got)
	}
	if got := meta.GetMetaFreeList(); got != InvalidPageID {
		t.Errorf("free list = %#x, want InvalidPageID", got)
	}
	if got := meta.GetMetaRootPage(); got != InvalidPageID {
		t.Errorf("root page = %#x, want InvalidPageID", got)
	}
	if got := meta.GetPageType(); got != PageTypeMeta {
		t.Errorf("page type = %d, want PageTypeMeta", got)
	}
	if got := meta.GetPageId(); got != 0 {
		t.Errorf("page id = %d, want 0", got)
	}
}

// ============================================================
// OpenDB
// ============================================================

// After NewDB + Close + OpenDB the meta page state must match what NewDB created.
func TestOpenDB_MetaStateMatchesNewDB(t *testing.T) {
	path := newTempPath(t)

	pm := mustNewDB(t, path)
	mustClose(t, pm)

	pm2 := mustOpenDB(t, path)
	defer pm2.Close()

	meta := metaOf(t, pm2)

	if got := meta.GetMetaPageMagicNumber(); got != MagicNumber {
		t.Errorf("magic = %#x, want %#x", got, MagicNumber)
	}
	if got := meta.GetMetaPageVersion(); got != 1 {
		t.Errorf("version = %d, want 1", got)
	}
	if got := meta.GetMetaPageCount(); got != 1 {
		t.Errorf("page count = %d, want 1", got)
	}
	if got := meta.GetMetaFreeList(); got != InvalidPageID {
		t.Errorf("free list = %#x, want InvalidPageID", got)
	}
	if got := meta.GetMetaRootPage(); got != InvalidPageID {
		t.Errorf("root page = %#x, want InvalidPageID", got)
	}
}

// OpenDB on a non-existent path must return an error.
func TestOpenDB_NonExistentPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does_not_exist.db")
	_, err := OpenDB(path)
	if err == nil {
		t.Error("OpenDB on non-existent path should return error")
	}
}

// OpenDB on a file with the wrong magic number must return an error.
func TestOpenDB_CorruptedMagicNumber(t *testing.T) {
	path := newTempPath(t)

	// Write a full-size file but with an invalid magic number.
	buf := make([]byte, PageSize)
	binary.LittleEndian.PutUint32(buf[OffsetMetaMagic:], 0xDEADBEEF)
	if err := os.WriteFile(path, buf, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := OpenDB(path)
	if err == nil {
		t.Error("OpenDB should return error for corrupted magic number")
	}
}

// OpenDB on a file smaller than PageSize must return an error.
func TestOpenDB_FileTooSmall(t *testing.T) {
	path := newTempPath(t)
	if err := os.WriteFile(path, []byte("tiny"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := OpenDB(path)
	if err == nil {
		t.Error("OpenDB on file smaller than PageSize should return error")
	}
}

// ============================================================
// AllocatePage + ReadPage
// ============================================================

// The first AllocatePage must return page ID 1.
func TestAllocatePage_FirstPageID(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	page, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	if page.GetPageId() != 1 {
		t.Errorf("first allocated page ID = %d, want 1", page.GetPageId())
	}
}

// A freshly allocated page can be read back with a matching page ID.
func TestAllocatePage_ReadBack(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	allocated := mustAllocate(t, pm)
	id := allocated.GetPageId()

	read, err := pm.ReadPage(id)
	if err != nil {
		t.Fatalf("ReadPage(%d): %v", id, err)
	}
	if read.GetPageId() != id {
		t.Errorf("ReadPage ID = %d, want %d", read.GetPageId(), id)
	}
}

// Successive AllocatePage calls must return sequential page IDs 1, 2, 3, ...
func TestAllocatePage_SequentialIDs(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	for want := uint32(1); want <= 5; want++ {
		page, err := pm.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage[%d]: %v", want, err)
		}
		if page.GetPageId() != want {
			t.Errorf("page[%d] ID = %d, want %d", want, page.GetPageId(), want)
		}
	}
}

// Each AllocatePage must increment the meta page count by 1.
func TestAllocatePage_MetaPageCountIncrements(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	meta := metaOf(t, pm)
	if got := meta.GetMetaPageCount(); got != 1 {
		t.Fatalf("initial page count = %d, want 1", got)
	}

	for i := 1; i <= 3; i++ {
		if _, err := pm.AllocatePage(); err != nil {
			t.Fatalf("AllocatePage[%d]: %v", i, err)
		}
		if got := meta.GetMetaPageCount(); got != uint32(i+1) {
			t.Errorf("after %d allocs, page count = %d, want %d", i, got, i+1)
		}
	}
}

// ReadPage on an ID beyond the page count must return an error.
func TestReadPage_OutOfBounds(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	// Only page 0 exists; page 1 is out of bounds.
	_, err := pm.ReadPage(1)
	if err == nil {
		t.Error("ReadPage on out-of-bounds ID should return error")
	}
}

// ReadPage(0) must return the meta page with the correct magic number.
func TestReadPage_MetaPage(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	page, err := pm.ReadPage(0)
	if err != nil {
		t.Fatalf("ReadPage(0): %v", err)
	}
	if page.GetMetaPageMagicNumber() != MagicNumber {
		t.Errorf("meta page magic = %#x, want %#x", page.GetMetaPageMagicNumber(), MagicNumber)
	}
	if page.GetPageId() != 0 {
		t.Errorf("meta page ID = %d, want 0", page.GetPageId())
	}
}

// ============================================================
// WritePage — persistence across restarts
// ============================================================

// A byte pattern written to an allocated page must survive Close + OpenDB + ReadPage.
func TestWritePage_Persistence(t *testing.T) {
	path := newTempPath(t)

	pm, err := NewDB(path)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}

	page, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	pageID := page.GetPageId()

	sentinel := []byte("persistent-sentinel-payload")
	copy(page.Data[CommonHeaderSize:], sentinel)

	if err := pm.WritePage(page); err != nil {
		t.Fatalf("WritePage: %v", err)
	}
	mustClose(t, pm)

	pm2 := mustOpenDB(t, path)
	defer pm2.Close()

	readBack, err := pm2.ReadPage(pageID)
	if err != nil {
		t.Fatalf("ReadPage after reopen: %v", err)
	}
	got := readBack.Data[CommonHeaderSize : CommonHeaderSize+len(sentinel)]
	if !bytes.Equal(got, sentinel) {
		t.Errorf("data after reopen = %q, want %q", got, sentinel)
	}
}

// A record inserted into a leaf page, written, and read back after restart must match.
func TestWritePage_Persistence_WithRecord(t *testing.T) {
	path := newTempPath(t)

	pm, err := NewDB(path)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}

	page, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	pageID := page.GetPageId()

	// Initialize page as a leaf so InsertRecord works.
	page.setPageType(PageTypeLeaf)
	page.setFreeSpaceStart(uint16(LeafHeaderSize))
	page.setFreeSpaceEnd(PageSize)
	page.setLeftSibling(InvalidPageID)
	page.setRightSibling(InvalidPageID)

	record := []byte("hello persistent record")
	idx, ok := page.InsertRecord(record)
	if !ok {
		t.Fatal("InsertRecord failed")
	}

	if err := pm.WritePage(page); err != nil {
		t.Fatalf("WritePage: %v", err)
	}
	mustClose(t, pm)

	pm2 := mustOpenDB(t, path)
	defer pm2.Close()

	readBack, err := pm2.ReadPage(pageID)
	if err != nil {
		t.Fatalf("ReadPage after reopen: %v", err)
	}
	got, ok := readBack.GetRecord(idx)
	if !ok {
		t.Fatal("GetRecord returned false after reopen")
	}
	if !bytes.Equal(got, record) {
		t.Errorf("record after reopen = %q, want %q", got, record)
	}
}

// WritePage with a page ID beyond the current count must return an error.
func TestWritePage_OutOfBounds(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	bad := &Page{}
	bad.setPageId(999)

	err := pm.WritePage(bad)
	if err == nil {
		t.Error("WritePage with out-of-bounds ID should return error")
	}
}

// ============================================================
// FreePage
// ============================================================

// After freeing the middle page the meta free list head must equal that page ID.
func TestFreePage_FreeListHead(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	p1 := mustAllocate(t, pm)
	p2 := mustAllocate(t, pm)
	p3 := mustAllocate(t, pm)
	_ = p1
	_ = p3
	middleID := p2.GetPageId()

	if err := pm.FreePage(middleID); err != nil {
		t.Fatalf("FreePage(%d): %v", middleID, err)
	}
	if got := metaOf(t, pm).GetMetaFreeList(); got != middleID {
		t.Errorf("free list head = %d, want %d", got, middleID)
	}
}

// AllocatePage after FreePage must reuse the freed page (same page ID returns).
func TestFreePage_ReuseFreedPage(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	p1 := mustAllocate(t, pm)
	p2 := mustAllocate(t, pm)
	p3 := mustAllocate(t, pm)
	_ = p1
	_ = p3
	middleID := p2.GetPageId()

	if err := pm.FreePage(middleID); err != nil {
		t.Fatalf("FreePage(%d): %v", middleID, err)
	}

	reused, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage after free: %v", err)
	}
	if reused.GetPageId() != middleID {
		t.Errorf("reused page ID = %d, want %d (the freed page)", reused.GetPageId(), middleID)
	}
}

// After the only free page is reallocated the free list head must be InvalidPageID.
func TestFreePage_FreeListEmptyAfterReuse(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	p1 := mustAllocate(t, pm)
	_ = p1
	p2 := mustAllocate(t, pm)

	if err := pm.FreePage(p2.GetPageId()); err != nil {
		t.Fatalf("FreePage(%d): %v", p2.GetPageId(), err)
	}

	if _, err := pm.AllocatePage(); err != nil {
		t.Fatalf("AllocatePage after free: %v", err)
	}

	if got := metaOf(t, pm).GetMetaFreeList(); got != InvalidPageID {
		t.Errorf("free list after reuse = %#x, want InvalidPageID", got)
	}
}

// Freeing multiple pages chains them correctly so all can be reallocated.
func TestFreePage_MultipleFreedPagesChained(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	p1 := mustAllocate(t, pm)
	p2 := mustAllocate(t, pm)
	p3 := mustAllocate(t, pm)

	freedIDs := map[uint32]bool{
		p1.GetPageId(): true,
		p2.GetPageId(): true,
		p3.GetPageId(): true,
	}

	// Free in reverse order to exercise chaining.
	for _, p := range []*Page{p3, p2, p1} {
		if err := pm.FreePage(p.GetPageId()); err != nil {
			t.Fatalf("FreePage(%d): %v", p.GetPageId(), err)
		}
	}

	// Reallocate three times; every returned page must come from the free list.
	for i := 0; i < 3; i++ {
		reused, err := pm.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage[%d] after chained free: %v", i, err)
		}
		if !freedIDs[reused.GetPageId()] {
			t.Errorf("AllocatePage[%d] returned ID %d, not from freed set %v", i, reused.GetPageId(), freedIDs)
		}
		delete(freedIDs, reused.GetPageId())
	}
}

// FreePage(0) must return an error (cannot free the meta page).
func TestFreePage_ErrorOnPage0(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	if err := pm.FreePage(0); err == nil {
		t.Error("FreePage(0) should return error (cannot free meta page)")
	}
}

// FreePage with an out-of-bounds page ID must return an error.
func TestFreePage_ErrorOnOutOfBounds(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	if err := pm.FreePage(999); err == nil {
		t.Error("FreePage with out-of-bounds ID should return error")
	}
}

// Freed page's internal next pointer must correctly link to the previous free list head.
func TestFreePage_NextPointerChain(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	p1 := mustAllocate(t, pm)
	p2 := mustAllocate(t, pm)

	// Free p1 first, then p2; p2's next pointer should point to p1.
	if err := pm.FreePage(p1.GetPageId()); err != nil {
		t.Fatalf("FreePage(p1): %v", err)
	}
	if err := pm.FreePage(p2.GetPageId()); err != nil {
		t.Fatalf("FreePage(p2): %v", err)
	}

	// Read p2 from disk; its free-next-page field should equal p1.GetPageId().
	freed, err := pm.ReadPage(p2.GetPageId())
	if err != nil {
		t.Fatalf("ReadPage(p2): %v", err)
	}
	if got := freed.getFreeNextPage(); got != p1.GetPageId() {
		t.Errorf("freed page next ptr = %d, want %d (p1)", got, p1.GetPageId())
	}
}

// ============================================================
// SetRootPageID + GetRootPageID
// ============================================================

// Initial root page ID must be InvalidPageID.
func TestGetRootPageID_InitialValue(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	if got := pm.GetRootPageId(); got != InvalidPageID {
		t.Errorf("initial GetRootPageId() = %#x, want InvalidPageID", got)
	}
}

// SetRootPageId must be visible via GetRootPageId within the same session.
func TestSetRootPageID_InMemory(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	if err := pm.SetRootPageId(42); err != nil {
		t.Fatalf("SetRootPageId(42): %v", err)
	}
	if got := pm.GetRootPageId(); got != 42 {
		t.Errorf("GetRootPageId() = %d, want 42", got)
	}
}

// Root page ID set before Close must survive a Close + OpenDB cycle.
func TestSetRootPageID_Persistence(t *testing.T) {
	path := newTempPath(t)

	pm, err := NewDB(path)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	if err := pm.SetRootPageId(77); err != nil {
		t.Fatalf("SetRootPageId(77): %v", err)
	}
	mustClose(t, pm)

	pm2 := mustOpenDB(t, path)
	defer pm2.Close()

	if got := pm2.GetRootPageId(); got != 77 {
		t.Errorf("GetRootPageId() after reopen = %d, want 77", got)
	}
}

// Multiple SetRootPageId calls must update to the latest value.
func TestSetRootPageID_OverwritesPrevious(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	for _, id := range []uint32{1, 5, 99, InvalidPageID} {
		if err := pm.SetRootPageId(id); err != nil {
			t.Fatalf("SetRootPageId(%d): %v", id, err)
		}
		if got := pm.GetRootPageId(); got != id {
			t.Errorf("GetRootPageId() = %d, want %d", got, id)
		}
	}
}

// ============================================================
// Delete
// ============================================================

// After Delete the file must no longer exist.
func TestDelete_FileNoLongerExists(t *testing.T) {
	path := newTempPath(t)

	pm, err := NewDB(path)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	if err := pm.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("file should not exist after Delete")
	}
}

// A database can be recreated at the same path after Delete.
func TestDelete_AllowsRecreation(t *testing.T) {
	path := newTempPath(t)

	pm := mustNewDB(t, path)
	if err := pm.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	pm2, err := NewDB(path)
	if err != nil {
		t.Errorf("NewDB after Delete should succeed: %v", err)
	} else {
		pm2.Close()
	}
}

// ============================================================
// Edge cases
// ============================================================

// Methods called after Close must not crash (panic recovery tests).
func TestEdgeCase_MethodsAfterClose(t *testing.T) {
	checkNoPanic := func(t *testing.T, name string, fn func()) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s panicked after Close: %v", name, r)
				}
			}()
			fn()
		})
	}

	t.Run("ReadPage", func(t *testing.T) {
		path := newTempPath(t)
		pm := mustNewDB(t, path)
		mustClose(t, pm)
		checkNoPanic(t, "ReadPage", func() { _, _ = pm.ReadPage(0) })
	})

	t.Run("WritePage", func(t *testing.T) {
		path := newTempPath(t)
		pm := mustNewDB(t, path)
		mustClose(t, pm)
		checkNoPanic(t, "WritePage", func() {
			p := &Page{}
			_ = pm.WritePage(p)
		})
	})

	t.Run("AllocatePage", func(t *testing.T) {
		path := newTempPath(t)
		pm := mustNewDB(t, path)
		mustClose(t, pm)
		checkNoPanic(t, "AllocatePage", func() { _, _ = pm.AllocatePage() })
	})

	t.Run("FreePage", func(t *testing.T) {
		path := newTempPath(t)
		pm := mustNewDB(t, path)
		mustClose(t, pm)
		checkNoPanic(t, "FreePage", func() { _ = pm.FreePage(0) })
	})
}

// Close must be idempotent (double-close must not crash).
func TestEdgeCase_DoubleClose(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)

	mustClose(t, pm)

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("second Close panicked: %v", r)
		}
	}()
	pm.Close() // second close: must not crash
}

// FreePage(0) — the meta page — must always return an error.
func TestEdgeCase_FreeMetaPage(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	if err := pm.FreePage(0); err == nil {
		t.Error("FreePage(0) should return error")
	}
}

// WritePage with a page ID greater than the page count must return an error.
func TestEdgeCase_WritePageBeyondCount(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	bad := &Page{}
	bad.setPageId(uint32(^uint32(0) - 1)) // near-max ID
	if err := pm.WritePage(bad); err == nil {
		t.Error("WritePage beyond page count should return error")
	}
}

// ReadPage with a page ID equal to or beyond the count must return an error.
func TestEdgeCase_ReadPageBeyondCount(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	if _, err := pm.ReadPage(100); err == nil {
		t.Error("ReadPage beyond page count should return error")
	}
}

// NewDB on a path that already exists must fail with an error.
func TestEdgeCase_NewDBOnExistingPath(t *testing.T) {
	path := newTempPath(t)

	pm := mustNewDB(t, path)
	mustClose(t, pm)

	_, err := NewDB(path)
	if err == nil {
		t.Error("NewDB on path that already has a database should return error")
	}
}

// Allocating many pages must never produce duplicate page IDs.
func TestAllocatePage_NoDuplicateIDs(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	seen := make(map[uint32]bool)
	for i := 0; i < 20; i++ {
		p, err := pm.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage[%d]: %v", i, err)
		}
		id := p.GetPageId()
		if seen[id] {
			t.Errorf("duplicate page ID %d at iteration %d", id, i)
		}
		seen[id] = true
	}
}

// FreePage then AllocatePage must not increase the total page count.
func TestFreePage_ReuseDoesNotInflatePageCount(t *testing.T) {
	path := newTempPath(t)
	pm := mustNewDB(t, path)
	defer pm.Close()

	p1 := mustAllocate(t, pm)
	_ = p1
	p2 := mustAllocate(t, pm)
	countBefore := metaOf(t, pm).GetMetaPageCount()

	if err := pm.FreePage(p2.GetPageId()); err != nil {
		t.Fatalf("FreePage: %v", err)
	}

	if _, err := pm.AllocatePage(); err != nil {
		t.Fatalf("AllocatePage after free: %v", err)
	}

	countAfter := metaOf(t, pm).GetMetaPageCount()
	if countAfter != countBefore {
		t.Errorf("page count changed from %d to %d after free+alloc (should stay same)", countBefore, countAfter)
	}
}

// Meta page changes (page count, free list) must persist across Close + OpenDB.
func TestMetaState_PersistsAcrossRestart(t *testing.T) {
	path := newTempPath(t)

	pm, err := NewDB(path)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}

	// Allocate 3 pages so count becomes 4.
	for i := 0; i < 3; i++ {
		if _, err := pm.AllocatePage(); err != nil {
			t.Fatalf("AllocatePage[%d]: %v", i, err)
		}
	}
	wantCount := metaOf(t, pm).GetMetaPageCount()
	mustClose(t, pm)

	pm2 := mustOpenDB(t, path)
	defer pm2.Close()

	if got := metaOf(t, pm2).GetMetaPageCount(); got != wantCount {
		t.Errorf("page count after reopen = %d, want %d", got, wantCount)
	}
}
