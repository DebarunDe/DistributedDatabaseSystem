package pagemanager

import (
	"container/list"
	"errors"
	"fmt"
	"testing"
)

// ============================================================
// Test infrastructure
// ============================================================

// spyDisk is a fake PageManager that intercepts ReadPage and WritePage.
// Optional function fields override the default behaviour of each method.
// Methods with no override panic to catch accidental calls.
type spyDisk struct {
	pages         map[uint32]*Page
	readCount     int
	writeCount    int
	writeFn       func(*Page) error
	readFn        func(uint32) (*Page, error)
	allocateFn    func() (*Page, error)
	allocateCount int
	freeFn        func(uint32) error
	freeCount     int
	freedPages    []uint32
	getRootFn     func() uint32
	setRootFn     func(uint32) error
	setRootCount  int
	lastRootSet   uint32
	closeFn       func() error
	closeCalled   bool
	deleteFn      func() error
	deleteCalled  bool
}

func newSpyDisk(pages ...*Page) *spyDisk {
	s := &spyDisk{pages: make(map[uint32]*Page)}
	for _, p := range pages {
		cp := *p
		s.pages[p.GetPageId()] = &cp
	}
	return s
}

func (s *spyDisk) ReadPage(id uint32) (*Page, error) {
	s.readCount++
	if s.readFn != nil {
		return s.readFn(id)
	}
	if p, ok := s.pages[id]; ok {
		cp := *p
		return &cp, nil
	}
	return nil, fmt.Errorf("spy: page %d not found", id)
}

func (s *spyDisk) WritePage(p *Page) error {
	if s.writeFn != nil {
		return s.writeFn(p)
	}
	s.writeCount++
	cp := *p
	s.pages[p.GetPageId()] = &cp
	return nil
}

func (s *spyDisk) AllocatePage() (*Page, error) {
	s.allocateCount++
	if s.allocateFn != nil {
		return s.allocateFn()
	}
	panic("spyDisk.AllocatePage not implemented")
}

func (s *spyDisk) FreePage(id uint32) error {
	s.freeCount++
	s.freedPages = append(s.freedPages, id)
	if s.freeFn != nil {
		return s.freeFn(id)
	}
	panic("spyDisk.FreePage not implemented")
}

func (s *spyDisk) GetRootPageId() uint32 {
	if s.getRootFn != nil {
		return s.getRootFn()
	}
	panic("spyDisk.GetRootPageId not implemented")
}

func (s *spyDisk) SetRootPageId(id uint32) error {
	s.setRootCount++
	s.lastRootSet = id
	if s.setRootFn != nil {
		return s.setRootFn(id)
	}
	panic("spyDisk.SetRootPageId not implemented")
}

func (s *spyDisk) Close() error {
	s.closeCalled = true
	if s.closeFn != nil {
		return s.closeFn()
	}
	panic("spyDisk.Close not implemented")
}

func (s *spyDisk) Delete() error {
	s.deleteCalled = true
	if s.deleteFn != nil {
		return s.deleteFn()
	}
	panic("spyDisk.Delete not implemented")
}

// newBP creates a BufferPoolImpl backed by the given PageManager.
func newBP(disk PageManager, size int) *BufferPoolImpl {
	return &BufferPoolImpl{
		disk:      disk,
		cache:     make(map[uint32]*CacheValue),
		cacheSize: size,
		lru:       list.New(),
	}
}

// makePage returns a bare Page with the given ID.
func makePage(id uint32) *Page {
	p := &Page{}
	p.setPageId(id)
	return p
}

// markByte sets a recognisable sentinel byte in the payload area of a page.
func markByte(p *Page, v byte) {
	p.Data[CommonHeaderSize] = v
}

// lruIDs returns the page IDs in the LRU list from front (MRU) to back (LRU).
func lruIDs(bp *BufferPoolImpl) []uint32 {
	ids := make([]uint32, 0, bp.lru.Len())
	for e := bp.lru.Front(); e != nil; e = e.Next() {
		ids = append(ids, e.Value.(uint32))
	}
	return ids
}

// ============================================================
// Evict
// ============================================================

// Evict must be a no-op when the cache is below capacity.
func TestEvict_NoEvictionUnderCapacity(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2))
	bp := newBP(disk, 3)

	for _, id := range []uint32{1, 2} {
		if _, err := bp.ReadPage(id); err != nil {
			t.Fatalf("ReadPage(%d): %v", id, err)
		}
	}

	if got := bp.lru.Len(); got != 2 {
		t.Errorf("LRU len = %d after 2 reads into size-3 pool, want 2", got)
	}
	if got := len(bp.cache); got != 2 {
		t.Errorf("cache size = %d, want 2", got)
	}
}

// Evict must be a no-op when the cache is exactly at capacity (Len == cacheSize).
func TestEvict_NoEvictionAtCapacity(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	bp := newBP(disk, 3)

	for _, id := range []uint32{1, 2, 3} {
		if _, err := bp.ReadPage(id); err != nil {
			t.Fatalf("ReadPage(%d): %v", id, err)
		}
	}

	if got := bp.lru.Len(); got != 3 {
		t.Errorf("LRU len = %d after filling size-3 pool, want 3", got)
	}
	if got := len(bp.cache); got != 3 {
		t.Errorf("cache size = %d, want 3", got)
	}
}

// Adding a page beyond cacheSize must evict exactly one entry.
func TestEvict_EvictsWhenOverCapacity(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3), makePage(4))
	bp := newBP(disk, 3)

	for _, id := range []uint32{1, 2, 3, 4} {
		if _, err := bp.ReadPage(id); err != nil {
			t.Fatalf("ReadPage(%d): %v", id, err)
		}
	}

	if got := bp.lru.Len(); got != 3 {
		t.Errorf("LRU len = %d after 4 reads into size-3 pool, want 3", got)
	}
	if got := len(bp.cache); got != 3 {
		t.Errorf("cache size = %d, want 3", got)
	}
}

// Evict must remove the page at the back of the LRU list, not a recently accessed one.
func TestEvict_EvictsLRU(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3), makePage(4))
	bp := newBP(disk, 3)

	// Load 1, 2, 3 → LRU order (front→back): 3, 2, 1
	for _, id := range []uint32{1, 2, 3} {
		if _, err := bp.ReadPage(id); err != nil {
			t.Fatalf("ReadPage(%d): %v", id, err)
		}
	}

	// Re-access page 1; LRU order becomes: 1, 3, 2 → page 2 is now LRU.
	if _, err := bp.ReadPage(1); err != nil {
		t.Fatalf("ReadPage(1) second time: %v", err)
	}

	// Adding page 4 must evict page 2 (LRU), not pages 1 or 3.
	if _, err := bp.ReadPage(4); err != nil {
		t.Fatalf("ReadPage(4): %v", err)
	}

	if _, exists := bp.cache[2]; exists {
		t.Error("page 2 (LRU) should have been evicted")
	}
	for _, id := range []uint32{1, 3, 4} {
		if _, exists := bp.cache[id]; !exists {
			t.Errorf("page %d should still be in cache", id)
		}
	}
}

// Evicting a dirty page must call disk.WritePage exactly once for that page.
func TestEvict_DirtyPageFlushedToDisk(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	bp := newBP(disk, 2)

	// ReadPage(1) then mark dirty — WritePage requires the page to be in cache.
	if _, err := bp.ReadPage(1); err != nil {
		t.Fatalf("ReadPage(1): %v", err)
	}
	if err := bp.WritePage(makePage(1)); err != nil {
		t.Fatalf("WritePage(1): %v", err)
	}
	// ReadPage(2) → cache: {1:dirty, 2:clean}. At capacity, no eviction.
	if _, err := bp.ReadPage(2); err != nil {
		t.Fatalf("ReadPage(2): %v", err)
	}

	writesBefore := disk.writeCount

	// ReadPage(3) → triggers eviction of page 1 (LRU, dirty) → one disk write.
	if _, err := bp.ReadPage(3); err != nil {
		t.Fatalf("ReadPage(3): %v", err)
	}

	if delta := disk.writeCount - writesBefore; delta != 1 {
		t.Errorf("disk.writeCount delta = %d after evicting dirty page, want 1", delta)
	}
	if _, exists := bp.cache[1]; exists {
		t.Error("page 1 should have been evicted from cache")
	}
}

// Evicting a clean page must NOT call disk.WritePage.
func TestEvict_CleanPageNotFlushedToDisk(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	bp := newBP(disk, 2)

	if _, err := bp.ReadPage(1); err != nil {
		t.Fatalf("ReadPage(1): %v", err)
	}
	if _, err := bp.ReadPage(2); err != nil {
		t.Fatalf("ReadPage(2): %v", err)
	}

	writesBefore := disk.writeCount

	// ReadPage(3) → evicts page 1 (LRU, clean) → no disk write.
	if _, err := bp.ReadPage(3); err != nil {
		t.Fatalf("ReadPage(3): %v", err)
	}

	if delta := disk.writeCount - writesBefore; delta != 0 {
		t.Errorf("disk.writeCount delta = %d after evicting clean page, want 0", delta)
	}
}

// If disk.WritePage returns an error while evicting a dirty page, the error must
// propagate. Because eviction happens before the new page is loaded, page 3 is
// never inserted into the cache or LRU — no rollback is required.
func TestEvict_DiskWriteError_EvictionAborted(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	bp := newBP(disk, 2)

	// Load page 1, mark it dirty; then fill cache with page 2.
	if _, err := bp.ReadPage(1); err != nil {
		t.Fatalf("ReadPage(1): %v", err)
	}
	if err := bp.WritePage(makePage(1)); err != nil {
		t.Fatalf("WritePage(1): %v", err)
	}
	if _, err := bp.ReadPage(2); err != nil {
		t.Fatalf("ReadPage(2): %v", err)
	}

	diskErr := errors.New("disk full")
	disk.writeFn = func(*Page) error { return diskErr }

	// ReadPage(3) triggers eviction of dirty page 1 before page 3 is loaded;
	// the disk write fails so the error is returned immediately.
	_, err := bp.ReadPage(3)
	if err == nil {
		t.Fatal("ReadPage should propagate the eviction error")
	}

	// Page 3 was never added — it must be absent from both cache and LRU.
	if _, exists := bp.cache[3]; exists {
		t.Error("page 3 should not be in cache when eviction failed before it was loaded")
	}
	for _, id := range lruIDs(bp) {
		if id == 3 {
			t.Error("page 3 should not be in LRU when eviction failed before it was loaded")
		}
	}

	// The dirty page that could not be flushed must still be in cache.
	if cv, exists := bp.cache[1]; !exists {
		t.Error("dirty page 1 should still be in cache after failed eviction")
	} else if !cv.modified {
		t.Error("dirty page 1 should still be marked modified")
	}
}

// ============================================================
// BufferPool ReadPage
// ============================================================

// A cache miss must load the page from disk and insert it into the cache.
func TestBufferPool_ReadPage_CacheMiss_LoadsFromDisk(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	bp := newBP(disk, 3)

	got, err := bp.ReadPage(1)
	if err != nil {
		t.Fatalf("ReadPage(1): %v", err)
	}
	if got.GetPageId() != 1 {
		t.Errorf("returned page ID = %d, want 1", got.GetPageId())
	}
	if _, exists := bp.cache[1]; !exists {
		t.Error("page 1 should be in cache after cache miss")
	}
	if disk.readCount != 1 {
		t.Errorf("disk.readCount = %d, want 1", disk.readCount)
	}
}

// A cache hit must return the same *Page pointer and must not read from disk again.
func TestBufferPool_ReadPage_CacheHit_ReturnsCachedPage(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	bp := newBP(disk, 3)

	first, _ := bp.ReadPage(1)
	readsBefore := disk.readCount

	second, err := bp.ReadPage(1)
	if err != nil {
		t.Fatalf("second ReadPage(1): %v", err)
	}
	if second != first {
		t.Error("cache hit should return the same *Page pointer")
	}
	if disk.readCount != readsBefore {
		t.Errorf("disk was read on cache hit: readCount before=%d after=%d", readsBefore, disk.readCount)
	}
}

// A cache hit must move the accessed page to the front of the LRU list.
func TestBufferPool_ReadPage_CacheHit_MovesToFront(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3), makePage(4))
	bp := newBP(disk, 3)

	// Load 1, 2, 3 → LRU order (front→back): 3, 2, 1
	for _, id := range []uint32{1, 2, 3} {
		_, _ = bp.ReadPage(id)
	}

	// Re-read page 1 → LRU order becomes 1, 3, 2 (page 2 is LRU).
	_, _ = bp.ReadPage(1)

	// Reading page 4 should evict page 2 (new LRU).
	_, _ = bp.ReadPage(4)

	if _, exists := bp.cache[2]; exists {
		t.Error("page 2 (LRU after re-read of page 1) should have been evicted")
	}
	if _, exists := bp.cache[1]; !exists {
		t.Error("page 1 (recently accessed) should still be in cache")
	}
}

// A disk read error must be propagated and the page must not be left in the cache.
func TestBufferPool_ReadPage_DiskError(t *testing.T) {
	disk := newSpyDisk() // empty: every read fails
	bp := newBP(disk, 3)

	_, err := bp.ReadPage(99)
	if err == nil {
		t.Error("ReadPage should return error when disk read fails")
	}
	if _, exists := bp.cache[99]; exists {
		t.Error("failed page should not be left in cache")
	}
	if bp.lru.Len() != 0 {
		t.Errorf("LRU should be empty after failed read, got len=%d", bp.lru.Len())
	}
}

// ============================================================
// BufferPool WritePage
// ============================================================

// WritePage on a page that is not in cache must return an error and leave the cache unchanged.
func TestBufferPool_WritePage_CacheMiss_ReturnsError(t *testing.T) {
	disk := newSpyDisk()
	bp := newBP(disk, 3)

	err := bp.WritePage(makePage(1))
	if err == nil {
		t.Error("WritePage should return error when the page is not in cache")
	}
	if _, exists := bp.cache[1]; exists {
		t.Error("page 1 should not be in cache after a failed WritePage")
	}
	if bp.lru.Len() != 0 {
		t.Errorf("LRU should be empty after failed WritePage, got len=%d", bp.lru.Len())
	}
}

// WritePage on a cached page must mark it dirty. The caller modifies the *Page
// returned by ReadPage in place; WritePage just records the dirty status.
func TestBufferPool_WritePage_CacheHit_UpdatesCacheEntry(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	bp := newBP(disk, 3)

	page, _ := bp.ReadPage(1) // brings page 1 in as clean
	markByte(page, 0xAB)      // modify the cached page in place

	if err := bp.WritePage(page); err != nil {
		t.Fatalf("WritePage(1): %v", err)
	}

	cv := bp.cache[1]
	if !cv.modified {
		t.Error("page 1 should be dirty after WritePage on a cached page")
	}
	if cv.page.Data[CommonHeaderSize] != 0xAB {
		t.Errorf("cached page data[CommonHeaderSize] = %#x, want 0xAB", cv.page.Data[CommonHeaderSize])
	}
}

// WritePage on a cached page must move it to the front of the LRU list.
func TestBufferPool_WritePage_CacheHit_MovesToFront(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3), makePage(4))
	bp := newBP(disk, 3)

	// Fill cache. LRU order (front→back): 3, 2, 1
	for _, id := range []uint32{1, 2, 3} {
		_, _ = bp.ReadPage(id)
	}

	// Write page 1 → should move it to front; page 2 becomes LRU.
	_ = bp.WritePage(makePage(1))

	// Reading page 4 must evict page 2 (LRU).
	_, _ = bp.ReadPage(4)

	if _, exists := bp.cache[2]; exists {
		t.Error("page 2 should have been evicted; WritePage(1) should have moved it to front")
	}
	if _, exists := bp.cache[1]; !exists {
		t.Error("page 1 should still be in cache after being written")
	}
}

// A page dirtied via in-place modification and WritePage must be flushed to disk on eviction.
func TestBufferPool_WritePage_DirtyPageFlushedOnEviction(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	bp := newBP(disk, 2)

	// Load page 1, modify in place, mark dirty.
	p1, err := bp.ReadPage(1)
	if err != nil {
		t.Fatalf("ReadPage(1): %v", err)
	}
	markByte(p1, 0xFF)
	if err := bp.WritePage(p1); err != nil {
		t.Fatalf("WritePage(1): %v", err)
	}
	if _, err := bp.ReadPage(2); err != nil {
		t.Fatalf("ReadPage(2): %v", err)
	}

	writesBefore := disk.writeCount

	// ReadPage(3) evicts dirty page 1 → spy.WritePage called once.
	if _, err := bp.ReadPage(3); err != nil {
		t.Fatalf("ReadPage(3): %v", err)
	}

	if delta := disk.writeCount - writesBefore; delta != 1 {
		t.Errorf("disk.writeCount delta = %d on dirty eviction, want 1", delta)
	}
	if disk.pages[1].Data[CommonHeaderSize] != 0xFF {
		t.Errorf("flushed page 1 data[CommonHeaderSize] = %#x, want 0xFF", disk.pages[1].Data[CommonHeaderSize])
	}
}

// ============================================================
// Integration
// ============================================================

// A page modified in place and marked dirty via WritePage must be readable from
// cache with the modification visible, without a disk trip.
func TestBufferPool_ReadAfterWrite_ReturnsSameData(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	bp := newBP(disk, 3)

	page, err := bp.ReadPage(1)
	if err != nil {
		t.Fatalf("ReadPage(1): %v", err)
	}
	markByte(page, 0x42)
	if err := bp.WritePage(page); err != nil {
		t.Fatalf("WritePage(1): %v", err)
	}

	readsBefore := disk.readCount
	got, err := bp.ReadPage(1)
	if err != nil {
		t.Fatalf("ReadPage(1) second time: %v", err)
	}
	if got.Data[CommonHeaderSize] != 0x42 {
		t.Errorf("data[CommonHeaderSize] = %#x, want 0x42", got.Data[CommonHeaderSize])
	}
	if disk.readCount != readsBefore {
		t.Error("ReadPage after WritePage should be served from cache, not disk")
	}
}

// After a dirty page is evicted it must be readable from disk with its written data intact.
func TestBufferPool_DirtyEviction_DataPersists(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	bp := newBP(disk, 2)

	// Load page 1, modify in place, mark dirty.
	p1, err := bp.ReadPage(1)
	if err != nil {
		t.Fatalf("ReadPage(1): %v", err)
	}
	markByte(p1, 0xDE)
	if err := bp.WritePage(p1); err != nil {
		t.Fatalf("WritePage(1): %v", err)
	}
	if _, err := bp.ReadPage(2); err != nil {
		t.Fatalf("ReadPage(2): %v", err)
	}

	// ReadPage(3) evicts dirty page 1 → flushed to disk.
	if _, err := bp.ReadPage(3); err != nil {
		t.Fatalf("ReadPage(3): %v", err)
	}
	if _, exists := bp.cache[1]; exists {
		t.Fatal("page 1 should have been evicted")
	}

	// Re-reading page 1 must come from disk and carry the persisted sentinel.
	got, err := bp.ReadPage(1)
	if err != nil {
		t.Fatalf("ReadPage(1) after eviction: %v", err)
	}
	if got.Data[CommonHeaderSize] != 0xDE {
		t.Errorf("re-read page 1 data[CommonHeaderSize] = %#x, want 0xDE", got.Data[CommonHeaderSize])
	}
}

// The access pattern (not insertion order) must determine which page is evicted.
func TestBufferPool_LRU_AccessPatternDeterminesEviction(t *testing.T) {
	// Insert 1, 2, 3. Then access 1, then 3. Page 2 becomes LRU.
	// Insert 4 → should evict page 2.
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3), makePage(4))
	bp := newBP(disk, 3)

	for _, id := range []uint32{1, 2, 3} {
		_, _ = bp.ReadPage(id)
	}
	_, _ = bp.ReadPage(1) // LRU order → 1, 3, 2
	_, _ = bp.ReadPage(3) // LRU order → 3, 1, 2 (page 2 is LRU)
	_, _ = bp.ReadPage(4) // evicts page 2

	if _, exists := bp.cache[2]; exists {
		t.Error("page 2 (LRU) should have been evicted")
	}
	for _, id := range []uint32{1, 3, 4} {
		if _, exists := bp.cache[id]; !exists {
			t.Errorf("page %d should still be in cache", id)
		}
	}
}

// A size-1 pool must evict the single cached page every time a new page is read.
func TestBufferPool_SizeOne_EvictsOnEveryNewPage(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	bp := newBP(disk, 1)

	_, _ = bp.ReadPage(1)
	if got := len(bp.cache); got != 1 {
		t.Errorf("after ReadPage(1): cache size = %d, want 1", got)
	}

	_, _ = bp.ReadPage(2) // evicts 1
	if _, exists := bp.cache[1]; exists {
		t.Error("page 1 should have been evicted by size-1 pool")
	}
	if _, exists := bp.cache[2]; !exists {
		t.Error("page 2 should be in cache")
	}

	_, _ = bp.ReadPage(3) // evicts 2
	if _, exists := bp.cache[2]; exists {
		t.Error("page 2 should have been evicted by size-1 pool")
	}
	if _, exists := bp.cache[3]; !exists {
		t.Error("page 3 should be in cache")
	}
}

// Reading n+1 distinct pages into a size-n pool must always retain the two most recent.
func TestBufferPool_MultipleEvictions_LRUIntact(t *testing.T) {
	const cacheSize = 2
	ids := []uint32{1, 2, 3, 4, 5}

	pages := make([]*Page, len(ids))
	for i, id := range ids {
		pages[i] = makePage(id)
	}
	disk := newSpyDisk(pages...)
	bp := newBP(disk, cacheSize)

	for i, id := range ids {
		if _, err := bp.ReadPage(id); err != nil {
			t.Fatalf("ReadPage(%d): %v", id, err)
		}

		// Current page must be in cache.
		if _, exists := bp.cache[id]; !exists {
			t.Errorf("after ReadPage(%d): page %d should be in cache", id, id)
		}
		// Previous page must still be in cache (it was MRU before current).
		if i >= 1 {
			prev := ids[i-1]
			if _, exists := bp.cache[prev]; !exists {
				t.Errorf("after ReadPage(%d): previous page %d should still be in cache", id, prev)
			}
		}
		// The page two steps back must have been evicted.
		if i >= 2 {
			evicted := ids[i-2]
			if _, exists := bp.cache[evicted]; exists {
				t.Errorf("after ReadPage(%d): page %d should have been evicted", id, evicted)
			}
		}
	}
}

// Cache/LRU lengths must always stay in sync across a mixed read/write workload.
func TestBufferPool_CacheAndLRUAlwaysInSync(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3), makePage(4), makePage(5))
	bp := newBP(disk, 3)

	checkSync := func(t *testing.T, label string) {
		t.Helper()
		if bp.lru.Len() != len(bp.cache) {
			t.Errorf("%s: LRU len=%d != cache len=%d", label, bp.lru.Len(), len(bp.cache))
		}
		for e := bp.lru.Front(); e != nil; e = e.Next() {
			id := e.Value.(uint32)
			if _, exists := bp.cache[id]; !exists {
				t.Errorf("%s: LRU contains page %d but it is absent from cache", label, id)
			}
		}
	}

	ops := []struct {
		write bool
		id    uint32
	}{
		{false, 1}, {false, 2}, {false, 3},
		{true, 1}, {false, 4}, {true, 3},
		{false, 5}, {false, 2}, {true, 4},
	}

	for _, op := range ops {
		if op.write {
			_ = bp.WritePage(makePage(op.id))
			checkSync(t, fmt.Sprintf("after WritePage(%d)", op.id))
		} else {
			_, _ = bp.ReadPage(op.id)
			checkSync(t, fmt.Sprintf("after ReadPage(%d)", op.id))
		}
	}
}

// ============================================================
// BufferPool ReadPage — extended coverage
// ============================================================

// A page loaded on a cache miss must be inserted with modified=false so that
// it is not unnecessarily flushed to disk when evicted.
func TestBufferPool_ReadPage_CacheMiss_NotMarkedDirty(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	bp := newBP(disk, 3)

	if _, err := bp.ReadPage(1); err != nil {
		t.Fatalf("ReadPage(1): %v", err)
	}

	cv, exists := bp.cache[1]
	if !exists {
		t.Fatal("page 1 should be in cache")
	}
	if cv.modified {
		t.Error("page loaded on cache miss should not be marked dirty")
	}
}

// After a cache miss the new page must be at the front of the LRU list (most recently used).
func TestBufferPool_ReadPage_CacheMiss_NewPageAtFrontOfLRU(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2))
	bp := newBP(disk, 3)

	_, _ = bp.ReadPage(1)
	_, _ = bp.ReadPage(2)

	if got := bp.lru.Front().Value.(uint32); got != 2 {
		t.Errorf("LRU front = %d after ReadPage(2), want 2", got)
	}
	if got := bp.lru.Back().Value.(uint32); got != 1 {
		t.Errorf("LRU back = %d after ReadPage(2), want 1", got)
	}
}

// Reading the same page N times must result in exactly one disk read — all
// subsequent accesses must be served from cache.
func TestBufferPool_ReadPage_MultipleHits_OnlyOneDiskRead(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	bp := newBP(disk, 3)

	for i := 0; i < 5; i++ {
		if _, err := bp.ReadPage(1); err != nil {
			t.Fatalf("ReadPage(1) iteration %d: %v", i, err)
		}
	}

	if disk.readCount != 1 {
		t.Errorf("disk.readCount = %d after 5 reads of same page, want 1", disk.readCount)
	}
	if bp.lru.Len() != 1 {
		t.Errorf("LRU len = %d after repeated reads of page 1, want 1 (no duplicate entries)", bp.lru.Len())
	}
}

// Repeated reads of the same page must not create duplicate entries in the LRU list.
func TestBufferPool_ReadPage_RepeatedHits_NoDuplicateLRUEntries(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	bp := newBP(disk, 5)

	for _, id := range []uint32{1, 2, 3, 1, 2, 1} {
		if _, err := bp.ReadPage(id); err != nil {
			t.Fatalf("ReadPage(%d): %v", id, err)
		}
	}

	if got := bp.lru.Len(); got != 3 {
		t.Errorf("LRU len = %d after reading 3 distinct pages repeatedly, want 3", got)
	}
	if got := len(bp.cache); got != 3 {
		t.Errorf("cache size = %d, want 3", got)
	}
}

// After a page is evicted, re-reading it must go back to disk.
func TestBufferPool_ReadPage_EvictedPage_HitsDiskAgain(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	bp := newBP(disk, 2)

	_, _ = bp.ReadPage(1) // cache: {1}
	_, _ = bp.ReadPage(2) // cache: {1, 2}
	_, _ = bp.ReadPage(3) // evicts 1; cache: {2, 3}

	if _, exists := bp.cache[1]; exists {
		t.Fatal("page 1 should have been evicted")
	}

	readsBefore := disk.readCount

	// Re-reading page 1 must fetch it from disk again.
	if _, err := bp.ReadPage(1); err != nil {
		t.Fatalf("ReadPage(1) after eviction: %v", err)
	}

	if disk.readCount == readsBefore {
		t.Error("re-reading an evicted page should require a disk read")
	}
	if _, exists := bp.cache[1]; !exists {
		t.Error("re-read page 1 should be in cache again")
	}
}

// A page written via WritePage is in cache as dirty; a subsequent ReadPage must
// serve it from cache without touching disk.
func TestBufferPool_ReadPage_DirtyPage_ServedFromCache(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	bp := newBP(disk, 3)

	// Load, modify in place, mark dirty.
	page, _ := bp.ReadPage(1)
	markByte(page, 0xCC)
	if err := bp.WritePage(page); err != nil {
		t.Fatalf("WritePage(1): %v", err)
	}

	readsBefore := disk.readCount

	got, err := bp.ReadPage(1)
	if err != nil {
		t.Fatalf("ReadPage(1): %v", err)
	}
	if disk.readCount != readsBefore {
		t.Error("ReadPage on a dirty cached page should not read from disk")
	}
	if got.Data[CommonHeaderSize] != 0xCC {
		t.Errorf("ReadPage returned data[CommonHeaderSize]=%#x, want 0xCC (the written value)", got.Data[CommonHeaderSize])
	}
}

// The LRU list must reflect insertion order: the most recently loaded page is at
// the front and the least recently loaded page is at the back.
func TestBufferPool_ReadPage_LRUOrder_ReflectsInsertionOrder(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	bp := newBP(disk, 5)

	for _, id := range []uint32{1, 2, 3} {
		if _, err := bp.ReadPage(id); err != nil {
			t.Fatalf("ReadPage(%d): %v", id, err)
		}
	}

	ids := lruIDs(bp)
	want := []uint32{3, 2, 1}
	if len(ids) != len(want) {
		t.Fatalf("LRU len = %d, want %d", len(ids), len(want))
	}
	for i, id := range ids {
		if id != want[i] {
			t.Errorf("LRU[%d] = %d, want %d", i, id, want[i])
		}
	}
}

// When eviction of a dirty page fails, ReadPage returns the error before the new
// page is ever loaded or inserted. The dirty page stays in cache; LRU and cache
// remain consistent.
func TestBufferPool_ReadPage_FailedEviction_FullRollback(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	bp := newBP(disk, 2)

	// Load page 1 and mark it dirty; fill cache with page 2.
	if _, err := bp.ReadPage(1); err != nil {
		t.Fatalf("ReadPage(1): %v", err)
	}
	if err := bp.WritePage(makePage(1)); err != nil {
		t.Fatalf("WritePage(1): %v", err)
	}
	if _, err := bp.ReadPage(2); err != nil {
		t.Fatalf("ReadPage(2): %v", err)
	}

	disk.writeFn = func(*Page) error { return errors.New("disk error") }

	_, err := bp.ReadPage(3)
	if err == nil {
		t.Fatal("ReadPage should return error when eviction fails")
	}

	// Page 3 was never loaded — it must be absent from both structures.
	if _, exists := bp.cache[3]; exists {
		t.Error("page 3 should not be in cache; eviction failed before it was loaded")
	}
	for _, id := range lruIDs(bp) {
		if id == 3 {
			t.Error("page 3 should not be in LRU; eviction failed before it was loaded")
		}
	}

	// The dirty page that could not be flushed must still be in cache.
	if cv, exists := bp.cache[1]; !exists {
		t.Error("dirty page 1 should still be in cache after failed eviction")
	} else if !cv.modified {
		t.Error("dirty page 1 should still be marked modified")
	}

	// Cache and LRU must be in sync.
	if bp.lru.Len() != len(bp.cache) {
		t.Errorf("LRU len=%d != cache len=%d after failed eviction", bp.lru.Len(), len(bp.cache))
	}
}

// The page data returned on a cache miss must exactly match what the disk provided.
func TestBufferPool_ReadPage_DataIntegrity_MatchesDisk(t *testing.T) {
	p := makePage(7)
	for i := CommonHeaderSize; i < CommonHeaderSize+8; i++ {
		p.Data[i] = byte(i * 3)
	}
	disk := newSpyDisk(p)
	bp := newBP(disk, 3)

	got, err := bp.ReadPage(7)
	if err != nil {
		t.Fatalf("ReadPage(7): %v", err)
	}
	for i := CommonHeaderSize; i < CommonHeaderSize+8; i++ {
		if got.Data[i] != byte(i*3) {
			t.Errorf("data[%d] = %#x, want %#x", i, got.Data[i], byte(i*3))
		}
	}
}

// ReadPage must work correctly for page ID 0.
func TestBufferPool_ReadPage_PageZero(t *testing.T) {
	p0 := makePage(0)
	markByte(p0, 0x55)
	disk := newSpyDisk(p0)
	bp := newBP(disk, 3)

	got, err := bp.ReadPage(0)
	if err != nil {
		t.Fatalf("ReadPage(0): %v", err)
	}
	if got.GetPageId() != 0 {
		t.Errorf("page ID = %d, want 0", got.GetPageId())
	}
	if got.Data[CommonHeaderSize] != 0x55 {
		t.Errorf("data[CommonHeaderSize] = %#x, want 0x55", got.Data[CommonHeaderSize])
	}
}

// ============================================================
// BufferPool WritePage — extended coverage
// ============================================================

// The canonical write pattern: obtain the *Page from ReadPage, modify it in place,
// then call WritePage to mark it dirty. ReadPage must reflect the modification.
func TestBufferPool_WritePage_InPlaceModification(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	bp := newBP(disk, 3)

	page, err := bp.ReadPage(1)
	if err != nil {
		t.Fatalf("ReadPage(1): %v", err)
	}
	page.Data[CommonHeaderSize] = 0x99
	if err := bp.WritePage(page); err != nil {
		t.Fatalf("WritePage(1): %v", err)
	}

	got, err := bp.ReadPage(1)
	if err != nil {
		t.Fatalf("ReadPage(1) second time: %v", err)
	}
	if got != page {
		t.Error("ReadPage should return the same pointer (still in cache)")
	}
	if got.Data[CommonHeaderSize] != 0x99 {
		t.Errorf("data[CommonHeaderSize] = %#x, want 0x99", got.Data[CommonHeaderSize])
	}
	if !bp.cache[1].modified {
		t.Error("page should be marked dirty after WritePage")
	}
}

// A cache-hit write must not trigger eviction or grow the cache.
func TestBufferPool_WritePage_CacheHit_NoEviction(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	bp := newBP(disk, 2)

	// Fill cache to capacity: {1, 2}.
	_, _ = bp.ReadPage(1)
	_, _ = bp.ReadPage(2)

	// Write page 1 again (cache hit). Cache is still at capacity — no eviction should happen.
	if err := bp.WritePage(makePage(1)); err != nil {
		t.Fatalf("WritePage(1) on cache hit: %v", err)
	}

	if got := len(bp.cache); got != 2 {
		t.Errorf("cache size = %d after hit-write on full cache, want 2 (no eviction)", got)
	}
	if got := bp.lru.Len(); got != 2 {
		t.Errorf("LRU len = %d after hit-write on full cache, want 2", got)
	}
	// Page 2 should not have been evicted.
	if _, exists := bp.cache[2]; !exists {
		t.Error("page 2 should still be in cache after a hit-write on page 1")
	}
}

// WritePage does not replace the cached *Page pointer. The caller must use the
// pointer returned by ReadPage and modify it in place.
func TestBufferPool_WritePage_CacheHit_DoesNotReplacePagePointer(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	bp := newBP(disk, 3)

	original, _ := bp.ReadPage(1)

	// Pass a different *Page object with the same ID to WritePage.
	other := makePage(1)
	markByte(other, 0xBE)
	if err := bp.WritePage(other); err != nil {
		t.Fatalf("WritePage(1): %v", err)
	}

	// The cache must still hold the original pointer, not the one passed to WritePage.
	if bp.cache[1].page != original {
		t.Error("WritePage should not replace the cached *Page pointer")
	}
	// The modification on 'other' must NOT be visible through the cache.
	if bp.cache[1].page.Data[CommonHeaderSize] == 0xBE {
		t.Error("cached page should not reflect modifications made to the page passed to WritePage")
	}
	if !bp.cache[1].modified {
		t.Error("page should still be marked dirty")
	}
}

// Writing to a clean (read-only) cached page must mark it dirty.
func TestBufferPool_WritePage_CacheHit_CleanPageBecomesMarkedDirty(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	bp := newBP(disk, 3)

	_, _ = bp.ReadPage(1)
	if bp.cache[1].modified {
		t.Fatal("page 1 should be clean after ReadPage")
	}

	if err := bp.WritePage(makePage(1)); err != nil {
		t.Fatalf("WritePage(1): %v", err)
	}
	if !bp.cache[1].modified {
		t.Error("page 1 should be marked dirty after WritePage on a clean cache entry")
	}
}

// Writing the same page N times must result in exactly one entry in the cache and LRU,
// and the latest in-place modification must be visible.
func TestBufferPool_WritePage_MultipleWrites_SamePage_SingleLRUEntry(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	bp := newBP(disk, 5)

	page, err := bp.ReadPage(1)
	if err != nil {
		t.Fatalf("ReadPage(1): %v", err)
	}
	for i := 0; i < 4; i++ {
		page.Data[CommonHeaderSize] = byte(i)
		if err := bp.WritePage(page); err != nil {
			t.Fatalf("WritePage(1) iteration %d: %v", i, err)
		}
	}

	if got := len(bp.cache); got != 1 {
		t.Errorf("cache size = %d after 4 writes to same page, want 1", got)
	}
	if got := bp.lru.Len(); got != 1 {
		t.Errorf("LRU len = %d after 4 writes to same page, want 1", got)
	}
	if bp.cache[1].page.Data[CommonHeaderSize] != 3 {
		t.Errorf("cached data[CommonHeaderSize] = %d, want 3 (the last in-place modification)", bp.cache[1].page.Data[CommonHeaderSize])
	}
}

// After a page is evicted from the cache, WritePage must return an error for that page
// until it is loaded again via ReadPage.
func TestBufferPool_WritePage_AfterEviction_RequiresReadFirst(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	bp := newBP(disk, 2)

	// Load page 1, mark dirty, fill cache with page 2.
	p1, _ := bp.ReadPage(1)
	_ = bp.WritePage(p1)
	_, _ = bp.ReadPage(2)

	// ReadPage(3) evicts page 1 (LRU, dirty) — flushed to disk.
	_, _ = bp.ReadPage(3)
	if _, exists := bp.cache[1]; exists {
		t.Fatal("page 1 should have been evicted")
	}

	// WritePage for the now-evicted page must return an error.
	if err := bp.WritePage(p1); err == nil {
		t.Error("WritePage should return error for a page that is no longer in cache")
	}

	// Re-loading page 1 must make WritePage succeed again.
	p1reloaded, err := bp.ReadPage(1)
	if err != nil {
		t.Fatalf("ReadPage(1) after eviction: %v", err)
	}
	if err := bp.WritePage(p1reloaded); err != nil {
		t.Errorf("WritePage should succeed after page is re-loaded: %v", err)
	}
}

// The LRU list must correctly reflect the order of writes when mixed with reads.
func TestBufferPool_WritePage_LRUOrder_AfterMixedOps(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	bp := newBP(disk, 5)

	// Load 1, 2, 3 → order (front→back): 3, 2, 1
	_, _ = bp.ReadPage(1)
	_, _ = bp.ReadPage(2)
	_, _ = bp.ReadPage(3)
	// Write 1 (cache hit, move to front) → order: 1, 3, 2
	_ = bp.WritePage(makePage(1))

	want := []uint32{1, 3, 2}
	got := lruIDs(bp)
	if len(got) != len(want) {
		t.Fatalf("LRU len = %d, want %d; order: %v", len(got), len(want), got)
	}
	for i, id := range got {
		if id != want[i] {
			t.Errorf("LRU[%d] = %d, want %d", i, id, want[i])
		}
	}
}

// WritePage must work correctly for page ID 0.
func TestBufferPool_WritePage_PageZero(t *testing.T) {
	disk := newSpyDisk(makePage(0))
	bp := newBP(disk, 3)

	p0, err := bp.ReadPage(0)
	if err != nil {
		t.Fatalf("ReadPage(0): %v", err)
	}
	markByte(p0, 0x77)
	if err := bp.WritePage(p0); err != nil {
		t.Fatalf("WritePage(0): %v", err)
	}

	cv, exists := bp.cache[0]
	if !exists {
		t.Fatal("page 0 should be in cache")
	}
	if !cv.modified {
		t.Error("page 0 should be marked dirty")
	}
	if cv.page.Data[CommonHeaderSize] != 0x77 {
		t.Errorf("cached page 0 data[CommonHeaderSize] = %#x, want 0x77", cv.page.Data[CommonHeaderSize])
	}
}

// ============================================================
// BufferPool AllocatePage
// ============================================================

// AllocatePage must add the new page to the cache as clean and place it at the
// front of the LRU list.
func TestBufferPool_AllocatePage_AddsToCache(t *testing.T) {
	disk := newSpyDisk()
	disk.allocateFn = func() (*Page, error) { return makePage(1), nil }
	bp := newBP(disk, 3)

	got, err := bp.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	if got.GetPageId() != 1 {
		t.Errorf("returned page ID = %d, want 1", got.GetPageId())
	}
	cv, exists := bp.cache[1]
	if !exists {
		t.Fatal("allocated page should be in cache")
	}
	if cv.modified {
		t.Error("newly allocated page should not be marked dirty")
	}
	if bp.lru.Front() == nil || bp.lru.Front().Value.(uint32) != 1 {
		t.Error("newly allocated page should be at front of LRU")
	}
	if disk.allocateCount != 1 {
		t.Errorf("disk.AllocatePage called %d times, want 1", disk.allocateCount)
	}
}

// AllocatePage must trigger eviction of the LRU page when the cache is at capacity.
func TestBufferPool_AllocatePage_TriggersEviction(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2))
	disk.allocateFn = func() (*Page, error) { return makePage(3), nil }
	bp := newBP(disk, 2)

	// Fill cache: {1 (LRU), 2 (MRU)}
	_, _ = bp.ReadPage(1)
	_, _ = bp.ReadPage(2)

	if _, err := bp.AllocatePage(); err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}

	if got := len(bp.cache); got != 2 {
		t.Errorf("cache size = %d after AllocatePage on full pool, want 2", got)
	}
	if _, exists := bp.cache[1]; exists {
		t.Error("page 1 (LRU) should have been evicted")
	}
	if _, exists := bp.cache[3]; !exists {
		t.Error("newly allocated page 3 should be in cache")
	}
}

// AllocatePage must propagate a disk allocation error and leave the cache untouched.
func TestBufferPool_AllocatePage_DiskError_ReturnsError(t *testing.T) {
	disk := newSpyDisk()
	disk.allocateFn = func() (*Page, error) { return nil, errors.New("disk full") }
	bp := newBP(disk, 3)

	_, err := bp.AllocatePage()
	if err == nil {
		t.Fatal("AllocatePage should return error when disk allocation fails")
	}
	if len(bp.cache) != 0 {
		t.Errorf("cache should be empty after failed AllocatePage, got %d entries", len(bp.cache))
	}
	if bp.lru.Len() != 0 {
		t.Errorf("LRU should be empty after failed AllocatePage, got len=%d", bp.lru.Len())
	}
}

// When eviction of a dirty page fails during AllocatePage, the just-allocated
// page must be freed via disk.FreePage (rollback) and the error must be returned.
func TestBufferPool_AllocatePage_EvictionFails_Rollback(t *testing.T) {
	const allocatedID = uint32(3)
	disk := newSpyDisk(makePage(1), makePage(2))
	disk.allocateFn = func() (*Page, error) { return makePage(allocatedID), nil }
	bp := newBP(disk, 2)

	// Fill cache: page 1 (LRU, dirty), page 2 (MRU).
	_, _ = bp.ReadPage(1)
	_ = bp.WritePage(makePage(1))
	_, _ = bp.ReadPage(2)

	// Make disk writes fail so eviction of dirty page 1 fails.
	disk.writeFn = func(*Page) error { return errors.New("disk error") }
	disk.freeFn = func(uint32) error { return nil }

	_, err := bp.AllocatePage()
	if err == nil {
		t.Fatal("AllocatePage should return error when eviction fails")
	}

	// The newly allocated page must have been freed (rolled back).
	if disk.freeCount != 1 {
		t.Errorf("disk.FreePage called %d times after eviction failure, want 1", disk.freeCount)
	}
	if len(disk.freedPages) == 0 || disk.freedPages[0] != allocatedID {
		t.Errorf("freed page IDs = %v, want [%d]", disk.freedPages, allocatedID)
	}
	if _, exists := bp.cache[allocatedID]; exists {
		t.Error("rolled-back page should not be in cache")
	}
}

// When eviction fails and the rollback FreePage also fails, both errors must be
// reported in the returned error and the page must remain absent from cache.
func TestBufferPool_AllocatePage_EvictionFails_FreeAlsoFails(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2))
	disk.allocateFn = func() (*Page, error) { return makePage(3), nil }
	bp := newBP(disk, 2)

	_, _ = bp.ReadPage(1)
	_ = bp.WritePage(makePage(1))
	_, _ = bp.ReadPage(2)

	disk.writeFn = func(*Page) error { return errors.New("disk write error") }
	disk.freeFn = func(uint32) error { return errors.New("disk free error") }

	_, err := bp.AllocatePage()
	if err == nil {
		t.Fatal("AllocatePage should return error when both eviction and rollback fail")
	}
	if _, exists := bp.cache[3]; exists {
		t.Error("page 3 should not be in cache after double failure")
	}
}

// AllocatePage on an empty pool must add the page without triggering any eviction.
func TestBufferPool_AllocatePage_EmptyPool_NoEviction(t *testing.T) {
	disk := newSpyDisk()
	disk.allocateFn = func() (*Page, error) { return makePage(1), nil }
	bp := newBP(disk, 3)

	if _, err := bp.AllocatePage(); err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	if got := len(bp.cache); got != 1 {
		t.Errorf("cache size = %d, want 1", got)
	}
	if got := bp.lru.Len(); got != 1 {
		t.Errorf("LRU len = %d, want 1", got)
	}
}

// Cache and LRU must stay in sync after AllocatePage causes an eviction.
func TestBufferPool_AllocatePage_CacheAndLRUConsistentAfterEviction(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	nextID := uint32(4)
	disk.allocateFn = func() (*Page, error) {
		p := makePage(nextID)
		nextID++
		return p, nil
	}
	bp := newBP(disk, 3)

	for _, id := range []uint32{1, 2, 3} {
		_, _ = bp.ReadPage(id)
	}
	for i := 0; i < 3; i++ {
		if _, err := bp.AllocatePage(); err != nil {
			t.Fatalf("AllocatePage iteration %d: %v", i, err)
		}
		if bp.lru.Len() != len(bp.cache) {
			t.Errorf("after AllocatePage %d: LRU len=%d != cache len=%d", i, bp.lru.Len(), len(bp.cache))
		}
		if got := len(bp.cache); got != 3 {
			t.Errorf("after AllocatePage %d: cache size = %d, want 3", i, got)
		}
	}
}

// ============================================================
// BufferPool FreePage
// ============================================================

// FreePage must remove a clean cached page from both the cache and LRU.
func TestBufferPool_FreePage_CachedCleanPage_RemovedFromCache(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	disk.freeFn = func(uint32) error { return nil }
	bp := newBP(disk, 3)

	_, _ = bp.ReadPage(1)

	if err := bp.FreePage(1); err != nil {
		t.Fatalf("FreePage(1): %v", err)
	}
	if _, exists := bp.cache[1]; exists {
		t.Error("page 1 should be removed from cache after FreePage")
	}
	if bp.lru.Len() != 0 {
		t.Errorf("LRU len = %d, want 0 after freeing the only cached page", bp.lru.Len())
	}
}

// FreePage must remove a dirty cached page without flushing it to disk first.
func TestBufferPool_FreePage_DirtyPage_RemovedWithoutFlush(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	disk.freeFn = func(uint32) error { return nil }
	bp := newBP(disk, 3)

	p, _ := bp.ReadPage(1)
	markByte(p, 0xFF)
	_ = bp.WritePage(p)

	writesBefore := disk.writeCount

	if err := bp.FreePage(1); err != nil {
		t.Fatalf("FreePage(1): %v", err)
	}
	if delta := disk.writeCount - writesBefore; delta != 0 {
		t.Errorf("disk.writeCount delta = %d after FreePage on dirty page, want 0 (no flush)", delta)
	}
	if _, exists := bp.cache[1]; exists {
		t.Error("dirty page 1 should be removed from cache after FreePage")
	}
}

// FreePage on a page that is not in cache must still succeed (disk op runs).
func TestBufferPool_FreePage_UncachedPage_Succeeds(t *testing.T) {
	disk := newSpyDisk()
	disk.freeFn = func(uint32) error { return nil }
	bp := newBP(disk, 3)

	if err := bp.FreePage(42); err != nil {
		t.Fatalf("FreePage(42) on uncached page: %v", err)
	}
	if disk.freeCount != 1 {
		t.Errorf("disk.FreePage called %d times, want 1", disk.freeCount)
	}
}

// When disk.FreePage returns an error, FreePage must propagate it and leave
// the cache entry intact.
func TestBufferPool_FreePage_DiskError_CacheUnchanged(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	disk.freeFn = func(uint32) error { return errors.New("disk error") }
	bp := newBP(disk, 3)

	_, _ = bp.ReadPage(1)

	if err := bp.FreePage(1); err == nil {
		t.Fatal("FreePage should return error when disk.FreePage fails")
	}
	if _, exists := bp.cache[1]; !exists {
		t.Error("page 1 should still be in cache after failed FreePage")
	}
	if bp.lru.Len() != 1 {
		t.Errorf("LRU len = %d, want 1 after failed FreePage", bp.lru.Len())
	}
}

// Cache and LRU must stay in sync after FreePage removes the middle page.
func TestBufferPool_FreePage_CacheAndLRUConsistent(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	disk.freeFn = func(uint32) error { return nil }
	bp := newBP(disk, 5)

	for _, id := range []uint32{1, 2, 3} {
		_, _ = bp.ReadPage(id)
	}
	if err := bp.FreePage(2); err != nil {
		t.Fatalf("FreePage(2): %v", err)
	}

	if bp.lru.Len() != len(bp.cache) {
		t.Errorf("LRU len=%d != cache len=%d after FreePage", bp.lru.Len(), len(bp.cache))
	}
	for e := bp.lru.Front(); e != nil; e = e.Next() {
		id := e.Value.(uint32)
		if _, exists := bp.cache[id]; !exists {
			t.Errorf("LRU contains page %d but it is absent from cache", id)
		}
	}
	if _, exists := bp.cache[2]; exists {
		t.Error("freed page 2 should not be in cache")
	}
}

// ============================================================
// BufferPool GetRootPageId / SetRootPageId
// ============================================================

// GetRootPageId must return whatever the disk layer reports.
func TestBufferPool_GetRootPageId_DelegatesToDisk(t *testing.T) {
	disk := newSpyDisk()
	disk.getRootFn = func() uint32 { return 7 }
	bp := newBP(disk, 3)

	if got := bp.GetRootPageId(); got != 7 {
		t.Errorf("GetRootPageId = %d, want 7", got)
	}
}

// SetRootPageId must forward the page ID to the disk layer.
func TestBufferPool_SetRootPageId_DelegatesToDisk(t *testing.T) {
	disk := newSpyDisk()
	disk.setRootFn = func(uint32) error { return nil }
	bp := newBP(disk, 3)

	if err := bp.SetRootPageId(42); err != nil {
		t.Fatalf("SetRootPageId(42): %v", err)
	}
	if disk.setRootCount != 1 {
		t.Errorf("disk.SetRootPageId called %d times, want 1", disk.setRootCount)
	}
	if disk.lastRootSet != 42 {
		t.Errorf("disk.SetRootPageId received %d, want 42", disk.lastRootSet)
	}
}

// SetRootPageId must propagate a disk error to the caller.
func TestBufferPool_SetRootPageId_DiskError_Propagated(t *testing.T) {
	disk := newSpyDisk()
	disk.setRootFn = func(uint32) error { return errors.New("disk error") }
	bp := newBP(disk, 3)

	if err := bp.SetRootPageId(1); err == nil {
		t.Fatal("SetRootPageId should propagate disk error")
	}
}

// ============================================================
// BufferPool Close
// ============================================================

// Close must flush all dirty pages to disk and then call disk.Close.
func TestBufferPool_Close_FlushesDirtyPages(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2), makePage(3))
	disk.closeFn = func() error { return nil }
	bp := newBP(disk, 5)

	// Dirty pages 1 and 3; leave page 2 clean.
	p1, _ := bp.ReadPage(1)
	markByte(p1, 0xAA)
	_ = bp.WritePage(p1)
	_, _ = bp.ReadPage(2)
	p3, _ := bp.ReadPage(3)
	markByte(p3, 0xBB)
	_ = bp.WritePage(p3)

	writesBefore := disk.writeCount

	if err := bp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if delta := disk.writeCount - writesBefore; delta != 2 {
		t.Errorf("disk.writeCount delta = %d, want 2 (only dirty pages flushed)", delta)
	}
	if !disk.closeCalled {
		t.Error("disk.Close should be called after flush")
	}
}

// Close must not write clean pages to disk.
func TestBufferPool_Close_CleanPages_NotFlushed(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2))
	disk.closeFn = func() error { return nil }
	bp := newBP(disk, 5)

	_, _ = bp.ReadPage(1)
	_, _ = bp.ReadPage(2)

	writesBefore := disk.writeCount

	if err := bp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if delta := disk.writeCount - writesBefore; delta != 0 {
		t.Errorf("disk.writeCount delta = %d after Close with only clean pages, want 0", delta)
	}
}

// After a successful Close the cache and LRU list must be empty.
func TestBufferPool_Close_ClearsCache(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2))
	disk.closeFn = func() error { return nil }
	bp := newBP(disk, 5)

	_, _ = bp.ReadPage(1)
	_, _ = bp.ReadPage(2)

	if err := bp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := len(bp.cache); got != 0 {
		t.Errorf("cache size = %d after Close, want 0", got)
	}
	if got := bp.lru.Len(); got != 0 {
		t.Errorf("LRU len = %d after Close, want 0", got)
	}
}

// Close on an empty buffer pool must succeed and still call disk.Close.
func TestBufferPool_Close_EmptyPool_Succeeds(t *testing.T) {
	disk := newSpyDisk()
	disk.closeFn = func() error { return nil }
	bp := newBP(disk, 3)

	if err := bp.Close(); err != nil {
		t.Fatalf("Close on empty pool: %v", err)
	}
	if !disk.closeCalled {
		t.Error("disk.Close should be called even for an empty pool")
	}
}

// If flushing a dirty page fails during Close, the error must be returned and
// disk.Close must NOT be called.
func TestBufferPool_Close_DirtyFlushError_DiskCloseNotCalled(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	bp := newBP(disk, 3)

	p, _ := bp.ReadPage(1)
	_ = bp.WritePage(p)

	disk.writeFn = func(*Page) error { return errors.New("flush error") }

	if err := bp.Close(); err == nil {
		t.Fatal("Close should return error when dirty page flush fails")
	}
	if disk.closeCalled {
		t.Error("disk.Close should not be called when dirty page flush fails")
	}
}

// The data written to disk during Close must reflect in-place modifications.
func TestBufferPool_Close_FlushedData_MatchesModification(t *testing.T) {
	disk := newSpyDisk(makePage(1))
	disk.closeFn = func() error { return nil }
	bp := newBP(disk, 3)

	page, _ := bp.ReadPage(1)
	markByte(page, 0xCD)
	_ = bp.WritePage(page)

	if err := bp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if disk.pages[1].Data[CommonHeaderSize] != 0xCD {
		t.Errorf("flushed page 1 data[CommonHeaderSize] = %#x, want 0xCD", disk.pages[1].Data[CommonHeaderSize])
	}
}

// ============================================================
// BufferPool Delete
// ============================================================

// Delete must clear the cache and LRU without flushing dirty pages, then call disk.Delete.
func TestBufferPool_Delete_ClearsCache_CallsDiskDelete(t *testing.T) {
	disk := newSpyDisk(makePage(1), makePage(2))
	disk.deleteFn = func() error { return nil }
	bp := newBP(disk, 5)

	p1, _ := bp.ReadPage(1)
	markByte(p1, 0xFF)
	_ = bp.WritePage(p1)
	_, _ = bp.ReadPage(2)

	writesBefore := disk.writeCount

	if err := bp.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if delta := disk.writeCount - writesBefore; delta != 0 {
		t.Errorf("disk.writeCount delta = %d after Delete, want 0 (dirty pages must not be flushed)", delta)
	}
	if got := len(bp.cache); got != 0 {
		t.Errorf("cache size = %d after Delete, want 0", got)
	}
	if got := bp.lru.Len(); got != 0 {
		t.Errorf("LRU len = %d after Delete, want 0", got)
	}
	if !disk.deleteCalled {
		t.Error("disk.Delete should be called")
	}
}

// Delete must propagate a disk.Delete error to the caller.
func TestBufferPool_Delete_DiskError_Propagated(t *testing.T) {
	disk := newSpyDisk()
	disk.deleteFn = func() error { return errors.New("delete error") }
	bp := newBP(disk, 3)

	if err := bp.Delete(); err == nil {
		t.Fatal("Delete should propagate disk error")
	}
}

// Delete on an empty buffer pool must succeed.
func TestBufferPool_Delete_EmptyPool_Succeeds(t *testing.T) {
	disk := newSpyDisk()
	disk.deleteFn = func() error { return nil }
	bp := newBP(disk, 3)

	if err := bp.Delete(); err != nil {
		t.Fatalf("Delete on empty pool: %v", err)
	}
	if !disk.deleteCalled {
		t.Error("disk.Delete should be called")
	}
}
