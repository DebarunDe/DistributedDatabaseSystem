package pagemanager

import (
	"container/list"
	"fmt"
	"os"
)

// PageManager interface
type PageManager interface {
	AllocatePage() (*Page, error)
	ReadPage(pageId uint32) (*Page, error)
	WritePage(page *Page) error
	FreePage(pageId uint32) error
	GetRootPageId() uint32
	SetRootPageId(pageId uint32) error
	Close() error
	Delete() error
}

// PageManagerImpl struct
type PageManagerImpl struct {
	file     *os.File
	metaPage *Page
}

// BufferPoolImpl struct
type CacheValue struct {
	modified bool
	page     *Page
	node     *list.Element // pointer to the corresponding node in the LRU list for O(1) access during eviction
}

type BufferPoolImpl struct {
	disk      PageManager
	cache     map[uint32]*CacheValue
	cacheSize int
	lru       *list.List // list of page IDs, with the most recently used at the front and least recently used at the back
}

// Bufferpool specific methods
// Evict evicts the least recently used page from the buffer pool.
func (bp *BufferPoolImpl) Evict() error {
	if bp.lru.Len() < bp.cacheSize {
		return nil // No eviction needed
	}

	// Get the least recently used page (the back of the list)
	lruElement := bp.lru.Back()
	cacheValue := bp.cache[lruElement.Value.(uint32)]

	// If the page is modified, write it back to disk
	if cacheValue.modified {
		if err := bp.disk.WritePage(cacheValue.page); err != nil {
			// If writing back to disk fails, we should not evict the page to avoid data loss. Instead, we can return an error.
			return fmt.Errorf("failed to write modified page to disk: %w", err)
		}
	}

	// Remove the page from the cache and the LRU list
	delete(bp.cache, lruElement.Value.(uint32))
	bp.lru.Remove(lruElement)

	return nil
}

// AllocatePage allocates a new page.
func (pm *PageManagerImpl) AllocatePage() (*Page, error) {
	if pm.file == nil {
		return nil, fmt.Errorf("page manager is closed")
	}
	freeListHead := pm.metaPage.GetMetaFreeList()
	newPageId := uint32(0)
	nextFreeListHead := InvalidPageID

	if freeListHead != InvalidPageID {
		//read free page from disk
		freePage, err := pm.ReadPage(freeListHead)
		if err != nil {
			return nil, fmt.Errorf("failed to read free page: %w", err)
		}

		newPageId = freeListHead
		nextFreeListHead = freePage.getFreeNextPage()
	} else {
		// get current page count from meta page
		newPageId = pm.metaPage.GetMetaPageCount()
	}

	// create new blank page with just correct page id
	newPage := &Page{}
	newPage.setPageId(newPageId)
	if _, err := pm.file.WriteAt(newPage.Data[:], int64(newPageId)*PageSize); err != nil {
		return nil, fmt.Errorf("failed to write new page: %w", err)
	}

	if freeListHead != InvalidPageID {
		pm.metaPage.setMetaFreeList(nextFreeListHead)
	} else {
		pm.metaPage.setMetaPageCount(newPageId + 1)
	}

	// write updated meta page back to disk
	if err := pm.WritePage(pm.metaPage); err != nil {
		return nil, fmt.Errorf("failed to write updated meta page: %w", err)
	}

	return newPage, nil
}

func (bp *BufferPoolImpl) AllocatePage() (*Page, error) {
	// Allocate a new page from disk
	page, err := bp.disk.AllocatePage()
	if err != nil {
		return nil, fmt.Errorf("failed to allocate page from disk: %w", err)
	}

	if err := bp.Evict(); err != nil {
		//rollback the allocated page on disk since we failed to evict a page from the buffer pool to make room for the new page
		if freeErr := bp.disk.FreePage(page.GetPageId()); freeErr != nil {
			return nil, fmt.Errorf("failed to evict page and rollback allocated page: evict error: %v, free error: %v", err, freeErr)
		}
		return nil, fmt.Errorf("failed to evict page: %w", err)
	}

	// Add the new page to the cache
	cacheValue := CacheValue{
		modified: false,
		page:     page,
	}

	cacheValue.node = bp.lru.PushFront(page.GetPageId())
	bp.cache[page.GetPageId()] = &cacheValue

	return page, nil
}

// ReadPage reads a page from disk.
func (pm *PageManagerImpl) ReadPage(pageId uint32) (*Page, error) {
	if pm.file == nil {
		return nil, fmt.Errorf("page manager is closed")
	}
	if pm.metaPage.GetMetaPageCount() <= pageId {
		return nil, fmt.Errorf("invalid page id: %d, page count: %d", pageId, pm.metaPage.GetMetaPageCount())
	}

	fileOffset := int64(pageId) * PageSize

	p := &Page{}
	if _, err := pm.file.ReadAt(p.Data[:], fileOffset); err != nil {
		return nil, fmt.Errorf("failed to read page at offset %d: %w", fileOffset, err)
	}

	if p.GetPageId() != pageId {
		return nil, fmt.Errorf("page corruption detected, page id mismatch: expected %d, got %d", pageId, p.GetPageId())
	}

	return p, nil
}

func (bp *BufferPoolImpl) ReadPage(pageId uint32) (*Page, error) {
	// Check if page is in cache
	if cacheValue, exists := bp.cache[pageId]; exists {
		// Move the accessed page to the front of the LRU list
		bp.lru.MoveToFront(cacheValue.node)
		return cacheValue.page, nil
	}

	if err := bp.Evict(); err != nil {
		// If eviction fails, return early
		return nil, fmt.Errorf("failed to evict page: %w", err)
	}

	// If not in cache, read from disk
	page, err := bp.disk.ReadPage(pageId)
	if err != nil {
		return nil, fmt.Errorf("failed to read page from disk: %w", err)
	}

	// Add the page to the cache
	cacheValue := CacheValue{
		modified: false,
		page:     page,
	}
	cacheValue.node = bp.lru.PushFront(pageId)

	bp.cache[pageId] = &cacheValue

	return page, nil
}

// WritePage writes a page to disk.
func (pm *PageManagerImpl) WritePage(p *Page) error {
	if pm.file == nil {
		return fmt.Errorf("page manager is closed")
	}
	pageId := p.GetPageId()

	if pm.metaPage.GetMetaPageCount() <= pageId {
		return fmt.Errorf("invalid page id: %d, page count: %d", pageId, pm.metaPage.GetMetaPageCount())
	}

	fileOffset := int64(pageId) * PageSize

	if _, err := pm.file.WriteAt(p.Data[:], fileOffset); err != nil {
		return fmt.Errorf("failed to write page at offset %d: %w", fileOffset, err)
	}

	return nil
}

func (bp *BufferPoolImpl) WritePage(p *Page) error {
	// Check if page is in cache
	cacheValue := bp.cache[p.GetPageId()]
	if cacheValue == nil {
		return fmt.Errorf("page with id %d not found in buffer pool cache", p.GetPageId())
	}

	cacheValue.modified = true
	bp.lru.MoveToFront(cacheValue.node)

	return nil
}

// FreePage frees a page.
func (pm *PageManagerImpl) FreePage(pageId uint32) error {
	if pm.file == nil {
		return fmt.Errorf("page manager is closed")
	}
	if pm.metaPage.GetMetaPageCount() <= pageId {
		return fmt.Errorf("invalid page id: %d, page count: %d", pageId, pm.metaPage.GetMetaPageCount())
	}

	if pageId == 0 {
		return fmt.Errorf("cannot free page 0 (meta page)")
	}

	//read current free list head from meta page
	freeListHead := pm.metaPage.GetMetaFreeList()

	//read page to be freed from disk
	pageToFree, err := pm.ReadPage(pageId)
	if err != nil {
		return fmt.Errorf("failed to read page to free: %w", err)
	}

	pageToFree.setFreeNextPage(freeListHead)

	//write the updated page back to disk
	if err := pm.WritePage(pageToFree); err != nil {
		return fmt.Errorf("failed to write freed page: %w", err)
	}

	//update meta page free list head to point to the newly freed page
	pm.metaPage.setMetaFreeList(pageId)

	//write updated meta page back to disk
	if err := pm.WritePage(pm.metaPage); err != nil {
		return fmt.Errorf("failed to write updated meta page: %w", err)
	}

	return nil
}

func (bp *BufferPoolImpl) FreePage(pageId uint32) error {
	// Free the page on disk
	if err := bp.disk.FreePage(pageId); err != nil {
		return fmt.Errorf("failed to free page on disk: %w", err)
	}

	// Check if page is in cache
	cacheValue := bp.cache[pageId]
	if cacheValue != nil {
		// since the page is being freed, we can remove it from the cache and LRU list
		bp.lru.Remove(cacheValue.node)
		delete(bp.cache, pageId)
	}

	return nil
}

// GetRootPageId returns the root page id.
func (pm *PageManagerImpl) GetRootPageId() uint32 {
	return pm.metaPage.GetMetaRootPage()
}

func (bp *BufferPoolImpl) GetRootPageId() uint32 {
	// The root page ID is stored in the meta page, which is managed by the disk layer. We can read it directly from the disk.
	rootPageId := bp.disk.GetRootPageId()
	return rootPageId
}

// SetRootPageId sets the root page id.
func (pm *PageManagerImpl) SetRootPageId(pageId uint32) error {
	if pm.file == nil {
		return fmt.Errorf("page manager is closed")
	}
	pm.metaPage.setMetaRootPage(pageId)
	if err := pm.WritePage(pm.metaPage); err != nil {
		return fmt.Errorf("failed to write meta page: %w", err)
	}
	return nil
}

func (bp *BufferPoolImpl) SetRootPageId(pageId uint32) error {
	// The root page ID is stored in the meta page, which is managed by the disk layer. We can update it directly on the disk.
	if err := bp.disk.SetRootPageId(pageId); err != nil {
		return fmt.Errorf("failed to set root page id on disk: %w", err)
	}
	return nil
}

// Close closes the page manager.
func (pm *PageManagerImpl) Close() error {
	if pm.file == nil {
		return fmt.Errorf("page manager is closed")
	}
	//write meta page back to disk
	if err := pm.WritePage(pm.metaPage); err != nil {
		return fmt.Errorf("failed to write meta page to disk: %w", err)
	}

	if err := pm.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	if err := pm.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	pm.file = nil

	return nil
}

func (bp *BufferPoolImpl) Close() error {
	// Flush all modified pages in the buffer pool to disk
	for pageId, cacheValue := range bp.cache {
		if cacheValue.modified {
			if err := bp.disk.WritePage(cacheValue.page); err != nil {
				return fmt.Errorf("failed to write modified page with id %d to disk: %w", pageId, err)
			}
		}
	}

	// Clear the buffer pool cache and LRU list
	bp.cache = make(map[uint32]*CacheValue)
	bp.lru.Init()

	if err := bp.disk.Close(); err != nil {
		return fmt.Errorf("failed to close underlying disk page manager: %w", err)
	}

	return nil
}

// Delete deletes the page manager and the underlying file.
func (pm *PageManagerImpl) Delete() error {
	if pm.file == nil {
		return fmt.Errorf("page manager is closed")
	}
	path := pm.file.Name()

	if err := pm.Close(); err != nil {
		return fmt.Errorf("failed to close page manager: %w", err)
	}

	if err := os.Remove(path); err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}

	return nil
}

func (bp *BufferPoolImpl) Delete() error {
	// clear the buffer pool cache and LRU list
	bp.cache = make(map[uint32]*CacheValue)
	bp.lru.Init()

	if err := bp.disk.Delete(); err != nil {
		return fmt.Errorf("failed to delete underlying disk page manager: %w", err)
	}

	return nil
}

// entry points
func NewDB(path string) (PageManager, error) {
	//check if file already exists, return error if it does, if not create it
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}

	p := NewMetaPage()

	if _, err = file.WriteAt(p.Data[:], 0); err != nil {
		file.Close()
		os.Remove(path)
		return nil, err
	}

	if err = file.Sync(); err != nil {
		file.Close()
		os.Remove(path)
		return nil, err
	}

	return &PageManagerImpl{
		file:     file,
		metaPage: p,
	}, nil
}

func NewBufferPool(disk PageManager, cacheSize int) PageManager {
	return &BufferPoolImpl{
		disk:      disk,
		cache:     make(map[uint32]*CacheValue),
		cacheSize: cacheSize,
		lru:       list.New(),
	}
}

func OpenDB(path string) (PageManager, error) {
	//open file
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	//check file size
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if info.Size() < PageSize {
		file.Close()
		return nil, fmt.Errorf("file too small: %d bytes, need at least %d", info.Size(), PageSize)
	}

	//read page 0 (meta page)
	p := &Page{}
	_, err = file.ReadAt(p.Data[:], 0)
	if err != nil {
		file.Close()
		return nil, err
	}

	//validate magic number
	if p.GetMetaPageMagicNumber() != MagicNumber {
		file.Close()
		return nil, fmt.Errorf("invalid magic number: got %#x, want %#x", p.GetMetaPageMagicNumber(), MagicNumber)
	}

	return &PageManagerImpl{
		file:     file,
		metaPage: p,
	}, nil
}
