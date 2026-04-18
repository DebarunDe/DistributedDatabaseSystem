package pagemanager

import (
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

// ReadPage reads a page from disk.
func (pm *PageManagerImpl) ReadPage(pageId uint32) (*Page, error) {
	if pm.file == nil {
		return nil, fmt.Errorf("page manager is closed")
	}
	if pm.metaPage.GetMetaPageCount() <= pageId {
		return nil, fmt.Errorf("invalid page id: %d, page count: %d", pageId, pm.metaPage.GetMetaPageCount())
	}

	fileOffset := pageId * PageSize

	p := &Page{}
	if _, err := pm.file.ReadAt(p.Data[:], int64(fileOffset)); err != nil {
		return nil, fmt.Errorf("failed to read page at offset %d: %w", fileOffset, err)
	}

	if p.GetPageId() != pageId {
		return nil, fmt.Errorf("page corruption detected, page id mismatch: expected %d, got %d", pageId, p.GetPageId())
	}

	return p, nil
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

	fileOffset := pageId * PageSize

	if _, err := pm.file.WriteAt(p.Data[:], int64(fileOffset)); err != nil {
		return fmt.Errorf("failed to write page at offset %d: %w", fileOffset, err)
	}

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

// GetRootPageId returns the root page id.
func (pm *PageManagerImpl) GetRootPageId() uint32 {
	return pm.metaPage.GetMetaRootPage()
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
	}, err
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
		return nil, os.ErrInvalid
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
