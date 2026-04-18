package pagemanager

import "encoding/binary"

/*
COMMON HEADER (24 bytes):
  [0]       Page type (uint8)
  [1-4]     Page ID (uint32)
  [5-6]     Free space start (uint16) — end of slot directory
  [7-8]     Free space end (uint16) — start of record area
  [9-10]    Row count (uint16)
  [11-18]   Page LSN (uint64)
  [19-23]   Reserved (5 bytes)

LEAF HEADER (additional 8 bytes, bytes 24-31):
  [24-27]   Left sibling (uint32)
  [28-31]   Right sibling (uint32)

INTERNAL HEADER (additional 8 bytes, bytes 24-31):
  [24-27]   Rightmost child (uint32)
  [28-29]   Level (uint16)
  [30-31]   Reserved (2 bytes)

META PAGE 0:
  [24-27]   Magic number ("TEST" = 0x54455354)
  [28-29]   Format version (uint16)
  [30-33]   Page count (uint32)
  [34-37]   Free list head (uint32)
  [38-41]   Root page ID (uint32)
  [42-49]   Checkpoint LSN (uint64)
  [50-4095] Reserved

SLOT DIRECTORY (grows downward from byte 32):
  Each slot: 4 bytes
    [0-1]   Offset (15 bits) + used flag (1 bit)
    [2-3]   Length (15 bits) + overflow flag (1 bit)

RECORD AREA (grows upward from byte 4095):
  Variable-length records, packed from bottom
  If overflow flag set: last 4 bytes = overflow page ID

FREE SPACE:
  Between end of slot directory and start of record area
  Page is full when they would collide
  Compaction triggered on insert when total free >= needed but contiguous free < needed
*/

const (
	PageSize = 4096

	// Common header field offsets
	OffsetPageType       = 0
	OffsetPageID         = 1
	OffsetFreeSpaceStart = 5
	OffsetFreeSpaceEnd   = 7
	OffsetRowCount       = 9
	OffsetPageLSN        = 11
	OffsetReserved       = 19
	CommonHeaderSize     = 24

	// Leaf header field offsets
	OffsetLeftSibling  = CommonHeaderSize
	OffsetRightSibling = CommonHeaderSize + 4
	LeafHeaderSize     = CommonHeaderSize + 8

	// Internal header field offsets
	OffsetRightmostChild = CommonHeaderSize
	OffsetLevel          = CommonHeaderSize + 4
	InternalHeaderSize   = CommonHeaderSize + 8

	// Free page linked-list field offset
	OffsetFreeNextPage = CommonHeaderSize

	// Meta page field offsets
	OffsetMetaMagic      = CommonHeaderSize
	OffsetMetaVersion    = CommonHeaderSize + 4
	OffsetMetaPageCount  = CommonHeaderSize + 6
	OffsetMetaFreeList   = CommonHeaderSize + 10
	OffsetMetaRootPage   = CommonHeaderSize + 14
	OffsetMetaCheckpoint = CommonHeaderSize + 18
)

// Page Type Definitions
const (
	PageTypeMeta     uint8 = 0
	PageTypeLeaf     uint8 = 1
	PageTypeInternal uint8 = 2
	PageTypeOverflow uint8 = 3
)

const InvalidPageID uint32 = 0xFFFFFFFF
const MagicNumber uint32 = 0x54455354 // "TEST"

// Slot Type Definitions
const (
	SlotUsed uint16 = 0x8000
	SlotFree uint16 = 0x0000

	SlotOverflow uint16 = 0x8000
	SlotNormal   uint16 = 0x0000
)

type Page struct {
	Data [PageSize]byte
}

// low-level read/write functions for page data
func readUint8(page *Page, offset int) uint8 {
	return page.Data[offset]
}

func readUint16(page *Page, offset int) uint16 {
	return binary.LittleEndian.Uint16(page.Data[offset:])
}

func readUint32(page *Page, offset int) uint32 {
	return binary.LittleEndian.Uint32(page.Data[offset:])
}

func readUint64(page *Page, offset int) uint64 {
	return binary.LittleEndian.Uint64(page.Data[offset:])
}

func writeUint8(page *Page, offset int, value uint8) {
	page.Data[offset] = value
}

func writeUint16(page *Page, offset int, value uint16) {
	binary.LittleEndian.PutUint16(page.Data[offset:], value)
}

func writeUint32(page *Page, offset int, value uint32) {
	binary.LittleEndian.PutUint32(page.Data[offset:], value)
}

func writeUint64(page *Page, offset int, value uint64) {
	binary.LittleEndian.PutUint64(page.Data[offset:], value)
}

// Common header accessors
func (p *Page) GetPageType() uint8 {
	return readUint8(p, OffsetPageType)
}

func (p *Page) setPageType(pageType uint8) {
	writeUint8(p, OffsetPageType, pageType)
}

func (p *Page) GetPageId() uint32 {
	return readUint32(p, OffsetPageID)
}

func (p *Page) setPageId(pageId uint32) {
	writeUint32(p, OffsetPageID, pageId)
}

func (p *Page) GetFreeSpaceStart() uint16 {
	return readUint16(p, OffsetFreeSpaceStart)
}

func (p *Page) setFreeSpaceStart(offset uint16) {
	writeUint16(p, OffsetFreeSpaceStart, offset)
}

func (p *Page) GetFreeSpaceEnd() uint16 {
	return readUint16(p, OffsetFreeSpaceEnd)
}

func (p *Page) setFreeSpaceEnd(offset uint16) {
	writeUint16(p, OffsetFreeSpaceEnd, offset)
}

func (p *Page) GetRowCount() uint16 {
	return readUint16(p, OffsetRowCount)
}

func (p *Page) setRowCount(count uint16) {
	writeUint16(p, OffsetRowCount, count)
}

func (p *Page) GetPageLSN() uint64 {
	return readUint64(p, OffsetPageLSN)
}

func (p *Page) setPageLSN(lsn uint64) {
	writeUint64(p, OffsetPageLSN, lsn)
}

// Leaf page accessors
func (p *Page) GetLeftSibling() uint32 {
	return readUint32(p, OffsetLeftSibling)
}

func (p *Page) setLeftSibling(siblingId uint32) {
	writeUint32(p, OffsetLeftSibling, siblingId)
}

func (p *Page) GetRightSibling() uint32 {
	return readUint32(p, OffsetRightSibling)
}

func (p *Page) setRightSibling(siblingId uint32) {
	writeUint32(p, OffsetRightSibling, siblingId)
}

// Free page accessors
func (p *Page) getFreeNextPage() uint32 {
	return readUint32(p, OffsetFreeNextPage)
}

func (p *Page) setFreeNextPage(pageId uint32) {
	writeUint32(p, OffsetFreeNextPage, pageId)
}

// Internal page accessors
func (p *Page) GetRightMostChild() uint32 {
	return readUint32(p, OffsetRightmostChild)
}

func (p *Page) setRightMostChild(childId uint32) {
	writeUint32(p, OffsetRightmostChild, childId)
}

func (p *Page) GetLevel() uint16 {
	return readUint16(p, OffsetLevel)
}

func (p *Page) setLevel(level uint16) {
	writeUint16(p, OffsetLevel, level)
}

func (p *Page) GetMetaPageMagicNumber() uint32 {
	return readUint32(p, OffsetMetaMagic)
}

func (p *Page) GetMetaPageVersion() uint16 {
	return readUint16(p, OffsetMetaVersion)
}

func (p *Page) setMetaPageVersion(version uint16) {
	writeUint16(p, OffsetMetaVersion, version)
}

func (p *Page) GetMetaPageCount() uint32 {
	return readUint32(p, OffsetMetaPageCount)
}

func (p *Page) setMetaPageCount(count uint32) {
	writeUint32(p, OffsetMetaPageCount, count)
}

func (p *Page) GetMetaFreeList() uint32 {
	return readUint32(p, OffsetMetaFreeList)
}

func (p *Page) setMetaFreeList(pageId uint32) {
	writeUint32(p, OffsetMetaFreeList, pageId)
}

func (p *Page) GetMetaRootPage() uint32 {
	return readUint32(p, OffsetMetaRootPage)
}

func (p *Page) setMetaRootPage(pageId uint32) {
	writeUint32(p, OffsetMetaRootPage, pageId)
}

func (p *Page) GetMetaCheckpoint() uint64 {
	return readUint64(p, OffsetMetaCheckpoint)
}

func (p *Page) setMetaCheckpoint(lsn uint64) {
	writeUint64(p, OffsetMetaCheckpoint, lsn)
}

// Slot directory accessors
func (p *Page) headerSize() int {
	switch p.GetPageType() {
	case PageTypeLeaf:
		return LeafHeaderSize
	case PageTypeInternal:
		return InternalHeaderSize
	default:
		return CommonHeaderSize
	}
}

func (p *Page) GetSlotOffset(slotIndex int) (uint16, bool) {
	slotOffset := readUint16(p, p.headerSize()+slotIndex*4)
	used := (slotOffset & SlotUsed) != 0
	return slotOffset & 0x7FFF, used
}

func (p *Page) setSlotOffset(slotIndex int, offset uint16, used bool) {
	if used {
		offset |= SlotUsed
	} else {
		offset &= ^SlotUsed
	}

	writeUint16(p, p.headerSize()+slotIndex*4, offset)
}

func (p *Page) GetSlotLength(slotIndex int) (uint16, bool) {
	slotLength := readUint16(p, p.headerSize()+slotIndex*4+2)
	overflow := (slotLength & SlotOverflow) != 0
	return slotLength & 0x7FFF, overflow
}

func (p *Page) setSlotLength(slotIndex int, length uint16, overflow bool) {
	if overflow {
		length |= SlotOverflow
	} else {
		length &= ^SlotOverflow
	}

	writeUint16(p, p.headerSize()+slotIndex*4+2, length)
}

// Record accessors
func (p *Page) GetRecord(slotIndex int) ([]byte, bool) {
	offset, used := p.GetSlotOffset(slotIndex)

	if !used {
		return nil, false
	}

	length, _ := p.GetSlotLength(slotIndex)

	if offset+length > PageSize {
		return nil, false
	}

	return p.Data[offset : offset+length], true
}

func (p *Page) setRecord(slotIndex int, record []byte, overflow bool) {
	if len(record) > 0x7FFF {
		panic("Record too large for slot")
	}

	offset := p.GetFreeSpaceEnd() - uint16(len(record))

	p.setSlotOffset(slotIndex, offset, true)
	p.setSlotLength(slotIndex, uint16(len(record)), overflow)
	copy(p.Data[offset:], record)

	// Update free space end
	p.setFreeSpaceEnd(offset)
}

func (p *Page) DeleteRecord(slotIndex int) {
	if p.GetPageType() == PageTypeMeta || p.GetPageType() == PageTypeOverflow {
		panic("cannot delete records of meta/overflow pages")
	}

	p.setSlotOffset(slotIndex, 0, false)
	p.setSlotLength(slotIndex, 0, false)
}

func (p *Page) updateRecord(slotIndex int, record []byte, overflow bool) bool {
	if len(record) > 0x7FFF {
		panic("Record too large for slot")
	}

	if p.GetPageType() == PageTypeMeta || p.GetPageType() == PageTypeOverflow {
		panic("cannot update records into meta/overflow pages")
	}

	offset, used := p.GetSlotOffset(slotIndex)

	if !used {
		panic("Cannot update non-existent record")
	}

	length, _ := p.GetSlotLength(slotIndex)
	newLen := uint16(len(record))

	if newLen > length {
		// Growing: old record bytes become dead space after DeleteRecord and are
		// NOT contiguous with the free gap, so only the contiguous gap can be used.
		// No new slot entry is needed (we reuse the existing slot).
		if newLen > p.GetFreeSpace() {
			return false
		}
		p.DeleteRecord(slotIndex)
		p.setRecord(slotIndex, record, overflow)
	} else {
		// Fits in existing space: overwrite in place
		copy(p.Data[offset:], record)
		p.setSlotLength(slotIndex, newLen, overflow)
	}
	return true
}

func (p *Page) InsertRecord(record []byte) (int, bool) {
	rowCount := p.GetRowCount()
	slotIndex := int(rowCount)

	if p.GetPageType() == PageTypeMeta || p.GetPageType() == PageTypeOverflow {
		panic("cannot insert records into meta/overflow pages")
	}

	if !p.CanAccommodate(len(record)) {
		return -1, false
	}

	p.setFreeSpaceStart(uint16(p.headerSize() + (slotIndex+1)*4))
	p.setRecord(slotIndex, record, false)
	p.setRowCount(uint16(slotIndex + 1))

	return slotIndex, true
}

// Free space Management Functions

// GetFreeSpace returns the total free space available on the page
func (p *Page) GetFreeSpace() uint16 {
	freeSpaceStart := p.GetFreeSpaceStart()
	freeSpaceEnd := p.GetFreeSpaceEnd()

	if freeSpaceEnd <= freeSpaceStart {
		return 0
	}

	return freeSpaceEnd - freeSpaceStart
}

// IsFull checks if the page is full
func (p *Page) IsFull() bool {
	return p.GetFreeSpace() == 0
}

// CanAccommodate checks if the page can accommodate a record of the given size
func (p *Page) CanAccommodate(recordSize int) bool {
	// Each record requires space for the slot entry (4 bytes) plus record data
	requiredSpace := uint64(recordSize) + 4

	return uint64(p.GetFreeSpace()) >= requiredSpace
}

// CompactPage compacts the page to create contiguous free space
func (p *Page) CompactPage() {
	// Create a temporary buffer to hold compacted records
	var temp [PageSize]byte
	copy(temp[:], p.Data[:])

	// Reset free space pointers
	p.setFreeSpaceStart(uint16(p.headerSize())) // Start of slot directory
	p.setFreeSpaceEnd(PageSize)                 // End of record area

	rowCount := p.GetRowCount()
	newSlotIndex := 0

	for i := 0; i < int(rowCount); i++ {
		// Read slot metadata from temp (the snapshot), not p.Data, so that
		// writing compacted slots back into p.Data cannot corrupt unread entries.
		rawOffset := binary.LittleEndian.Uint16(temp[p.headerSize()+i*4:])
		used := (rawOffset & SlotUsed) != 0
		if !used {
			continue // Skip deleted slots
		}
		offset := rawOffset & 0x7FFF

		rawLength := binary.LittleEndian.Uint16(temp[p.headerSize()+i*4+2:])
		overflow := (rawLength & SlotOverflow) != 0
		length := rawLength & 0x7FFF

		if offset+length > PageSize {
			continue // Skip invalid slots
		}

		recordData := temp[offset : offset+length]

		// Write record data to new location
		newOffset := p.GetFreeSpaceEnd() - uint16(len(recordData))
		copy(p.Data[newOffset:], recordData)

		// Update slot entry with new offset and length
		p.setSlotOffset(newSlotIndex, newOffset, true)
		p.setSlotLength(newSlotIndex, uint16(len(recordData)), overflow)

		newSlotIndex++
		p.setFreeSpaceEnd(newOffset)
	}

	// Update row count to reflect any deleted records
	p.setRowCount(uint16(newSlotIndex))
	p.setFreeSpaceStart(uint16(p.headerSize() + newSlotIndex*4))
}

// Page functions
func NewPage(pageType uint8, pageId uint32) *Page {
	p := &Page{}

	p.setPageType(pageType)
	p.setPageId(pageId)
	p.setFreeSpaceStart(uint16(p.headerSize())) // Start of slot directory
	p.setFreeSpaceEnd(PageSize)                 // End of record area
	p.setRowCount(0)
	p.setPageLSN(0)

	return p
}

// Leaf page functions
func NewLeafPage(pageId uint32, leftSibling uint32, rightSibling uint32) *Page {
	p := NewPage(PageTypeLeaf, pageId)
	p.setLeftSibling(leftSibling)
	p.setRightSibling(rightSibling)

	return p
}

// Internal page functions
func NewInternalPage(pageId uint32, level uint16, rightmostChild uint32) *Page {
	p := NewPage(PageTypeInternal, pageId)
	p.setRightMostChild(rightmostChild)
	p.setLevel(level)

	return p
}

// Meta page functions
func NewMetaPage() *Page {
	p := NewPage(PageTypeMeta, 0)

	// Initialize meta page fields
	p.setMetaPageVersion(1)
	p.setMetaPageCount(1)
	p.setMetaFreeList(InvalidPageID)
	p.setMetaRootPage(InvalidPageID)
	p.setMetaCheckpoint(0)

	// Write magic number
	writeUint32(p, OffsetMetaMagic, 0x54455354) // "TEST"

	return p
}
