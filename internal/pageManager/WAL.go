package pagemanager

import (
	"encoding/binary"
	"hash/crc32"
)

/*
WAL RECORD (4112 bytes):
[0-7] LSN
[8-11] Page ID
[12-4107] Page Data
[4108-4111] CRC32
*/

const (
	WAL_RecordSize = 4112

	//Record Offsets
	WAL_OffsetLSN      = 0
	WAL_OffsetPageID   = 8
	WAL_OffsetPageData = 12
	WAL_OffsetCRC32    = 4108
)

type WAL struct {
	Data [WAL_RecordSize]byte
}

// Record Assessors
func (w *WAL) GetLSN() uint64 {
	return binary.BigEndian.Uint64(w.Data[WAL_OffsetLSN:])
}

func (w *WAL) GetPageID() uint32 {
	return binary.BigEndian.Uint32(w.Data[WAL_OffsetPageID:])
}

func (w *WAL) GetPageData() []byte {
	return w.Data[WAL_OffsetPageData:WAL_OffsetCRC32]
}

func (w *WAL) GetCRC32() uint32 {
	return binary.BigEndian.Uint32(w.Data[WAL_OffsetCRC32:])
}

// Record Setters
func (w *WAL) SetLSN(lsn uint64) {
	binary.BigEndian.PutUint64(w.Data[WAL_OffsetLSN:], lsn)
}

func (w *WAL) SetPageID(pageID uint32) {
	binary.BigEndian.PutUint32(w.Data[WAL_OffsetPageID:], pageID)
}

func (w *WAL) SetPageData(page *Page) {
	copy(w.Data[WAL_OffsetPageData:WAL_OffsetCRC32], page.Data[:])
}

func (w *WAL) SetCRC32(crc uint32) {
	binary.BigEndian.PutUint32(w.Data[WAL_OffsetCRC32:], crc)
}

// Utility functions
// CalculateCRC32 calculates the CRC32 checksum of the entire WAL record (LSN + PageID + PageData).
func (w *WAL) CalculateCRC32() uint32 {
	return crc32.ChecksumIEEE(w.Data[:WAL_OffsetCRC32])
}

// ValidateCRC32 checks if the CRC32 checksum in the WAL record matches the calculated checksum of the page data.
func (w *WAL) ValidateCRC32() bool {
	return w.GetCRC32() == w.CalculateCRC32()
}
