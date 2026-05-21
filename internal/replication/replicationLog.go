package replication

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
)

// NewReplicationManager opens or creates the replication log at path and recovers
// nextLSN by scanning any existing records.
func NewReplicationManager(path string) (*ReplicationManager, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("NewReplicationManager: %w", err)
	}

	rm := &ReplicationManager{file: file}
	rm.cond = sync.NewCond(&rm.mu)

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("NewReplicationManager: seek for recovery: %w", err)
	}

	var lenBuf [4]byte
	for {
		_, err := io.ReadFull(file, lenBuf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("NewReplicationManager: reading length prefix: %w", err)
		}

		totalLen := binary.BigEndian.Uint32(lenBuf[:])
		if totalLen < REPL_OffsetValue {
			break
		}

		body := make([]byte, totalLen)
		_, err = io.ReadFull(file, body)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("NewReplicationManager: reading record body: %w", err)
		}

		lsn := binary.BigEndian.Uint64(body[REPL_OffsetLSN : REPL_OffsetLSN+8])
		if lsn >= rm.nextLSN {
			rm.nextLSN = lsn + 1
		}
	}

	return rm, nil
}

// encodeEntry encodes a ReplicationLogEntry into the full wire format:
// [4 bytes totalLen][8 bytes LSN][1 byte Op][8 bytes Key][N bytes fields].
func encodeEntry(e ReplicationLogEntry) ([]byte, error) {
	var fieldBytes []byte
	for _, f := range e.Fields {
		fb, err := btree.EncodeField(f)
		if err != nil {
			return nil, fmt.Errorf("encodeEntry: encoding field tag %d: %w", f.Tag, err)
		}
		fieldBytes = append(fieldBytes, fb...)
	}

	totalLen := uint32(REPL_OffsetValue + len(fieldBytes))
	buf := make([]byte, 4+totalLen)

	const base = 4
	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint64(buf[base+REPL_OffsetLSN:], e.LSN)
	buf[base+REPL_OffsetOp] = byte(e.Op)
	binary.BigEndian.PutUint64(buf[base+REPL_OffsetKey:], e.Key)
	copy(buf[base+REPL_OffsetValue:], fieldBytes)

	return buf, nil
}

// decodeEntry decodes a record body (after the 4-byte length prefix has been consumed).
func decodeEntry(data []byte) (ReplicationLogEntry, error) {
	if len(data) < REPL_OffsetValue {
		return ReplicationLogEntry{}, fmt.Errorf("decodeEntry: record body too short: got %d bytes, need at least %d", len(data), REPL_OffsetValue)
	}

	lsn := binary.BigEndian.Uint64(data[REPL_OffsetLSN : REPL_OffsetLSN+8])
	op := ReplOp(data[REPL_OffsetOp])
	key := binary.BigEndian.Uint64(data[REPL_OffsetKey : REPL_OffsetKey+8])

	fields, consumed := btree.DecodeFields(data[REPL_OffsetValue:])
	if consumed != len(data)-REPL_OffsetValue {
		return ReplicationLogEntry{}, fmt.Errorf("decodeEntry: %d unparseable trailing bytes in fields section", len(data)-REPL_OffsetValue-consumed)
	}

	return ReplicationLogEntry{LSN: lsn, Op: op, Key: key, Fields: fields}, nil
}

func (rm *ReplicationManager) appendOne(op ReplOp, key uint64, fields []btree.Field) (uint64, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	assignedLSN := rm.nextLSN
	entry := ReplicationLogEntry{LSN: assignedLSN, Op: op, Key: key, Fields: fields}

	buf, err := encodeEntry(entry)
	if err != nil {
		return 0, fmt.Errorf("Append: %w", err)
	}

	if _, err := rm.file.Write(buf); err != nil {
		return 0, fmt.Errorf("Append: writing record: %w", err)
	}

	// Advance before sync so a sync failure doesn't leave this LSN reusable.
	rm.nextLSN++

	if err := rm.file.Sync(); err != nil {
		return 0, fmt.Errorf("Append: syncing file: %w", err)
	}

	rm.cond.Broadcast()
	return assignedLSN, nil
}

// Append assigns the next LSN, encodes the entry, writes it to the log, and syncs.
// return error.
func (rm *ReplicationManager) Append(entries []ReplicationLogEntry) error {
	for _, e := range entries {
		_, err := rm.appendOne(e.Op, e.Key, e.Fields)
		if err != nil {
			return fmt.Errorf("Append: appending entry with key %d: %w", e.Key, err)
		}
	}

	return nil
}

// ReadFrom returns all log entries with LSN >= startLSN, scanning the file from the start.
func (rm *ReplicationManager) ReadFrom(startLSN uint64) ([]ReplicationLogEntry, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, err := rm.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("ReadFrom: seek to start: %w", err)
	}

	var entries []ReplicationLogEntry
	var lenBuf [4]byte

	for {
		_, err := io.ReadFull(rm.file, lenBuf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("ReadFrom: reading length prefix: %w", err)
		}

		totalLen := binary.BigEndian.Uint32(lenBuf[:])
		if totalLen < REPL_OffsetValue {
			break
		}

		body := make([]byte, totalLen)
		_, err = io.ReadFull(rm.file, body)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("ReadFrom: reading record body: %w", err)
		}

		entry, err := decodeEntry(body)
		if err != nil {
			break
		}

		if entry.LSN >= startLSN {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}
