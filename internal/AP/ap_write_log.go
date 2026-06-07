package ap

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	"github.com/your-username/DistributedDatabaseSystem/internal/raft"
)

// Wire format per entry (mirrors replication log with timestamp added):
//
//	[4 bytes totalLen][8 bytes LSN][1 byte Op][8 bytes Key][8 bytes Timestamp][N bytes Fields]
const (
	APLog_OffsetLSN       = 0
	APLog_OffsetOp        = 8
	APLog_OffsetKey       = 9
	APLog_OffsetTimestamp = 17
	APLog_OffsetFields    = 25
)

// APWriteEntry is a single record in the AP write-ahead log.
type APWriteEntry struct {
	LSN       uint64
	Op        raft.ReplOp
	Key       uint64
	Timestamp int64
	Fields    []btree.Field
}

// APWriteLog is a durable, append-only log for AP-mode writes.
// It mirrors the structure of ReplicationManager but carries a per-entry timestamp.
type APWriteLog struct {
	mu      sync.Mutex
	nextLSN uint64
	file    *os.File
	cond    *sync.Cond
}

// NewAPWriteLog opens or creates the log file at path and recovers nextLSN by
// scanning any existing records.
func NewAPWriteLog(path string) (*APWriteLog, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("NewAPWriteLog: %w", err)
	}
	l := &APWriteLog{file: file}
	l.cond = sync.NewCond(&l.mu)

	// Recover nextLSN by scanning existing records.
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("NewAPWriteLog: seek: %w", err)
	}
	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(file, lenBuf[:]); err != nil {
			break
		}
		totalLen := binary.BigEndian.Uint32(lenBuf[:])
		if totalLen < APLog_OffsetFields {
			break
		}
		body := make([]byte, totalLen)
		if _, err := io.ReadFull(file, body); err != nil {
			break
		}
		lsn := binary.BigEndian.Uint64(body[APLog_OffsetLSN : APLog_OffsetLSN+8])
		if lsn >= l.nextLSN {
			l.nextLSN = lsn + 1
		}
	}
	return l, nil
}

// Append assigns the next LSN, encodes the entry, writes it durably, and
// notifies any waiting readers. Returns the assigned LSN.
func (l *APWriteLog) Append(op raft.ReplOp, key uint64, ts int64, fields []btree.Field) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := l.nextLSN
	entry := APWriteEntry{LSN: lsn, Op: op, Key: key, Timestamp: ts, Fields: fields}
	buf, err := encodeAPEntry(entry)
	if err != nil {
		return 0, fmt.Errorf("APWriteLog.Append: %w", err)
	}
	if _, err := l.file.Write(buf); err != nil {
		return 0, fmt.Errorf("APWriteLog.Append: write: %w", err)
	}
	l.nextLSN++
	if err := l.file.Sync(); err != nil {
		return 0, fmt.Errorf("APWriteLog.Append: sync: %w", err)
	}
	l.cond.Broadcast()
	return lsn, nil
}

// ReadFrom returns all entries with LSN >= startLSN by scanning the file from the start.
func (l *APWriteLog) ReadFrom(startLSN uint64) ([]APWriteEntry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.readFromLocked(startLSN)
}

func (l *APWriteLog) readFromLocked(startLSN uint64) ([]APWriteEntry, error) {
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("APWriteLog.ReadFrom: seek: %w", err)
	}
	var entries []APWriteEntry
	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(l.file, lenBuf[:]); err != nil {
			break
		}
		totalLen := binary.BigEndian.Uint32(lenBuf[:])
		if totalLen < APLog_OffsetFields {
			break
		}
		body := make([]byte, totalLen)
		if _, err := io.ReadFull(l.file, body); err != nil {
			break
		}
		e, err := decodeAPEntry(body)
		if err != nil {
			break
		}
		if e.LSN >= startLSN {
			entries = append(entries, e)
		}
	}
	return entries, nil
}

// NextLSN returns the next LSN that will be assigned (useful for tests).
func (l *APWriteLog) NextLSN() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextLSN
}

// Close closes the underlying file.
func (l *APWriteLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

// encodeAPEntry serialises one entry into the full wire format.
func encodeAPEntry(e APWriteEntry) ([]byte, error) {
	var fieldBytes []byte
	for _, f := range e.Fields {
		fb, err := btree.EncodeField(f)
		if err != nil {
			return nil, fmt.Errorf("encodeAPEntry: field tag %d: %w", f.Tag, err)
		}
		fieldBytes = append(fieldBytes, fb...)
	}
	totalLen := uint32(APLog_OffsetFields + len(fieldBytes))
	buf := make([]byte, 4+totalLen)
	const base = 4
	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint64(buf[base+APLog_OffsetLSN:], e.LSN)
	buf[base+APLog_OffsetOp] = byte(e.Op)
	binary.BigEndian.PutUint64(buf[base+APLog_OffsetKey:], e.Key)
	binary.BigEndian.PutUint64(buf[base+APLog_OffsetTimestamp:], uint64(e.Timestamp))
	copy(buf[base+APLog_OffsetFields:], fieldBytes)
	return buf, nil
}

// decodeAPEntry decodes a record body (4-byte length prefix already consumed).
func decodeAPEntry(data []byte) (APWriteEntry, error) {
	if len(data) < APLog_OffsetFields {
		return APWriteEntry{}, fmt.Errorf("decodeAPEntry: body too short (%d bytes)", len(data))
	}
	lsn := binary.BigEndian.Uint64(data[APLog_OffsetLSN : APLog_OffsetLSN+8])
	op := raft.ReplOp(data[APLog_OffsetOp])
	key := binary.BigEndian.Uint64(data[APLog_OffsetKey : APLog_OffsetKey+8])
	ts := int64(binary.BigEndian.Uint64(data[APLog_OffsetTimestamp : APLog_OffsetTimestamp+8]))
	fields, _ := btree.DecodeFields(data[APLog_OffsetFields:])
	return APWriteEntry{LSN: lsn, Op: op, Key: key, Timestamp: ts, Fields: fields}, nil
}
