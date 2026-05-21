package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"strings"
	"testing"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
	pb "github.com/your-username/DistributedDatabaseSystem/proto/repl"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ============================================================
// Helpers
// ============================================================

func newManagerForTest(t *testing.T) *ReplicationManager {
	t.Helper()
	path := t.TempDir() + "/repl.log"
	rm, err := NewReplicationManager(path)
	if err != nil {
		t.Fatalf("NewReplicationManager: %v", err)
	}
	t.Cleanup(func() { _ = rm.file.Close() })
	return rm
}

func mustAppend(t *testing.T, rm *ReplicationManager, op ReplOp, key uint64, fields []btree.Field) uint64 {
	t.Helper()
	lsn := rm.nextLSN
	err := rm.Append([]ReplicationLogEntry{{Op: op, Key: key, Fields: fields}})
	if err != nil {
		t.Fatalf("Append(op=%d, key=%d): %v", op, key, err)
	}
	return lsn
}

// ============================================================
// Offset constant consistency
// ============================================================

func TestOffsetConstants_Layout(t *testing.T) {
	if REPL_OffsetLSN != 0 {
		t.Errorf("REPL_OffsetLSN = %d, want 0", REPL_OffsetLSN)
	}
	if REPL_OffsetOp != REPL_OffsetLSN+8 {
		t.Errorf("REPL_OffsetOp = %d, want %d (LSN end)", REPL_OffsetOp, REPL_OffsetLSN+8)
	}
	if REPL_OffsetKey != REPL_OffsetOp+1 {
		t.Errorf("REPL_OffsetKey = %d, want %d (Op end)", REPL_OffsetKey, REPL_OffsetOp+1)
	}
	if REPL_OffsetValue != REPL_OffsetKey+8 {
		t.Errorf("REPL_OffsetValue = %d, want %d (Key end)", REPL_OffsetValue, REPL_OffsetKey+8)
	}
}

// ============================================================
// encodeEntry — wire format
// ============================================================

func TestEncodeEntry_NoFields_BufLength(t *testing.T) {
	e := ReplicationLogEntry{}
	buf, err := encodeEntry(e)
	if err != nil {
		t.Fatalf("encodeEntry: %v", err)
	}
	// 4-byte prefix + 17-byte body (LSN+Op+Key, no fields)
	if len(buf) != 21 {
		t.Errorf("buf len = %d, want 21", len(buf))
	}
}

func TestEncodeEntry_TotalLen_NoFields(t *testing.T) {
	e := ReplicationLogEntry{}
	buf, _ := encodeEntry(e)
	totalLen := binary.BigEndian.Uint32(buf[0:4])
	if totalLen != REPL_OffsetValue {
		t.Errorf("totalLen = %d, want %d (REPL_OffsetValue)", totalLen, REPL_OffsetValue)
	}
}

func TestEncodeEntry_TotalLen_WithIntField(t *testing.T) {
	fields := []btree.Field{{Tag: 1, Value: btree.IntValue{V: 0}}}
	e := ReplicationLogEntry{Fields: fields}
	buf, err := encodeEntry(e)
	if err != nil {
		t.Fatalf("encodeEntry: %v", err)
	}
	// IntValue encodes as 2 header + 8 payload = 10 bytes
	wantTotalLen := uint32(REPL_OffsetValue + 10)
	got := binary.BigEndian.Uint32(buf[0:4])
	if got != wantTotalLen {
		t.Errorf("totalLen = %d, want %d", got, wantTotalLen)
	}
}

func TestEncodeEntry_BufLen_EqualsFourPlusTotalLen(t *testing.T) {
	fields := []btree.Field{
		{Tag: 1, Value: btree.StringValue{V: "hello"}},
		{Tag: 2, Value: btree.IntValue{V: 99}},
	}
	e := ReplicationLogEntry{LSN: 7, Op: ReplPut, Key: 100, Fields: fields}
	buf, err := encodeEntry(e)
	if err != nil {
		t.Fatalf("encodeEntry: %v", err)
	}
	totalLen := binary.BigEndian.Uint32(buf[0:4])
	if uint32(len(buf)) != 4+totalLen {
		t.Errorf("len(buf) = %d, want 4+totalLen = %d", len(buf), 4+totalLen)
	}
}

func TestEncodeEntry_LSN_BigEndian(t *testing.T) {
	lsn := uint64(0xDEADBEEFCAFEBABE)
	e := ReplicationLogEntry{LSN: lsn}
	buf, _ := encodeEntry(e)
	// LSN at bytes [4 : 12] (base=4, REPL_OffsetLSN=0)
	got := binary.BigEndian.Uint64(buf[4:12])
	if got != lsn {
		t.Errorf("LSN in wire = %#x, want %#x", got, lsn)
	}
}

func TestEncodeEntry_Op_Put(t *testing.T) {
	e := ReplicationLogEntry{Op: ReplPut}
	buf, _ := encodeEntry(e)
	// Op at byte 4 + REPL_OffsetOp = 12
	if buf[4+REPL_OffsetOp] != byte(ReplPut) {
		t.Errorf("Op byte = %d, want %d (ReplPut)", buf[4+REPL_OffsetOp], byte(ReplPut))
	}
}

func TestEncodeEntry_Op_Delete(t *testing.T) {
	e := ReplicationLogEntry{Op: ReplDelete}
	buf, _ := encodeEntry(e)
	if buf[4+REPL_OffsetOp] != byte(ReplDelete) {
		t.Errorf("Op byte = %d, want %d (ReplDelete)", buf[4+REPL_OffsetOp], byte(ReplDelete))
	}
}

func TestEncodeEntry_Key_BigEndian(t *testing.T) {
	key := uint64(0x0102030405060708)
	e := ReplicationLogEntry{Key: key}
	buf, _ := encodeEntry(e)
	// Key at bytes [4+REPL_OffsetKey : 4+REPL_OffsetKey+8] = [13:21]
	got := binary.BigEndian.Uint64(buf[4+REPL_OffsetKey : 4+REPL_OffsetKey+8])
	if got != key {
		t.Errorf("Key in wire = %#x, want %#x", got, key)
	}
}

func TestEncodeEntry_Key_BoundaryValues(t *testing.T) {
	cases := []uint64{0, 1, ^uint64(0), 1 << 40}
	for _, key := range cases {
		e := ReplicationLogEntry{Key: key}
		buf, err := encodeEntry(e)
		if err != nil {
			t.Fatalf("encodeEntry(key=%d): %v", key, err)
		}
		got := binary.BigEndian.Uint64(buf[4+REPL_OffsetKey : 4+REPL_OffsetKey+8])
		if got != key {
			t.Errorf("key %d: wire produced %d", key, got)
		}
	}
}

func TestEncodeEntry_FieldError_Propagates(t *testing.T) {
	fields := []btree.Field{
		{Tag: 1, Value: btree.StringValue{V: strings.Repeat("x", 65536)}},
	}
	_, err := encodeEntry(ReplicationLogEntry{Fields: fields})
	if err == nil {
		t.Fatal("encodeEntry: expected error for oversized string field")
	}
}

// ============================================================
// decodeEntry
// ============================================================

func TestDecodeEntry_ZeroBody_AllZeroValues(t *testing.T) {
	data := make([]byte, REPL_OffsetValue)
	got, err := decodeEntry(data)
	if err != nil {
		t.Fatalf("decodeEntry: %v", err)
	}
	if got.LSN != 0 {
		t.Errorf("LSN = %d, want 0", got.LSN)
	}
	if got.Op != 0 {
		t.Errorf("Op = %d, want 0", got.Op)
	}
	if got.Key != 0 {
		t.Errorf("Key = %d, want 0", got.Key)
	}
	if len(got.Fields) != 0 {
		t.Errorf("Fields: want 0, got %d", len(got.Fields))
	}
}

func TestDecodeEntry_TooShort_Error(t *testing.T) {
	for _, n := range []int{0, 1, 8, 16} {
		data := make([]byte, n)
		_, err := decodeEntry(data)
		if err == nil {
			t.Errorf("decodeEntry(%d bytes): expected error", n)
		}
	}
}

func TestDecodeEntry_TrailingByte_Error(t *testing.T) {
	// A single byte after the header is not a parseable field (needs ≥2 bytes).
	data := make([]byte, REPL_OffsetValue+1)
	_, err := decodeEntry(data)
	if err == nil {
		t.Fatal("decodeEntry: expected error for 1 unparseable trailing byte")
	}
}

func TestDecodeEntry_AllOpValues(t *testing.T) {
	for _, op := range []ReplOp{ReplPut, ReplDelete} {
		e := ReplicationLogEntry{Op: op, Key: 1}
		buf, _ := encodeEntry(e)
		got, err := decodeEntry(buf[4:])
		if err != nil {
			t.Fatalf("op=%d: decodeEntry: %v", op, err)
		}
		if got.Op != op {
			t.Errorf("Op: want %d, got %d", op, got.Op)
		}
	}
}

func TestDecodeEntry_LSN_RoundTrip(t *testing.T) {
	cases := []uint64{0, 1, 42, ^uint64(0)}
	for _, lsn := range cases {
		e := ReplicationLogEntry{LSN: lsn}
		buf, _ := encodeEntry(e)
		got, err := decodeEntry(buf[4:])
		if err != nil {
			t.Fatalf("lsn=%d: decodeEntry: %v", lsn, err)
		}
		if got.LSN != lsn {
			t.Errorf("lsn %d: round-trip gave %d", lsn, got.LSN)
		}
	}
}

func TestDecodeEntry_Key_RoundTrip(t *testing.T) {
	cases := []uint64{0, 1, ^uint64(0), 1<<40 + 7}
	for _, key := range cases {
		e := ReplicationLogEntry{Key: key}
		buf, _ := encodeEntry(e)
		got, err := decodeEntry(buf[4:])
		if err != nil {
			t.Fatalf("key=%d: decodeEntry: %v", key, err)
		}
		if got.Key != key {
			t.Errorf("key %d: round-trip gave %d", key, got.Key)
		}
	}
}

func TestDecodeEntry_WithFields_RoundTrip(t *testing.T) {
	fields := []btree.Field{
		{Tag: 1, Value: btree.IntValue{V: -42}},
		{Tag: 2, Value: btree.StringValue{V: "world"}},
		{Tag: 3, Value: btree.NullValue{}},
	}
	e := ReplicationLogEntry{LSN: 5, Op: ReplPut, Key: 99, Fields: fields}
	buf, err := encodeEntry(e)
	if err != nil {
		t.Fatalf("encodeEntry: %v", err)
	}
	got, err := decodeEntry(buf[4:])
	if err != nil {
		t.Fatalf("decodeEntry: %v", err)
	}

	if got.LSN != 5 {
		t.Errorf("LSN: want 5, got %d", got.LSN)
	}
	if got.Op != ReplPut {
		t.Errorf("Op: want ReplPut, got %d", got.Op)
	}
	if got.Key != 99 {
		t.Errorf("Key: want 99, got %d", got.Key)
	}
	if len(got.Fields) != 3 {
		t.Fatalf("Fields: want 3, got %d", len(got.Fields))
	}
	if v := got.Fields[0].Value.(btree.IntValue).V; v != -42 {
		t.Errorf("field 0: want -42, got %d", v)
	}
	if v := got.Fields[1].Value.(btree.StringValue).V; v != "world" {
		t.Errorf("field 1: want 'world', got %q", v)
	}
	if _, ok := got.Fields[2].Value.(btree.NullValue); !ok {
		t.Errorf("field 2: want NullValue, got %T", got.Fields[2].Value)
	}
}

// ============================================================
// NewReplicationManager
// ============================================================

func TestNewReplicationManager_CreatesFile(t *testing.T) {
	path := t.TempDir() + "/repl.log"
	rm, err := NewReplicationManager(path)
	if err != nil {
		t.Fatalf("NewReplicationManager: %v", err)
	}
	defer func() { _ = rm.file.Close() }()

	if _, err := os.Stat(path); err != nil {
		t.Errorf("log file not created at %s: %v", path, err)
	}
}

func TestNewReplicationManager_EmptyFile_NextLSNZero(t *testing.T) {
	rm := newManagerForTest(t)
	if rm.nextLSN != 0 {
		t.Errorf("nextLSN = %d, want 0 for empty file", rm.nextLSN)
	}
}

func TestNewReplicationManager_ExistingRecords_RecoverNextLSN(t *testing.T) {
	path := t.TempDir() + "/repl.log"

	rm1, err := NewReplicationManager(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	for i := 0; i < 3; i++ {
		mustAppend(t, rm1, ReplPut, uint64(i), nil)
	}
	_ = rm1.file.Close()

	rm2, err := NewReplicationManager(path)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer func() { _ = rm2.file.Close() }()

	if rm2.nextLSN != 3 {
		t.Errorf("recovered nextLSN = %d, want 3", rm2.nextLSN)
	}
}

func TestNewReplicationManager_PartialLengthPrefix_Tolerated(t *testing.T) {
	path := t.TempDir() + "/repl.log"

	rm1, _ := NewReplicationManager(path)
	mustAppend(t, rm1, ReplPut, 1, nil) // LSN 0
	_ = rm1.file.Close()

	// Append only 2 bytes — too short to be a valid length prefix.
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	_, _ = f.Write([]byte{0x00, 0x00})
	_ = f.Close()

	rm2, err := NewReplicationManager(path)
	if err != nil {
		t.Fatalf("NewReplicationManager with partial prefix: %v", err)
	}
	defer func() { _ = rm2.file.Close() }()

	if rm2.nextLSN != 1 {
		t.Errorf("nextLSN = %d, want 1 (only one valid record)", rm2.nextLSN)
	}
}

func TestNewReplicationManager_PartialBody_Tolerated(t *testing.T) {
	path := t.TempDir() + "/repl.log"

	rm1, _ := NewReplicationManager(path)
	mustAppend(t, rm1, ReplPut, 42, nil) // LSN 0
	_ = rm1.file.Close()

	// Write a length prefix promising REPL_OffsetValue bytes but deliver only 5.
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], REPL_OffsetValue)
	_, _ = f.Write(lenBuf[:])
	_, _ = f.Write(make([]byte, 5))
	_ = f.Close()

	rm2, err := NewReplicationManager(path)
	if err != nil {
		t.Fatalf("NewReplicationManager: %v", err)
	}
	defer func() { _ = rm2.file.Close() }()

	if rm2.nextLSN != 1 {
		t.Errorf("nextLSN = %d, want 1 (valid record before partial body)", rm2.nextLSN)
	}
}

func TestNewReplicationManager_SmallTotalLen_StopsScan(t *testing.T) {
	path := t.TempDir() + "/repl.log"

	rm1, _ := NewReplicationManager(path)
	mustAppend(t, rm1, ReplPut, 1, nil) // LSN 0
	_ = rm1.file.Close()

	// Append a length prefix of 5 (< REPL_OffsetValue=17 → treated as corrupt).
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 5)
	_, _ = f.Write(lenBuf[:])
	_ = f.Close()

	rm2, err := NewReplicationManager(path)
	if err != nil {
		t.Fatalf("NewReplicationManager: %v", err)
	}
	defer func() { _ = rm2.file.Close() }()

	// Scan stops at the corrupt prefix; LSN 0 was recovered from the valid record.
	if rm2.nextLSN != 1 {
		t.Errorf("nextLSN = %d, want 1", rm2.nextLSN)
	}
}

// ============================================================
// Append
// ============================================================

func TestAppend_FirstLSN_IsZero(t *testing.T) {
	rm := newManagerForTest(t)
	err := rm.Append([]ReplicationLogEntry{{Op: ReplPut, Key: 42}})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	entries, err := rm.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	if entries[0].LSN != 0 {
		t.Errorf("first LSN = %d, want 0", entries[0].LSN)
	}
}

func TestAppend_LSNs_MonotonicallyIncrease(t *testing.T) {
	rm := newManagerForTest(t)
	for i := uint64(0); i < 5; i++ {
		lsn := mustAppend(t, rm, ReplPut, i, nil)
		if lsn != i {
			t.Errorf("Append %d: returned LSN %d, want %d", i, lsn, i)
		}
	}
	if rm.nextLSN != 5 {
		t.Errorf("nextLSN after 5 appends = %d, want 5", rm.nextLSN)
	}
}

func TestAppend_ReturnedLSN_MatchesReadBack(t *testing.T) {
	rm := newManagerForTest(t)
	lsn := mustAppend(t, rm, ReplPut, 77, nil)

	entries, err := rm.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	if entries[0].LSN != lsn {
		t.Errorf("read-back LSN = %d, want %d (returned by Append)", entries[0].LSN, lsn)
	}
}

func TestAppend_FileSizeGrows(t *testing.T) {
	rm := newManagerForTest(t)

	info0, _ := rm.file.Stat()
	size0 := info0.Size()

	mustAppend(t, rm, ReplPut, 1, nil)
	info1, _ := rm.file.Stat()
	if info1.Size() <= size0 {
		t.Errorf("size after first append = %d, want > %d", info1.Size(), size0)
	}

	mustAppend(t, rm, ReplDelete, 2, nil)
	info2, _ := rm.file.Stat()
	if info2.Size() <= info1.Size() {
		t.Errorf("size after second append = %d, want > %d", info2.Size(), info1.Size())
	}
}

func TestAppend_DeleteOp_NilFields_Succeeds(t *testing.T) {
	rm := newManagerForTest(t)
	lsn := mustAppend(t, rm, ReplDelete, 99, nil)

	entries, err := rm.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	if entries[0].Op != ReplDelete {
		t.Errorf("Op: want ReplDelete, got %d", entries[0].Op)
	}
	if entries[0].LSN != lsn {
		t.Errorf("LSN: want %d, got %d", lsn, entries[0].LSN)
	}
	if len(entries[0].Fields) != 0 {
		t.Errorf("Fields: want empty, got %d", len(entries[0].Fields))
	}
}

func TestAppend_AllFieldTypes_Persisted(t *testing.T) {
	rm := newManagerForTest(t)
	fields := []btree.Field{
		{Tag: 1, Value: btree.NullValue{}},
		{Tag: 2, Value: btree.IntValue{V: -1000}},
		{Tag: 3, Value: btree.StringValue{V: "hello"}},
		{Tag: 4, Value: btree.ListValue{
			ElemType: btree.FieldTypeInt,
			Elems:    []btree.Value{btree.IntValue{V: 1}, btree.IntValue{V: 2}},
		}},
	}
	mustAppend(t, rm, ReplPut, 7, fields)

	entries, err := rm.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	got := entries[0].Fields
	if len(got) != 4 {
		t.Fatalf("want 4 fields, got %d", len(got))
	}
	if _, ok := got[0].Value.(btree.NullValue); !ok {
		t.Errorf("field 0: want NullValue, got %T", got[0].Value)
	}
	if v := got[1].Value.(btree.IntValue).V; v != -1000 {
		t.Errorf("field 1: want -1000, got %d", v)
	}
	if v := got[2].Value.(btree.StringValue).V; v != "hello" {
		t.Errorf("field 2: want 'hello', got %q", v)
	}
	lv := got[3].Value.(btree.ListValue)
	if len(lv.Elems) != 2 {
		t.Fatalf("field 3 list: want 2 elems, got %d", len(lv.Elems))
	}
	if lv.Elems[0].(btree.IntValue).V != 1 || lv.Elems[1].(btree.IntValue).V != 2 {
		t.Errorf("field 3 list values: want [1,2], got [%v,%v]", lv.Elems[0], lv.Elems[1])
	}
}

// ============================================================
// ReadFrom
// ============================================================

func TestReadFrom_EmptyFile_ReturnsEmpty(t *testing.T) {
	rm := newManagerForTest(t)
	entries, err := rm.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom on empty file: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("want 0 entries, got %d", len(entries))
	}
}

func TestReadFrom_StartZero_ReturnsAll(t *testing.T) {
	rm := newManagerForTest(t)
	for i := 0; i < 5; i++ {
		mustAppend(t, rm, ReplPut, uint64(i*10), nil)
	}

	entries, err := rm.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 5 {
		t.Errorf("want 5 entries, got %d", len(entries))
	}
	for i, e := range entries {
		if e.LSN != uint64(i) {
			t.Errorf("entries[%d].LSN = %d, want %d", i, e.LSN, i)
		}
	}
}

func TestReadFrom_FiltersLSN_CorrectCount(t *testing.T) {
	rm := newManagerForTest(t)
	for i := 0; i < 5; i++ {
		mustAppend(t, rm, ReplPut, uint64(i), nil) // LSNs 0–4
	}

	entries, _ := rm.ReadFrom(2)
	if len(entries) != 3 { // LSNs 2, 3, 4
		t.Errorf("ReadFrom(2): want 3 entries, got %d", len(entries))
	}
}

func TestReadFrom_FiltersLSN_FirstEntryMatchesStart(t *testing.T) {
	rm := newManagerForTest(t)
	for i := 0; i < 5; i++ {
		mustAppend(t, rm, ReplPut, uint64(i), nil)
	}

	entries, _ := rm.ReadFrom(3)
	if len(entries) == 0 {
		t.Fatal("ReadFrom(3): want entries, got none")
	}
	if entries[0].LSN != 3 {
		t.Errorf("ReadFrom(3): first entry LSN = %d, want 3", entries[0].LSN)
	}
}

func TestReadFrom_StartLSNAboveAll_ReturnsEmpty(t *testing.T) {
	rm := newManagerForTest(t)
	for i := 0; i < 3; i++ {
		mustAppend(t, rm, ReplPut, uint64(i), nil)
	}

	entries, _ := rm.ReadFrom(100)
	if len(entries) != 0 {
		t.Errorf("ReadFrom(100): want 0, got %d", len(entries))
	}
}

func TestReadFrom_StartLSN_IsInclusive(t *testing.T) {
	rm := newManagerForTest(t)
	mustAppend(t, rm, ReplPut, 1, nil) // LSN 0
	mustAppend(t, rm, ReplPut, 2, nil) // LSN 1
	mustAppend(t, rm, ReplPut, 3, nil) // LSN 2

	entries, _ := rm.ReadFrom(1) // should include LSN 1 and 2
	if len(entries) != 2 {
		t.Fatalf("ReadFrom(1): want 2 entries, got %d", len(entries))
	}
	if entries[0].LSN != 1 {
		t.Errorf("first entry LSN = %d, want 1", entries[0].LSN)
	}
	if entries[1].LSN != 2 {
		t.Errorf("second entry LSN = %d, want 2", entries[1].LSN)
	}
}

func TestReadFrom_KeysPreserved(t *testing.T) {
	rm := newManagerForTest(t)
	keys := []uint64{0, 1, ^uint64(0), 12345, 1 << 40}
	for _, k := range keys {
		mustAppend(t, rm, ReplPut, k, nil)
	}

	entries, err := rm.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != len(keys) {
		t.Fatalf("want %d entries, got %d", len(keys), len(entries))
	}
	for i, want := range keys {
		if entries[i].Key != want {
			t.Errorf("entries[%d].Key = %d, want %d", i, entries[i].Key, want)
		}
	}
}

func TestReadFrom_OpsPreserved(t *testing.T) {
	rm := newManagerForTest(t)
	mustAppend(t, rm, ReplPut, 1, nil)
	mustAppend(t, rm, ReplDelete, 1, nil)
	mustAppend(t, rm, ReplPut, 2, nil)

	entries, _ := rm.ReadFrom(0)
	if len(entries) != 3 {
		t.Fatalf("want 3 entries, got %d", len(entries))
	}
	if entries[0].Op != ReplPut {
		t.Errorf("entries[0].Op = %d, want ReplPut", entries[0].Op)
	}
	if entries[1].Op != ReplDelete {
		t.Errorf("entries[1].Op = %d, want ReplDelete", entries[1].Op)
	}
	if entries[2].Op != ReplPut {
		t.Errorf("entries[2].Op = %d, want ReplPut", entries[2].Op)
	}
}

func TestReadFrom_FieldsRoundTrip(t *testing.T) {
	rm := newManagerForTest(t)
	fields := []btree.Field{
		{Tag: 5, Value: btree.StringValue{V: "test-value"}},
		{Tag: 6, Value: btree.IntValue{V: 12345}},
	}
	mustAppend(t, rm, ReplPut, 42, fields)

	entries, _ := rm.ReadFrom(0)
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	got := entries[0].Fields
	if len(got) != 2 {
		t.Fatalf("want 2 fields, got %d", len(got))
	}
	if got[0].Tag != 5 {
		t.Errorf("field 0 tag: want 5, got %d", got[0].Tag)
	}
	if v := got[0].Value.(btree.StringValue).V; v != "test-value" {
		t.Errorf("field 0 string: want 'test-value', got %q", v)
	}
	if got[1].Tag != 6 {
		t.Errorf("field 1 tag: want 6, got %d", got[1].Tag)
	}
	if v := got[1].Value.(btree.IntValue).V; v != 12345 {
		t.Errorf("field 1 int: want 12345, got %d", v)
	}
}

func TestReadFrom_Idempotent(t *testing.T) {
	rm := newManagerForTest(t)
	mustAppend(t, rm, ReplPut, 1, nil)
	mustAppend(t, rm, ReplPut, 2, nil)

	e1, _ := rm.ReadFrom(0)
	e2, _ := rm.ReadFrom(0)
	if len(e1) != len(e2) {
		t.Fatalf("ReadFrom not idempotent: first=%d, second=%d entries", len(e1), len(e2))
	}
	for i := range e1 {
		if e1[i].LSN != e2[i].LSN || e1[i].Key != e2[i].Key {
			t.Errorf("entry %d differs between calls: (%d,%d) vs (%d,%d)",
				i, e1[i].LSN, e1[i].Key, e2[i].LSN, e2[i].Key)
		}
	}
}

func TestReadFrom_CanAppendAfterRead(t *testing.T) {
	// Verifies that O_APPEND writes still go to the end after ReadFrom seeks the file.
	rm := newManagerForTest(t)
	mustAppend(t, rm, ReplPut, 1, nil) // LSN 0
	_, _ = rm.ReadFrom(0)              // advances file pointer to end
	mustAppend(t, rm, ReplPut, 2, nil) // LSN 1 — must land at end of file

	entries, err := rm.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("after read+append: want 2 entries, got %d", len(entries))
	}
	if entries[1].Key != 2 {
		t.Errorf("second entry key = %d, want 2", entries[1].Key)
	}
}

func TestReadFrom_PartialTailRecord_Tolerated(t *testing.T) {
	path := t.TempDir() + "/repl.log"

	rm1, _ := NewReplicationManager(path)
	mustAppend(t, rm1, ReplPut, 1, nil) // LSN 0
	mustAppend(t, rm1, ReplPut, 2, nil) // LSN 1
	_ = rm1.file.Close()

	// Write a valid length prefix but zero body bytes following it.
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], REPL_OffsetValue)
	_, _ = f.Write(lenBuf[:]) // no body follows
	_ = f.Close()

	rm2, err := NewReplicationManager(path)
	if err != nil {
		t.Fatalf("NewReplicationManager: %v", err)
	}
	defer func() { _ = rm2.file.Close() }()

	entries, err := rm2.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("want 2 valid entries (not the partial tail), got %d", len(entries))
	}
}

// ============================================================
// Recovery — reopening the file
// ============================================================

func TestRecovery_NextLSN_AfterCleanClose(t *testing.T) {
	path := t.TempDir() + "/repl.log"

	rm1, _ := NewReplicationManager(path)
	mustAppend(t, rm1, ReplPut, 10, nil)   // LSN 0
	mustAppend(t, rm1, ReplPut, 20, nil)   // LSN 1
	mustAppend(t, rm1, ReplDelete, 5, nil) // LSN 2
	_ = rm1.file.Close()

	rm2, err := NewReplicationManager(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = rm2.file.Close() }()

	if rm2.nextLSN != 3 {
		t.Errorf("nextLSN after reopen = %d, want 3", rm2.nextLSN)
	}
}

func TestRecovery_NextLSN_SingleRecord(t *testing.T) {
	path := t.TempDir() + "/repl.log"

	rm1, _ := NewReplicationManager(path)
	mustAppend(t, rm1, ReplDelete, 0, nil) // LSN 0
	_ = rm1.file.Close()

	rm2, err := NewReplicationManager(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = rm2.file.Close() }()

	if rm2.nextLSN != 1 {
		t.Errorf("nextLSN = %d, want 1", rm2.nextLSN)
	}
}

func TestRecovery_ReadFrom_SameEntriesAfterReopen(t *testing.T) {
	path := t.TempDir() + "/repl.log"

	rm1, _ := NewReplicationManager(path)
	fields := []btree.Field{{Tag: 1, Value: btree.IntValue{V: 100}}}
	mustAppend(t, rm1, ReplPut, 10, fields)
	mustAppend(t, rm1, ReplDelete, 20, nil)
	_ = rm1.file.Close()

	rm2, err := NewReplicationManager(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = rm2.file.Close() }()

	entries, err := rm2.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom after reopen: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("want 2 entries, got %d", len(entries))
	}
	if entries[0].Key != 10 || entries[0].Op != ReplPut {
		t.Errorf("entry 0: Key=%d Op=%d, want Key=10 Op=ReplPut", entries[0].Key, entries[0].Op)
	}
	if entries[1].Key != 20 || entries[1].Op != ReplDelete {
		t.Errorf("entry 1: Key=%d Op=%d, want Key=20 Op=ReplDelete", entries[1].Key, entries[1].Op)
	}
	if v := entries[0].Fields[0].Value.(btree.IntValue).V; v != 100 {
		t.Errorf("entry 0 field: want 100, got %d", v)
	}
}

func TestRecovery_AppendAfterReopen_ContinuesLSN(t *testing.T) {
	path := t.TempDir() + "/repl.log"

	rm1, _ := NewReplicationManager(path)
	mustAppend(t, rm1, ReplPut, 1, nil) // LSN 0
	mustAppend(t, rm1, ReplPut, 2, nil) // LSN 1
	_ = rm1.file.Close()

	rm2, err := NewReplicationManager(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = rm2.file.Close() }()

	lsn := mustAppend(t, rm2, ReplPut, 3, nil) // should be LSN 2
	if lsn != 2 {
		t.Errorf("first append after reopen: LSN = %d, want 2", lsn)
	}

	entries, _ := rm2.ReadFrom(0)
	if len(entries) != 3 {
		t.Fatalf("total entries: want 3, got %d", len(entries))
	}
}

// ============================================================
// Scenarios
// ============================================================

func TestScenario_PutAndDelete(t *testing.T) {
	rm := newManagerForTest(t)
	fields := []btree.Field{
		{Tag: 1, Value: btree.StringValue{V: "Alice"}},
		{Tag: 2, Value: btree.IntValue{V: 30}},
	}

	putLSN := mustAppend(t, rm, ReplPut, 1001, fields)
	delLSN := mustAppend(t, rm, ReplDelete, 1001, nil)

	entries, err := rm.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("want 2 entries, got %d", len(entries))
	}

	put := entries[0]
	if put.LSN != putLSN || put.Op != ReplPut || put.Key != 1001 {
		t.Errorf("put entry: LSN=%d Op=%d Key=%d", put.LSN, put.Op, put.Key)
	}
	if len(put.Fields) != 2 {
		t.Fatalf("put entry: want 2 fields, got %d", len(put.Fields))
	}
	if v := put.Fields[0].Value.(btree.StringValue).V; v != "Alice" {
		t.Errorf("name field: want 'Alice', got %q", v)
	}

	del := entries[1]
	if del.LSN != delLSN || del.Op != ReplDelete || del.Key != 1001 {
		t.Errorf("delete entry: LSN=%d Op=%d Key=%d", del.LSN, del.Op, del.Key)
	}
	if len(del.Fields) != 0 {
		t.Errorf("delete entry: want 0 fields, got %d", len(del.Fields))
	}
}

func TestScenario_ManyEntries_RoundTrip(t *testing.T) {
	const n = 100
	rm := newManagerForTest(t)

	for i := 0; i < n; i++ {
		mustAppend(t, rm, ReplPut, uint64(i), []btree.Field{
			{Tag: 1, Value: btree.IntValue{V: int64(i)}},
		})
	}

	entries, err := rm.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != n {
		t.Fatalf("want %d entries, got %d", n, len(entries))
	}
	for i, e := range entries {
		if e.LSN != uint64(i) {
			t.Errorf("entries[%d].LSN = %d, want %d", i, e.LSN, i)
		}
		if e.Key != uint64(i) {
			t.Errorf("entries[%d].Key = %d, want %d", i, e.Key, i)
		}
		if v := e.Fields[0].Value.(btree.IntValue).V; v != int64(i) {
			t.Errorf("entries[%d] field 0: want %d, got %d", i, i, v)
		}
	}
}

func TestScenario_UnicodeStringFields(t *testing.T) {
	rm := newManagerForTest(t)
	unicode := "こんにちは世界🌍"

	mustAppend(t, rm, ReplPut, 1, []btree.Field{
		{Tag: 1, Value: btree.StringValue{V: unicode}},
	})

	entries, err := rm.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	if v := entries[0].Fields[0].Value.(btree.StringValue).V; v != unicode {
		t.Errorf("unicode: want %q, got %q", unicode, v)
	}
}

func TestScenario_MultipleFieldTypes_ComplexRecord(t *testing.T) {
	rm := newManagerForTest(t)
	fields := []btree.Field{
		{Tag: 1, Value: btree.IntValue{V: 9999}},
		{Tag: 2, Value: btree.StringValue{V: "widget"}},
		{Tag: 3, Value: btree.NullValue{}},
		{Tag: 4, Value: btree.ListValue{
			ElemType: btree.FieldTypeString,
			Elems:    []btree.Value{btree.StringValue{V: "a"}, btree.StringValue{V: "b"}, btree.StringValue{V: "c"}},
		}},
		{Tag: 5, Value: btree.IntValue{V: -1}},
	}
	mustAppend(t, rm, ReplPut, 55555, fields)

	entries, err := rm.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	got := entries[0].Fields
	if len(got) != 5 {
		t.Fatalf("want 5 fields, got %d", len(got))
	}
	if got[0].Value.(btree.IntValue).V != 9999 {
		t.Errorf("field 0: want 9999")
	}
	if got[1].Value.(btree.StringValue).V != "widget" {
		t.Errorf("field 1: want 'widget'")
	}
	if _, ok := got[2].Value.(btree.NullValue); !ok {
		t.Errorf("field 2: want NullValue, got %T", got[2].Value)
	}
	lv := got[3].Value.(btree.ListValue)
	if len(lv.Elems) != 3 {
		t.Fatalf("field 3 list: want 3 elems, got %d", len(lv.Elems))
	}
	for i, want := range []string{"a", "b", "c"} {
		if v := lv.Elems[i].(btree.StringValue).V; v != want {
			t.Errorf("list[%d]: want %q, got %q", i, want, v)
		}
	}
	if got[4].Value.(btree.IntValue).V != -1 {
		t.Errorf("field 4: want -1")
	}
}

func TestScenario_ReadFrom_IncrementalWindows(t *testing.T) {
	const n = 10
	rm := newManagerForTest(t)
	for i := 0; i < n; i++ {
		mustAppend(t, rm, ReplPut, uint64(i), nil)
	}

	cases := []struct {
		startLSN uint64
		wantLen  int
	}{
		{0, 10},
		{5, 5},
		{9, 1},
		{10, 0},
		{100, 0},
	}
	for _, c := range cases {
		entries, err := rm.ReadFrom(c.startLSN)
		if err != nil {
			t.Fatalf("ReadFrom(%d): %v", c.startLSN, err)
		}
		if len(entries) != c.wantLen {
			t.Errorf("ReadFrom(%d): want %d entries, got %d", c.startLSN, c.wantLen, len(entries))
		}
	}
}

func TestScenario_EmptyStringAndZeroInt(t *testing.T) {
	rm := newManagerForTest(t)
	fields := []btree.Field{
		{Tag: 1, Value: btree.StringValue{V: ""}},
		{Tag: 2, Value: btree.IntValue{V: 0}},
		{Tag: 3, Value: btree.ListValue{ElemType: btree.FieldTypeInt, Elems: nil}},
	}
	mustAppend(t, rm, ReplPut, 0, fields)

	entries, _ := rm.ReadFrom(0)
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	got := entries[0].Fields
	if len(got) != 3 {
		t.Fatalf("want 3 fields, got %d", len(got))
	}
	if v := got[0].Value.(btree.StringValue).V; v != "" {
		t.Errorf("empty string: got %q", v)
	}
	if v := got[1].Value.(btree.IntValue).V; v != 0 {
		t.Errorf("zero int: got %d", v)
	}
	lv := got[2].Value.(btree.ListValue)
	if len(lv.Elems) != 0 {
		t.Errorf("empty list: got %d elems", len(lv.Elems))
	}
}

// ============================================================
// ReplicationClient test infrastructure
// ============================================================

// newTestBTreeForClient creates a fresh BTree + BufferPool in a temp dir.
func newTestBTreeForClient(t *testing.T) (*btree.BTree, pagemanager.PageManager) {
	t.Helper()
	path := t.TempDir() + "/client.db"
	disk, err := pagemanager.NewDB(path)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	wal, err := pagemanager.NewWAL(disk, path)
	if err != nil {
		_ = disk.Close()
		t.Fatalf("NewWAL: %v", err)
	}
	bp := pagemanager.NewBufferPool(wal, 64)
	t.Cleanup(func() { _ = bp.Close() })
	return btree.NewBTree(bp), bp
}

// fakeReplServer is a controllable in-process ReplicationService.
//
//   - batches: slices of entries sent as individual PullResponse messages.
//   - blockAfter: if true, the handler waits for context cancellation after
//     sending all batches (simulating a live leader); otherwise it returns
//     immediately so the client stream ends cleanly.
//   - errAfter: if non-nil, the handler returns this error after batches
//     (causes a gRPC status error on the client side).
//   - startLSNSeen: if non-nil, the StartLsn from the first PullRequest is
//     forwarded into this channel.
type fakeReplServer struct {
	pb.UnimplementedReplicationServiceServer
	batches      [][]*pb.ReplicationLogEntry
	blockAfter   bool
	errAfter     error
	startLSNSeen chan uint64
}

func (f *fakeReplServer) StreamUpdates(req *pb.PullRequest, stream pb.ReplicationService_StreamUpdatesServer) error {
	if f.startLSNSeen != nil {
		select {
		case f.startLSNSeen <- req.StartLsn:
		default:
		}
	}
	for _, batch := range f.batches {
		if err := stream.Send(&pb.PullResponse{Entries: batch}); err != nil {
			return err
		}
	}
	if f.errAfter != nil {
		return f.errAfter
	}
	if f.blockAfter {
		<-stream.Context().Done()
	}
	return nil
}

// startFakeServer registers srv on a random local port and returns its address.
func startFakeServer(t *testing.T, srv pb.ReplicationServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterReplicationServiceServer(s, srv)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.GracefulStop)
	return lis.Addr().String()
}

// runClient starts rc.Start(ctx) in a background goroutine and returns the
// result channel.  The caller should drain the channel exactly once.
func runClient(rc *ReplicationClient, ctx context.Context) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- rc.Start(ctx) }()
	return ch
}

// errCheckpointPM wraps a real PageManager but always returns a fixed error
// from SetMetaCheckpointLSN.
type errCheckpointPM struct {
	pagemanager.PageManager
	err error
}

func (e *errCheckpointPM) SetMetaCheckpointLSN(_ uint64) error { return e.err }

// Proto entry helpers.
func putEntry(lsn, key uint64, fvs ...*pb.FieldValue) *pb.ReplicationLogEntry {
	return &pb.ReplicationLogEntry{Lsn: lsn, Op: int32(ReplPut), Key: key, Fields: fvs}
}
func delEntry(lsn, key uint64) *pb.ReplicationLogEntry {
	return &pb.ReplicationLogEntry{Lsn: lsn, Op: int32(ReplDelete), Key: key}
}
func intFV(v int64) *pb.FieldValue {
	return &pb.FieldValue{Value: &pb.FieldValue_IntValue{IntValue: v}}
}
func strFV(v string) *pb.FieldValue {
	return &pb.FieldValue{Value: &pb.FieldValue_StringValue{StringValue: v}}
}

// mustSearch asserts that key exists in bt and returns its fields.
func mustSearch(t *testing.T, bt *btree.BTree, key uint64) []btree.Field {
	t.Helper()
	fields, found, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Search(%d): %v", key, err)
	}
	if !found {
		t.Fatalf("key %d not found in btree", key)
	}
	return fields
}

// mustAbsent asserts that key is absent from bt.
func mustAbsent(t *testing.T, bt *btree.BTree, key uint64) {
	t.Helper()
	_, found, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Search(%d): %v", key, err)
	}
	if found {
		t.Fatalf("key %d should be absent but was found", key)
	}
}

// ============================================================
// protoToField
// ============================================================

func TestProtoToField_IntValue(t *testing.T) {
	f := protoToField(&pb.FieldValue{Value: &pb.FieldValue_IntValue{IntValue: 42}}, 3)
	if f.Tag != 3 {
		t.Errorf("Tag: got %d, want 3", f.Tag)
	}
	v, ok := f.Value.(btree.IntValue)
	if !ok {
		t.Fatalf("Value type: got %T, want IntValue", f.Value)
	}
	if v.V != 42 {
		t.Errorf("V: got %d, want 42", v.V)
	}
}

func TestProtoToField_IntValue_Negative(t *testing.T) {
	f := protoToField(&pb.FieldValue{Value: &pb.FieldValue_IntValue{IntValue: -999}}, 1)
	v := f.Value.(btree.IntValue)
	if v.V != -999 {
		t.Errorf("V: got %d, want -999", v.V)
	}
}

func TestProtoToField_StringValue(t *testing.T) {
	f := protoToField(&pb.FieldValue{Value: &pb.FieldValue_StringValue{StringValue: "hello"}}, 2)
	v, ok := f.Value.(btree.StringValue)
	if !ok {
		t.Fatalf("Value type: got %T, want StringValue", f.Value)
	}
	if v.V != "hello" {
		t.Errorf("V: got %q, want 'hello'", v.V)
	}
}

func TestProtoToField_StringValue_Empty(t *testing.T) {
	f := protoToField(&pb.FieldValue{Value: &pb.FieldValue_StringValue{StringValue: ""}}, 1)
	v := f.Value.(btree.StringValue)
	if v.V != "" {
		t.Errorf("expected empty string, got %q", v.V)
	}
}

func TestProtoToField_BoolValue_True(t *testing.T) {
	f := protoToField(&pb.FieldValue{Value: &pb.FieldValue_BoolValue{BoolValue: true}}, 1)
	v, ok := f.Value.(btree.StringValue)
	if !ok {
		t.Fatalf("bool true: got %T, want StringValue", f.Value)
	}
	if v.V != "TRUE" {
		t.Errorf("bool true: got %q, want 'TRUE'", v.V)
	}
}

func TestProtoToField_BoolValue_False(t *testing.T) {
	f := protoToField(&pb.FieldValue{Value: &pb.FieldValue_BoolValue{BoolValue: false}}, 1)
	v := f.Value.(btree.StringValue)
	if v.V != "FALSE" {
		t.Errorf("bool false: got %q, want 'FALSE'", v.V)
	}
}

func TestProtoToField_ListValue_WithElements(t *testing.T) {
	f := protoToField(&pb.FieldValue{
		Value: &pb.FieldValue_ListValue{ListValue: &pb.FieldList{
			ElemType: uint32(btree.FieldTypeInt),
			Elems:    []*pb.FieldValue{intFV(10), intFV(20)},
		}},
	}, 5)
	lv, ok := f.Value.(btree.ListValue)
	if !ok {
		t.Fatalf("Value type: got %T, want ListValue", f.Value)
	}
	if lv.ElemType != btree.FieldTypeInt {
		t.Errorf("ElemType: got %d, want FieldTypeInt", lv.ElemType)
	}
	if len(lv.Elems) != 2 {
		t.Fatalf("Elems: want 2, got %d", len(lv.Elems))
	}
	if lv.Elems[0].(btree.IntValue).V != 10 {
		t.Errorf("Elems[0]: want 10, got %v", lv.Elems[0])
	}
	if lv.Elems[1].(btree.IntValue).V != 20 {
		t.Errorf("Elems[1]: want 20, got %v", lv.Elems[1])
	}
}

func TestProtoToField_ListValue_Empty(t *testing.T) {
	f := protoToField(&pb.FieldValue{
		Value: &pb.FieldValue_ListValue{ListValue: &pb.FieldList{
			ElemType: uint32(btree.FieldTypeString),
			Elems:    nil,
		}},
	}, 2)
	lv := f.Value.(btree.ListValue)
	if len(lv.Elems) != 0 {
		t.Errorf("empty list: want 0 elems, got %d", len(lv.Elems))
	}
}

func TestProtoToField_NilValue_ReturnsNullField(t *testing.T) {
	f := protoToField(&pb.FieldValue{}, 7)
	if _, ok := f.Value.(btree.NullValue); !ok {
		t.Errorf("nil oneof: got %T, want NullValue", f.Value)
	}
	if f.Tag != 7 {
		t.Errorf("Tag: got %d, want 7", f.Tag)
	}
}

func TestProtoToField_TagPreserved(t *testing.T) {
	for _, tag := range []uint8{0, 1, 10, 255} {
		f := protoToField(&pb.FieldValue{Value: &pb.FieldValue_IntValue{}}, tag)
		if f.Tag != tag {
			t.Errorf("tag %d round-trip: got %d", tag, f.Tag)
		}
	}
}

// ============================================================
// NewReplicationClient
// ============================================================

func TestNewReplicationClient_LastAppliedLSN(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)
	rc := NewReplicationClient(bt, pm, 42, "localhost:9", nil)
	if rc.lastAppliedLSN != 42 {
		t.Errorf("lastAppliedLSN: got %d, want 42", rc.lastAppliedLSN)
	}
}

func TestNewReplicationClient_LeaderAddr(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)
	rc := NewReplicationClient(bt, pm, 0, "leader:5556", nil)
	if rc.leaderAddr != "leader:5556" {
		t.Errorf("leaderAddr: got %q, want 'leader:5556'", rc.leaderAddr)
	}
}

func TestNewReplicationClient_NilOnBatchAllowed(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)
	rc := NewReplicationClient(bt, pm, 0, "x:1", nil)
	if rc.onBatch != nil {
		t.Error("onBatch should be nil when nil is passed")
	}
}

// ============================================================
// ReplicationClient.Start
// ============================================================

func TestClientStart_AppliesPutEntry(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeReplServer{
		batches:    [][]*pb.ReplicationLogEntry{{putEntry(0, 42, intFV(100))}},
		blockAfter: true,
	}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, func() error { cancel(); return nil })
	if err := <-runClient(rc, ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	fields := mustSearch(t, bt, 42)
	if fields[0].Value.(btree.IntValue).V != 100 {
		t.Errorf("field 0: got %v, want IntValue{100}", fields[0].Value)
	}
}

func TestClientStart_AppliesDeleteEntry(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)
	// pre-insert the key so there is something to delete
	if err := bt.Insert(55, []btree.Field{{Tag: 1, Value: btree.IntValue{V: 1}}}); err != nil {
		t.Fatalf("pre-insert: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeReplServer{
		batches:    [][]*pb.ReplicationLogEntry{{delEntry(0, 55)}},
		blockAfter: true,
	}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, func() error { cancel(); return nil })
	if err := <-runClient(rc, ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	mustAbsent(t, bt, 55)
}

func TestClientStart_MultiplePutsInOneBatch(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	batch := []*pb.ReplicationLogEntry{
		putEntry(0, 10, intFV(1)),
		putEntry(1, 20, intFV(2)),
		putEntry(2, 30, intFV(3)),
	}
	fake := &fakeReplServer{batches: [][]*pb.ReplicationLogEntry{batch}, blockAfter: true}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, func() error { cancel(); return nil })
	if err := <-runClient(rc, ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	mustSearch(t, bt, 10)
	mustSearch(t, bt, 20)
	mustSearch(t, bt, 30)
}

func TestClientStart_MultipleBatches(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)

	received := make(chan struct{}, 10)
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeReplServer{
		batches: [][]*pb.ReplicationLogEntry{
			{putEntry(0, 100, intFV(1))},
			{putEntry(1, 200, intFV(2))},
		},
		blockAfter: true,
	}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, func() error {
		received <- struct{}{}
		return nil
	})
	errCh := runClient(rc, ctx)

	<-received // batch 1 applied
	<-received // batch 2 applied
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Start: %v", err)
	}

	mustSearch(t, bt, 100)
	mustSearch(t, bt, 200)
}

func TestClientStart_AdvancesLastAppliedLSNPerEntry(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	batch := []*pb.ReplicationLogEntry{
		putEntry(0, 10, intFV(1)),
		putEntry(1, 20, intFV(2)),
		putEntry(2, 30, intFV(3)),
	}
	fake := &fakeReplServer{batches: [][]*pb.ReplicationLogEntry{batch}, blockAfter: true}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, func() error { cancel(); return nil })
	if err := <-runClient(rc, ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// after applying entries with LSNs 0,1,2 the next expected LSN is 3
	if rc.lastAppliedLSN != 3 {
		t.Errorf("lastAppliedLSN: got %d, want 3", rc.lastAppliedLSN)
	}
}

func TestClientStart_CheckpointSavedAfterSingleBatch(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeReplServer{
		batches:    [][]*pb.ReplicationLogEntry{{putEntry(0, 1, intFV(1))}},
		blockAfter: true,
	}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, func() error { cancel(); return nil })
	if err := <-runClient(rc, ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// after Start returns all writes including SetMetaCheckpointLSN are done
	if got := pm.GetMetaCheckpointLSN(); got != 1 {
		t.Errorf("checkpoint after batch: got %d, want 1", got)
	}
}

func TestClientStart_CheckpointSavedAfterMultipleBatches(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)

	received := make(chan struct{}, 10)
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeReplServer{
		batches: [][]*pb.ReplicationLogEntry{
			{putEntry(0, 1, intFV(1))},
			{putEntry(1, 2, intFV(2))},
		},
		blockAfter: true,
	}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, func() error {
		received <- struct{}{}
		return nil
	})
	errCh := runClient(rc, ctx)

	<-received
	<-received
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Start: %v", err)
	}
	// checkpoint reflects both batches: LSN 0 and LSN 1 applied → next = 2
	if got := pm.GetMetaCheckpointLSN(); got != 2 {
		t.Errorf("checkpoint after 2 batches: got %d, want 2", got)
	}
}

func TestClientStart_CallsOnBatchAfterEachBatch(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)

	received := make(chan struct{}, 10)
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeReplServer{
		batches: [][]*pb.ReplicationLogEntry{
			{putEntry(0, 1, intFV(1))},
			{putEntry(1, 2, intFV(2))},
			{putEntry(2, 3, intFV(3))},
		},
		blockAfter: true,
	}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, func() error {
		received <- struct{}{}
		return nil
	})
	errCh := runClient(rc, ctx)

	for i := 0; i < 3; i++ {
		<-received
	}
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestClientStart_NilOnBatchSucceeds(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)
	// server closes the stream after sending — Start returns EOF error; that's fine
	fake := &fakeReplServer{batches: [][]*pb.ReplicationLogEntry{{putEntry(0, 7, intFV(7))}}}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, nil)
	// must not panic; any error is acceptable
	_ = rc.Start(context.Background())
	mustSearch(t, bt, 7)
}

func TestClientStart_ContextCancellationReturnsNil(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	seen := make(chan uint64, 1)
	// server blocks after receiving the request; client cancels once connected
	fake := &fakeReplServer{blockAfter: true, startLSNSeen: seen}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, nil)
	errCh := runClient(rc, ctx)
	<-seen // stream established — cancel now so the nil-return path is exercised
	cancel()
	if err := <-errCh; err != nil {
		t.Errorf("Start after ctx cancel: want nil, got %v", err)
	}
}

func TestClientStart_StreamErrorReturnsError(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)
	fake := &fakeReplServer{errAfter: errors.New("server exploded")}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, nil)
	if err := rc.Start(context.Background()); err == nil {
		t.Error("Start: expected error from stream failure, got nil")
	}
}

func TestClientStart_OnBatchErrorPropagates(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)
	boom := errors.New("batch callback failed")
	fake := &fakeReplServer{
		batches:    [][]*pb.ReplicationLogEntry{{putEntry(0, 1, intFV(1))}},
		blockAfter: true,
	}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, func() error { return boom })
	if err := rc.Start(context.Background()); err == nil {
		t.Error("Start: expected onBatch error to propagate, got nil")
	}
}

func TestClientStart_SetMetaCheckpointLSNErrorPropagates(t *testing.T) {
	bt, realPM := newTestBTreeForClient(t)
	pm := &errCheckpointPM{PageManager: realPM, err: errors.New("disk full")}
	// server sends 1 batch and closes — SetMetaCheckpointLSN runs before EOF recv
	fake := &fakeReplServer{batches: [][]*pb.ReplicationLogEntry{{putEntry(0, 1, intFV(1))}}}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, nil)
	if err := rc.Start(context.Background()); err == nil {
		t.Error("Start: expected checkpoint error to propagate, got nil")
	}
}

func TestClientStart_StartLSNPassedToServer(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)

	seen := make(chan uint64, 1)
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeReplServer{blockAfter: true, startLSNSeen: seen}
	addr := startFakeServer(t, fake)
	const startLSN uint64 = 77
	rc := NewReplicationClient(bt, pm, startLSN, addr, nil)
	errCh := runClient(rc, ctx)
	// wait for the stream to be established before cancelling
	got := <-seen
	cancel()
	<-errCh

	if got != startLSN {
		t.Errorf("PullRequest.StartLsn: got %d, want %d", got, startLSN)
	}
}

func TestClientStart_FieldsCorrectlyApplied(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)

	boolField := &pb.FieldValue{Value: &pb.FieldValue_BoolValue{BoolValue: true}}
	listField := &pb.FieldValue{Value: &pb.FieldValue_ListValue{ListValue: &pb.FieldList{
		ElemType: uint32(btree.FieldTypeInt),
		Elems:    []*pb.FieldValue{intFV(1), intFV(2)},
	}}}
	batch := []*pb.ReplicationLogEntry{
		putEntry(0, 99, intFV(-5), strFV("name"), boolField, listField),
	}
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeReplServer{batches: [][]*pb.ReplicationLogEntry{batch}, blockAfter: true}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, func() error { cancel(); return nil })
	if err := <-runClient(rc, ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	fields := mustSearch(t, bt, 99)
	if len(fields) != 4 {
		t.Fatalf("fields: want 4, got %d", len(fields))
	}
	if fields[0].Value.(btree.IntValue).V != -5 {
		t.Errorf("field 0 (int): got %v", fields[0].Value)
	}
	if fields[1].Value.(btree.StringValue).V != "name" {
		t.Errorf("field 1 (string): got %v", fields[1].Value)
	}
	if fields[2].Value.(btree.StringValue).V != "TRUE" {
		t.Errorf("field 2 (bool): got %v", fields[2].Value)
	}
	lv := fields[3].Value.(btree.ListValue)
	if len(lv.Elems) != 2 {
		t.Errorf("field 3 (list): want 2 elems, got %d", len(lv.Elems))
	}
}

func TestClientStart_PutOverwritesExistingKey(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)
	// pre-insert key 5 with value 10
	if err := bt.Insert(5, []btree.Field{{Tag: 1, Value: btree.IntValue{V: 10}}}); err != nil {
		t.Fatalf("pre-insert: %v", err)
	}
	// replicate a put for the same key with a new value
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeReplServer{
		batches:    [][]*pb.ReplicationLogEntry{{putEntry(0, 5, intFV(99))}},
		blockAfter: true,
	}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, func() error { cancel(); return nil })
	if err := <-runClient(rc, ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	fields := mustSearch(t, bt, 5)
	if fields[0].Value.(btree.IntValue).V != 99 {
		t.Errorf("overwrite: got %v, want IntValue{99}", fields[0].Value)
	}
}

func TestClientStart_DeleteNonExistentKeySucceeds(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)
	// deleting a key that was never inserted must not fail
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeReplServer{
		batches:    [][]*pb.ReplicationLogEntry{{delEntry(0, 999)}},
		blockAfter: true,
	}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, func() error { cancel(); return nil })
	if err := <-runClient(rc, ctx); err != nil {
		t.Fatalf("Start on delete of absent key: %v", err)
	}
}

func TestClientStart_LargeNumberOfEntries(t *testing.T) {
	bt, pm := newTestBTreeForClient(t)
	const n = 200
	batch := make([]*pb.ReplicationLogEntry, n)
	for i := range batch {
		batch[i] = putEntry(uint64(i), uint64(i+1000), intFV(int64(i)))
	}

	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeReplServer{batches: [][]*pb.ReplicationLogEntry{batch}, blockAfter: true}
	addr := startFakeServer(t, fake)
	rc := NewReplicationClient(bt, pm, 0, addr, func() error { cancel(); return nil })
	if err := <-runClient(rc, ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if rc.lastAppliedLSN != n {
		t.Errorf("lastAppliedLSN: got %d, want %d", rc.lastAppliedLSN, n)
	}
	for i := 0; i < n; i++ {
		mustSearch(t, bt, uint64(i+1000))
	}
}

// insecure is only referenced via the grpc.WithTransportCredentials call inside
// ReplicationClient.Start itself; the import satisfies the compiler.
var _ = insecure.NewCredentials
