package partition

import (
	"testing"
)

// ---- Constants ----------------------------------------------------------

func TestMaxKeysPerRange(t *testing.T) {
	if MaxKeysPerRange != 1000 {
		t.Errorf("MaxKeysPerRange = %d, want 1000", MaxKeysPerRange)
	}
}

func TestMinRangeSplitBytes(t *testing.T) {
	const wantMB = 1 << 20
	if MinRangeSplitBytes != wantMB {
		t.Errorf("MinRangeSplitBytes = %d, want %d (1 MB)", MinRangeSplitBytes, wantMB)
	}
}

// ---- RangeOp iota -------------------------------------------------------

func TestRangeOp_IotaValues(t *testing.T) {
	cases := []struct {
		op   RangeOp
		want int
		name string
	}{
		{RangeOpScan, 0, "RangeOpScan"},
		{RangeOpInsert, 1, "RangeOpInsert"},
		{RangeOpUpdate, 2, "RangeOpUpdate"},
		{RangeOpDelete, 3, "RangeOpDelete"},
	}
	for _, tc := range cases {
		if int(tc.op) != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, tc.op, tc.want)
		}
	}
}

func TestRangeOp_Distinct(t *testing.T) {
	ops := []RangeOp{RangeOpScan, RangeOpInsert, RangeOpUpdate, RangeOpDelete}
	seen := make(map[RangeOp]bool)
	for _, op := range ops {
		if seen[op] {
			t.Errorf("duplicate RangeOp value: %d", op)
		}
		seen[op] = true
	}
}

// ---- RangeDescriptor ----------------------------------------------------

func TestRangeDescriptor_ZeroValue(t *testing.T) {
	var rd RangeDescriptor
	if rd.RangeID != 0 {
		t.Errorf("RangeID zero value = %d, want 0", rd.RangeID)
	}
	if rd.StartKey != 0 {
		t.Errorf("StartKey zero value = %d, want 0", rd.StartKey)
	}
	if rd.EndKey != 0 {
		t.Errorf("EndKey zero value = %d, want 0", rd.EndKey)
	}
	if rd.LeaderID != 0 {
		t.Errorf("LeaderID zero value = %d, want 0", rd.LeaderID)
	}
	if rd.Size != 0 {
		t.Errorf("Size zero value = %d, want 0", rd.Size)
	}
	if rd.Replicas != nil {
		t.Errorf("Replicas zero value should be nil, got %v", rd.Replicas)
	}
}

func TestRangeDescriptor_FieldAssignment(t *testing.T) {
	replicas := map[uint64]string{1: "addr1", 2: "addr2"}
	rd := RangeDescriptor{
		RangeID:  42,
		StartKey: 100,
		EndKey:   200,
		Replicas: replicas,
		LeaderID: 1,
		Size:     4096,
	}

	if rd.RangeID != 42 {
		t.Errorf("RangeID = %d, want 42", rd.RangeID)
	}
	if rd.StartKey != 100 {
		t.Errorf("StartKey = %d, want 100", rd.StartKey)
	}
	if rd.EndKey != 200 {
		t.Errorf("EndKey = %d, want 200", rd.EndKey)
	}
	if rd.LeaderID != 1 {
		t.Errorf("LeaderID = %d, want 1", rd.LeaderID)
	}
	if rd.Size != 4096 {
		t.Errorf("Size = %d, want 4096", rd.Size)
	}
	if len(rd.Replicas) != 2 {
		t.Errorf("Replicas len = %d, want 2", len(rd.Replicas))
	}
}

func TestRangeDescriptor_StartKeyInclusiveEndKeyExclusive(t *testing.T) {
	// Confirm the documented invariant: StartKey is inclusive, EndKey is exclusive.
	// [100, 200) means key 100 is owned, key 200 is not.
	rd := RangeDescriptor{StartKey: 100, EndKey: 200}

	if rd.StartKey > rd.EndKey {
		t.Errorf("StartKey (%d) must be <= EndKey (%d)", rd.StartKey, rd.EndKey)
	}
}

func TestRangeDescriptor_ReplicasMap(t *testing.T) {
	rd := RangeDescriptor{
		Replicas: map[uint64]string{1: "node1:8080", 2: "node2:8080"},
	}

	if addr, ok := rd.Replicas[1]; !ok || addr != "node1:8080" {
		t.Errorf("Replicas[1] = %q, want \"node1:8080\"", addr)
	}
}

// ---- RangeStats ---------------------------------------------------------

func TestRangeStats_ZeroValue(t *testing.T) {
	var rs RangeStats
	if rs.KeyCount != 0 || rs.TotalBytes != 0 {
		t.Errorf("RangeStats zero value = %+v, want all zeros", rs)
	}
}

func TestRangeStats_FieldAssignment(t *testing.T) {
	rs := RangeStats{KeyCount: 500, TotalBytes: 1 << 19}
	if rs.KeyCount != 500 {
		t.Errorf("KeyCount = %d, want 500", rs.KeyCount)
	}
	if rs.TotalBytes != 1<<19 {
		t.Errorf("TotalBytes = %d, want %d", rs.TotalBytes, 1<<19)
	}
}

func TestRangeStats_ExceedsSplitThreshold(t *testing.T) {
	rs := RangeStats{TotalBytes: MinRangeSplitBytes + 1}
	if rs.TotalBytes <= MinRangeSplitBytes {
		t.Errorf("expected TotalBytes to exceed MinRangeSplitBytes")
	}
}

// ---- RangeRequest -------------------------------------------------------

func TestRangeRequest_ZeroValue(t *testing.T) {
	var req RangeRequest
	if req.Op != 0 {
		t.Errorf("Op zero value = %d, want 0 (RangeOpScan)", req.Op)
	}
	if req.TableID != 0 || req.StartKey != 0 || req.EndKey != 0 || req.Key != 0 {
		t.Errorf("unexpected non-zero fields in zero-value RangeRequest: %+v", req)
	}
}

func TestRangeRequest_ScanFields(t *testing.T) {
	req := RangeRequest{
		Op:       RangeOpScan,
		TableID:  5,
		StartKey: 100,
		EndKey:   200,
		Columns:  []int{0, 2},
	}
	if req.Op != RangeOpScan {
		t.Errorf("Op = %d, want %d", req.Op, RangeOpScan)
	}
	if req.TableID != 5 {
		t.Errorf("TableID = %d, want 5", req.TableID)
	}
	if req.StartKey != 100 || req.EndKey != 200 {
		t.Errorf("key range = [%d, %d), want [100, 200)", req.StartKey, req.EndKey)
	}
	if len(req.Columns) != 2 {
		t.Errorf("Columns len = %d, want 2", len(req.Columns))
	}
}

func TestRangeRequest_InsertFields(t *testing.T) {
	req := RangeRequest{
		Op:      RangeOpInsert,
		TableID: 3,
		Key:     42,
	}
	if req.Op != RangeOpInsert {
		t.Errorf("Op = %d, want %d", req.Op, RangeOpInsert)
	}
	if req.Key != 42 {
		t.Errorf("Key = %d, want 42", req.Key)
	}
}

func TestRangeRequest_UpdateFields(t *testing.T) {
	req := RangeRequest{
		Op:        RangeOpUpdate,
		Key:       10,
		UpdateCol: 3,
	}
	if req.Op != RangeOpUpdate {
		t.Errorf("Op = %d, want %d", req.Op, RangeOpUpdate)
	}
	if req.UpdateCol != 3 {
		t.Errorf("UpdateCol = %d, want 3", req.UpdateCol)
	}
}

func TestRangeRequest_DeleteFields(t *testing.T) {
	req := RangeRequest{
		Op:  RangeOpDelete,
		Key: 99,
	}
	if req.Op != RangeOpDelete {
		t.Errorf("Op = %d, want %d", req.Op, RangeOpDelete)
	}
	if req.Key != 99 {
		t.Errorf("Key = %d, want 99", req.Key)
	}
}

// ---- ResultRow ----------------------------------------------------------

func TestResultRow_ZeroValue(t *testing.T) {
	var row ResultRow
	if row.Key != 0 {
		t.Errorf("Key zero value = %d, want 0", row.Key)
	}
	if row.Fields != nil {
		t.Errorf("Fields zero value should be nil, got %v", row.Fields)
	}
}

func TestResultRow_KeyAssignment(t *testing.T) {
	row := ResultRow{Key: 77}
	if row.Key != 77 {
		t.Errorf("Key = %d, want 77", row.Key)
	}
}

// ---- ResultSet ----------------------------------------------------------

func TestResultSet_ZeroValue(t *testing.T) {
	var rs ResultSet
	if rs.Columns != nil || rs.ColTypes != nil || rs.Rows != nil {
		t.Errorf("ResultSet zero value should have all nil slices: %+v", rs)
	}
}

func TestResultSet_FieldAssignment(t *testing.T) {
	rs := ResultSet{
		Columns:  []string{"id", "name", "age"},
		ColTypes: []string{"int", "text", "int"},
		Rows:     []ResultRow{{Key: 1}, {Key: 2}},
	}

	if len(rs.Columns) != 3 {
		t.Errorf("Columns len = %d, want 3", len(rs.Columns))
	}
	if len(rs.ColTypes) != 3 {
		t.Errorf("ColTypes len = %d, want 3", len(rs.ColTypes))
	}
	if len(rs.Rows) != 2 {
		t.Errorf("Rows len = %d, want 2", len(rs.Rows))
	}
	if rs.Rows[0].Key != 1 || rs.Rows[1].Key != 2 {
		t.Errorf("Rows keys = %d, %d; want 1, 2", rs.Rows[0].Key, rs.Rows[1].Key)
	}
}

func TestResultSet_ColumnTypesParallel(t *testing.T) {
	rs := ResultSet{
		Columns:  []string{"id", "name"},
		ColTypes: []string{"int", "text"},
	}
	if len(rs.Columns) != len(rs.ColTypes) {
		t.Errorf("Columns and ColTypes must be parallel slices: len=%d vs len=%d",
			len(rs.Columns), len(rs.ColTypes))
	}
}

// ---- RangeResponse ------------------------------------------------------

func TestRangeResponse_ZeroValue(t *testing.T) {
	var rr RangeResponse
	if rr.Rows != nil {
		t.Errorf("Rows zero value should be nil, got %v", rr.Rows)
	}
	if rr.Error != nil {
		t.Errorf("Error zero value should be nil, got %v", rr.Error)
	}
}

func TestRangeResponse_WithRows(t *testing.T) {
	rr := RangeResponse{
		Rows:  []ResultRow{{Key: 10}, {Key: 20}},
		Error: nil,
	}
	if len(rr.Rows) != 2 {
		t.Errorf("Rows len = %d, want 2", len(rr.Rows))
	}
	if rr.Error != nil {
		t.Errorf("Error = %v, want nil", rr.Error)
	}
}

func TestRangeResponse_WithError(t *testing.T) {
	rr := RangeResponse{Error: &testError{"something went wrong"}}
	if rr.Error == nil {
		t.Fatal("Error should be non-nil")
	}
	if rr.Error.Error() != "something went wrong" {
		t.Errorf("Error.Error() = %q, want \"something went wrong\"", rr.Error.Error())
	}
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }
