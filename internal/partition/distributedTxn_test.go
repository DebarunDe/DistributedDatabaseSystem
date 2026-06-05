package partition

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	rs "github.com/your-username/DistributedDatabaseSystem/proto/rangeservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// ---------------------------------------------------------------------------
// fakeTxnServer — in-process gRPC server implementing all 2PC RPCs
// ---------------------------------------------------------------------------

type fakeTxnServer struct {
	rs.UnimplementedRangeServiceServer
	mu sync.Mutex

	prepareCalls     []*rs.PrepareRequest
	commitCalls      []*rs.CommitRequest
	abortCalls       []*rs.AbortRequest
	writeRecordCalls []*rs.WriteCommitRecordRequest
	callOrder        []string // ordered log: "prepare", "commit", "abort", "write_record"

	prepareFn     func(*rs.PrepareRequest) (*rs.PrepareResponse, error)
	commitFn      func(*rs.CommitRequest) (*rs.CommitResponse, error)
	abortFn       func(*rs.AbortRequest) (*rs.AbortResponse, error)
	writeRecordFn func(*rs.WriteCommitRecordRequest) (*rs.WriteCommitRecordResponse, error)
}

func (s *fakeTxnServer) Prepare(_ context.Context, req *rs.PrepareRequest) (*rs.PrepareResponse, error) {
	s.mu.Lock()
	s.prepareCalls = append(s.prepareCalls, req)
	s.callOrder = append(s.callOrder, "prepare")
	fn := s.prepareFn
	s.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return &rs.PrepareResponse{Success: true}, nil
}

func (s *fakeTxnServer) Commit(_ context.Context, req *rs.CommitRequest) (*rs.CommitResponse, error) {
	s.mu.Lock()
	s.commitCalls = append(s.commitCalls, req)
	s.callOrder = append(s.callOrder, "commit")
	fn := s.commitFn
	s.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return &rs.CommitResponse{Success: true}, nil
}

func (s *fakeTxnServer) Abort(_ context.Context, req *rs.AbortRequest) (*rs.AbortResponse, error) {
	s.mu.Lock()
	s.abortCalls = append(s.abortCalls, req)
	s.callOrder = append(s.callOrder, "abort")
	fn := s.abortFn
	s.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return &rs.AbortResponse{Success: true}, nil
}

func (s *fakeTxnServer) WriteCommitRecord(_ context.Context, req *rs.WriteCommitRecordRequest) (*rs.WriteCommitRecordResponse, error) {
	s.mu.Lock()
	s.writeRecordCalls = append(s.writeRecordCalls, req)
	s.callOrder = append(s.callOrder, "write_record")
	fn := s.writeRecordFn
	s.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return &rs.WriteCommitRecordResponse{Success: true}, nil
}

func (s *fakeTxnServer) prepareCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.prepareCalls)
}

func (s *fakeTxnServer) commitCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.commitCalls)
}

func (s *fakeTxnServer) abortCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.abortCalls)
}

func (s *fakeTxnServer) writeRecordCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.writeRecordCalls)
}

func (s *fakeTxnServer) getCallOrder() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.callOrder))
	copy(out, s.callOrder)
	return out
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func startFakeTxnServer(t *testing.T, srv *fakeTxnServer) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	rs.RegisterRangeServiceServer(grpcSrv, srv)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(func() { grpcSrv.Stop(); _ = lis.Close() })

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// newTxnDTM builds a DistributedTxnManager whose gateway maps nodeID → fake server.
func newTxnDTM(t *testing.T, servers map[uint64]*fakeTxnServer) *DistributedTxnManager {
	t.Helper()
	gw := &Gateway{conns: make(map[uint64]*grpc.ClientConn)}
	for nodeID, srv := range servers {
		gw.conns[nodeID] = startFakeTxnServer(t, srv)
	}
	return NewDistributedTxnManager(gw)
}

// makeDescriptors builds n RangeDescriptors with consecutive 1000-key slots.
// Each descriptor's LeaderID equals its 1-based index.
func makeDescriptors(n int) []*RangeDescriptor {
	descs := make([]*RangeDescriptor, n)
	for i := range descs {
		descs[i] = &RangeDescriptor{
			RangeID:  uint64(i + 1),
			StartKey: uint64(i) * 1000,
			EndKey:   uint64(i+1) * 1000,
			LeaderID: uint64(i + 1),
		}
	}
	return descs
}

func nopBuildPrepare(desc *RangeDescriptor, txnId uint64) *rs.PrepareRequest {
	return &rs.PrepareRequest{TxnId: txnId}
}

// ---------------------------------------------------------------------------
// TxnRecordStore
// ---------------------------------------------------------------------------

func TestTxnRecordStore_StoreAndGet(t *testing.T) {
	s := NewTxnRecordStore()
	r := &TxnCommitRecord{TxnId: 1, Status: DistTxnCommitted, RangeIDs: []uint64{10, 20}}
	s.Store(r)
	got, ok := s.Get(1)
	if !ok {
		t.Fatal("Get after Store returned ok=false")
	}
	if got.TxnId != 1 || got.Status != DistTxnCommitted || len(got.RangeIDs) != 2 {
		t.Errorf("unexpected record: %+v", got)
	}
}

func TestTxnRecordStore_GetMissing_ReturnsFalse(t *testing.T) {
	s := NewTxnRecordStore()
	_, ok := s.Get(999)
	if ok {
		t.Error("Get on empty store should return ok=false")
	}
}

func TestTxnRecordStore_Delete_RemovesRecord(t *testing.T) {
	s := NewTxnRecordStore()
	s.Store(&TxnCommitRecord{TxnId: 5, Status: DistTxnCommitted})
	s.Delete(5)
	_, ok := s.Get(5)
	if ok {
		t.Error("Get after Delete should return ok=false")
	}
}

func TestTxnRecordStore_Delete_NonExistent_IsNoOp(t *testing.T) {
	s := NewTxnRecordStore()
	s.Delete(42) // must not panic
}

func TestTxnRecordStore_Overwrite_SecondStoreReplacesFirst(t *testing.T) {
	s := NewTxnRecordStore()
	s.Store(&TxnCommitRecord{TxnId: 1, Status: DistTxnPending})
	s.Store(&TxnCommitRecord{TxnId: 1, Status: DistTxnCommitted})
	got, ok := s.Get(1)
	if !ok || got.Status != DistTxnCommitted {
		t.Errorf("second Store should overwrite: got %+v ok=%v", got, ok)
	}
}

func TestTxnRecordStore_MultipleRecordsCoexist(t *testing.T) {
	s := NewTxnRecordStore()
	for i := uint64(0); i < 5; i++ {
		s.Store(&TxnCommitRecord{TxnId: i, Status: DistTxnCommitted})
	}
	for i := uint64(0); i < 5; i++ {
		if _, ok := s.Get(i); !ok {
			t.Errorf("Get(%d) returned ok=false", i)
		}
	}
}

func TestTxnRecordStore_DeleteLeavesOtherRecordsIntact(t *testing.T) {
	s := NewTxnRecordStore()
	s.Store(&TxnCommitRecord{TxnId: 1, Status: DistTxnCommitted})
	s.Store(&TxnCommitRecord{TxnId: 2, Status: DistTxnCommitted})
	s.Delete(1)
	if _, ok := s.Get(2); !ok {
		t.Error("Delete(1) should not affect TxnId=2")
	}
}

func TestTxnRecordStore_ConcurrentAccess_NoDataRace(t *testing.T) {
	s := NewTxnRecordStore()
	var wg sync.WaitGroup
	for i := uint64(0); i < 100; i++ {
		wg.Add(3)
		go func(id uint64) { defer wg.Done(); s.Store(&TxnCommitRecord{TxnId: id}) }(i)
		go func(id uint64) { defer wg.Done(); s.Get(id) }(i)
		go func(id uint64) { defer wg.Done(); s.Delete(id) }(i)
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// EncodeTxnRecord / DecodeTxnRecord
// ---------------------------------------------------------------------------

func TestEncodeTxnRecord_KeyIsTransactionId(t *testing.T) {
	r := &TxnCommitRecord{TxnId: 42, Status: DistTxnCommitted}
	key, _ := EncodeTxnRecord(r)
	if key != 42 {
		t.Errorf("key=%d, want 42", key)
	}
}

func TestEncodeTxnRecord_FieldCountIsRangeCountPlusOne(t *testing.T) {
	r := &TxnCommitRecord{TxnId: 1, Status: DistTxnCommitted, RangeIDs: []uint64{10, 20, 30}}
	_, fields := EncodeTxnRecord(r)
	if len(fields) != 4 { // 1 status + 3 range IDs
		t.Errorf("fields len=%d, want 4", len(fields))
	}
}

func TestEncodeTxnRecord_NoRanges_SingleStatusField(t *testing.T) {
	r := &TxnCommitRecord{TxnId: 1, Status: DistTxnAborted}
	_, fields := EncodeTxnRecord(r)
	if len(fields) != 1 {
		t.Errorf("fields len=%d, want 1 (status only)", len(fields))
	}
}

func TestEncodeTxnRecord_StatusStoredInFirstField(t *testing.T) {
	r := &TxnCommitRecord{TxnId: 1, Status: DistTxnAborted}
	_, fields := EncodeTxnRecord(r)
	iv, ok := fields[0].Value.(btree.IntValue)
	if !ok {
		t.Fatal("fields[0] is not btree.IntValue")
	}
	if iv.V != int64(DistTxnAborted) {
		t.Errorf("status field value=%d, want %d", iv.V, int64(DistTxnAborted))
	}
}

func TestEncodeTxnRecord_RangeIDsInSubsequentFields(t *testing.T) {
	wantIDs := []uint64{100, 200, 300}
	r := &TxnCommitRecord{TxnId: 1, Status: DistTxnCommitted, RangeIDs: wantIDs}
	_, fields := EncodeTxnRecord(r)
	for i, want := range wantIDs {
		iv, ok := fields[i+1].Value.(btree.IntValue)
		if !ok {
			t.Fatalf("fields[%d] not btree.IntValue", i+1)
		}
		if uint64(iv.V) != want {
			t.Errorf("fields[%d]=%d, want %d", i+1, iv.V, want)
		}
	}
}

func TestDecodeTxnRecord_EmptyFields_ReturnsOnlyTxnId(t *testing.T) {
	got := DecodeTxnRecord(5, nil)
	if got.TxnId != 5 {
		t.Errorf("TxnId=%d, want 5", got.TxnId)
	}
	if got.Status != DistTxnPending {
		t.Errorf("Status=%d, want DistTxnPending", got.Status)
	}
	if len(got.RangeIDs) != 0 {
		t.Errorf("RangeIDs should be empty, got %v", got.RangeIDs)
	}
}

func TestDecodeTxnRecord_RoundTrip_NoRanges(t *testing.T) {
	orig := &TxnCommitRecord{TxnId: 7, Status: DistTxnCommitted}
	key, fields := EncodeTxnRecord(orig)
	got := DecodeTxnRecord(key, fields)
	if got.TxnId != orig.TxnId || got.Status != orig.Status || len(got.RangeIDs) != 0 {
		t.Errorf("round trip mismatch: got %+v, want %+v", got, orig)
	}
}

func TestDecodeTxnRecord_RoundTrip_MultipleRanges(t *testing.T) {
	orig := &TxnCommitRecord{TxnId: 99, Status: DistTxnPrepared, RangeIDs: []uint64{1, 2, 3, 4}}
	key, fields := EncodeTxnRecord(orig)
	got := DecodeTxnRecord(key, fields)
	if got.TxnId != orig.TxnId {
		t.Errorf("TxnId: got %d, want %d", got.TxnId, orig.TxnId)
	}
	if got.Status != orig.Status {
		t.Errorf("Status: got %d, want %d", got.Status, orig.Status)
	}
	if len(got.RangeIDs) != len(orig.RangeIDs) {
		t.Fatalf("RangeIDs len: got %d, want %d", len(got.RangeIDs), len(orig.RangeIDs))
	}
	for i, id := range orig.RangeIDs {
		if got.RangeIDs[i] != id {
			t.Errorf("RangeIDs[%d]: got %d, want %d", i, got.RangeIDs[i], id)
		}
	}
}

func TestDecodeTxnRecord_AllStatusesPreserved(t *testing.T) {
	for _, status := range []DistTxnStatus{DistTxnPending, DistTxnPrepared, DistTxnCommitted, DistTxnAborted} {
		r := &TxnCommitRecord{TxnId: 1, Status: status, RangeIDs: []uint64{10}}
		key, fields := EncodeTxnRecord(r)
		got := DecodeTxnRecord(key, fields)
		if got.Status != status {
			t.Errorf("status %d: round trip gave %d", status, got.Status)
		}
	}
}

// ---------------------------------------------------------------------------
// DistTxnStatus iota values
// ---------------------------------------------------------------------------

func TestDistTxnStatus_Values(t *testing.T) {
	cases := []struct {
		status DistTxnStatus
		want   int
		name   string
	}{
		{DistTxnPending, 0, "DistTxnPending"},
		{DistTxnPrepared, 1, "DistTxnPrepared"},
		{DistTxnCommitted, 2, "DistTxnCommitted"},
		{DistTxnAborted, 3, "DistTxnAborted"},
	}
	for _, tc := range cases {
		if int(tc.status) != tc.want {
			t.Errorf("%s=%d, want %d", tc.name, tc.status, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// NewDistributedTxnManager
// ---------------------------------------------------------------------------

func TestNewDistributedTxnManager_NextTxnIdStartsAt1Shift32(t *testing.T) {
	gw := &Gateway{conns: make(map[uint64]*grpc.ClientConn)}
	dtm := NewDistributedTxnManager(gw)
	dtm.mu.Lock()
	got := dtm.nextTxnId
	dtm.mu.Unlock()
	if got != 1<<32 {
		t.Errorf("nextTxnId=%d, want %d", got, uint64(1<<32))
	}
}

func TestNewDistributedTxnManager_ActiveMapEmpty(t *testing.T) {
	gw := &Gateway{conns: make(map[uint64]*grpc.ClientConn)}
	dtm := NewDistributedTxnManager(gw)
	dtm.mu.Lock()
	n := len(dtm.active)
	dtm.mu.Unlock()
	if n != 0 {
		t.Errorf("active map len=%d, want 0", n)
	}
}

func TestNewDistributedTxnManager_GatewayStored(t *testing.T) {
	gw := &Gateway{conns: make(map[uint64]*grpc.ClientConn)}
	dtm := NewDistributedTxnManager(gw)
	if dtm.gw != gw {
		t.Error("gateway not stored correctly")
	}
}

// ---------------------------------------------------------------------------
// ExecuteDistributed — happy path
// ---------------------------------------------------------------------------

func TestExecuteDistributed_SingleRange_ReturnsNil(t *testing.T) {
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(1)

	if err := dtm.ExecuteDistributed(descs, 0, 999, nopBuildPrepare); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecuteDistributed_SingleRange_PrepareAndCommitCalled(t *testing.T) {
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(1)

	_ = dtm.ExecuteDistributed(descs, 0, 999, nopBuildPrepare)

	if srv.prepareCount() != 1 {
		t.Errorf("prepareCount=%d, want 1", srv.prepareCount())
	}
	if srv.commitCount() != 1 {
		t.Errorf("commitCount=%d, want 1", srv.commitCount())
	}
	if srv.abortCount() != 0 {
		t.Errorf("abortCount=%d, want 0", srv.abortCount())
	}
}

func TestExecuteDistributed_TwoRanges_PrepareCalledOnEach(t *testing.T) {
	srv1 := &fakeTxnServer{}
	srv2 := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv1, 2: srv2})
	descs := makeDescriptors(2)

	if err := dtm.ExecuteDistributed(descs, 0, 1999, nopBuildPrepare); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv1.prepareCount() != 1 {
		t.Errorf("srv1 prepareCount=%d, want 1", srv1.prepareCount())
	}
	if srv2.prepareCount() != 1 {
		t.Errorf("srv2 prepareCount=%d, want 1", srv2.prepareCount())
	}
}

func TestExecuteDistributed_TwoRanges_CommitCalledOnBoth(t *testing.T) {
	srv1 := &fakeTxnServer{}
	srv2 := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv1, 2: srv2})
	descs := makeDescriptors(2)

	_ = dtm.ExecuteDistributed(descs, 0, 1999, nopBuildPrepare)

	if srv1.commitCount() != 1 {
		t.Errorf("srv1 commitCount=%d, want 1", srv1.commitCount())
	}
	if srv2.commitCount() != 1 {
		t.Errorf("srv2 commitCount=%d, want 1", srv2.commitCount())
	}
}

func TestExecuteDistributed_ThreeRanges_CommitSentToAll(t *testing.T) {
	// All three ranges behind the same node to keep setup simple.
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(3)
	for _, d := range descs {
		d.LeaderID = 1
	}

	if err := dtm.ExecuteDistributed(descs, 0, 2999, nopBuildPrepare); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv.commitCount() != 3 {
		t.Errorf("commitCount=%d, want 3", srv.commitCount())
	}
}

func TestExecuteDistributed_WriteCommitRecordOnlyOnAnchor(t *testing.T) {
	srv1 := &fakeTxnServer{} // anchor (ranges[0])
	srv2 := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv1, 2: srv2})
	descs := makeDescriptors(2)

	_ = dtm.ExecuteDistributed(descs, 0, 1999, nopBuildPrepare)

	if srv1.writeRecordCount() != 1 {
		t.Errorf("anchor srv1 writeRecordCount=%d, want 1", srv1.writeRecordCount())
	}
	if srv2.writeRecordCount() != 0 {
		t.Errorf("non-anchor srv2 writeRecordCount=%d, want 0", srv2.writeRecordCount())
	}
}

func TestExecuteDistributed_WriteCommitRecordPrecedesFirstCommit(t *testing.T) {
	// Use a single node for all ranges so we get a total call-order log.
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(2)
	for _, d := range descs {
		d.LeaderID = 1
	}

	if err := dtm.ExecuteDistributed(descs, 0, 1999, nopBuildPrepare); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	order := srv.getCallOrder()
	writeIdx, commitIdx := -1, -1
	for i, op := range order {
		if op == "write_record" && writeIdx == -1 {
			writeIdx = i
		}
		if op == "commit" && commitIdx == -1 {
			commitIdx = i
		}
	}
	if writeIdx == -1 {
		t.Fatalf("write_record not found in call order: %v", order)
	}
	if commitIdx == -1 {
		t.Fatalf("commit not found in call order: %v", order)
	}
	if writeIdx >= commitIdx {
		t.Errorf("write_record (idx %d) must precede first commit (idx %d); order: %v",
			writeIdx, commitIdx, order)
	}
}

func TestExecuteDistributed_TxnIdStartsAbove1Shift32(t *testing.T) {
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(1)

	_ = dtm.ExecuteDistributed(descs, 0, 999, nopBuildPrepare)

	srv.mu.Lock()
	firstPrepare := srv.prepareCalls[0]
	srv.mu.Unlock()

	if firstPrepare.TxnId < 1<<32 {
		t.Errorf("TxnId=%d should be >= 1<<32 to avoid collisions with local transactions", firstPrepare.TxnId)
	}
}

func TestExecuteDistributed_SuccessiveTxnIdsIncrement(t *testing.T) {
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(1)

	_ = dtm.ExecuteDistributed(descs, 0, 999, nopBuildPrepare)
	_ = dtm.ExecuteDistributed(descs, 0, 999, nopBuildPrepare)

	srv.mu.Lock()
	calls := make([]*rs.PrepareRequest, len(srv.prepareCalls))
	copy(calls, srv.prepareCalls)
	srv.mu.Unlock()

	if len(calls) < 2 {
		t.Fatalf("expected 2 prepare calls, got %d", len(calls))
	}
	if calls[1].TxnId != calls[0].TxnId+1 {
		t.Errorf("second TxnId=%d, want %d", calls[1].TxnId, calls[0].TxnId+1)
	}
}

func TestExecuteDistributed_TxnRemovedFromActiveMapOnCommit(t *testing.T) {
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(1)

	if err := dtm.ExecuteDistributed(descs, 0, 999, nopBuildPrepare); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dtm.mu.Lock()
	remaining := len(dtm.active)
	dtm.mu.Unlock()

	if remaining != 0 {
		t.Errorf("active map has %d entries after commit, want 0", remaining)
	}
}

func TestExecuteDistributed_BuildPrepareReceivesTxnId(t *testing.T) {
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(1)

	var capturedTxnId uint64
	buildPrepare := func(desc *RangeDescriptor, txnId uint64) *rs.PrepareRequest {
		capturedTxnId = txnId
		return &rs.PrepareRequest{TxnId: txnId}
	}

	_ = dtm.ExecuteDistributed(descs, 0, 999, buildPrepare)

	if capturedTxnId < 1<<32 {
		t.Errorf("txnId passed to buildPrepare=%d, want >= 1<<32", capturedTxnId)
	}
}

// ---------------------------------------------------------------------------
// ExecuteDistributed — abort path
// ---------------------------------------------------------------------------

func TestExecuteDistributed_OnePrepareFailsReturnsError(t *testing.T) {
	srv1 := &fakeTxnServer{}
	srv2 := &fakeTxnServer{
		prepareFn: func(*rs.PrepareRequest) (*rs.PrepareResponse, error) {
			return &rs.PrepareResponse{Success: false, Error: "lock conflict"}, nil
		},
	}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv1, 2: srv2})
	descs := makeDescriptors(2)

	err := dtm.ExecuteDistributed(descs, 0, 1999, nopBuildPrepare)
	if err == nil {
		t.Fatal("expected error when one prepare fails, got nil")
	}
}

func TestExecuteDistributed_AbortCalledOnSuccessfullyPreparedRanges(t *testing.T) {
	srv1 := &fakeTxnServer{} // Prepare succeeds → must receive Abort
	srv2 := &fakeTxnServer{
		prepareFn: func(*rs.PrepareRequest) (*rs.PrepareResponse, error) {
			return &rs.PrepareResponse{Success: false}, nil // Prepare fails → no Abort expected
		},
	}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv1, 2: srv2})
	descs := makeDescriptors(2)

	_ = dtm.ExecuteDistributed(descs, 0, 1999, nopBuildPrepare)

	if srv1.abortCount() != 1 {
		t.Errorf("srv1 abortCount=%d, want 1 (range was prepared)", srv1.abortCount())
	}
	if srv2.abortCount() != 0 {
		t.Errorf("srv2 abortCount=%d, want 0 (prepare failed, no abort needed)", srv2.abortCount())
	}
}

func TestExecuteDistributed_AllPreparesFail_NoAbortCalls(t *testing.T) {
	srv := &fakeTxnServer{
		prepareFn: func(*rs.PrepareRequest) (*rs.PrepareResponse, error) {
			return &rs.PrepareResponse{Success: false}, nil
		},
	}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(3)
	for _, d := range descs {
		d.LeaderID = 1
	}

	_ = dtm.ExecuteDistributed(descs, 0, 2999, nopBuildPrepare)

	if srv.abortCount() != 0 {
		t.Errorf("abortCount=%d, want 0 (no range was prepared)", srv.abortCount())
	}
}

func TestExecuteDistributed_CommitNotCalledAfterAbort(t *testing.T) {
	srv := &fakeTxnServer{
		prepareFn: func(*rs.PrepareRequest) (*rs.PrepareResponse, error) {
			return &rs.PrepareResponse{Success: false}, nil
		},
	}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(1)

	_ = dtm.ExecuteDistributed(descs, 0, 999, nopBuildPrepare)

	if srv.commitCount() != 0 {
		t.Errorf("commitCount=%d, want 0 after prepare failure", srv.commitCount())
	}
	if srv.writeRecordCount() != 0 {
		t.Errorf("writeRecordCount=%d, want 0 after prepare failure", srv.writeRecordCount())
	}
}

func TestExecuteDistributed_TxnRemovedFromActiveMapOnAbort(t *testing.T) {
	srv := &fakeTxnServer{
		prepareFn: func(*rs.PrepareRequest) (*rs.PrepareResponse, error) {
			return &rs.PrepareResponse{Success: false}, nil
		},
	}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(1)

	_ = dtm.ExecuteDistributed(descs, 0, 999, nopBuildPrepare)

	dtm.mu.Lock()
	remaining := len(dtm.active)
	dtm.mu.Unlock()

	if remaining != 0 {
		t.Errorf("active map has %d entries after abort, want 0", remaining)
	}
}

func TestExecuteDistributed_MissingConnection_ReturnsError(t *testing.T) {
	gw := &Gateway{conns: make(map[uint64]*grpc.ClientConn)} // no connections registered
	dtm := NewDistributedTxnManager(gw)
	descs := makeDescriptors(1)

	err := dtm.ExecuteDistributed(descs, 0, 999, nopBuildPrepare)
	if err == nil {
		t.Fatal("expected error when no gRPC connection is available, got nil")
	}
}

// ---------------------------------------------------------------------------
// abortRanges
// ---------------------------------------------------------------------------

func TestAbortRanges_SucceedsOnFirstTry(t *testing.T) {
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(2)
	for _, d := range descs {
		d.LeaderID = 1
	}

	dtm.abortRanges(descs, 1<<32)

	if srv.abortCount() != 2 {
		t.Errorf("abortCount=%d, want 2", srv.abortCount())
	}
}

func TestAbortRanges_AbortTxnIdMatchesTxn(t *testing.T) {
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(1)
	const txnId = uint64(1 << 32)

	dtm.abortRanges(descs, txnId)

	srv.mu.Lock()
	req := srv.abortCalls[0]
	srv.mu.Unlock()

	if req.TxnId != txnId {
		t.Errorf("AbortRequest.TxnId=%d, want %d", req.TxnId, txnId)
	}
}

func TestAbortRanges_RetriesOnTransientFailure(t *testing.T) {
	var attempts int32
	srv := &fakeTxnServer{
		abortFn: func(*rs.AbortRequest) (*rs.AbortResponse, error) {
			n := atomic.AddInt32(&attempts, 1)
			if n == 1 {
				return &rs.AbortResponse{Success: false}, nil // fail once to trigger retry
			}
			return &rs.AbortResponse{Success: true}, nil
		},
	}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(1)

	dtm.abortRanges(descs, 1<<32) // blocks until abort succeeds

	if got := atomic.LoadInt32(&attempts); got < 2 {
		t.Errorf("expected at least 2 abort attempts, got %d", got)
	}
}

func TestAbortRanges_EmptySlice_IsNoOp(t *testing.T) {
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})

	dtm.abortRanges(nil, 1<<32) // must not panic or block

	if srv.abortCount() != 0 {
		t.Errorf("abortCount=%d, want 0 for empty slice", srv.abortCount())
	}
}

// ---------------------------------------------------------------------------
// writeCommitRecord
// ---------------------------------------------------------------------------

func TestWriteCommitRecord_WriteCommitRecordSentOnlyToAnchor(t *testing.T) {
	srv1 := &fakeTxnServer{}
	srv2 := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv1, 2: srv2})
	descs := makeDescriptors(2)

	txn := &DistributedTxn{
		TxnId: 1 << 32, Status: DistTxnPrepared, Ranges: descs, AnchorRange: descs[0],
	}
	dtm.writeCommitRecord(txn)

	if srv1.writeRecordCount() != 1 {
		t.Errorf("anchor srv1 writeRecordCount=%d, want 1", srv1.writeRecordCount())
	}
	if srv2.writeRecordCount() != 0 {
		t.Errorf("non-anchor srv2 writeRecordCount=%d, want 0", srv2.writeRecordCount())
	}
}

func TestWriteCommitRecord_CommitSentToAllRanges(t *testing.T) {
	srv1 := &fakeTxnServer{}
	srv2 := &fakeTxnServer{}
	srv3 := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv1, 2: srv2, 3: srv3})
	descs := makeDescriptors(3)

	txn := &DistributedTxn{
		TxnId: 1 << 32, Status: DistTxnPrepared, Ranges: descs, AnchorRange: descs[0],
	}
	dtm.writeCommitRecord(txn)

	for i, srv := range []*fakeTxnServer{srv1, srv2, srv3} {
		if srv.commitCount() != 1 {
			t.Errorf("srv%d commitCount=%d, want 1", i+1, srv.commitCount())
		}
	}
}

func TestWriteCommitRecord_SetsTransactionStatusToCommitted(t *testing.T) {
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(1)

	txn := &DistributedTxn{
		TxnId: 1 << 32, Status: DistTxnPrepared, Ranges: descs, AnchorRange: descs[0],
	}
	dtm.writeCommitRecord(txn)

	dtm.mu.Lock()
	status := txn.Status
	dtm.mu.Unlock()

	if status != DistTxnCommitted {
		t.Errorf("txn.Status=%d after writeCommitRecord, want DistTxnCommitted", status)
	}
}

func TestWriteCommitRecord_WriteRecordPrecedesFirstCommit(t *testing.T) {
	// Single server for all ranges gives us a total ordering of calls.
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(2)
	for _, d := range descs {
		d.LeaderID = 1
	}

	txn := &DistributedTxn{
		TxnId: 1 << 32, Status: DistTxnPrepared, Ranges: descs, AnchorRange: descs[0],
	}
	dtm.writeCommitRecord(txn)

	order := srv.getCallOrder()
	writeIdx, commitIdx := -1, -1
	for i, op := range order {
		if op == "write_record" && writeIdx == -1 {
			writeIdx = i
		}
		if op == "commit" && commitIdx == -1 {
			commitIdx = i
		}
	}
	if writeIdx == -1 || commitIdx == -1 {
		t.Fatalf("write_record or commit missing; order: %v", order)
	}
	if writeIdx >= commitIdx {
		t.Errorf("write_record (idx %d) must precede first commit (idx %d); order: %v",
			writeIdx, commitIdx, order)
	}
}

func TestWriteCommitRecord_RequestContainsAllRangeIDs(t *testing.T) {
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(3)
	for _, d := range descs {
		d.LeaderID = 1
	}

	txn := &DistributedTxn{
		TxnId: 1 << 32, Status: DistTxnPrepared, Ranges: descs, AnchorRange: descs[0],
	}
	dtm.writeCommitRecord(txn)

	srv.mu.Lock()
	req := srv.writeRecordCalls[0]
	srv.mu.Unlock()

	if len(req.RangeIds) != 3 {
		t.Fatalf("WriteCommitRecord.RangeIds len=%d, want 3", len(req.RangeIds))
	}
	wantIDs := map[uint64]bool{1: true, 2: true, 3: true}
	for _, id := range req.RangeIds {
		if !wantIDs[id] {
			t.Errorf("unexpected RangeId %d in WriteCommitRecord", id)
		}
	}
}

func TestWriteCommitRecord_RetriesWriteRecordOnTransientFailure(t *testing.T) {
	var attempts int32
	srv := &fakeTxnServer{
		writeRecordFn: func(*rs.WriteCommitRecordRequest) (*rs.WriteCommitRecordResponse, error) {
			n := atomic.AddInt32(&attempts, 1)
			if n == 1 {
				return &rs.WriteCommitRecordResponse{Success: false}, nil
			}
			return &rs.WriteCommitRecordResponse{Success: true}, nil
		},
	}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(1)

	txn := &DistributedTxn{
		TxnId: 1 << 32, Status: DistTxnPrepared, Ranges: descs, AnchorRange: descs[0],
	}
	dtm.writeCommitRecord(txn)

	if got := atomic.LoadInt32(&attempts); got < 2 {
		t.Errorf("expected at least 2 writeRecord attempts, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

func TestExecuteDistributed_ConcurrentTransactions_UniqueIDs(t *testing.T) {
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(1)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = dtm.ExecuteDistributed(descs, 0, 999, nopBuildPrepare)
		}()
	}
	wg.Wait()

	srv.mu.Lock()
	calls := make([]*rs.PrepareRequest, len(srv.prepareCalls))
	copy(calls, srv.prepareCalls)
	srv.mu.Unlock()

	seen := make(map[uint64]bool)
	for _, req := range calls {
		if seen[req.TxnId] {
			t.Errorf("duplicate TxnId=%d found in concurrent transactions", req.TxnId)
		}
		seen[req.TxnId] = true
	}
}

func TestExecuteDistributed_ConcurrentTransactions_ActiveMapNeverLeaks(t *testing.T) {
	srv := &fakeTxnServer{}
	dtm := newTxnDTM(t, map[uint64]*fakeTxnServer{1: srv})
	descs := makeDescriptors(1)

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = dtm.ExecuteDistributed(descs, 0, 999, nopBuildPrepare)
		}()
	}
	wg.Wait()

	dtm.mu.Lock()
	remaining := len(dtm.active)
	dtm.mu.Unlock()

	if remaining != 0 {
		t.Errorf("active map has %d leaked entries after all transactions completed", remaining)
	}
}
