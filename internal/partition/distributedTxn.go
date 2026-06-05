package partition

import (
	"context"
	"fmt"
	"sync"
	"time"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	"github.com/your-username/DistributedDatabaseSystem/internal/raft"
	rs "github.com/your-username/DistributedDatabaseSystem/proto/rangeservice"
)

type DistTxnStatus int

const (
	DistTxnPending DistTxnStatus = iota
	DistTxnPrepared
	DistTxnCommitted
	DistTxnAborted
)

type DistributedTxn struct {
	TxnId       uint64
	Status      DistTxnStatus
	Ranges      []*RangeDescriptor
	AnchorRange *RangeDescriptor
}

type PendingTxn struct {
	TxnId      uint64
	Commands   []raft.RaftCommand
	LockedKeys []uint64
}

type prepareResult struct {
	rangeID uint64
	resp    *rs.PrepareResponse
	err     error
}

// TxnCommitRecord is written to the anchor range's Raft log before commit
// messages are sent. On crash recovery, this record lets a new coordinator
// finish the protocol for any in-flight transactions.
type TxnCommitRecord struct {
	TxnId    uint64
	Status   DistTxnStatus
	RangeIDs []uint64
}

// TxnRecordStore holds commit records applied from the Raft log.
// It is populated by applyFn and read by the recovery path on startup.
type TxnRecordStore struct {
	mu      sync.Mutex
	records map[uint64]*TxnCommitRecord
}

func NewTxnRecordStore() *TxnRecordStore {
	return &TxnRecordStore{records: make(map[uint64]*TxnCommitRecord)}
}

func (s *TxnRecordStore) Store(r *TxnCommitRecord) {
	s.mu.Lock()
	s.records[r.TxnId] = r
	s.mu.Unlock()
}

func (s *TxnRecordStore) Get(txnId uint64) (*TxnCommitRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[txnId]
	return r, ok
}

func (s *TxnRecordStore) Delete(txnId uint64) {
	s.mu.Lock()
	delete(s.records, txnId)
	s.mu.Unlock()
}

// EncodeTxnRecord packs a TxnCommitRecord into the Fields of a RaftCommand.
// Fields[0] = status, Fields[1:] = rangeIDs, each as btree.IntValue.
func EncodeTxnRecord(r *TxnCommitRecord) (uint64, []btree.Field) {
	fields := make([]btree.Field, 1+len(r.RangeIDs))
	fields[0] = btree.Field{Value: btree.IntValue{V: int64(r.Status)}}
	for i, id := range r.RangeIDs {
		fields[i+1] = btree.Field{Value: btree.IntValue{V: int64(id)}}
	}
	return r.TxnId, fields
}

// DecodeTxnRecord reconstructs a TxnCommitRecord from a RaftCommand.
func DecodeTxnRecord(key uint64, fields []btree.Field) *TxnCommitRecord {
	r := &TxnCommitRecord{TxnId: key}
	if len(fields) == 0 {
		return r
	}
	if v, ok := fields[0].Value.(btree.IntValue); ok {
		r.Status = DistTxnStatus(v.V)
	}
	for _, f := range fields[1:] {
		if v, ok := f.Value.(btree.IntValue); ok {
			r.RangeIDs = append(r.RangeIDs, uint64(v.V))
		}
	}
	return r
}

type DistributedTxnManager struct {
	mu        sync.Mutex
	nextTxnId uint64
	active    map[uint64]*DistributedTxn
	gw        *Gateway
}

func NewDistributedTxnManager(gw *Gateway) *DistributedTxnManager {
	return &DistributedTxnManager{
		nextTxnId: 1 << 32,
		active:    make(map[uint64]*DistributedTxn),
		gw:        gw,
	}
}

func (gw *Gateway) sendPrepare(leaderID uint64, req *rs.PrepareRequest) (*rs.PrepareResponse, error) {
	conn, ok := gw.conns[leaderID]
	if !ok {
		return nil, fmt.Errorf("no connection for leader node %d", leaderID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return rs.NewRangeServiceClient(conn).Prepare(ctx, req)
}

func (gw *Gateway) sendAbort(leaderID uint64, req *rs.AbortRequest) (*rs.AbortResponse, error) {
	conn, ok := gw.conns[leaderID]
	if !ok {
		return nil, fmt.Errorf("no connection for leader node %d", leaderID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return rs.NewRangeServiceClient(conn).Abort(ctx, req)
}

func (gw *Gateway) sendCommit(leaderID uint64, req *rs.CommitRequest) (*rs.CommitResponse, error) {
	conn, ok := gw.conns[leaderID]
	if !ok {
		return nil, fmt.Errorf("no connection for leader node %d", leaderID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return rs.NewRangeServiceClient(conn).Commit(ctx, req)
}

func (gw *Gateway) sendWriteCommitRecord(leaderID uint64, req *rs.WriteCommitRecordRequest) (*rs.WriteCommitRecordResponse, error) {
	conn, ok := gw.conns[leaderID]
	if !ok {
		return nil, fmt.Errorf("no connection for leader node %d", leaderID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return rs.NewRangeServiceClient(conn).WriteCommitRecord(ctx, req)
}

const maxBackoff = 30 * time.Second

func (dtm *DistributedTxnManager) abortRanges(preparedRanges []*RangeDescriptor, txnId uint64) {
	var wg sync.WaitGroup
	for _, rd := range preparedRanges {
		wg.Add(1)
		go func(desc *RangeDescriptor) {
			defer wg.Done()
			req := &rs.AbortRequest{TxnId: txnId}
			backoff := 100 * time.Millisecond
			for {
				resp, err := dtm.gw.sendAbort(desc.LeaderID, req)
				if err == nil && resp.GetSuccess() {
					return
				}
				time.Sleep(backoff)
				if backoff < maxBackoff {
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
			}
		}(rd)
	}
	wg.Wait()
}

func (dtm *DistributedTxnManager) writeCommitRecord(txn *DistributedTxn) {
	rangeIDs := make([]uint64, len(txn.Ranges))
	for i, rd := range txn.Ranges {
		rangeIDs[i] = rd.RangeID
	}
	commitRecordReq := &rs.WriteCommitRecordRequest{
		TxnId:    txn.TxnId,
		Status:   uint32(DistTxnCommitted),
		RangeIds: rangeIDs,
	}

	// durably record the commit decision on the anchor before sending any
	// commit messages
	backoff := 100 * time.Millisecond
	for {
		resp, err := dtm.gw.sendWriteCommitRecord(txn.AnchorRange.LeaderID, commitRecordReq)
		if err == nil && resp.GetSuccess() {
			break
		}
		time.Sleep(backoff)
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}

	commitReq := &rs.CommitRequest{
		TxnId: txn.TxnId,
	}

	//commit record is durable; now commit data on anchor, retry until success
	backoff = 100 * time.Millisecond
	for {
		resp, err := dtm.gw.sendCommit(txn.AnchorRange.LeaderID, commitReq)
		if err == nil && resp.GetSuccess() {
			break
		}
		time.Sleep(backoff)
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}

	//written to anchor range, can confirm committed and move to other ranges
	dtm.mu.Lock()
	txn.Status = DistTxnCommitted
	dtm.mu.Unlock()

	var wg sync.WaitGroup
	for _, rd := range txn.Ranges {
		if rd.RangeID == txn.AnchorRange.RangeID {
			continue
		}

		wg.Add(1)
		go func(desc *RangeDescriptor) {
			defer wg.Done()
			backoff := 100 * time.Millisecond
			for {
				resp, err := dtm.gw.sendCommit(desc.LeaderID, commitReq)
				if err == nil && resp.GetSuccess() {
					return
				}
				time.Sleep(backoff)
				if backoff < maxBackoff {
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
			}
		}(rd)
	}
	wg.Wait()
}

func (dtm *DistributedTxnManager) ExecuteDistributed(
	ranges []*RangeDescriptor,
	queryStart, queryEnd uint64,
	buildPrepare func(desc *RangeDescriptor, txnId uint64) *rs.PrepareRequest,
) error {
	rangeByID := make(map[uint64]*RangeDescriptor, len(ranges))
	for _, rd := range ranges {
		rangeByID[rd.RangeID] = rd
	}

	dtm.mu.Lock()
	txnId := dtm.nextTxnId
	dtm.nextTxnId++
	txn := &DistributedTxn{
		TxnId:       txnId,
		Status:      DistTxnPending,
		Ranges:      ranges,
		AnchorRange: ranges[0],
	}
	dtm.active[txnId] = txn
	dtm.mu.Unlock()

	//prepare phase
	prepareCh := make(chan prepareResult, len(ranges))
	for _, rd := range ranges {
		go func(desc *RangeDescriptor) {
			req := buildPrepare(desc, txnId)
			resp, err := dtm.gw.sendPrepare(desc.LeaderID, req)
			prepareCh <- prepareResult{desc.RangeID, resp, err}
		}(rd)
	}

	allYes := true
	var preparedRanges []*RangeDescriptor
	for range ranges {
		r := <-prepareCh
		if r.err != nil || !r.resp.GetSuccess() {
			allYes = false
		} else {
			preparedRanges = append(preparedRanges, rangeByID[r.rangeID])
		}
	}

	if !allYes {
		//abort phase
		dtm.mu.Lock()
		txn.Status = DistTxnAborted
		dtm.mu.Unlock()

		dtm.abortRanges(preparedRanges, txnId)

		dtm.mu.Lock()
		delete(dtm.active, txnId)
		dtm.mu.Unlock()

		return fmt.Errorf("transaction %d aborted during prepare phase", txnId)
	}

	//commit phase
	dtm.mu.Lock()
	txn.Status = DistTxnPrepared
	dtm.mu.Unlock()

	dtm.writeCommitRecord(txn)

	dtm.mu.Lock()
	delete(dtm.active, txnId)
	dtm.mu.Unlock()

	return nil
}
