package partition

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	lock "github.com/your-username/DistributedDatabaseSystem/internal/Lock"
	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	"github.com/your-username/DistributedDatabaseSystem/internal/raft"
	rs "github.com/your-username/DistributedDatabaseSystem/proto/rangeservice"
)

type RangeServer struct {
	rs.UnimplementedRangeServiceServer

	bt      *btree.BTree
	tm      *lock.TransactionManager
	sc      *sqllayer.SchemaCatalog
	records *TxnRecordStore

	mu      sync.Mutex
	pending map[uint64]*PendingTxn
}

func NewRangeServer(bt *btree.BTree, tm *lock.TransactionManager, sc *sqllayer.SchemaCatalog, records *TxnRecordStore) *RangeServer {
	return &RangeServer{
		bt:      bt,
		tm:      tm,
		sc:      sc,
		records: records,
		pending: make(map[uint64]*PendingTxn),
	}
}

// Execute handles single-range read/write operations forwarded by the Gateway.
// SCAN reads directly from the BTree. INSERT/UPDATE/DELETE run through the
// TransactionManager so every write is replicated via Raft before being applied.
func (s *RangeServer) Execute(_ context.Context, req *rs.RangeRequest) (*rs.RangeResponse, error) {
	switch req.Op {
	case rs.RangeOp_SCAN:
		return s.execRangeScan(req)
	case rs.RangeOp_INSERT:
		return s.execRangeInsert(req)
	case rs.RangeOp_UPDATE, rs.RangeOp_DELETE:
		return s.execRangeMutation(req)
	default:
		return &rs.RangeResponse{Error: fmt.Sprintf("unsupported op: %v", req.Op)}, nil
	}
}

func (s *RangeServer) execRangeScan(req *rs.RangeRequest) (*rs.RangeResponse, error) {
	rows, err := s.bt.RangeScan(req.StartKey, req.EndKey)
	if err != nil {
		return &rs.RangeResponse{Error: err.Error()}, nil
	}
	matched, err := s.filterRows(req.TableId, rows, req.Where)
	if err != nil {
		return &rs.RangeResponse{Error: err.Error()}, nil
	}
	resp := &rs.RangeResponse{}
	for _, row := range matched {
		rr := &rs.ResultRow{Key: row.Key}
		if len(req.Columns) == 0 {
			rr.Fields = fieldsToProto(row.Fields)
		} else {
			for _, ci := range req.Columns {
				if int(ci) < len(row.Fields) {
					rr.Fields = append(rr.Fields, fieldsToProto([]btree.Field{row.Fields[ci]})[0])
				}
			}
		}
		resp.Rows = append(resp.Rows, rr)
	}
	return resp, nil
}

func (s *RangeServer) execRangeInsert(req *rs.RangeRequest) (*rs.RangeResponse, error) {
	txn := s.tm.Begin()
	if err := s.tm.Lock(txn.Id, req.Key, lock.LockExclusive); err != nil {
		s.tm.Rollback(txn.Id)
		return &rs.RangeResponse{Error: err.Error()}, nil
	}
	fields := make([]btree.Field, len(req.Fields))
	for i, f := range req.Fields {
		fields[i] = protoFieldToBTree(f)
	}
	s.tm.AppendRedo(txn.Id, raft.RaftCommand{Op: raft.ReplPut, Key: req.Key, Fields: fields})
	if err := s.tm.Commit(txn.Id); err != nil {
		return &rs.RangeResponse{Error: err.Error()}, nil
	}
	return &rs.RangeResponse{}, nil
}

func (s *RangeServer) execRangeMutation(req *rs.RangeRequest) (*rs.RangeResponse, error) {
	rows, err := s.bt.RangeScan(req.StartKey, req.EndKey)
	if err != nil {
		return &rs.RangeResponse{Error: err.Error()}, nil
	}
	matched, err := s.filterRows(req.TableId, rows, req.Where)
	if err != nil {
		return &rs.RangeResponse{Error: err.Error()}, nil
	}

	txn := s.tm.Begin()
	for _, row := range matched {
		if err := s.tm.Lock(txn.Id, row.Key, lock.LockExclusive); err != nil {
			s.tm.Rollback(txn.Id)
			return &rs.RangeResponse{Error: err.Error()}, nil
		}
		var cmd raft.RaftCommand
		if req.Op == rs.RangeOp_DELETE {
			cmd = raft.RaftCommand{Op: raft.ReplDelete, Key: row.Key}
		} else {
			newFields := make([]btree.Field, len(row.Fields))
			copy(newFields, row.Fields)
			if len(req.Fields) > 0 && int(req.UpdateCol) < len(newFields) {
				updated := protoFieldToBTree(req.Fields[0])
				updated.Tag = uint8(req.UpdateCol)
				newFields[req.UpdateCol] = updated
			}
			cmd = raft.RaftCommand{Op: raft.ReplPut, Key: row.Key, Fields: newFields}
		}
		s.tm.AppendRedo(txn.Id, cmd)
	}
	if err := s.tm.Commit(txn.Id); err != nil {
		return &rs.RangeResponse{Error: err.Error()}, nil
	}
	return &rs.RangeResponse{}, nil
}

type scanRow = struct {
	Key    uint64
	Fields []btree.Field
}

// filterRows returns the subset of rows matching the Where expression.
// A nil expression matches all rows.
func (s *RangeServer) filterRows(tableID uint32, rows []scanRow, where *rs.Expression) ([]scanRow, error) {
	if where == nil {
		return rows, nil
	}
	schema := s.sc.FindTableByID(tableID)
	if schema == nil {
		return nil, fmt.Errorf("table ID %d not found", tableID)
	}
	var matched []scanRow
	for _, row := range rows {
		if matchProtoExpr(where, schema, row.Fields) {
			matched = append(matched, row)
		}
	}
	return matched, nil
}

func matchProtoExpr(expr *rs.Expression, schema *sqllayer.TableSchemaValue, fields []btree.Field) bool {
	if expr == nil {
		return true
	}
	switch e := expr.GetExpr().(type) {
	case *rs.Expression_Comparison:
		c := e.Comparison
		idx := sqllayer.FindColumnIndex(c.GetColumn(), schema)
		if idx < 0 || idx >= len(fields) {
			return false
		}
		switch v := fields[idx].Value.(type) {
		case btree.IntValue:
			lit, err := strconv.ParseInt(c.GetLiteralValue(), 10, 64)
			if err != nil {
				return false
			}
			return compareProtoInt(v.V, lit, c.GetOperator())
		case btree.StringValue:
			return compareProtoString(v.V, c.GetLiteralValue(), c.GetOperator())
		}
		return false
	case *rs.Expression_Logical:
		l := e.Logical
		switch strings.ToUpper(l.GetOperator()) {
		case "AND":
			return matchProtoExpr(l.GetLeft(), schema, fields) && matchProtoExpr(l.GetRight(), schema, fields)
		case "OR":
			return matchProtoExpr(l.GetLeft(), schema, fields) || matchProtoExpr(l.GetRight(), schema, fields)
		}
		return false
	}
	return false
}

func compareProtoInt(a, b int64, op string) bool {
	switch op {
	case "=":
		return a == b
	case "!=":
		return a != b
	case "<":
		return a < b
	case "<=":
		return a <= b
	case ">":
		return a > b
	case ">=":
		return a >= b
	}
	return false
}

func compareProtoString(a, b, op string) bool {
	switch op {
	case "=":
		return a == b
	case "!=":
		return a != b
	case "<":
		return a < b
	case "<=":
		return a <= b
	case ">":
		return a > b
	case ">=":
		return a >= b
	}
	return false
}

func (s *RangeServer) Prepare(ctx context.Context, req *rs.PrepareRequest) (*rs.PrepareResponse, error) {
	var commands []raft.RaftCommand
	var lockedKeys []uint64

	if req.Op == rs.RangeOp_INSERT {
		if err := s.tm.Lock(req.TxnId, req.Key, lock.LockExclusive); err != nil {
			s.tm.ReleaseAll(req.TxnId)
			return &rs.PrepareResponse{Success: false, Error: err.Error()}, nil
		}
		insertFields := make([]btree.Field, len(req.Fields))
		for i, f := range req.Fields {
			insertFields[i] = protoFieldToBTree(f)
		}
		lockedKeys = []uint64{req.Key}
		commands = []raft.RaftCommand{{Op: raft.ReplPut, Key: req.Key, Fields: insertFields}}
	} else {
		rows, err := s.bt.RangeScan(req.StartKey, req.EndKey)
		if err != nil {
			return &rs.PrepareResponse{Success: false, Error: err.Error()}, nil
		}
		matched, err := s.filterRows(req.TableId, rows, req.Where)
		if err != nil {
			return &rs.PrepareResponse{Success: false, Error: err.Error()}, nil
		}

		for _, row := range matched {
			if err := s.tm.Lock(req.TxnId, row.Key, lock.LockExclusive); err != nil {
				s.tm.ReleaseAll(req.TxnId)
				return &rs.PrepareResponse{Success: false, Error: err.Error()}, nil
			}
			lockedKeys = append(lockedKeys, row.Key)
			switch req.Op {
			case rs.RangeOp_UPDATE:
				newFields := make([]btree.Field, len(row.Fields))
				copy(newFields, row.Fields)
				if len(req.Fields) > 0 && int(req.UpdateCol) < len(newFields) {
					updated := protoFieldToBTree(req.Fields[0])
					updated.Tag = uint8(req.UpdateCol)
					newFields[req.UpdateCol] = updated
				}
				commands = append(commands, raft.RaftCommand{Op: raft.ReplPut, Key: row.Key, Fields: newFields})
			case rs.RangeOp_DELETE:
				commands = append(commands, raft.RaftCommand{Op: raft.ReplDelete, Key: row.Key})
			}
		}
	}

	s.mu.Lock()
	s.pending[req.TxnId] = &PendingTxn{
		TxnId:      req.TxnId,
		Commands:   commands,
		LockedKeys: lockedKeys,
	}
	s.mu.Unlock()

	return &rs.PrepareResponse{Success: true}, nil
}

func (s *RangeServer) Commit(ctx context.Context, req *rs.CommitRequest) (*rs.CommitResponse, error) {
	s.mu.Lock()
	pending := s.pending[req.TxnId]
	s.mu.Unlock()

	if pending == nil {
		return &rs.CommitResponse{Success: false, Error: "transaction not found"}, nil
	}

	if err := s.tm.CommitCommands(req.TxnId, pending.Commands); err != nil {
		return &rs.CommitResponse{Success: false, Error: err.Error()}, nil
	}

	s.mu.Lock()
	delete(s.pending, req.TxnId)
	s.mu.Unlock()

	return &rs.CommitResponse{Success: true}, nil
}

func (s *RangeServer) Abort(ctx context.Context, req *rs.AbortRequest) (*rs.AbortResponse, error) {
	s.mu.Lock()
	pending := s.pending[req.TxnId]
	s.mu.Unlock()

	if pending == nil {
		return &rs.AbortResponse{Success: true}, nil
	}

	s.tm.ReleaseAll(req.TxnId)

	s.mu.Lock()
	delete(s.pending, req.TxnId)
	s.mu.Unlock()

	return &rs.AbortResponse{Success: true}, nil
}

func (s *RangeServer) WriteCommitRecord(ctx context.Context, req *rs.WriteCommitRecordRequest) (*rs.WriteCommitRecordResponse, error) {
	record := &TxnCommitRecord{
		TxnId:    req.TxnId,
		Status:   DistTxnStatus(req.Status),
		RangeIDs: req.RangeIds,
	}
	key, fields := EncodeTxnRecord(record)
	cmd := raft.RaftCommand{Op: raft.ReplTxnRecord, Key: key, Fields: fields}
	if err := s.tm.ProposeCommands([]raft.RaftCommand{cmd}); err != nil {
		return &rs.WriteCommitRecordResponse{Success: false, Error: err.Error()}, nil
	}
	return &rs.WriteCommitRecordResponse{Success: true}, nil
}
