package partition

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	rs "github.com/your-username/DistributedDatabaseSystem/proto/rangeservice"
	"google.golang.org/grpc"
)

// Gateway routes SQL operations to the correct range leader(s).
// conns maps nodeID → gRPC connection to that node's RangeService port.
type Gateway struct {
	router *Router
	schema *sqllayer.SchemaCatalog
	conns  map[uint64]*grpc.ClientConn
	dtm    *DistributedTxnManager
}

// AddConn registers a gRPC connection to nodeID's RangeService port.
func (gw *Gateway) AddConn(nodeID uint64, conn *grpc.ClientConn) {
	gw.conns[nodeID] = conn
}

func NewGateway(router *Router, schema *sqllayer.SchemaCatalog) *Gateway {
	gw := &Gateway{
		router: router,
		schema: schema,
		conns:  make(map[uint64]*grpc.ClientConn),
	}
	gw.dtm = NewDistributedTxnManager(gw)
	return gw
}

func (gw *Gateway) Execute(stmt sqllayer.Statement) (*ResultSet, error) {
	switch s := stmt.(type) {
	case *sqllayer.InsertStatement:
		schema := gw.schema.FindTableSchema(s.Table)
		if schema == nil {
			return nil, fmt.Errorf("table %q not found", s.Table)
		}
		pkVal, err := sqllayer.LiteralToPrimaryKey(s.Values[0])
		if err != nil {
			return nil, fmt.Errorf("invalid primary key: %w", err)
		}
		key := sqllayer.EncodeKey(schema.TableId, pkVal)
		desc, err := gw.router.RouteKey(key)
		if err != nil {
			return nil, fmt.Errorf("route insert: %w", err)
		}
		fields := make([]btree.Field, len(s.Values))
		for i, lit := range s.Values {
			colType := schema.PrimaryKey.DataType
			if i > 0 {
				colType = schema.Columns[i-1].DataType
			}
			if fields[i], err = sqllayer.LiteralToField(lit, colType, uint8(i)); err != nil {
				return nil, fmt.Errorf("field %d: %w", i, err)
			}
		}
		return gw.sendToLeader(desc, &rs.RangeRequest{
			Op:      rs.RangeOp_INSERT,
			TableId: schema.TableId,
			Key:     key,
			Fields:  fieldsToProto(fields),
		})

	case *sqllayer.SelectStatement:
		schema := gw.schema.FindTableSchema(s.Table)
		if schema == nil {
			return nil, fmt.Errorf("table %q not found", s.Table)
		}

		// Expand SELECT * to the full ordered column list.
		colNames := s.Columns
		if len(colNames) == 1 && colNames[0] == "*" {
			colNames = make([]string, 1+len(schema.Columns))
			colNames[0] = schema.PrimaryKey.Name
			for i, c := range schema.Columns {
				colNames[i+1] = c.Name
			}
		}

		low, high := extractPKBounds(s.Where, schema.PrimaryKey.Name)
		startKey := sqllayer.EncodeKey(schema.TableId, low)
		endKey := sqllayer.EncodeKey(schema.TableId, high) // inclusive for scans
		routeEnd := endKey + 1                             // exclusive for RouteRange
		ranges, err := gw.router.RouteRange(startKey, routeEnd)
		if err != nil {
			return nil, fmt.Errorf("route select: %w", err)
		}
		cols := make([]int32, len(colNames))
		for i, name := range colNames {
			idx := sqllayer.FindColumnIndex(name, schema)
			if idx == -1 {
				return nil, fmt.Errorf("column %q not found in table %q", name, s.Table)
			}
			cols[i] = int32(idx)
		}

		var result *ResultSet
		if len(ranges) == 1 {
			result, err = gw.sendToLeader(ranges[0], &rs.RangeRequest{
				Op:       rs.RangeOp_SCAN,
				TableId:  schema.TableId,
				StartKey: startKey,
				EndKey:   endKey,
				Where:    exprToProto(s.Where),
				Columns:  cols,
			})
		} else {
			result, err = gw.scatterGather(ranges, startKey, endKey, schema.TableId, cols, s.Where)
		}
		if err != nil {
			return nil, err
		}
		// Attach column metadata so the client can render headers.
		result.Columns = colNames
		result.ColTypes = make([]string, len(colNames))
		for i, name := range colNames {
			idx := sqllayer.FindColumnIndex(name, schema)
			result.ColTypes[i] = colTypeByIndex(idx, schema)
		}
		return result, nil

	case *sqllayer.UpdateStatement:
		schema := gw.schema.FindTableSchema(s.Table)
		if schema == nil {
			return nil, fmt.Errorf("table %q not found", s.Table)
		}
		low, high := extractPKBounds(s.Where, schema.PrimaryKey.Name)
		startKey := sqllayer.EncodeKey(schema.TableId, low)
		endKey := sqllayer.EncodeKey(schema.TableId, high)
		ranges, err := gw.router.RouteRange(startKey, endKey+1)
		if err != nil {
			return nil, fmt.Errorf("route update: %w", err)
		}
		updateCol := int32(sqllayer.FindColumnIndex(s.Column, schema))
		if updateCol == -1 {
			return nil, fmt.Errorf("column %q not found in table %q", s.Column, s.Table)
		}
		field, err := sqllayer.LiteralToField(s.Value, colTypeByIndex(int(updateCol), schema), uint8(updateCol))
		if err != nil {
			return nil, fmt.Errorf("invalid update value: %w", err)
		}
		req := &rs.RangeRequest{
			Op:        rs.RangeOp_UPDATE,
			TableId:   schema.TableId,
			StartKey:  startKey,
			EndKey:    endKey,
			Where:     exprToProto(s.Where),
			Fields:    fieldsToProto([]btree.Field{field}),
			UpdateCol: updateCol,
		}
		if len(ranges) == 1 {
			return gw.sendToLeader(ranges[0], req)
		}
		if err := gw.dtm.ExecuteDistributed(ranges, startKey, endKey,
			buildUpdatePrepare(schema.TableId, startKey, endKey, s.Where, updateCol, field),
		); err != nil {
			return nil, err
		}
		return &ResultSet{}, nil

	case *sqllayer.DeleteStatement:
		schema := gw.schema.FindTableSchema(s.Table)
		if schema == nil {
			return nil, fmt.Errorf("table %q not found", s.Table)
		}
		low, high := extractPKBounds(s.Where, schema.PrimaryKey.Name)
		startKey := sqllayer.EncodeKey(schema.TableId, low)
		endKey := sqllayer.EncodeKey(schema.TableId, high)
		ranges, err := gw.router.RouteRange(startKey, endKey+1)
		if err != nil {
			return nil, fmt.Errorf("route delete: %w", err)
		}
		req := &rs.RangeRequest{
			Op:       rs.RangeOp_DELETE,
			TableId:  schema.TableId,
			StartKey: startKey,
			EndKey:   endKey,
			Where:    exprToProto(s.Where),
		}
		if len(ranges) == 1 {
			return gw.sendToLeader(ranges[0], req)
		}
		if err := gw.dtm.ExecuteDistributed(ranges, startKey, endKey,
			buildDeletePrepare(schema.TableId, startKey, endKey, s.Where),
		); err != nil {
			return nil, err
		}
		return &ResultSet{}, nil

	case *sqllayer.CreateTableStatement:
		pk := s.Columns[0]
		rest := s.Columns[1:]
		colNames := make([]string, len(rest))
		colTypes := make([]string, len(rest))
		for i, c := range rest {
			colNames[i] = c.Name
			colTypes[i] = c.DataType
		}
		key, fields, err := gw.schema.BuildCreateTableCommand(s.Table, pk.Name, pk.DataType, colNames, colTypes)
		if err != nil {
			return nil, fmt.Errorf("create table %q: %w", s.Table, err)
		}
		desc, err := gw.router.RouteKey(key)
		if err != nil {
			return nil, fmt.Errorf("route create table: %w", err)
		}
		return gw.sendToLeader(desc, &rs.RangeRequest{
			Op:      rs.RangeOp_INSERT,
			TableId: 0,
			Key:     key,
			Fields:  fieldsToProto(fields),
		})

	case *sqllayer.DropTableStatement:
		tableSchema := gw.schema.FindTableSchema(s.Table)
		if tableSchema == nil {
			return nil, fmt.Errorf("table %q not found", s.Table)
		}
		dataStart := sqllayer.EncodeKey(tableSchema.TableId, 0)
		dataEnd := sqllayer.EncodeKey(tableSchema.TableId, ^uint32(0))
		dataRanges, err := gw.router.RouteRange(dataStart, dataEnd+1)
		if err != nil {
			return nil, fmt.Errorf("route data ranges: %w", err)
		}
		// delete data atomically across all ranges before removing the schema entry
		if err := gw.dtm.ExecuteDistributed(dataRanges, dataStart, dataEnd,
			buildDeletePrepare(tableSchema.TableId, dataStart, dataEnd, nil),
		); err != nil {
			return nil, fmt.Errorf("drop table data: %w", err)
		}
		schemaKey := sqllayer.EncodeKey(0, tableSchema.TableId)
		schemaDesc, err := gw.router.RouteKey(schemaKey)
		if err != nil {
			return nil, fmt.Errorf("route drop schema: %w", err)
		}
		if _, err := gw.sendToLeader(schemaDesc, &rs.RangeRequest{
			Op:       rs.RangeOp_DELETE,
			TableId:  0,
			StartKey: schemaKey,
			EndKey:   schemaKey + 1,
		}); err != nil {
			return nil, fmt.Errorf("drop schema entry: %w", err)
		}
		return &ResultSet{}, nil

	default:
		return nil, fmt.Errorf("unsupported statement type %T", stmt)
	}
}

// scatterGather fans a SCAN out across multiple ranges, scoping each request to
// the intersection of the query bounds and the range's own key interval.
func (gw *Gateway) scatterGather(
	ranges []*RangeDescriptor,
	queryStart, queryEnd uint64,
	tableId uint32,
	cols []int32,
	where sqllayer.Expression,
) (*ResultSet, error) {
	type result struct {
		rs  *ResultSet
		err error
	}
	ch := make(chan result, len(ranges))

	for _, desc := range ranges {
		go func(desc *RangeDescriptor) {
			scopedStart := max64(queryStart, desc.StartKey)
			scopedEnd := min64(queryEnd, desc.EndKey-1) // desc.EndKey is exclusive; scan is inclusive
			out, err := gw.sendToLeader(desc, &rs.RangeRequest{
				Op:       rs.RangeOp_SCAN,
				TableId:  tableId,
				StartKey: scopedStart,
				EndKey:   scopedEnd,
				Where:    exprToProto(where),
				Columns:  cols,
			})
			ch <- result{out, err}
		}(desc)
	}

	merged := &ResultSet{}
	for range ranges {
		r := <-ch
		if r.err != nil {
			return nil, r.err
		}
		if merged.Columns == nil {
			merged.Columns = r.rs.Columns
			merged.ColTypes = r.rs.ColTypes
		}
		merged.Rows = append(merged.Rows, r.rs.Rows...)
	}
	return merged, nil
}

// buildUpdatePrepare returns a buildPrepare closure for an UPDATE, scoping
// start/end keys to each range's bounds.
func buildUpdatePrepare(tableId uint32, queryStart, queryEnd uint64, where sqllayer.Expression, updateCol int32, field btree.Field) func(*RangeDescriptor, uint64) *rs.PrepareRequest {
	return func(desc *RangeDescriptor, txnId uint64) *rs.PrepareRequest {
		return &rs.PrepareRequest{
			TxnId:     txnId,
			Op:        rs.RangeOp_UPDATE,
			TableId:   tableId,
			StartKey:  max64(queryStart, desc.StartKey),
			EndKey:    min64(queryEnd, desc.EndKey-1),
			Where:     exprToProto(where),
			Fields:    fieldsToProto([]btree.Field{field}),
			UpdateCol: updateCol,
		}
	}
}

// buildDeletePrepare returns a buildPrepare closure for a DELETE, scoping
// start/end keys to each range's bounds.
func buildDeletePrepare(tableId uint32, queryStart, queryEnd uint64, where sqllayer.Expression) func(*RangeDescriptor, uint64) *rs.PrepareRequest {
	return func(desc *RangeDescriptor, txnId uint64) *rs.PrepareRequest {
		return &rs.PrepareRequest{
			TxnId:    txnId,
			Op:       rs.RangeOp_DELETE,
			TableId:  tableId,
			StartKey: max64(queryStart, desc.StartKey),
			EndKey:   min64(queryEnd, desc.EndKey-1),
			Where:    exprToProto(where),
		}
	}
}

func (gw *Gateway) sendToLeader(desc *RangeDescriptor, req *rs.RangeRequest) (*ResultSet, error) {
	conn, ok := gw.conns[desc.LeaderID]
	if !ok {
		return nil, fmt.Errorf("no connection for leader node %d", desc.LeaderID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := rs.NewRangeServiceClient(conn).Execute(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("execute on leader %d: %w", desc.LeaderID, err)
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("range error: %s", resp.Error)
	}

	result := &ResultSet{}
	for _, row := range resp.Rows {
		rr := ResultRow{Key: row.Key}
		for _, f := range row.Fields {
			rr.Fields = append(rr.Fields, protoFieldToBTree(f))
		}
		result.Rows = append(result.Rows, rr)
	}
	return result, nil
}

// extractPKBounds walks a WHERE expression tree and returns the tightest
// [low, high] key interval for the primary key column. Returns [0, MaxUint32]
// (full table scan) when the expression imposes no PK constraint.
func extractPKBounds(expr sqllayer.Expression, pkName string) (low, high uint32) {
	if expr == nil {
		return 0, math.MaxUint32
	}
	switch e := expr.(type) {
	case *sqllayer.ComparisonExpr:
		if e.Column != pkName {
			return 0, math.MaxUint32
		}
		val64, err := strconv.ParseUint(e.Value.Value, 10, 32)
		if err != nil {
			return 0, math.MaxUint32
		}
		val := uint32(val64)
		switch e.Operator {
		case "=":
			return val, val
		case ">":
			if val < math.MaxUint32 {
				return val + 1, math.MaxUint32
			}
			return math.MaxUint32, math.MaxUint32
		case ">=":
			return val, math.MaxUint32
		case "<":
			if val > 0 {
				return 0, val - 1
			}
			return 0, 0
		case "<=":
			return 0, val
		}
	case *sqllayer.LogicalExpr:
		lLow, lHigh := extractPKBounds(e.Left, pkName)
		rLow, rHigh := extractPKBounds(e.Right, pkName)
		switch e.Operator {
		case "AND":
			return max32(lLow, rLow), min32(lHigh, rHigh)
		case "OR":
			return min32(lLow, rLow), max32(lHigh, rHigh)
		}
	}
	return 0, math.MaxUint32
}

// --- proto conversion helpers ------------------------------------------------

func fieldsToProto(fields []btree.Field) []*rs.Field {
	out := make([]*rs.Field, len(fields))
	for i, f := range fields {
		pf := &rs.Field{Tag: uint32(f.Tag)}
		switch v := f.Value.(type) {
		case btree.IntValue:
			pf.Value = &rs.FieldValue{Value: &rs.FieldValue_IntVal{IntVal: v.V}}
		case btree.StringValue:
			pf.Value = &rs.FieldValue{Value: &rs.FieldValue_StrVal{StrVal: v.V}}
		default:
			// NullValue, ListValue, and any future types are serialised with
			// btree.EncodeField so no information is lost on the round-trip.
			if b, err := btree.EncodeField(f); err == nil {
				pf.Value = &rs.FieldValue{Value: &rs.FieldValue_BytesVal{BytesVal: b}}
			}
		}
		out[i] = pf
	}
	return out
}

func protoFieldToBTree(f *rs.Field) btree.Field {
	bf := btree.Field{Tag: uint8(f.Tag)}
	if f.Value == nil {
		return bf
	}
	switch v := f.Value.Value.(type) {
	case *rs.FieldValue_IntVal:
		bf.Value = btree.IntValue{V: v.IntVal}
	case *rs.FieldValue_StrVal:
		bf.Value = btree.StringValue{V: v.StrVal}
	case *rs.FieldValue_BytesVal:
		// Decode the opaque btree-encoded bytes back to a Field.
		if fields, _ := btree.DecodeFields(v.BytesVal); len(fields) > 0 {
			bf.Tag = fields[0].Tag
			bf.Value = fields[0].Value
		}
	}
	return bf
}

func exprToProto(expr sqllayer.Expression) *rs.Expression {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *sqllayer.ComparisonExpr:
		return &rs.Expression{Expr: &rs.Expression_Comparison{
			Comparison: &rs.ComparisonExpr{
				Column:       e.Column,
				Operator:     e.Operator,
				LiteralValue: e.Value.Value,
				LiteralType:  int32(e.Value.Type),
			},
		}}
	case *sqllayer.LogicalExpr:
		return &rs.Expression{Expr: &rs.Expression_Logical{
			Logical: &rs.LogicalExpr{
				Operator: e.Operator,
				Left:     exprToProto(e.Left),
				Right:    exprToProto(e.Right),
			},
		}}
	}
	return nil
}

func colTypeByIndex(idx int, schema *sqllayer.TableSchemaValue) string {
	if idx == 0 {
		return schema.PrimaryKey.DataType
	}
	return schema.Columns[idx-1].DataType
}

func max32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func min32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
