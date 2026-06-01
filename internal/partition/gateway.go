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
}

// AddConn registers a gRPC connection to nodeID's RangeService port.
func (gw *Gateway) AddConn(nodeID uint64, conn *grpc.ClientConn) {
	gw.conns[nodeID] = conn
}

func NewGateway(router *Router, schema *sqllayer.SchemaCatalog) *Gateway {
	return &Gateway{
		router: router,
		schema: schema,
		conns:  make(map[uint64]*grpc.ClientConn),
	}
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
		low, high := extractPKBounds(s.Where, schema.PrimaryKey.Name)
		startKey := sqllayer.EncodeKey(schema.TableId, low)
		endKey := sqllayer.EncodeKey(schema.TableId, high) // inclusive for scans
		routeEnd := endKey + 1                             // exclusive for RouteRange
		ranges, err := gw.router.RouteRange(startKey, routeEnd)
		if err != nil {
			return nil, fmt.Errorf("route select: %w", err)
		}
		cols := make([]int32, len(s.Columns))
		for i, name := range s.Columns {
			idx := sqllayer.FindColumnIndex(name, schema)
			if idx == -1 {
				return nil, fmt.Errorf("column %q not found in table %q", name, s.Table)
			}
			cols[i] = int32(idx)
		}
		if len(ranges) == 1 {
			return gw.sendToLeader(ranges[0], &rs.RangeRequest{
				Op:       rs.RangeOp_SCAN,
				TableId:  schema.TableId,
				StartKey: startKey,
				EndKey:   endKey,
				Where:    exprToProto(s.Where),
				Columns:  cols,
			})
		}
		return gw.scatterGather(ranges, startKey, endKey, schema.TableId, cols, s.Where)

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
		return gw.scatterGatherMutation(ranges, startKey, endKey, req)

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
		return gw.scatterGatherMutation(ranges, startKey, endKey, req)

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
		dataStart := sqllayer.EncodeKey(tableSchema.TableId, 0)
		dataEnd := sqllayer.EncodeKey(tableSchema.TableId, ^uint32(0))
		dataRanges, err := gw.router.RouteRange(dataStart, dataEnd+1)
		if err != nil {
			return nil, fmt.Errorf("route data ranges: %w", err)
		}
		for _, rd := range dataRanges {
			scopedStart := max64(dataStart, rd.StartKey)
			scopedEnd := min64(dataEnd, rd.EndKey-1) // rd.EndKey is exclusive; convert to inclusive
			if _, err := gw.sendToLeader(rd, &rs.RangeRequest{
				Op:       rs.RangeOp_DELETE,
				TableId:  tableSchema.TableId,
				StartKey: scopedStart,
				EndKey:   scopedEnd,
			}); err != nil {
				return nil, fmt.Errorf("delete data in range %d: %w", rd.RangeID, err)
			}
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

// scatterGatherMutation fans an UPDATE or DELETE across ranges, scoping each
// request's key bounds to the range's interval. Results are not merged.
func (gw *Gateway) scatterGatherMutation(
	ranges []*RangeDescriptor,
	queryStart, queryEnd uint64,
	req *rs.RangeRequest,
) (*ResultSet, error) {
	type result struct{ err error }
	ch := make(chan result, len(ranges))

	for _, desc := range ranges {
		go func(desc *RangeDescriptor) {
			_, err := gw.sendToLeader(desc, &rs.RangeRequest{
				Op:        req.Op,
				TableId:   req.TableId,
				StartKey:  max64(queryStart, desc.StartKey),
				EndKey:    min64(queryEnd, desc.EndKey-1), // desc.EndKey is exclusive; scan is inclusive
				Where:     req.Where,
				Fields:    req.Fields,
				UpdateCol: req.UpdateCol,
			})
			ch <- result{err}
		}(desc)
	}

	for range ranges {
		if r := <-ch; r.err != nil {
			return nil, r.err
		}
	}
	return &ResultSet{}, nil
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
