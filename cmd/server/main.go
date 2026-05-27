package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	lock "github.com/your-username/DistributedDatabaseSystem/internal/Lock"
	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
	"github.com/your-username/DistributedDatabaseSystem/internal/raft"
	pb "github.com/your-username/DistributedDatabaseSystem/proto/db"
	raftpb "github.com/your-username/DistributedDatabaseSystem/proto/raft"
)

type server struct {
	pb.UnimplementedSQLServiceServer
	tm *lock.TransactionManager
	ex *sqllayer.Executor
	sc *sqllayer.SchemaCatalog
}

type raftServiceServer struct {
	raftpb.UnimplementedRaftServiceServer
	rn *raft.RaftNode
}

func (s *raftServiceServer) RequestVote(_ context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return s.rn.HandleRequestVote(req), nil
}

func (s *raftServiceServer) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	return s.rn.HandleAppendEntries(req), nil
}

const defaultCacheSize = 256

func openOrCreate(path string) (pagemanager.PageManager, error) {
	var (
		disk pagemanager.PageManager
		err  error
	)
	if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
		disk, err = pagemanager.NewDB(path)
	} else {
		disk, err = pagemanager.OpenDB(path)
	}
	if err != nil {
		return nil, err
	}
	wal, err := pagemanager.NewWAL(disk, path)
	if err != nil {
		_ = disk.Close()
		return nil, err
	}
	return pagemanager.NewBufferPool(wal, defaultCacheSize), nil
}

func fieldToProto(f btree.Field, colType string) *pb.FieldValue {
	switch v := f.Value.(type) {
	case btree.IntValue:
		return &pb.FieldValue{Value: &pb.FieldValue_IntValue{IntValue: v.V}}
	case btree.StringValue:
		if colType == "BOOL" {
			return &pb.FieldValue{Value: &pb.FieldValue_BoolValue{BoolValue: v.V == "TRUE"}}
		}
		return &pb.FieldValue{Value: &pb.FieldValue_StringValue{StringValue: v.V}}
	default:
		return &pb.FieldValue{}
	}
}

func resultSetToProto(rs *sqllayer.ResultSet) *pb.SQLResponse {
	resp := &pb.SQLResponse{Columns: rs.Columns}
	for _, row := range rs.Rows {
		pbRow := &pb.ResultRow{}
		for i, f := range row.Fields {
			pbRow.Fields = append(pbRow.Fields, fieldToProto(f, rs.ColTypes[i]))
		}
		resp.Rows = append(resp.Rows, pbRow)
	}
	return resp
}

func (s *server) Execute(ctx context.Context, req *pb.SQLRequest) (*pb.SQLResponse, error) {
	tokens, err := sqllayer.Tokenize(req.Sql)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "tokenize: %v", err)
	}

	stmt, err := sqllayer.Parse(tokens)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse: %v", err)
	}

	txn := s.tm.Begin()
	result, err := s.ex.Execute(stmt, txn.Id)
	if err != nil {
		s.tm.Rollback(txn.Id)
		return nil, status.Errorf(codes.Internal, "execute: %v", err)
	}
	if err := s.tm.Commit(txn.Id); err != nil {
		// CREATE TABLE reserves a table ID in maxTableId before Propose. On failure
		// reload schemas so maxTableId is reset to the last committed value.
		_ = s.sc.LoadSchemas()
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}

	if result == nil {
		return &pb.SQLResponse{}, nil
	}
	return resultSetToProto(result), nil
}

func parsePeers(s string) (map[uint64]string, error) {
	peers := make(map[uint64]string)
	if s == "" {
		return peers, nil
	}
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid peer %q: expected id=addr", part)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(kv[0]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid peer id %q: %w", kv[0], err)
		}
		peers[id] = strings.TrimSpace(kv[1])
	}
	return peers, nil
}

func startSignalHandler(servers ...*grpc.Server) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	for _, s := range servers {
		s.GracefulStop()
	}
}

func main() {
	dbPath := flag.String("db", "", "path to database file (required)")
	port := flag.String("port", "5555", "port to listen on for SQL")
	raftPort := flag.String("raft-port", "5556", "port to listen on for Raft RPC")
	nodeID := flag.Uint64("id", 0, "this node's Raft ID (0 = standalone, no Raft)")
	peersFlag := flag.String("peers", "", "comma-separated peer list: id=addr,id=addr (e.g. 2=localhost:5556)")
	flag.Parse()

	if *dbPath == "" {
		log.Fatal("-db flag is required")
	}

	pm, err := openOrCreate(*dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer func() {
		if err := pm.Close(); err != nil {
			log.Printf("close db: %v", err)
		}
	}()

	bt := btree.NewBTree(pm)
	sc := sqllayer.NewSchemaCatalog(bt)
	if err := sc.LoadSchemas(); err != nil {
		fmt.Fprintf(os.Stderr, "load schemas: %v\n", err)
		os.Exit(1)
	}

	var rn *raft.RaftNode
	if *nodeID != 0 {
		peers, err := parsePeers(*peersFlag)
		if err != nil {
			log.Fatalf("parse peers: %v", err)
		}
		rn, err = raft.NewRaftNode(*nodeID, peers, bt)
		if err != nil {
			log.Fatalf("create raft node: %v", err)
		}
	}

	// applyFn is the single function that writes a committed Raft command to the
	// BTree and refreshes derived state. It is used in two places:
	//   - standalone mode: called directly by TransactionManager.Commit
	//   - Raft mode: registered as the applyHook on the RaftNode so that every
	//     node (leader and follower alike) applies entries through the same path
	//     after consensus is reached.
	applyFn := func(op raft.ReplOp, key uint64, fields []btree.Field) error {
		var err error
		switch op {
		case raft.ReplPut:
			err = bt.Insert(key, fields)
		case raft.ReplDelete:
			err = bt.Delete(key)
		}
		if err != nil {
			return err
		}
		if key>>32 == 0 { // schema entry: tableId=0 in upper 32 bits
			return sc.LoadSchemas()
		}
		return nil
	}

	tm := lock.NewTransactionManager(bt, rn, applyFn)
	ex := sqllayer.NewExecutor(sc, bt, tm)

	if rn != nil {
		rn.SetApplyHook(applyFn)
	}

	srv := &server{tm: tm, ex: ex, sc: sc}

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterSQLServiceServer(grpcServer, srv)

	raftLis, err := net.Listen("tcp", ":"+*raftPort)
	if err != nil {
		log.Fatalf("raft listen: %v", err)
	}
	raftServer := grpc.NewServer()
	if rn != nil {
		raftpb.RegisterRaftServiceServer(raftServer, &raftServiceServer{rn: rn})
		go rn.Run()
		log.Printf("raft node %d listening on :%s", *nodeID, *raftPort)
	}

	log.Printf("SQL server listening on :%s", *port)

	go startSignalHandler(grpcServer, raftServer)
	go func() {
		if err := raftServer.Serve(raftLis); err != nil {
			log.Fatalf("raft serve: %v", err)
		}
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
