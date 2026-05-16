package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	lock "github.com/your-username/DistributedDatabaseSystem/internal/Lock"
	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
	pb "github.com/your-username/DistributedDatabaseSystem/proto/db"
)

type server struct {
	pb.UnimplementedSQLServiceServer
	tm *lock.TransactionManager
	ex *sqllayer.Executor
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
	s.tm.Commit(txn.Id)

	if result == nil {
		return &pb.SQLResponse{}, nil
	}
	resp := resultSetToProto(result)
	return resp, nil
}

func startSignalHandler(grpcServer *grpc.Server) {
	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	grpcServer.GracefulStop()
}

func main() {
	dbPath := flag.String("db", "", "path to database file (required)")
	port := flag.String("port", "5555", "port to listen on")
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
	tm := lock.NewTransactionManager(bt)
	ex := sqllayer.NewExecutor(sc, bt, tm)
	srv := &server{tm: tm, ex: ex}

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSQLServiceServer(grpcServer, srv)

	log.Printf("server listening on %s", *port)

	go startSignalHandler(grpcServer)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
