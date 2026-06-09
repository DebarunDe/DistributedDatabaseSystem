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

	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ap "github.com/your-username/DistributedDatabaseSystem/internal/AP"
	lock "github.com/your-username/DistributedDatabaseSystem/internal/Lock"
	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	"github.com/your-username/DistributedDatabaseSystem/internal/httpapi"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
	"github.com/your-username/DistributedDatabaseSystem/internal/partition"
	"github.com/your-username/DistributedDatabaseSystem/internal/raft"
	pb "github.com/your-username/DistributedDatabaseSystem/proto/db"
	raftpb "github.com/your-username/DistributedDatabaseSystem/proto/raft"
	rs "github.com/your-username/DistributedDatabaseSystem/proto/rangeservice"
	"google.golang.org/grpc/credentials/insecure"
)

type server struct {
	pb.UnimplementedSQLServiceServer
	tm *lock.TransactionManager
	ex *sqllayer.Executor
	sc *sqllayer.SchemaCatalog
	gw *partition.Gateway // non-nil in Raft mode; routes via RangeService
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

	if s.gw != nil {
		result, err := s.gw.Execute(stmt)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "gateway: %v", err)
		}
		if result == nil {
			return &pb.SQLResponse{}, nil
		}
		return partitionResultSetToProto(result), nil
	}

	txn := s.tm.Begin()
	result, err := s.ex.Execute(stmt, txn.Id)
	if err != nil {
		s.tm.Rollback(txn.Id)
		return nil, status.Errorf(codes.Internal, "execute: %v", err)
	}
	if err := s.tm.Commit(txn.Id); err != nil {
		_ = s.sc.LoadSchemas()
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}
	if result == nil {
		return &pb.SQLResponse{}, nil
	}
	return resultSetToProto(result), nil
}

func partitionResultSetToProto(rs *partition.ResultSet) *pb.SQLResponse {
	resp := &pb.SQLResponse{Columns: rs.Columns}
	for _, row := range rs.Rows {
		pbRow := &pb.ResultRow{}
		colType := ""
		for i, f := range row.Fields {
			if i < len(rs.ColTypes) {
				colType = rs.ColTypes[i]
			}
			pbRow.Fields = append(pbRow.Fields, fieldToProto(f, colType))
		}
		resp.Rows = append(resp.Rows, pbRow)
	}
	return resp
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
	// SIGHUP is sent when a terminal window is closed.  Ignore it so closing
	// the terminal of one node does not propagate to other nodes that share the
	// same session (e.g. when all three servers are started in the same shell).
	signal.Ignore(syscall.SIGHUP)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	for _, s := range servers {
		s.GracefulStop()
	}
}

// detectOutboundIP returns the IP address this host uses to reach external
// destinations. It opens a UDP socket (no packets are sent) to discover which
// local interface the OS would route through.
func detectOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

func main() {
	dbPath := flag.String("db", "", "path to database file (required)")
	port := flag.String("port", "5555", "port to listen on for SQL")
	raftPort := flag.String("raft-port", "5556", "port to listen on for Raft RPC")
	listenAddr := flag.String("listen-addr", "", "IP address to bind listeners (default: all interfaces, i.e. 0.0.0.0)")
	nodeID := flag.Uint64("id", 0, "this node's Raft ID (0 = standalone, no Raft)")
	peersFlag := flag.String("peers", "", "comma-separated peer list: id=addr,id=addr (e.g. 2=192.168.1.2:5556)")
	advertiseAddr := flag.String("advertise-addr", "", "address peers use to reach this node's Raft port (e.g. 192.168.1.1:5556); defaults to <detected-outbound-ip>:<raft-port>")
	httpPort := flag.String("http-port", "8080", "port for REST API")
	apiKeysFlag := flag.String("api-keys", "", "comma-separated API keys")
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
	txnRecordStore := partition.NewTxnRecordStore()

	timestamps := ap.NewTimestampStore()
	apLog, err := ap.NewAPWriteLog(*dbPath + "_ap.log")
	if err != nil {
		log.Fatalf("open ap log: %v", err)
	}
	defer func() { _ = apLog.Close() }()

	applyFn := func(op raft.ReplOp, key uint64, fields []btree.Field) error {
		switch op {
		case raft.ReplTxnRecord:
			txnRecordStore.Store(partition.DecodeTxnRecord(key, fields))
			return nil
		case raft.ReplPut:
			if err := bt.Insert(key, fields); err != nil {
				return err
			}
		case raft.ReplDelete:
			if err := bt.Delete(key); err != nil {
				return err
			}
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

	// Partition layer — only active in Raft mode.
	var gw *partition.Gateway
	if rn != nil {
		peers, _ := parsePeers(*peersFlag)

		// Build the full node map (peers + self) for the coordinator.
		allNodes := make(map[uint64]string, len(peers)+1)
		for id, addr := range peers {
			allNodes[id] = addr
		}
		self := *advertiseAddr
		if self == "" {
			self = detectOutboundIP() + ":" + *raftPort
		}
		allNodes[*nodeID] = self

		coordinator, err := partition.NewCoordinator(allNodes), error(nil)
		_ = err // NewCoordinator does not return an error
		router, err := partition.NewRouter(coordinator)
		if err != nil {
			log.Fatalf("create router: %v", err)
		}
		gw = partition.NewGateway(router, sc)

		// Populate gateway connections — one per peer, plus self.
		for id, conn := range rn.PeerConns() {
			gw.AddConn(id, conn)
		}
		// Self-connection: dial the same address this server is bound to.
		// When --listen-addr is empty the server binds all interfaces, so we
		// fall back to loopback (127.0.0.1). When it is set to a specific IP we
		// must use that IP, because the server is not listening on any other.
		selfDialHost := *listenAddr
		if selfDialHost == "" {
			selfDialHost = "127.0.0.1"
		}
		selfConn, err := grpc.NewClient(
			selfDialHost+":"+*raftPort,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatalf("dial self: %v", err)
		}
		gw.AddConn(*nodeID, selfConn)

		// Register split-trigger hooks on the Raft node.
		rn.SetRangeLookup(func(key uint64) (rangeID, startKey, endKey uint64, found bool) {
			rd := coordinator.LookupKey(key)
			if rd == nil {
				return 0, 0, 0, false
			}
			return rd.RangeID, rd.StartKey, rd.EndKey, true
		})
		rn.SetSplitHook(func(rangeID, splitKey uint64) error {
			return coordinator.RequestSplit(rangeID, splitKey)
		})
		rn.SetLeaderRangesFunc(func() []raft.RangeInfo {
			all := coordinator.GetAllRanges()
			out := make([]raft.RangeInfo, 0, len(all))
			for _, rd := range all {
				if rd.LeaderID == *nodeID {
					out = append(out, raft.RangeInfo{
						RangeID:  rd.RangeID,
						StartKey: rd.StartKey,
						EndKey:   rd.EndKey,
						LeaderID: rd.LeaderID,
					})
				}
			}
			return out
		})
		rn.SetLeaderChangeHook(func(leaderID uint64) {
			for _, rd := range coordinator.GetAllRanges() {
				coordinator.UpdateLeader(rd.RangeID, leaderID)
			}
			// Router caches descriptors — refresh it so RouteKey sees the new LeaderID.
			if err := router.Refresh(); err != nil {
				log.Printf("router refresh after leader change: %v", err)
			}
			if leaderID != 0 {
				log.Printf("leader is now node %d", leaderID)
			}
		})
	}

	srv := &server{tm: tm, ex: ex, sc: sc, gw: gw}

	apiKeys := make(map[string]bool)
	for _, k := range strings.Split(*apiKeysFlag, ",") {
		if k = strings.TrimSpace(k); k != "" {
			apiKeys[k] = true
		}
	}
	httpSrv := httpapi.NewHTTPServer(gw, sc, apiKeys)
	go func() {
		addr := *listenAddr + ":" + *httpPort
		log.Printf("HTTP REST API listening on %s", addr)
		if err := http.ListenAndServe(addr, httpSrv.Routes()); err != nil {
			log.Fatalf("http serve: %v", err)
		}
	}()

	lis, err := net.Listen("tcp", *listenAddr+":"+*port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterSQLServiceServer(grpcServer, srv)

	raftLis, err := net.Listen("tcp", *listenAddr+":"+*raftPort)
	if err != nil {
		log.Fatalf("raft listen: %v", err)
	}
	raftServer := grpc.NewServer()
	if rn != nil {
		raftpb.RegisterRaftServiceServer(raftServer, &raftServiceServer{rn: rn})
		rs.RegisterRangeServiceServer(raftServer, partition.NewRangeServer(bt, tm, sc, txnRecordStore, timestamps, apLog))
		go rn.Run()
		log.Printf("raft node %d listening on :%s", *nodeID, *raftPort)

		// Start the AP syncer so eventual-consistency tables replicate across peers.
		syncerCtx, syncerCancel := context.WithCancel(context.Background())
		_ = syncerCancel // cancelled via signal handler below
		apSyncer := ap.NewAPSyncer(bt, timestamps, apLog, nil /* peers wired up separately */, sc)
		go apSyncer.Run(syncerCtx)
	}

	log.Printf("SQL server listening on :%s", *port)

	go startSignalHandler(grpcServer, raftServer)
	go func() {
		if err := raftServer.Serve(raftLis); err != nil {
			// Non-fatal: gRPC can return ErrServerStopped or a transient
			// accept error when a peer crashes and resets connections.
			log.Printf("raft serve stopped: %v", err)
		}
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
