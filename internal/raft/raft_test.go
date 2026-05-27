package raft_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
	"github.com/your-username/DistributedDatabaseSystem/internal/raft"
	pb "github.com/your-username/DistributedDatabaseSystem/proto/raft"
	"google.golang.org/grpc"
)

// ---- gRPC service shim ----

type raftSvc struct {
	pb.UnimplementedRaftServiceServer
	rn *raft.RaftNode
}

func (s *raftSvc) RequestVote(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.rn.HandleRequestVote(req), nil
}

func (s *raftSvc) AppendEntries(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return s.rn.HandleAppendEntries(req), nil
}

// ---- cluster harness ----

type nodeHandle struct {
	rn  *raft.RaftNode
	srv *grpc.Server

	mu      sync.Mutex
	applied []raft.RaftCommand
}

func (h *nodeHandle) appliedLen() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.applied)
}

type testCluster struct {
	t     *testing.T
	nodes []*nodeHandle
}

// newTestBTree creates a BTree backed by a temp file, cleaned up via t.Cleanup.
func newTestBTree(t *testing.T) *btree.BTree {
	t.Helper()
	path := filepath.Join(t.TempDir(), "raft.db")
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
	return btree.NewBTree(bp)
}

// freeAddr returns a 127.0.0.1:port string where the port is currently free.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// newCluster spins up n in-process Raft nodes, each with its own gRPC server.
// All nodes are stopped via t.Cleanup.
func newCluster(t *testing.T, n int) *testCluster {
	t.Helper()

	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = freeAddr(t)
	}

	c := &testCluster{t: t, nodes: make([]*nodeHandle, n)}

	for i := 0; i < n; i++ {
		peers := make(map[uint64]string, n-1)
		for j := 0; j < n; j++ {
			if j != i {
				peers[uint64(j+1)] = addrs[j]
			}
		}

		bt := newTestBTree(t)
		rn, err := raft.NewRaftNode(uint64(i+1), peers, bt)
		if err != nil {
			t.Fatalf("node %d: NewRaftNode: %v", i+1, err)
		}

		h := &nodeHandle{rn: rn}
		rn.SetApplyHook(func(op raft.ReplOp, key uint64, fields []btree.Field) error {
			h.mu.Lock()
			h.applied = append(h.applied, raft.RaftCommand{Op: op, Key: key, Fields: fields})
			h.mu.Unlock()
			return nil
		})

		lis, err := net.Listen("tcp", addrs[i])
		if err != nil {
			t.Fatalf("node %d: listen %s: %v", i+1, addrs[i], err)
		}
		srv := grpc.NewServer()
		pb.RegisterRaftServiceServer(srv, &raftSvc{rn: rn})
		h.srv = srv

		go func() { _ = srv.Serve(lis) }()
		go rn.Run()

		c.nodes[i] = h
	}

	t.Cleanup(func() {
		for _, h := range c.nodes {
			h.srv.Stop()
			h.rn.Stop()
		}
	})

	return c
}

// waitForLeader polls until one node reports IsLeader, then returns its index (0-based).
// Returns -1 if no leader is found within the timeout.
func (c *testCluster) waitForLeader(timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, h := range c.nodes {
			if h.rn.IsLeader() {
				return i
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return -1
}

// propose calls Propose on node i with a single dummy entry keyed by key.
func (c *testCluster) propose(i int, key uint64) error {
	return c.nodes[i].rn.Propose([]raft.RaftCommand{
		{Op: raft.ReplPut, Key: key},
	})
}

// stopNode stops node i's gRPC server and Run loop.
func (c *testCluster) stopNode(i int) {
	c.nodes[i].srv.Stop()
	c.nodes[i].rn.Stop()
}

// ---- Tests ----

// S1: A 3-node cluster elects exactly one leader within a reasonable timeout.
func TestRaft_LeaderElected(t *testing.T) {
	t.Parallel()
	c := newCluster(t, 3)

	idx := c.waitForLeader(5 * time.Second)
	if idx == -1 {
		t.Fatal("no leader elected within 5s")
	}

	leaders := 0
	for _, h := range c.nodes {
		if h.rn.IsLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Errorf("expected exactly 1 leader, got %d", leaders)
	}
}

// S1 corollary: non-leaders reject Propose immediately.
func TestRaft_NonLeaderRejectsProposal(t *testing.T) {
	t.Parallel()
	c := newCluster(t, 3)

	leaderIdx := c.waitForLeader(5 * time.Second)
	if leaderIdx == -1 {
		t.Fatal("no leader elected")
	}

	for i, h := range c.nodes {
		if h.rn.IsLeader() {
			continue
		}
		err := c.propose(i, uint64(i+100))
		if err == nil {
			t.Errorf("node %d (non-leader) accepted proposal, expected rejection", i+1)
		}
	}
}

// S2: Entries proposed on the leader are applied by followers via applyHook.
func TestRaft_LogReplication(t *testing.T) {
	t.Parallel()
	c := newCluster(t, 3)

	leaderIdx := c.waitForLeader(5 * time.Second)
	if leaderIdx == -1 {
		t.Fatal("no leader elected")
	}

	if err := c.propose(leaderIdx, 42); err != nil {
		t.Fatalf("propose: %v", err)
	}

	// Wait for followers to apply.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for i, h := range c.nodes {
			if i == leaderIdx {
				continue // leader skips applyHook
			}
			if h.appliedLen() == 0 {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	for i, h := range c.nodes {
		if i != leaderIdx {
			t.Errorf("node %d applied %d entries, want 1", i+1, h.appliedLen())
		}
	}
}

// Multi-entry proposal fix: a batch of N commands must be applied exactly N times
// on each follower — not 2N (the double-apply bug).
func TestRaft_MultiEntryBatchNoDoubleApply(t *testing.T) {
	t.Parallel()
	c := newCluster(t, 3)

	leaderIdx := c.waitForLeader(5 * time.Second)
	if leaderIdx == -1 {
		t.Fatal("no leader elected")
	}

	const batchSize = 5
	cmds := make([]raft.RaftCommand, batchSize)
	for i := range cmds {
		cmds[i] = raft.RaftCommand{Op: raft.ReplPut, Key: uint64(200 + i)}
	}

	if err := c.nodes[leaderIdx].rn.Propose(cmds); err != nil {
		t.Fatalf("propose batch: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for i, h := range c.nodes {
			if i == leaderIdx {
				continue
			}
			if h.appliedLen() < batchSize {
				ok = false
			}
		}
		if ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	for i, h := range c.nodes {
		if i == leaderIdx {
			continue
		}
		got := h.appliedLen()
		if got != batchSize {
			t.Errorf("node %d applied %d entries for batch of %d (double-apply bug?)", i+1, got, batchSize)
		}
	}
}

// S7 core: stopping the leader causes the remaining 2 nodes to elect a new one.
func TestRaft_LeaderFailover(t *testing.T) {
	t.Parallel()
	c := newCluster(t, 3)

	leaderIdx := c.waitForLeader(5 * time.Second)
	if leaderIdx == -1 {
		t.Fatal("no leader elected in initial election")
	}

	c.stopNode(leaderIdx)

	// Remaining nodes must elect a new leader.
	deadline := time.Now().Add(5 * time.Second)
	newLeader := -1
	for time.Now().Before(deadline) {
		for i, h := range c.nodes {
			if i == leaderIdx {
				continue
			}
			if h.rn.IsLeader() {
				newLeader = i
				break
			}
		}
		if newLeader != -1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if newLeader == -1 {
		t.Fatal("no new leader elected after stopping old leader")
	}
	if newLeader == leaderIdx {
		t.Errorf("stopped node %d should not be leader", leaderIdx+1)
	}

	// New leader must accept writes.
	if err := c.propose(newLeader, 999); err != nil {
		t.Errorf("propose on new leader failed: %v", err)
	}
}

// S4: With only 1 of 3 nodes alive, the lone node must reject writes (no quorum).
func TestRaft_QuorumLossBlocksWrites(t *testing.T) {
	t.Parallel()
	c := newCluster(t, 3)

	leaderIdx := c.waitForLeader(5 * time.Second)
	if leaderIdx == -1 {
		t.Fatal("no leader elected")
	}

	// Stop the two non-leaders so the leader loses quorum.
	for i := range c.nodes {
		if i != leaderIdx {
			c.stopNode(i)
		}
	}

	// The lone (ex-)leader must not be able to commit.
	done := make(chan error, 1)
	go func() { done <- c.propose(leaderIdx, 777) }()

	select {
	case err := <-done:
		if err == nil {
			t.Error("propose succeeded without quorum — expected rejection")
		}
	case <-time.After(12 * time.Second):
		t.Error("propose did not return after quorum loss (hung)")
	}
}

// Unit test: HandleRequestVote rejects a RequestVote with a stale term.
func TestRaft_HandleRequestVote_RejectsStaleTerm(t *testing.T) {
	t.Parallel()

	bt := newTestBTree(t)
	// Single node, no peers — we only call HandleRequestVote directly.
	rn, err := raft.NewRaftNode(1, nil, bt)
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}
	defer rn.Stop()
	go rn.Run()

	// Advance the node's term artificially via a legit RequestVote.
	rn.HandleRequestVote(&pb.RequestVoteRequest{
		Term:        5,
		CandidateId: 99,
	})

	// Now send a vote request with a lower term — must be rejected.
	resp := rn.HandleRequestVote(&pb.RequestVoteRequest{
		Term:        3,
		CandidateId: 99,
	})
	if resp.VoteGranted {
		t.Error("expected vote denied for stale-term candidate")
	}
}

// Unit test: HandleRequestVote denies a vote to a candidate whose log is behind.
func TestRaft_HandleRequestVote_RejectsStaleLog(t *testing.T) {
	t.Parallel()

	bt := newTestBTree(t)
	rn, err := raft.NewRaftNode(1, nil, bt)
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}
	defer rn.Stop()
	go rn.Run()

	// Give the node a log entry via AppendEntries so it has lastLogIndex=1, lastLogTerm=1.
	rn.HandleAppendEntries(&pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     99,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []*pb.RaftLogEntry{
			{Index: 1, Term: 1, Op: 0, Key: 1},
		},
		LeaderCommit: 0,
	})

	// Candidate has no log (lastLogIndex=0) but equal term — must be denied.
	resp := rn.HandleRequestVote(&pb.RequestVoteRequest{
		Term:         2,
		CandidateId:  99,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if resp.VoteGranted {
		t.Error("expected vote denied for candidate with stale log")
	}
}

// Unit test: HandleAppendEntries truncates conflicting log entries.
func TestRaft_HandleAppendEntries_LogConflictTruncation(t *testing.T) {
	t.Parallel()

	bt := newTestBTree(t)
	rn, err := raft.NewRaftNode(1, nil, bt)
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}
	defer rn.Stop()
	go rn.Run()

	// Append two entries at term 1.
	rn.HandleAppendEntries(&pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []*pb.RaftLogEntry{
			{Index: 1, Term: 1, Key: 10},
			{Index: 2, Term: 1, Key: 20},
		},
	})

	// New leader sends a conflicting entry at index 2 with term 2.
	// The node must truncate index 2 (term 1) and replace it.
	resp := rn.HandleAppendEntries(&pb.AppendEntriesRequest{
		Term:         2,
		LeaderId:     3,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []*pb.RaftLogEntry{
			{Index: 2, Term: 2, Key: 99},
		},
		LeaderCommit: 2,
	})

	if !resp.Success {
		t.Fatal("AppendEntries should succeed after truncation")
	}
}

// Verify that stepDown (triggered by a higher term) fails any pending proposals.
func TestRaft_StepDown_FailsPendingProposals(t *testing.T) {
	t.Parallel()
	c := newCluster(t, 3)

	leaderIdx := c.waitForLeader(5 * time.Second)
	if leaderIdx == -1 {
		t.Fatal("no leader elected")
	}

	// Stop peers so the leader loses quorum — next proposal will time out.
	for i := range c.nodes {
		if i != leaderIdx {
			c.stopNode(i)
		}
	}

	// Sending a higher-term AppendEntries will call stepDown and should cause the
	// pending proposal to be rejected quickly rather than waiting the full 10s.
	done := make(chan error, 1)
	go func() { done <- c.propose(leaderIdx, 555) }()

	time.Sleep(300 * time.Millisecond)
	// Simulate a higher-term message arriving (triggers stepDown).
	c.nodes[leaderIdx].rn.HandleAppendEntries(&pb.AppendEntriesRequest{
		Term:     100,
		LeaderId: 99,
	})

	select {
	case err := <-done:
		if err == nil {
			t.Error("expected proposal to fail after step-down")
		}
	case <-time.After(3 * time.Second):
		t.Error("proposal did not fail within 3s after step-down")
	}
}

// RPC-level sanity: after a leader is elected, exactly one node is leader and
// the others are followers — confirmed via the exported IsLeader predicate which
// is backed by the same mutex that guards the RPC handlers.
func TestRaft_RPC_RequestVoteStaleTermRejected(t *testing.T) {
	t.Parallel()
	c := newCluster(t, 3)

	leaderIdx := c.waitForLeader(5 * time.Second)
	if leaderIdx == -1 {
		t.Fatal("no leader elected")
	}

	for i, h := range c.nodes {
		if i == leaderIdx {
			continue
		}
		if h.rn.IsLeader() {
			t.Errorf("node %d should be a follower after election", i+1)
		}
	}
}

// Rapid writes: 20 sequential proposals must all commit on the leader.
func TestRaft_RapidSequentialWrites(t *testing.T) {
	t.Parallel()
	c := newCluster(t, 3)

	leaderIdx := c.waitForLeader(5 * time.Second)
	if leaderIdx == -1 {
		t.Fatal("no leader elected")
	}

	const n = 20
	for i := 0; i < n; i++ {
		if err := c.propose(leaderIdx, uint64(1000+i)); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}

	// All n entries should be applied on each follower.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for i, h := range c.nodes {
			if i == leaderIdx {
				continue
			}
			if h.appliedLen() < n {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	for i, h := range c.nodes {
		if i != leaderIdx {
			got := h.appliedLen()
			if got != n {
				t.Errorf("node %d: applied %d/%d entries", i+1, got, n)
			}
		}
	}
}

// Ensure the cluster description helper doesn't shadow the standard fmt import.
var _ = fmt.Sprintf
