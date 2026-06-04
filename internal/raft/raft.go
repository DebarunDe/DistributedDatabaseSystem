package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"runtime/debug"
	"sync"
	"time"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pb "github.com/your-username/DistributedDatabaseSystem/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ReplOp int

const (
	ReplPut ReplOp = iota
	ReplDelete
	ReplTxnRecord // commit record written to anchor range for crash recovery
)

type RaftState int

const (
	RaftStateFollower RaftState = iota
	RaftStateCandidate
	RaftStateLeader
)

const (
	// Election timeouts — wider range reduces split-vote probability
	ElectionTimeoutMin = 150 // ms
	ElectionTimeoutMax = 500 // ms
	HeartbeatInterval  = 50  // ms
	RPCTimeout         = 200 // ms — max wait for a single peer RPC
)

type RaftCommand struct {
	Op     ReplOp
	Key    uint64
	Fields []btree.Field
}

type RaftLogEntry struct {
	Index   uint64
	Term    uint64
	Command RaftCommand
}

type Proposal struct {
	Entries []RaftCommand
	Result  chan error
}

// RangeInfo is a snapshot of a single range's key bounds and leadership,
// used to avoid importing the partition package from raft.
type RangeInfo struct {
	RangeID  uint64
	StartKey uint64
	EndKey   uint64
	LeaderID uint64
}

// rangeStatsEntry tracks approximate size and row-count for one range.
type rangeStatsEntry struct {
	keyCount   uint64
	totalBytes uint64
}

const (
	maxKeysPerRange    = 1000
	minRangeSplitBytes = 1 << 20 // 1 MB
)

type RaftNode struct {
	mu sync.Mutex

	// persistent state
	currentTerm uint64
	votedFor    uint64
	log         []RaftLogEntry

	// volatile state: all nodes
	id          uint64
	state       RaftState
	commitIndex uint64
	lastApplied uint64
	peers       []uint64
	peersAddr   map[uint64]string

	// volatile state: leaders
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	// Infrastructure
	bt        *btree.BTree
	applyHook func(op ReplOp, key uint64, fields []btree.Field) error

	// Split trigger — wired via Set* methods to avoid importing partition.
	splitFn        func(rangeID uint64, splitKey uint64) error
	rangeLookupFn  func(key uint64) (rangeID, startKey, endKey uint64, found bool)
	leaderRangesFn func() []RangeInfo
	rangeStats     map[uint64]*rangeStatsEntry

	// leaderChangeFn is called (outside the mutex) whenever the known leader
	// changes.  leaderID == 0 means no leader is currently known.
	leaderChangeFn func(leaderID uint64)
	knownLeaderID  uint64
	proposalCh     chan Proposal
	electionTimer  *time.Timer
	heartbeatTick  *time.Ticker
	peerClients    map[uint64]pb.RaftServiceClient
	peerConns      map[uint64]*grpc.ClientConn
	pendingProps   map[uint64]chan error //log index -> result channel
	stopCh         chan struct{}
	stopOnce       sync.Once
}

// Stop shuts down the node's Run loop. Safe to call multiple times.
func (rn *RaftNode) Stop() {
	rn.stopOnce.Do(func() { close(rn.stopCh) })
}

// PeerConns returns the raw gRPC connections to every peer, keyed by node ID.
// Callers may wrap these in additional service clients without opening new dials.
func (rn *RaftNode) PeerConns() map[uint64]*grpc.ClientConn {
	return rn.peerConns
}

// IsLeader reports whether this node currently believes itself to be the leader.
func (rn *RaftNode) IsLeader() bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.state == RaftStateLeader
}

// SetApplyHook registers a function called whenever this node commits a Raft
// entry as a follower. The hook writes the entry to the BTree and updates any
// derived state (e.g. the SchemaCatalog). Leader proposals skip the hook
// because the SQL executor already applied the write before calling Propose.
func (rn *RaftNode) SetApplyHook(fn func(op ReplOp, key uint64, fields []btree.Field) error) {
	rn.applyHook = fn
}

func NewRaftNode(id uint64, peersAddr map[uint64]string, bt *btree.BTree) (*RaftNode, error) {
	peers := make([]uint64, 0, len(peersAddr))
	peerClients := make(map[uint64]pb.RaftServiceClient, len(peersAddr))
	peerConns := make(map[uint64]*grpc.ClientConn, len(peersAddr))

	for peerID, addr := range peersAddr {
		peers = append(peers, peerID)
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("dial peer %d at %s: %w", peerID, addr, err)
		}
		peerConns[peerID] = conn
		peerClients[peerID] = pb.NewRaftServiceClient(conn)
	}

	d := ElectionTimeoutMin + rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)

	return &RaftNode{
		id:            id,
		state:         RaftStateFollower,
		peers:         peers,
		peersAddr:     peersAddr,
		peerClients:   peerClients,
		peerConns:     peerConns,
		nextIndex:     make(map[uint64]uint64),
		matchIndex:    make(map[uint64]uint64),
		pendingProps:  make(map[uint64]chan error),
		proposalCh:    make(chan Proposal, 16),
		electionTimer: time.NewTimer(time.Duration(d) * time.Millisecond),
		heartbeatTick: time.NewTicker(HeartbeatInterval * time.Millisecond),
		bt:            bt,
		stopCh:        make(chan struct{}),
		rangeStats:    make(map[uint64]*rangeStatsEntry),
	}, nil
}

// SetSplitHook registers the function called when a range exceeds the split
// threshold. fn should call coordinator.RequestSplit.
func (rn *RaftNode) SetSplitHook(fn func(rangeID uint64, splitKey uint64) error) {
	rn.splitFn = fn
}

// SetRangeLookup registers a function that maps a BTree key to its range's
// ID and key bounds. fn should delegate to coordinator.LookupKey.
func (rn *RaftNode) SetRangeLookup(fn func(key uint64) (rangeID, startKey, endKey uint64, found bool)) {
	rn.rangeLookupFn = fn
}

// SetLeaderRangesFunc registers a function that returns the ranges this node
// currently leads. fn should filter coordinator.GetAllRanges by LeaderID.
func (rn *RaftNode) SetLeaderRangesFunc(fn func() []RangeInfo) {
	rn.leaderRangesFn = fn
}

// SetLeaderChangeHook registers fn to be called whenever the cluster leader
// changes.  fn receives the new leader's node ID (0 = no leader known).
// fn is invoked outside the Raft mutex so it is safe to call back into the
// coordinator or gateway.
func (rn *RaftNode) SetLeaderChangeHook(fn func(leaderID uint64)) {
	rn.leaderChangeFn = fn
}


// updateRangeStats updates approximate per-range key-count and byte stats for
// one log entry. Must be called before the entry is applied to the BTree so
// that bt.Search reflects pre-apply state.
func (rn *RaftNode) updateRangeStats(entry RaftLogEntry) {
	if rn.rangeLookupFn == nil || entry.Command.Op == ReplTxnRecord {
		return
	}
	rangeID, _, _, ok := rn.rangeLookupFn(entry.Command.Key)
	if !ok {
		return
	}
	stats := rn.rangeStats[rangeID]
	if stats == nil {
		stats = &rangeStatsEntry{}
		rn.rangeStats[rangeID] = stats
	}
	newSize := estimateRowSize(entry.Command.Fields)
	switch entry.Command.Op {
	case ReplPut:
		existing, found, err := rn.bt.Search(entry.Command.Key)
		if err == nil && !found {
			stats.keyCount++
			stats.totalBytes += newSize
		} else if err == nil && found {
			oldSize := estimateRowSize(existing)
			if newSize >= oldSize {
				stats.totalBytes += newSize - oldSize
			} else {
				stats.totalBytes -= oldSize - newSize
			}
		}
	case ReplDelete:
		if stats.keyCount > 0 {
			stats.keyCount--
		}
		if stats.totalBytes >= newSize {
			stats.totalBytes -= newSize
		}
	}
}

// checkSplit checks every range this node leads and requests a split at the
// median key if both the key-count and byte thresholds are exceeded.
// Called from applyCommitted while holding rn.mu; the BTree scan is the
// expensive part — consider moving to a background goroutine in production.
func (rn *RaftNode) checkSplit() {
	if rn.splitFn == nil || rn.leaderRangesFn == nil {
		return
	}
	for _, ri := range rn.leaderRangesFn() {
		stats := rn.rangeStats[ri.RangeID]
		if stats == nil {
			continue
		}
		if stats.keyCount <= maxKeysPerRange || stats.totalBytes <= minRangeSplitBytes {
			continue
		}
		rows, err := rn.bt.RangeScan(ri.StartKey, ri.EndKey)
		if err != nil || len(rows) < 2 {
			continue
		}
		medianKey := rows[len(rows)/2].Key
		if err := rn.splitFn(ri.RangeID, medianKey); err != nil {
			log.Printf("raft: split range %d at key %d: %v", ri.RangeID, medianKey, err)
		}
	}
}

func estimateRowSize(fields []btree.Field) uint64 {
	var size uint64
	for _, f := range fields {
		switch v := f.Value.(type) {
		case btree.IntValue:
			size += 8
		case btree.StringValue:
			size += uint64(len(v.V))
		default:
			size += 8
		}
	}
	return size
}

func (rn *RaftNode) Propose(commands []RaftCommand) error {
	result := make(chan error, 1)
	select {
	case rn.proposalCh <- Proposal{Entries: commands, Result: result}:
	case <-time.After(2 * time.Second):
		return fmt.Errorf("not leader")
	}
	select {
	case err := <-result:
		return err
	case <-time.After(10 * time.Second):
		return fmt.Errorf("propose timeout: no consensus")
	}
}

func (rn *RaftNode) Run() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("raft: Run() panic: %v\n%s", r, debug.Stack())
		}
	}()
	for {
		select {
		case <-rn.electionTimer.C:
			rn.startElection()

		case <-rn.heartbeatTick.C:
			rn.mu.Lock()
			if rn.state == RaftStateLeader {
				// Reset the election timer so the leader never spuriously starts
				// its own election (the timer fires if no heartbeats arrive, but
				// the leader is the one sending them, not receiving them).
				rn.resetElectionTimer()
				rn.sendHeartbeats()
			}
			rn.mu.Unlock()

		case prop := <-rn.proposalCh:
			rn.mu.Lock()
			if rn.state == RaftStateLeader {
				rn.handleProposal(prop)
			} else {
				prop.Result <- fmt.Errorf("not leader")
			}
			rn.mu.Unlock()
		case <-rn.stopCh:
			return
		}
	}
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()

	rn.state = RaftStateCandidate
	rn.currentTerm++
	rn.votedFor = rn.id
	term := rn.currentTerm
	lastIdx, lastTerm := rn.getLastLogInfo()
	rn.resetElectionTimer()

	rn.mu.Unlock()

	votesFor := 1
	voteCh := make(chan bool, len(rn.peers))

	for _, peer := range rn.peers {
		go func(c pb.RaftServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout*time.Millisecond)
			defer cancel()
			resp, err := c.RequestVote(ctx, &pb.RequestVoteRequest{
				Term:         term,
				CandidateId:  rn.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				voteCh <- false
				return
			}

			rn.mu.Lock()
			if resp.Term > rn.currentTerm {
				rn.stepDown(resp.Term)
				rn.mu.Unlock()
				voteCh <- false
				return
			}
			rn.mu.Unlock()
			voteCh <- resp.VoteGranted
		}(rn.peerClients[peer])
	}

	majority := (len(rn.peers)+1)/2 + 1
	for range rn.peers {
		if <-voteCh {
			votesFor++
		}

		rn.mu.Lock()
		if rn.state != RaftStateCandidate || rn.currentTerm != term {
			rn.mu.Unlock()
			return
		}

		if votesFor >= majority {
			rn.becomeLeader()
			rn.mu.Unlock()
			return
		}
		rn.mu.Unlock()
	}
}

// notifyLeaderChange fires leaderChangeFn when leaderID differs from the last
// known value.  Must be called with rn.mu held; the hook itself is invoked in
// a separate goroutine so it never deadlocks on the mutex.
func (rn *RaftNode) notifyLeaderChange(leaderID uint64) {
	if leaderID == rn.knownLeaderID || rn.leaderChangeFn == nil {
		return
	}
	rn.knownLeaderID = leaderID
	fn := rn.leaderChangeFn
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("raft: leaderChangeFn panic: %v\n%s", r, debug.Stack())
			}
		}()
		fn(leaderID)
	}()
}

func (rn *RaftNode) becomeLeader() {
	rn.state = RaftStateLeader
	// Drain any pending election-timer event so the leader never accidentally
	// starts its own election.
	rn.resetElectionTimer()
	lastIdx, _ := rn.getLastLogInfo()
	rn.nextIndex = make(map[uint64]uint64)
	rn.matchIndex = make(map[uint64]uint64)

	for _, peer := range rn.peers {
		rn.nextIndex[peer] = lastIdx + 1
		rn.matchIndex[peer] = 0
	}

	rn.notifyLeaderChange(rn.id)
	rn.sendHeartbeats()
}

func (rn *RaftNode) stepDown(newTerm uint64) {
	rn.state = RaftStateFollower
	rn.votedFor = 0
	rn.currentTerm = newTerm
	rn.resetElectionTimer()

	failed := rn.pendingProps
	rn.pendingProps = make(map[uint64]chan error)
	go func() {
		for idx, ch := range failed {
			ch <- fmt.Errorf("stepping down, proposal at index %d failed", idx)
		}
	}()
}

// Leader Only
func (rn *RaftNode) handleProposal(prop Proposal) {
	if len(prop.Entries) == 0 {
		rn.sendHeartbeats()
		return
	}
	var lastIndex uint64
	for i, cmd := range prop.Entries {
		lastIdx, _ := rn.getLastLogInfo()
		entry := RaftLogEntry{
			Index:   lastIdx + 1,
			Term:    rn.currentTerm,
			Command: cmd,
		}
		rn.log = append(rn.log, entry)
		lastIndex = entry.Index
		if i < len(prop.Entries)-1 {
			// Intermediate entries: register a nil channel so applyCommitted
			// skips the B-tree write (leader already applied it) without
			// sending a result yet.
			rn.pendingProps[entry.Index] = nil
		}
	}
	// Only the last entry unblocks the caller.
	rn.pendingProps[lastIndex] = prop.Result

	rn.sendHeartbeats()
}

func (rn *RaftNode) sendAppendEntries(peerId uint64) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("raft: sendAppendEntries(%d) panic: %v\n%s", peerId, r, debug.Stack())
		}
	}()
	rn.mu.Lock()
	if rn.state != RaftStateLeader {
		rn.mu.Unlock()
		return
	}
	ni := rn.nextIndex[peerId]
	prevIdx := ni - 1
	prevTerm := uint64(0)
	if prevIdx > 0 && prevIdx <= uint64(len(rn.log)) {
		prevTerm = rn.log[prevIdx-1].Term
	}
	entries := make([]RaftLogEntry, len(rn.log[ni-1:]))
	copy(entries, rn.log[ni-1:])
	term := rn.currentTerm
	commitIndex := rn.commitIndex
	rn.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout*time.Millisecond)
	defer cancel()
	resp, err := rn.peerClients[peerId].AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     rn.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      convertToProtoEntries(entries),
		LeaderCommit: commitIndex,
	})

	if err != nil {
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	if resp.Term > rn.currentTerm {
		rn.stepDown(resp.Term)
		return
	}

	if rn.state != RaftStateLeader || rn.currentTerm != term {
		return
	}

	if resp.Success {
		rn.nextIndex[peerId] = ni + uint64(len(entries))
		rn.matchIndex[peerId] = rn.nextIndex[peerId] - 1
		rn.advanceCommitIndex()
	} else {
		rn.nextIndex[peerId]--
		if rn.nextIndex[peerId] < 1 {
			rn.nextIndex[peerId] = 1
		}
	}
}

func (rn *RaftNode) advanceCommitIndex() {
	for n := rn.commitIndex + 1; n <= uint64(len(rn.log)); n++ {
		if rn.log[n-1].Term != rn.currentTerm {
			continue
		}
		count := 1
		for _, peerID := range rn.peers {
			if rn.matchIndex[peerID] >= n {
				count++
			}
		}
		if count >= (len(rn.peers)+1)/2+1 {
			rn.commitIndex = n
		}
	}

	rn.applyCommitted()
}

func (rn *RaftNode) applyCommitted() {
	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entry := rn.log[rn.lastApplied-1]

		// Update range stats before applying so bt.Search reflects pre-apply state.
		rn.updateRangeStats(entry)

		// Apply to the local state machine on every node — leader and follower alike.
		// The executor no longer writes to the BTree before Propose, so the leader
		// must go through the same apply path as followers.
		if rn.applyHook != nil {
			if err := rn.applyHook(entry.Command.Op, entry.Command.Key, entry.Command.Fields); err != nil {
				log.Printf("raft: apply idx=%d key=%d: %v", entry.Index, entry.Command.Key, err)
			}
		} else {
			switch entry.Command.Op {
			case ReplPut:
				if err := rn.bt.Insert(entry.Command.Key, entry.Command.Fields); err != nil {
					log.Printf("raft: bt.Insert idx=%d key=%d: %v", entry.Index, entry.Command.Key, err)
				}
			case ReplDelete:
				if err := rn.bt.Delete(entry.Command.Key); err != nil {
					log.Printf("raft: bt.Delete idx=%d key=%d: %v", entry.Index, entry.Command.Key, err)
				}
			}
		}

		// Signal the Propose caller that this entry is committed and applied.
		// ch is nil for intermediate entries in a multi-entry proposal batch.
		if ch, ok := rn.pendingProps[entry.Index]; ok {
			if ch != nil {
				ch <- nil
			}
			delete(rn.pendingProps, entry.Index)
		}
	}

	if rn.state == RaftStateLeader {
		rn.checkSplit()
	}
}

func (rn *RaftNode) HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if req.Term < rn.currentTerm {
		return &pb.RequestVoteResponse{Term: rn.currentTerm, VoteGranted: false}
	}

	if req.Term > rn.currentTerm {
		rn.stepDown(req.Term)
	}

	if rn.votedFor != 0 && rn.votedFor != req.CandidateId {
		return &pb.RequestVoteResponse{Term: rn.currentTerm, VoteGranted: false}
	}

	myLastIdx, myLastTerm := rn.getLastLogInfo()
	if req.LastLogTerm < myLastTerm {
		return &pb.RequestVoteResponse{Term: rn.currentTerm, VoteGranted: false}
	}
	if req.LastLogTerm == myLastTerm && req.LastLogIndex < myLastIdx {
		return &pb.RequestVoteResponse{Term: rn.currentTerm, VoteGranted: false}
	}

	rn.votedFor = req.CandidateId
	rn.resetElectionTimer()
	return &pb.RequestVoteResponse{Term: rn.currentTerm, VoteGranted: true}
}

func (rn *RaftNode) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if req.Term < rn.currentTerm {
		return &pb.AppendEntriesResponse{Term: rn.currentTerm, Success: false}
	}

	if req.Term > rn.currentTerm {
		rn.stepDown(req.Term)
	}
	rn.state = RaftStateFollower
	rn.resetElectionTimer()
	rn.notifyLeaderChange(req.LeaderId)

	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > uint64(len(rn.log)) {
			return &pb.AppendEntriesResponse{Term: rn.currentTerm, Success: false}
		}
		if rn.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			rn.log = rn.log[:req.PrevLogIndex-1]
			return &pb.AppendEntriesResponse{Term: rn.currentTerm, Success: false}
		}
	}

	for i, pbEntry := range req.Entries {
		idx := req.PrevLogIndex + uint64(i) + 1
		if idx <= uint64(len(rn.log)) {
			if rn.log[idx-1].Term == pbEntry.Term {
				continue
			}
			rn.log = rn.log[:idx-1]
		}
		rn.log = append(rn.log, convertFromProtoEntry(pbEntry))
	}

	if req.LeaderCommit > rn.commitIndex {
		rn.commitIndex = min(req.LeaderCommit, uint64(len(rn.log)))
		rn.applyCommitted()
	}

	return &pb.AppendEntriesResponse{Term: rn.currentTerm, Success: true}
}

func (rn *RaftNode) getLastLogInfo() (index uint64, term uint64) {
	if len(rn.log) == 0 {
		return 0, 0
	}
	last := rn.log[len(rn.log)-1]
	return last.Index, last.Term
}

func (rn *RaftNode) resetElectionTimer() {
	d := ElectionTimeoutMin + rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)
	rn.electionTimer.Reset(time.Duration(d) * time.Millisecond)
}

func (rn *RaftNode) sendHeartbeats() {
	for _, peerID := range rn.peers {
		go rn.sendAppendEntries(peerID)
	}
}

func convertFromProtoEntry(e *pb.RaftLogEntry) RaftLogEntry {
	fields := make([]btree.Field, len(e.Fields))
	for i, fv := range e.Fields {
		fields[i] = convertFromProtoFieldValue(fv)
	}
	return RaftLogEntry{
		Index: e.Index,
		Term:  e.Term,
		Command: RaftCommand{
			Op:     ReplOp(e.Op),
			Key:    e.Key,
			Fields: fields,
		},
	}
}

func convertFromProtoFieldValue(fv *pb.FieldValue) btree.Field {
	var v btree.Value
	switch fv := fv.Value.(type) {
	case *pb.FieldValue_IntValue:
		v = btree.IntValue{V: fv.IntValue}
	case *pb.FieldValue_StringValue:
		v = btree.StringValue{V: fv.StringValue}
	case *pb.FieldValue_ListValue:
		elems := make([]btree.Value, len(fv.ListValue.Elems))
		for i, e := range fv.ListValue.Elems {
			elems[i] = convertFromProtoFieldValue(e).Value
		}
		v = btree.ListValue{ElemType: uint8(fv.ListValue.ElemType), Elems: elems}
	default:
		v = btree.NullValue{}
	}
	return btree.Field{Tag: uint8(fv.Tag), Value: v}
}

func convertToProtoEntries(entries []RaftLogEntry) []*pb.RaftLogEntry {
	out := make([]*pb.RaftLogEntry, len(entries))
	for i, e := range entries {
		fields := make([]*pb.FieldValue, len(e.Command.Fields))
		for j, f := range e.Command.Fields {
			fields[j] = convertToProtoFieldValue(f)
		}
		out[i] = &pb.RaftLogEntry{
			Index:  e.Index,
			Term:   e.Term,
			Op:     int32(e.Command.Op),
			Key:    e.Command.Key,
			Fields: fields,
		}
	}
	return out
}

func convertToProtoFieldValue(f btree.Field) *pb.FieldValue {
	out := &pb.FieldValue{Tag: uint32(f.Tag)}
	switch v := f.Value.(type) {
	case btree.IntValue:
		out.Value = &pb.FieldValue_IntValue{IntValue: v.V}
	case btree.StringValue:
		out.Value = &pb.FieldValue_StringValue{StringValue: v.V}
	case btree.ListValue:
		elems := make([]*pb.FieldValue, len(v.Elems))
		for i, e := range v.Elems {
			elems[i] = convertToProtoFieldValue(btree.Field{Value: e})
		}
		out.Value = &pb.FieldValue_ListValue{ListValue: &pb.FieldList{
			ElemType: uint32(v.ElemType),
			Elems:    elems,
		}}
	}
	return out
}
