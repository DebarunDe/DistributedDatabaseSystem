package ap

import (
	"context"
	"sort"
	"sync"
	"time"

	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	"github.com/your-username/DistributedDatabaseSystem/internal/raft"
)

// PeerPuller abstracts how entries are fetched from a remote peer.
// The gRPC implementation wraps an APSyncService stub; tests use DirectPeerPuller.
type PeerPuller interface {
	Pull(ctx context.Context, startLSN uint64) ([]APWriteEntry, error)
}

// DirectPeerPuller reads directly from another node's APWriteLog (for tests / single-process).
type DirectPeerPuller struct {
	log *APWriteLog
}

func NewDirectPeerPuller(log *APWriteLog) *DirectPeerPuller {
	return &DirectPeerPuller{log: log}
}

func (p *DirectPeerPuller) Pull(_ context.Context, startLSN uint64) ([]APWriteEntry, error) {
	return p.log.ReadFrom(startLSN)
}

// ConsistencyMetrics surfaces AP-mode trade-off indicators.
type ConsistencyMetrics struct {
	mu                sync.Mutex
	ReplicationLag    map[uint64]time.Duration // per-peer lag estimate
	ConflictsResolved uint64                   // times remote write won LWW
	StaleReads        uint64                   // AP reads served (potentially stale)
}

func newConsistencyMetrics() *ConsistencyMetrics {
	return &ConsistencyMetrics{ReplicationLag: make(map[uint64]time.Duration)}
}

func (m *ConsistencyMetrics) recordConflict() {
	m.mu.Lock()
	m.ConflictsResolved++
	m.mu.Unlock()
}

func (m *ConsistencyMetrics) recordStaleRead() {
	m.mu.Lock()
	m.StaleReads++
	m.mu.Unlock()
}

func (m *ConsistencyMetrics) setLag(peerID uint64, d time.Duration) {
	m.mu.Lock()
	m.ReplicationLag[peerID] = d
	m.mu.Unlock()
}

// Snapshot returns a copy of the metrics.
func (m *ConsistencyMetrics) Snapshot() ConsistencyMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	lag := make(map[uint64]time.Duration, len(m.ReplicationLag))
	for k, v := range m.ReplicationLag {
		lag[k] = v
	}
	return ConsistencyMetrics{
		ReplicationLag:    lag,
		ConflictsResolved: m.ConflictsResolved,
		StaleReads:        m.StaleReads,
	}
}

// APSyncer periodically pulls new AP-log entries from every peer and applies
// them locally using last-writer-wins (LWW) conflict resolution.
type APSyncer struct {
	mu          sync.Mutex
	bt          *btree.BTree
	timestamps  *TimestampStore
	localLog    *APWriteLog
	peers       map[uint64]PeerPuller // nodeID → pull client
	peerCursors map[uint64]uint64     // nodeID → next LSN to pull
	interval    time.Duration
	sc          *sqllayer.SchemaCatalog
	Metrics     *ConsistencyMetrics
}

// NewAPSyncer creates a syncer. peers maps nodeID → PeerPuller implementation.
func NewAPSyncer(
	bt *btree.BTree,
	timestamps *TimestampStore,
	localLog *APWriteLog,
	peers map[uint64]PeerPuller,
	sc *sqllayer.SchemaCatalog,
) *APSyncer {
	cursors := make(map[uint64]uint64, len(peers))
	for id := range peers {
		cursors[id] = 0
	}
	return &APSyncer{
		bt:          bt,
		timestamps:  timestamps,
		localLog:    localLog,
		peers:       peers,
		peerCursors: cursors,
		interval:    500 * time.Millisecond,
		sc:          sc,
		Metrics:     newConsistencyMetrics(),
	}
}

// Run starts the periodic sync loop. It returns when ctx is cancelled.
func (s *APSyncer) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.syncAll(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// SyncNow triggers an immediate sync of all peers (useful in tests).
func (s *APSyncer) SyncNow(ctx context.Context) {
	s.syncAll(ctx)
}

func (s *APSyncer) syncAll(ctx context.Context) {
	// Snapshot peer list under lock so we don't hold the lock during I/O.
	s.mu.Lock()
	peerIDs := make([]uint64, 0, len(s.peers))
	for id := range s.peers {
		peerIDs = append(peerIDs, id)
	}
	s.mu.Unlock()

	for _, id := range peerIDs {
		s.syncFromPeer(ctx, id)
	}
}

func (s *APSyncer) syncFromPeer(ctx context.Context, peerID uint64) {
	s.mu.Lock()
	startLSN := s.peerCursors[peerID]
	puller := s.peers[peerID]
	s.mu.Unlock()

	start := time.Now()
	entries, err := puller.Pull(ctx, startLSN)
	if err != nil {
		return
	}
	lag := time.Since(start)
	s.Metrics.setLag(peerID, lag)

	for _, e := range entries {
		s.applyWithConflictResolution(e)
		s.mu.Lock()
		if e.LSN+1 > s.peerCursors[peerID] {
			s.peerCursors[peerID] = e.LSN + 1
		}
		s.mu.Unlock()
	}
}

// applyWithConflictResolution applies a remote entry using LWW semantics.
// The remote write wins when its timestamp is strictly greater than the local one.
func (s *APSyncer) applyWithConflictResolution(e APWriteEntry) {
	if s.timestamps.CompareAndSet(e.Key, e.Timestamp) {
		// Remote wins — apply to BTree.
		switch e.Op {
		case raft.ReplPut:
			_ = s.bt.Insert(e.Key, e.Fields)
		case raft.ReplDelete:
			_ = s.bt.Delete(e.Key)
		}
		s.Metrics.recordConflict()
	}
	// Local wins — skip.
}

// RecordStaleRead increments the stale-read counter (called by RangeServer on AP scans).
func (s *APSyncer) RecordStaleRead() {
	s.Metrics.recordStaleRead()
}

// ReplicaPicker picks a replica in round-robin order for AP routing.
type ReplicaPicker struct {
	mu      sync.Mutex
	counter uint64
}

// Pick selects a nodeID from replicas using round-robin.
func (p *ReplicaPicker) Pick(replicas map[uint64]string) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := sortedKeys(replicas)
	if len(ids) == 0 {
		return 0
	}
	chosen := ids[p.counter%uint64(len(ids))]
	p.counter++
	return chosen
}

func sortedKeys(m map[uint64]string) []uint64 {
	keys := make([]uint64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}
