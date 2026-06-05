package lock

import (
	"log"
	"sync"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	"github.com/your-username/DistributedDatabaseSystem/internal/raft"
)

type TxnStatus int

const (
	TxnActive TxnStatus = iota
	TxnCommitted
	TxnAborted
)

type Transaction struct {
	Id      uint64
	Status  TxnStatus
	RedoLog []raft.RaftCommand
}

// ApplyFn is called once per committed RaftCommand to apply it to the local state
// machine. In standalone mode it writes directly to the BTree (and reloads schemas
// for schema-key writes). In Raft mode the same function is registered as the
// RaftNode's applyHook and is invoked from applyCommitted after consensus.
type ApplyFn func(op raft.ReplOp, key uint64, fields []btree.Field) error

type TransactionManager struct {
	mu      sync.Mutex
	nextId  uint64
	active  map[uint64]*Transaction
	lm      *LockManager
	bt      *btree.BTree
	rn      *raft.RaftNode
	applyFn ApplyFn
}

// NewTransactionManager creates a TransactionManager.
//
//   - rn: the Raft node (nil for standalone mode).
//   - applyFn: called after each committed command to update the BTree and any
//     derived state (e.g. SchemaCatalog). In Raft mode the same function should
//     be passed to rn.SetApplyHook so the leader and followers use identical logic.
func NewTransactionManager(bt *btree.BTree, rn *raft.RaftNode, applyFn ApplyFn) *TransactionManager {
	return &TransactionManager{
		nextId:  1,
		active:  make(map[uint64]*Transaction),
		lm:      NewLockManager(),
		bt:      bt,
		rn:      rn,
		applyFn: applyFn,
	}
}

func (tm *TransactionManager) Begin() *Transaction {
	tm.mu.Lock()
	t := &Transaction{
		Id:      tm.nextId,
		Status:  TxnActive,
		RedoLog: make([]raft.RaftCommand, 0),
	}
	tm.active[tm.nextId] = t
	tm.nextId++
	tm.mu.Unlock()
	return t
}

func (tm *TransactionManager) Lock(txnId uint64, rowKey uint64, lockType LockType) error {
	return tm.lm.Lock(txnId, rowKey, lockType)
}

func (tm *TransactionManager) AppendRedo(txnId uint64, entry raft.RaftCommand) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if t := tm.active[txnId]; t != nil {
		t.RedoLog = append(t.RedoLog, entry)
	}
}

// Commit finalises the transaction. The BTree is updated exactly once, after all
// validation has passed:
//
//   - Raft mode (rn != nil): commands are proposed to the cluster; applyCommitted
//     calls applyHook on every node (including the leader) after consensus.
//   - Standalone mode: commands are applied directly via applyFn.
func (tm *TransactionManager) Commit(txnId uint64) error {
	tm.mu.Lock()
	t := tm.active[txnId]
	if t == nil {
		tm.mu.Unlock()
		return nil
	}
	commands := make([]raft.RaftCommand, len(t.RedoLog))
	copy(commands, t.RedoLog)
	tm.mu.Unlock()

	if tm.rn != nil && len(commands) > 0 {
		// Raft mode: Propose blocks until the entry is committed AND applied on
		// this node via applyHook (which is the same applyFn set at startup).
		if err := tm.rn.Propose(commands); err != nil {
			tm.discard(txnId)
			return err
		}
	} else if tm.applyFn != nil && len(commands) > 0 {
		// Standalone mode: apply the RedoLog directly via applyFn.
		for _, cmd := range commands {
			if err := tm.applyFn(cmd.Op, cmd.Key, cmd.Fields); err != nil {
				log.Printf("commit: apply key=%d: %v", cmd.Key, err)
			}
		}
	}

	tm.mu.Lock()
	t.Status = TxnCommitted
	t.RedoLog = nil
	delete(tm.active, txnId)
	tm.mu.Unlock()
	tm.lm.UnlockAll(txnId)
	return nil
}

// discard removes a transaction from the active map. Used when Propose fails —
// the executor never wrote to the BTree before consensus, so there is nothing to undo.
func (tm *TransactionManager) discard(txnId uint64) {
	tm.mu.Lock()
	t := tm.active[txnId]
	if t != nil {
		t.Status = TxnAborted
		delete(tm.active, txnId)
	}
	tm.mu.Unlock()
	tm.lm.UnlockAll(txnId)
}

// Rollback discards all buffered operations for the transaction. Because the
// executor defers all BTree writes to after consensus, there is nothing to undo
// in the BTree — the RedoLog is simply dropped.
func (tm *TransactionManager) Rollback(txnId uint64) {
	tm.discard(txnId)
}

// ReleaseAll releases all locks held by txnId without touching tm.active.
// Use this for distributed transactions whose lifecycle is managed externally.
func (tm *TransactionManager) ReleaseAll(txnId uint64) {
	tm.lm.UnlockAll(txnId)
}

// ProposeCommands proposes commands to Raft (or applies them in standalone mode)
// without touching locks or tm.active. Use for metadata-only commands such as
// commit records that are not associated with a specific local transaction.
func (tm *TransactionManager) ProposeCommands(commands []raft.RaftCommand) error {
	if tm.rn != nil && len(commands) > 0 {
		return tm.rn.Propose(commands)
	} else if tm.applyFn != nil {
		for _, cmd := range commands {
			if err := tm.applyFn(cmd.Op, cmd.Key, cmd.Fields); err != nil {
				return err
			}
		}
	}
	return nil
}

// CommitCommands proposes commands to Raft (or applies them directly in standalone
// mode) and then releases all locks held by txnId. Use for distributed transactions
// whose commands are stored externally rather than via Begin/AppendRedo.
func (tm *TransactionManager) CommitCommands(txnId uint64, commands []raft.RaftCommand) error {
	if tm.rn != nil && len(commands) > 0 {
		if err := tm.rn.Propose(commands); err != nil {
			return err // retain locks so the coordinator can retry
		}
	} else if tm.applyFn != nil && len(commands) > 0 {
		for _, cmd := range commands {
			if err := tm.applyFn(cmd.Op, cmd.Key, cmd.Fields); err != nil {
				log.Printf("commit: apply key=%d: %v", cmd.Key, err)
			}
		}
	}
	tm.lm.UnlockAll(txnId)
	return nil
}
