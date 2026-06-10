# DistributedDatabaseSystem

[![CI](https://github.com/DebarunDe/DistributedDatabaseSystem/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/DebarunDe/DistributedDatabaseSystem/actions/workflows/ci.yml)

A distributed relational database built from the ground up in Go. It implements Raft consensus for strong consistency, range-based sharding for horizontal scalability, two-phase commit for cross-shard transactions, and a last-writer-wins eventual consistency mode — all backed by a custom B-tree storage engine with a write-ahead log.

Web applications can interact with the database through a RESTful HTTP API secured by bearer token authentication, or directly over gRPC.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Components](#components)
   - [Storage Engine](#storage-engine)
   - [SQL Layer](#sql-layer)
   - [Raft Consensus](#raft-consensus)
   - [Sharding & Partitioning](#sharding--partitioning)
   - [Distributed Transactions & Locking](#distributed-transactions--locking)
   - [Eventual Consistency (AP Mode)](#eventual-consistency-ap-mode)
   - [HTTP REST API](#http-rest-api)
   - [gRPC Services](#grpc-services)
3. [Building the Project](#building-the-project)
4. [Running the System](#running-the-system)
   - [Standalone (Single Node)](#standalone-single-node)
   - [Clustered (Multi-Node Raft)](#clustered-multi-node-raft)
   - [Interactive Tools](#interactive-tools)
5. [Integrating Your Web Application](#integrating-your-web-application)
   - [Authentication](#authentication)
   - [Table Management](#table-management)
   - [Row Operations](#row-operations)
   - [Consistency Control](#consistency-control)
   - [Complete Integration Example](#complete-integration-example)
6. [SQL Reference](#sql-reference)
7. [Configuration Reference](#configuration-reference)
8. [Running Tests](#running-tests)

---

## Architecture Overview

The system is organised into clearly separated layers. A request from a web application flows top-to-bottom through each layer before writes reach durable storage.

```
┌─────────────────────────────────────────────────────────┐
│                   Web Application                        │
│          REST / HTTP  ──────────  gRPC                   │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│                  HTTP API Layer                           │
│       Bearer-token auth · CORS · Chi router              │
│   /tables  /tables/{name}/rows  /tables/{name}/...       │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│                  SQL Layer                                │
│        Lexer → Parser → AST → Executor                   │
│   Type checking · Schema catalog · Row-level locking     │
└──────────────────────┬──────────────────────────────────┘
                       │
        ┌──────────────┴───────────────┐
        │ Standalone                   │ Cluster
        │                              │
┌───────▼──────┐            ┌──────────▼──────────────────┐
│  Transaction │            │   Gateway  (request router)  │
│  Manager     │            │   Coordinator + Router       │
└───────┬──────┘            └──────────┬──────────────────┘
        │                              │  (gRPC to each range)
        │                    ┌─────────▼───────────┐
        │                    │  Range Servers       │
        │                    │  (one per shard)     │
        │                    └─────────┬───────────┘
        │                              │
┌───────▼──────────────────────────────▼──────────────────┐
│                  Raft Consensus Layer                     │
│   Leader election · Log replication · Quorum commits     │
└──────────────────────┬──────────────────────────────────┘
                       │ apply hook
┌──────────────────────▼──────────────────────────────────┐
│                  B-Tree Storage Engine                    │
│         Buffer Pool (LRU 256 pages) → WAL → Disk        │
└─────────────────────────────────────────────────────────┘
```

Every shard in cluster mode is itself a mini Raft group. Writes are only acknowledged after a majority of replicas have durably recorded the entry. Reads from an AP (eventual consistency) table may be served by any replica without contacting the leader.

---

## Components

### Storage Engine

The storage engine is entirely custom — no third-party database library is used. It is composed of three stacked layers.

#### Page Manager — `internal/pageManager/`

The page manager owns the raw database file. Every unit of data on disk is a fixed 4096-byte page. The manager allocates new pages, maintains a free-list of released pages, and tracks a root page pointer used by the B-tree.

```
File layout
  Page 0:   Meta page  — root page ID, page count, free-list head, checkpoint LSN
  Page 1-N: Data pages — B-tree nodes or overflow pages
```

Each page carries a 24-byte common header followed by type-specific fields:

| Field | Offset | Size | Description |
|---|---|---|---|
| Page type | 0 | 1 B | Leaf, Internal, Meta, or Overflow |
| Page ID | 1 | 4 B | Stable identifier |
| Free space start | 5 | 2 B | End of the slot directory |
| Free space end | 7 | 2 B | Start of the record area |
| Row count | 9 | 2 B | Number of slots |
| Page LSN | 11 | 8 B | Log sequence number at last write |

Leaf and internal nodes additionally store sibling/child page pointers for B-tree traversal. Records are stored in a slot-directory scheme: slots grow from the top of the usable area downward while record bytes grow from the bottom upward, maximising space utilisation.

#### Write-Ahead Log (WAL) — `internal/pageManager/WAL.go`

Before any modified page reaches the data file, its changes are appended to the WAL. On startup the WAL replays all entries whose LSN is beyond the last checkpoint recorded in the meta page, guaranteeing atomicity and durability through crashes.

#### Buffer Pool — `internal/pageManager/bufferpool.go`

The buffer pool sits atop the WAL and provides an LRU cache of up to 256 pages in memory. Cache hits avoid disk I/O entirely. On a miss the pool fetches the page through the WAL layer, and on eviction of a dirty page it flushes through the WAL first to preserve the write-ahead guarantee.

```
Read path:  Buffer Pool cache hit → return page
            Buffer Pool cache miss → WAL.ReadPage() → Page Manager disk read → cache
Write path: Buffer Pool marks page dirty → on eviction: WAL.WritePage() → disk
```

#### B-Tree — `internal/bTree/`

The B-tree is the primary index structure. Every table row is stored as a key-value record in the tree:

```
Key   = (tableID << 32) | primaryKeyValue
Value = []Field  — serialised column values
```

This key encoding places all rows of one table in a contiguous region of the key space, which makes range scans efficient and enables clean per-table sharding.

**Field types:**

| Tag | Go type | SQL type | Wire encoding |
|---|---|---|---|
| IntValue | `int64` | `INT` | 8 bytes big-endian |
| StringValue | `string` | `TEXT` | 4-byte length prefix + UTF-8 bytes |
| ListValue | `[]FieldValue` | (composite) | Recursive encoding |
| NullValue | — | — | Single zero byte |

**Tree operations:**

- `Insert(key, fields)` — inserts or overwrites a row; splits pages when free space falls below 2 KB
- `Search(key)` — O(log n) single-key point lookup
- `RangeScan(startKey, endKey)` — traverses the leaf-node linked list between two keys
- `Delete(key)` — removes the record and frees the slot

When a page fills, it splits at the median record and promotes a separator key to the parent. This cascades upward; if the root splits, a new root page is allocated and the tree grows one level.

---

### SQL Layer

#### Lexer — `internal/SQLLayer/lexer.go`

`Tokenize(query string)` converts a raw SQL string into a slice of typed tokens. Recognised token kinds:

`KEYWORD`, `IDENTIFIER`, `OPERATOR`, `NUMBER`, `STRING`, `COMMA`, `LPAREN`, `RPAREN`, `DATATYPE`, `STAR`

Supported keywords: `SELECT FROM WHERE INSERT INTO VALUES UPDATE SET DELETE CREATE TABLE DROP ALTER CONSISTENCY STRONG EVENTUAL WITH AND OR`

Supported data types: `INT`, `TEXT`, `BOOL`

#### Parser — `internal/SQLLayer/parser.go`

The parser consumes the token stream and produces a typed AST node. Supported statement forms:

```sql
-- Query
SELECT [* | col1, col2, ...] FROM table
  [WHERE expr]
  [WITH CONSISTENCY STRONG | EVENTUAL]

-- Mutation
INSERT INTO table VALUES (val1, val2, ...)
UPDATE table SET column = value [WHERE expr]
DELETE FROM table [WHERE expr]

-- Schema
CREATE TABLE name (pk_col TYPE, col1 TYPE, col2 TYPE)
DROP TABLE name
ALTER TABLE name SET CONSISTENCY STRONG | EVENTUAL
```

WHERE expressions support `=`, `<`, `>`, `<=`, `>=`, `!=` comparisons combined with `AND` and `OR`.

The first column listed in `CREATE TABLE` is always the primary key. It must be of type `INT`.

#### Executor — `internal/SQLLayer/executor.go`

The executor walks the AST, validates it against the schema catalog, acquires the appropriate row-level locks, performs the operation against the B-tree, and buffers the write into the transaction's redo log.

```
executeSelect  → shared lock per matched row → RangeScan + filter + project
executeInsert  → exclusive lock on new key   → BTree.Insert + AppendRedo
executeUpdate  → exclusive lock per matched  → BTree.Insert (overwrite) + AppendRedo
executeDelete  → exclusive lock per matched  → BTree.Delete + AppendRedo
```

The executor itself never writes to disk directly. It stages operations in the `TransactionManager` redo log. The caller is responsible for calling `tm.Commit()` which drives the entry through Raft (cluster mode) or applies it directly (standalone mode).

#### Schema Catalog — `internal/SQLLayer/SchemaCatalog.go`

Table schemas are persisted inside the B-tree itself as rows in a reserved system table (table ID 0). This means schemas survive restarts and are replicated through Raft alongside ordinary data — no separate metadata store is needed.

The in-memory cache maps `tableName → TableSchemaValue`:

```go
type TableSchemaValue struct {
    TableId     uint32
    PrimaryKey  ColumnDef
    Columns     []ColumnDef
    Consistency ConsistencyMode  // ConsistencyCP or ConsistencyAP
}
```

On startup `LoadSchemas()` performs a range scan over table 0 and populates the cache. All subsequent schema lookups are served from memory with `sync.RWMutex` protection.

---

### Raft Consensus

**File:** `internal/raft/raft.go`

The Raft implementation drives leader election and log replication for each shard replica group. It is the mechanism that guarantees linearisable (CP) reads and writes.

#### Node State Machine

A node transitions between three states:

```
Follower ──(election timeout)──► Candidate ──(majority votes)──► Leader
   ▲                                                                 │
   └────────────────────(discover higher term)───────────────────────┘
```

| State | Behaviour |
|---|---|
| **Follower** | Passively receives AppendEntries heartbeats; resets election timer on each |
| **Candidate** | Increments term, votes for self, sends RequestVote to all peers |
| **Leader** | Accepts proposals from application code; broadcasts AppendEntries; drives commit |

**Timing constants:**

| Parameter | Value |
|---|---|
| Election timeout | 150 – 500 ms (randomised) |
| Heartbeat interval | 50 ms |
| RPC timeout | 200 ms |

#### Log Replication

```
Client write → Leader.Propose(commands)
  → Leader appends to log
  → Leader sends AppendEntries RPCs to all followers
  → Followers append and reply success
  → Leader waits for quorum acknowledgement
  → Leader increments commitIndex
  → Leader invokes applyHook(op, key, fields)  ← writes to B-tree
  → Followers apply when they see commitIndex advance
```

`Propose()` blocks the caller until consensus is reached (quorum has durably written the entry). This is what provides the linearisability guarantee for CP tables.

#### Hooks

The Raft node exposes several callback hooks so the surrounding server can integrate cleanly without Raft knowing about higher-level concepts:

| Hook | Signature | Purpose |
|---|---|---|
| `SetApplyHook` | `func(op, key, fields) error` | Called on each committed entry to write the B-tree |
| `SetSplitHook` | `func(rangeID, splitKey) error` | Called when a shard exceeds the split threshold |
| `SetRangeLookup` | `func(key) (rangeID, start, end, bool)` | Allows Raft to tag entries with their range |
| `SetLeaderChangeHook` | `func(leaderID)` | Notifies coordinator of leadership changes |

#### Automatic Range Splitting

After applying each committed entry the Raft node checks per-range statistics. When a range exceeds 1 000 keys **and** 1 MB of data, `splitFn()` is called with the median key. The coordinator divides the descriptor into two, the router cache is refreshed, and subsequent requests are routed to the appropriate child range.

---

### Sharding & Partitioning

**Directory:** `internal/partition/`

The sharding layer maps the full uint64 key space to a set of ranges, routes each incoming statement to the correct range server(s), and coordinates cross-shard operations.

#### Coordinator — `coordinator.go`

The coordinator is the authoritative source of range metadata. It maintains a sorted list of `RangeDescriptor` objects and provides O(log n) key lookups via binary search.

```go
type RangeDescriptor struct {
    RangeID  uint64
    StartKey uint64            // inclusive
    EndKey   uint64            // exclusive
    Replicas map[uint64]string // nodeID → gRPC address
    LeaderID uint64
    Size     uint64            // approximate bytes (for split decisions)
}
```

The initial state is a single range covering `[0, maxUint64)`. As the dataset grows, splits subdivide this range. The coordinator processes `RequestSplit(rangeID, splitKey)` calls from Raft nodes and atomically updates the descriptor list.

#### Router — `router.go`

The router is a client-side cache of range descriptors. It mirrors the coordinator's view and provides fast lookups without locking the coordinator on every request. After a topology change (split or leader change) the router calls `Refresh()` to reload its cache.

#### Gateway — `gateway.go`

The gateway is the central dispatcher inside the server process. Every SQL statement that arrives at a cluster-mode node goes through `gw.Execute(stmt)`.

**Routing logic by statement type:**

| Statement | Consistency | Target |
|---|---|---|
| INSERT | CP (strong) | Leader of the range containing the PK |
| INSERT | AP (eventual) | Any replica of the range containing the PK |
| SELECT (single range) | CP | Leader of that range |
| SELECT (multi-range) | CP | Scatter to leaders, gather results |
| SELECT | AP | Scatter to any replicas, gather results |
| UPDATE / DELETE | CP | Leaders of all touched ranges |
| UPDATE / DELETE | AP | Any replicas of touched ranges |

For multi-range operations the gateway issues parallel gRPC calls (`scatterGather`) and merges the result sets before returning to the caller.

#### Range Server — `rangeServer.go`

Each node runs a `RangeServer` gRPC service that handles `RangeService.Execute` calls from the gateway on other nodes (or locally). The range server dispatches to either the CP path (stages operation in the transaction manager, commits through Raft) or the AP path (writes directly to the local B-tree and appends to the AP write log).

---

### Distributed Transactions & Locking

#### Lock Manager — `internal/Lock/lockManager.go`

The lock manager provides row-level locking with two modes:

| Mode | Holders | Use case |
|---|---|---|
| `LockShared` | Many | SELECT |
| `LockExclusive` | One | INSERT / UPDATE / DELETE |

When a transaction requests a lock that conflicts with an existing holder, it is placed on the row's wait queue. Before blocking, the manager builds a wait-for graph across all active transactions and performs a DFS to detect cycles. A cycle means deadlock; the requesting transaction is immediately rejected with an error and must retry.

#### Transaction Manager — `internal/Lock/transactionManager.go`

The transaction manager wraps the lock manager and the Raft node into a unified interface for the executor.

```
tm.Begin()             → allocate Transaction ID, track in active map
tm.Lock(id, key, mode) → delegate to LockManager
tm.AppendRedo(id, cmd) → buffer RaftCommand into Transaction.RedoLog
tm.Commit(id)          → Raft mode:       Propose(RedoLog) → blocks until quorum
                         Standalone mode:  applyFn(each cmd)
tm.Rollback(id)        → discard RedoLog (no writes reach the tree)
tm.ReleaseAll(id)      → unlock every row held by this transaction
```

#### Two-Phase Commit (2PC) — `internal/partition/distributedTxn.go`

When a single SQL statement touches rows in multiple shards, the gateway escalates to a distributed transaction using two-phase commit.

```
Phase 1 — Prepare
  Gateway sends PrepareRequest to each involved range leader.
  Each leader:
    → Acquires locks on affected rows
    → Stages the mutations in its TransactionManager (does NOT commit yet)
    → Replies prepared=true or an error

Phase 1.5 — Write commit record
  Once all range leaders reply prepared:
    Gateway picks one range as the "anchor" and writes a
    TxnCommitRecord (txnId, status=Committed, []rangeIDs) through that
    range's Raft group.  This makes the commit decision durable before
    any range applies the mutation.

Phase 2 — Commit (or Abort)
  Gateway sends CommitRequest (or AbortRequest) to every range leader.
  Each leader commits or rolls back its staged mutations and releases locks.
```

**Crash recovery:** On restart, the coordinator scans `TxnRecordStore`. Any transaction with a `Committed` record but not all ranges confirmed can be re-driven to completion. Any transaction with no record (crash before phase 1.5) is safely aborted — no data was written.

---

### Eventual Consistency (AP Mode)

AP mode allows reads and writes to be served by any replica without contacting the Raft leader. This trades linearisability for lower latency and higher availability during network partitions.

#### Timestamp Store — `internal/AP/timestamp_store.go`

Each row key maps to a Unix nanosecond write timestamp. On every AP write the current wall clock is recorded. When a replica receives a remote entry during sync it applies last-writer-wins (LWW) semantics: the remote write is accepted only if `remoteTimestamp > localTimestamp`.

#### AP Write Log — `internal/AP/ap_write_log.go`

Every AP write (insert, update, delete) is appended to a per-node binary log before it touches the B-tree. Entries have the format:

```
[4 bytes: entry length][8 bytes: LSN][1 byte: op][8 bytes: key]
[8 bytes: timestamp][variable: serialised fields]
```

The LSN (log sequence number) is a monotonically increasing counter persisted across restarts. Peers pull from this log by providing the LSN of the last entry they have seen.

#### AP Syncer — `internal/AP/ap_syncer.go`

A background goroutine runs `APSyncer.Run()` for the lifetime of the server. On each tick it contacts each peer and calls `Pull(startLSN)` to fetch entries it has not yet seen. For each remote entry:

1. Call `timestamps.CompareAndSet(key, remoteTs)` — returns true if the remote timestamp is newer.
2. If true: apply the remote mutation to the local B-tree.
3. If false: discard (local copy is already newer).
4. Update `Metrics.ReplicationLag` and `Metrics.ConflictsResolved` for observability.

---

### HTTP REST API

**Directory:** `internal/httpapi/`

The HTTP server is the primary integration point for web applications. It translates JSON REST requests into SQL statements, routes them through the gateway (or directly through the executor in standalone mode), and returns JSON responses.

#### Middleware

**Authentication (`authMiddleware`):** Every request except `GET /health` must carry an `Authorization: Bearer <key>` header. The key is validated against the set configured with `-api-keys`. Requests with a missing or invalid key receive `401 Unauthorized`.

**CORS (`corsMiddleware`):** Sets `Access-Control-Allow-Origin: *` and appropriate `Allow-Headers` / `Allow-Methods`. Handles `OPTIONS` preflight automatically so browser-based applications can make cross-origin requests.

**Request logging:** Chi's built-in `middleware.Logger` logs every request with method, path, status code, and duration.

#### Endpoints

**Table management:**

| Method | Path | Description |
|---|---|---|
| `POST` | `/tables` | Create a new table |
| `GET` | `/tables` | List all tables with their consistency mode |
| `GET` | `/tables/{name}` | Describe a single table's schema |
| `DELETE` | `/tables/{name}` | Drop a table and all its rows |
| `PUT` | `/tables/{name}/consistency` | Switch a table between strong and eventual consistency |

**Row operations:**

| Method | Path | Description |
|---|---|---|
| `POST` | `/tables/{name}/rows` | Insert a row |
| `GET` | `/tables/{name}/rows` | Query rows (supports filtering and column projection) |
| `PUT` | `/tables/{name}/rows` | Update rows matching a condition |
| `DELETE` | `/tables/{name}/rows` | Delete rows matching a condition |

**Health:**

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Returns `{"status":"ok"}` — no authentication required |

---

### gRPC Services

Three gRPC services are exposed alongside the HTTP API.

#### SQLService — `proto/db/db.proto`

The primary SQL interface used by the CLI client and REPL.

```protobuf
service SQLService {
    rpc Execute(SQLRequest) returns (SQLResponse);
}
```

A `SQLRequest` carries a raw SQL string. The response carries column names and typed result rows.

#### RaftService — `proto/raft/raft.proto`

Internal service used only for peer-to-peer Raft communication. Not intended for external callers.

```protobuf
service RaftService {
    rpc RequestVote(RequestVoteRequest)     returns (RequestVoteResponse);
    rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
}
```

#### RangeService — `proto/rangeservice/range.proto`

Internal service used by the gateway to forward statements to individual range servers.

```protobuf
service RangeService {
    rpc Execute(RangeRequest)               returns (RangeResponse);
    rpc Prepare(PrepareRequest)             returns (PrepareResponse);
    rpc Commit(CommitRequest)               returns (CommitResponse);
    rpc Abort(AbortRequest)                 returns (AbortResponse);
    rpc WriteCommitRecord(...)              returns (WriteCommitRecordResponse);
}
```

---

## Building the Project

**Prerequisites:**

- Go 1.22 or later
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` (only required if you modify `.proto` files)

**Build all binaries:**

```bash
go build -o server  ./cmd/server
go build -o client  ./cmd/client
go build -o repl    ./cmd/repl
go build -o bench   ./cmd/bench
```

Or build everything at once:

```bash
go build ./...
```

**Run all tests:**

```bash
go test ./...
```

**Run tests with coverage:**

```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

---

## Running the System

### Standalone (Single Node)

Standalone mode runs a single server process with no clustering. Raft is disabled and all writes commit locally. This is the simplest way to get started.

```bash
./server \
  -db     /tmp/mydb.bin \
  -port   5555 \
  -http-port 8080 \
  -api-keys  "my-secret-key"
```

The server is now reachable:
- HTTP REST API on `http://localhost:8080`
- gRPC SQL service on `localhost:5555`

### Clustered (Multi-Node Raft)

A production cluster needs at minimum **three nodes** to tolerate the failure of one. Each node must be assigned a unique ID, told the Raft addresses of its peers, and given its own database file.

The example below runs a three-node cluster on a single machine using different ports. In a real deployment each node runs on a separate host and you substitute the actual IP addresses.

**Node 1:**

```bash
./server \
  -id            1 \
  -db            /tmp/db1.bin \
  -port          5555 \
  -raft-port     5556 \
  -http-port     8080 \
  -advertise-addr localhost:5556 \
  -peers         "2=localhost:5557,3=localhost:5558" \
  -api-keys      "my-secret-key"
```

**Node 2:**

```bash
./server \
  -id            2 \
  -db            /tmp/db2.bin \
  -port          5655 \
  -raft-port     5557 \
  -http-port     8081 \
  -advertise-addr localhost:5557 \
  -peers         "1=localhost:5556,3=localhost:5558" \
  -api-keys      "my-secret-key"
```

**Node 3:**

```bash
./server \
  -id            3 \
  -db            /tmp/db3.bin \
  -port          5755 \
  -raft-port     5558 \
  -http-port     8082 \
  -advertise-addr localhost:5558 \
  -peers         "1=localhost:5556,2=localhost:5557" \
  -api-keys      "my-secret-key"
```

Once all three nodes are running they hold an election and one becomes the Raft leader. You can send writes to any node's HTTP API — the gateway routes them to the current leader automatically.

**Peer format:** `-peers "id=raftAddr,id=raftAddr,..."`
- The IDs must match the `-id` flags of the corresponding nodes.
- Addresses are the Raft RPC addresses (`-raft-port`), not the SQL or HTTP ports.
- Every node must list all *other* nodes (not itself).

**Graceful shutdown:** Send `SIGINT` (Ctrl-C) or `SIGTERM`. The server drains in-flight requests before exiting.

### Interactive Tools

**gRPC client — execute raw SQL against a running server:**

```bash
./client -addr localhost:5555
> CREATE TABLE users (id INT, name TEXT, active BOOL)
> INSERT INTO users VALUES (1, 'Alice', true)
> SELECT * FROM users WHERE id = 1
```

**Standalone REPL — in-process SQL with a local database file (no networking):**

```bash
./repl -db /tmp/local.bin
> CREATE TABLE products (id INT, name TEXT, price INT)
> INSERT INTO products VALUES (42, 'Widget', 999)
> SELECT name, price FROM products WHERE price > 500
```

**Benchmark tool:**

```bash
# Benchmark everything with 10 000 operations
./bench -mode all -ops 10000 -cache-size 256

# Benchmark only writes against the buffer pool
./bench -backend bp -mode write -ops 50000 -cache-size 512

# Benchmark range scans with a fixed random seed
./bench -mode rangescan -ops 5000 -seed 42
```

Flags:

| Flag | Default | Description |
|---|---|---|
| `-backend` | `all` | `pm` (page manager), `bp` (buffer pool), or `all` |
| `-mode` | `all` | `write`, `read`, `rangescan`, or `all` |
| `-ops` | `10000` | Number of operations per benchmark |
| `-cache-size` | `256` | Number of pages in the buffer pool LRU |
| `-seed` | `42` | Random seed for reproducible key sequences |

Output includes P50, P95, P99 latency and operations per second.

---

## Integrating Your Web Application

Web applications connect to the HTTP REST API. Every request must include a bearer token, all request and response bodies are JSON, and all data mutations go through the SQL execution pipeline with full transactional guarantees.

### Authentication

Include your API key in the `Authorization` header of every request (except health checks):

```
Authorization: Bearer <your-api-key>
```

If the key is missing or incorrect the server returns:

```
HTTP 401 Unauthorized
{"error": "unauthorized"}
```

API keys are provided to the server at startup with the `-api-keys` flag. Multiple keys can be provided as a comma-separated list:

```bash
./server -api-keys "app-key-1,app-key-2,read-only-key"
```

---

### Table Management

#### Create a Table

```
POST /tables
Content-Type: application/json
Authorization: Bearer <key>
```

Request body:

```json
{
  "name": "users",
  "columns": [
    { "name": "id",    "type": "INT"  },
    { "name": "name",  "type": "TEXT" },
    { "name": "email", "type": "TEXT" },
    { "name": "age",   "type": "INT"  },
    { "name": "active","type": "BOOL" }
  ]
}
```

- The **first column** is always the primary key. It must be of type `INT`.
- Valid types: `INT`, `TEXT`, `BOOL`.
- Table names and column names are case-sensitive.

Response (success):

```json
{ "ok": true }
```

Example with curl:

```bash
curl -s -X POST http://localhost:8080/tables \
  -H "Authorization: Bearer my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "users",
    "columns": [
      {"name": "id",     "type": "INT"},
      {"name": "name",   "type": "TEXT"},
      {"name": "email",  "type": "TEXT"},
      {"name": "age",    "type": "INT"},
      {"name": "active", "type": "BOOL"}
    ]
  }'
```

#### List All Tables

```
GET /tables
Authorization: Bearer <key>
```

Response:

```json
[
  { "name": "users",    "consistency": "strong"   },
  { "name": "sessions", "consistency": "eventual" }
]
```

#### Describe a Table

```
GET /tables/{name}
Authorization: Bearer <key>
```

Response:

```json
{
  "name": "users",
  "primary_key": { "name": "id", "type": "INT" },
  "columns": [
    { "name": "name",   "type": "TEXT" },
    { "name": "email",  "type": "TEXT" },
    { "name": "age",    "type": "INT"  },
    { "name": "active", "type": "BOOL" }
  ],
  "consistency": "strong"
}
```

#### Drop a Table

```
DELETE /tables/{name}
Authorization: Bearer <key>
```

Response:

```json
{ "ok": true }
```

---

### Row Operations

#### Insert a Row

```
POST /tables/{name}/rows
Content-Type: application/json
Authorization: Bearer <key>
```

Request body — a `values` map with every column:

```json
{
  "values": {
    "id":     1,
    "name":   "Alice",
    "email":  "alice@example.com",
    "age":    30,
    "active": true
  }
}
```

- Values must match the column types declared at table creation.
- The primary key must be unique. Inserting a duplicate primary key overwrites the existing row.

Response (success):

```json
{ "ok": true }
```

Example:

```bash
curl -s -X POST http://localhost:8080/tables/users/rows \
  -H "Authorization: Bearer my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{"values": {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30, "active": true}}'
```

#### Query Rows

```
GET /tables/{name}/rows
Authorization: Bearer <key>
```

Query parameters (all optional):

| Parameter | Description | Example |
|---|---|---|
| `columns` | Comma-separated list of columns to return (default: all) | `columns=id,name,email` |
| `where` | Filter expression (same syntax as SQL WHERE) | `where=age > 25` |
| `consistency` | Override table's default consistency for this request | `consistency=eventual` |

Response:

```json
{
  "columns": ["id", "name", "email", "age", "active"],
  "rows": [
    { "id": 1, "name": "Alice", "email": "alice@example.com", "age": 30, "active": true },
    { "id": 2, "name": "Bob",   "email": "bob@example.com",   "age": 25, "active": false }
  ],
  "count": 2
}
```

Example — fetch only active users over 25, returning id and name:

```bash
curl -s "http://localhost:8080/tables/users/rows?columns=id,name&where=age>25" \
  -H "Authorization: Bearer my-secret-key"
```

Example — read with eventual consistency (any replica, lowest latency):

```bash
curl -s "http://localhost:8080/tables/users/rows?consistency=eventual" \
  -H "Authorization: Bearer my-secret-key"
```

#### Update Rows

```
PUT /tables/{name}/rows
Content-Type: application/json
Authorization: Bearer <key>
```

Request body:

```json
{
  "set":   { "active": false },
  "where": "age < 18"
}
```

- `set` is a single-column update (one key in the map).
- `where` is optional; if omitted the update applies to every row.

Response:

```json
{ "ok": true }
```

Example — deactivate a specific user:

```bash
curl -s -X PUT http://localhost:8080/tables/users/rows \
  -H "Authorization: Bearer my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{"set": {"active": false}, "where": "id = 7"}'
```

#### Delete Rows

```
DELETE /tables/{name}/rows
Content-Type: application/json
Authorization: Bearer <key>
```

Request body:

```json
{
  "where": "active = false"
}
```

- `where` is optional; if omitted **all rows are deleted**.

Response:

```json
{ "ok": true }
```

Example — delete a specific row:

```bash
curl -s -X DELETE http://localhost:8080/tables/users/rows \
  -H "Authorization: Bearer my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{"where": "id = 3"}'
```

---

### Consistency Control

Each table can operate in one of two consistency modes.

| Mode | SQL keyword | HTTP value | Guarantee |
|---|---|---|---|
| **Strong (CP)** | `STRONG` | `"strong"` | Linearisable reads and writes via Raft quorum. All replicas agree before the write is acknowledged. |
| **Eventual (AP)** | `EVENTUAL` | `"eventual"` | Writes are applied locally on any replica and propagated asynchronously. Lower latency, but a read may return a slightly stale value. |

**New tables default to strong consistency.**

#### Change a table's consistency mode

```
PUT /tables/{name}/consistency
Content-Type: application/json
Authorization: Bearer <key>
```

Request body:

```json
{ "mode": "eventual" }
```

Switch back to strong:

```json
{ "mode": "strong" }
```

#### Per-request consistency override

You can override the table's default mode for a single `GET /tables/{name}/rows` request using the `?consistency=` query parameter. This does not permanently change the table's mode.

```bash
# Force a strongly consistent read even on an AP table
curl "http://localhost:8080/tables/events/rows?consistency=strong" \
  -H "Authorization: Bearer my-secret-key"
```

**When to use eventual consistency:**

- Read-heavy tables where a few milliseconds of staleness is acceptable (analytics counters, activity feeds, user preferences).
- Workloads where writes arrive from many nodes simultaneously and you want to avoid the latency cost of Raft consensus on every write.
- Tables that are append-only and never updated in-place (conflict probability is low).

**When to keep strong consistency:**

- Financial or inventory records where two replicas must never disagree about a balance or stock level.
- Any table where a read-modify-write cycle must be atomic across the cluster.
- Authentication / authorisation data.

---

### Complete Integration Example

Below is a self-contained example using plain `curl` that demonstrates the full lifecycle: create a table, insert rows, query them, update one, and clean up.

```bash
BASE="http://localhost:8080"
KEY="my-secret-key"
AUTH='-H "Authorization: Bearer '"$KEY"'"'

# 1. Create the table
curl -s -X POST "$BASE/tables" \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "products",
    "columns": [
      {"name": "id",       "type": "INT"},
      {"name": "name",     "type": "TEXT"},
      {"name": "price",    "type": "INT"},
      {"name": "in_stock", "type": "BOOL"}
    ]
  }'

# 2. Insert rows
for ROW in \
  '{"values": {"id": 1, "name": "Widget",  "price": 999,  "in_stock": true}}' \
  '{"values": {"id": 2, "name": "Gadget",  "price": 1499, "in_stock": true}}' \
  '{"values": {"id": 3, "name": "Doohickey","price": 249,  "in_stock": false}}'; do
  curl -s -X POST "$BASE/tables/products/rows" \
    -H "Authorization: Bearer $KEY" \
    -H "Content-Type: application/json" \
    -d "$ROW"
done

# 3. Query all in-stock items
curl -s "$BASE/tables/products/rows?where=in_stock%3Dtrue" \
  -H "Authorization: Bearer $KEY"

# 4. Update — mark the Doohickey as back in stock
curl -s -X PUT "$BASE/tables/products/rows" \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"set": {"in_stock": true}, "where": "id = 3"}'

# 5. Switch the table to eventual consistency for faster reads
curl -s -X PUT "$BASE/tables/products/consistency" \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"mode": "eventual"}'

# 6. Clean up
curl -s -X DELETE "$BASE/tables/products" \
  -H "Authorization: Bearer $KEY"
```

---

## SQL Reference

The SQL dialect supported by the gRPC client and REPL is the same subset used internally by the HTTP API.

**Data types:**

| Type | Description | JSON equivalent |
|---|---|---|
| `INT` | 64-bit signed integer | `number` |
| `TEXT` | UTF-8 string | `string` |
| `BOOL` | Boolean | `true` / `false` |

**Create a table:**

```sql
CREATE TABLE orders (
    order_id INT,
    customer TEXT,
    total    INT,
    shipped  BOOL
)
```

The first column is the primary key.

**Insert:**

```sql
INSERT INTO orders VALUES (1001, 'Alice', 5999, false)
```

Values must be listed in column-definition order.

**Select:**

```sql
-- All columns
SELECT * FROM orders

-- Specific columns
SELECT order_id, customer, total FROM orders

-- With filter
SELECT * FROM orders WHERE total > 1000

-- Combined conditions
SELECT * FROM orders WHERE shipped = false AND total > 500

-- Consistency override
SELECT * FROM orders WITH CONSISTENCY EVENTUAL
```

**Update:**

```sql
UPDATE orders SET shipped = true WHERE order_id = 1001
```

**Delete:**

```sql
DELETE FROM orders WHERE shipped = true
```

**Schema management:**

```sql
-- Drop a table
DROP TABLE orders

-- Change consistency mode
ALTER TABLE orders SET CONSISTENCY EVENTUAL
ALTER TABLE orders SET CONSISTENCY STRONG
```

**Supported comparison operators:** `=`, `!=`, `<`, `>`, `<=`, `>=`

**Logical operators:** `AND`, `OR`

---

## Configuration Reference

All configuration is passed as command-line flags to the `server` binary.

| Flag | Default | Required | Description |
|---|---|---|---|
| `-db` | — | Yes | Path to the database file. Created if it does not exist. |
| `-port` | `5555` | No | Port for the gRPC SQL service. |
| `-raft-port` | `5556` | No | Port for the internal Raft RPC service (peer-to-peer only). |
| `-http-port` | `8080` | No | Port for the HTTP REST API. |
| `-listen-addr` | `0.0.0.0` | No | IP address to bind all listeners to. |
| `-id` | `0` | No | Raft node ID. Set to a non-zero value to enable clustering. Must be unique across the cluster. |
| `-peers` | `""` | No (cluster) | Comma-separated list of peer Raft addresses in `id=host:port` format. List all nodes *except* this one. |
| `-advertise-addr` | auto-detected | No (cluster) | The Raft address that this node advertises to peers. Set this explicitly when the node is behind NAT or on a host with multiple interfaces. |
| `-api-keys` | `""` | No | Comma-separated list of API keys accepted by the HTTP server. If empty, all requests are accepted without authentication (development only). |

**Standalone mode** (no clustering): omit `-id` or set it to `0`. The `-raft-port`, `-peers`, and `-advertise-addr` flags are ignored.

**Cluster mode**: set `-id` to a unique positive integer, list all other nodes in `-peers`, and set `-advertise-addr` to the address this node should be contacted on by its peers.

---

## Running Tests

```bash
# All unit and integration tests
go test ./...

# A specific package
go test ./internal/raft/...
go test ./internal/SQLLayer/...
go test ./internal/partition/...

# With verbose output
go test -v ./...

# With race detector
go test -race ./...

# With coverage report
go test -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

Integration tests in `internal/integration/` spin up full in-process Raft clusters and verify end-to-end behaviour including leader failover, log replication, and distributed transaction correctness.
