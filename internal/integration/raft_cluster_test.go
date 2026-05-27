//go:build integration

package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/your-username/DistributedDatabaseSystem/proto/db"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// serverBin is the path to the server binary, built once in TestMain.
var serverBin string

func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "raft-int-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "MkdirTemp: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmp)

	serverBin = filepath.Join(tmp, "server")
	out, err := exec.Command(
		"go", "build", "-o", serverBin,
		"github.com/your-username/DistributedDatabaseSystem/cmd/server",
	).CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "build server: %v\n%s\n", err, out)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// ---- infrastructure ----

// freePort binds a listener on :0, records the port, closes the listener, returns the port.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	p := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return p
}

// clusterNode holds the handles for one live server process.
type clusterNode struct {
	cmd     *exec.Cmd
	sqlPort int
	client  pb.SQLServiceClient
	conn    *grpc.ClientConn
}

// raftCluster manages a 3-node cluster of real server processes.
type raftCluster struct {
	t     *testing.T
	nodes [3]*clusterNode // index 0 = node ID 1, etc.
	dirs  [3]string
}

// startNode launches node with 1-based id. It reuses the cluster's pre-allocated
// ports and data dirs.
func (rc *raftCluster) startNode(id int) {
	t := rc.t
	t.Helper()
	i := id - 1

	peersStr := rc.peersFlag(id)
	sqlPort := rc.nodes[i].sqlPort
	raftPort := sqlPort + 100 // raft = sql+100 (consistent per cluster allocation)

	cmd := exec.Command(
		serverBin,
		"-db", filepath.Join(rc.dirs[i], "data.db"),
		"-port", fmt.Sprintf("%d", sqlPort),
		"-raft-port", fmt.Sprintf("%d", raftPort),
		"-id", fmt.Sprintf("%d", id),
		"-peers", peersStr,
	)
	cmd.Stderr = &testLogger{t: t, prefix: fmt.Sprintf("node%d", id)}
	if err := cmd.Start(); err != nil {
		t.Fatalf("node %d start: %v", id, err)
	}
	rc.nodes[i].cmd = cmd
}

// stopNode kills the process for node id (1-based) and waits for it to exit.
func (rc *raftCluster) stopNode(id int) {
	i := id - 1
	n := rc.nodes[i]
	if n.cmd == nil || n.cmd.Process == nil {
		return
	}
	_ = n.cmd.Process.Kill()
	_ = n.cmd.Wait()
	n.cmd = nil
}

// peersFlag builds the -peers flag value for node id, listing all other nodes.
func (rc *raftCluster) peersFlag(id int) string {
	var parts []string
	for j := 0; j < 3; j++ {
		peerID := j + 1
		if peerID == id {
			continue
		}
		raftPort := rc.nodes[j].sqlPort + 100
		parts = append(parts, fmt.Sprintf("%d=127.0.0.1:%d", peerID, raftPort))
	}
	return strings.Join(parts, ",")
}

// newRaftCluster allocates ports, data dirs, and starts 3 nodes. All nodes are
// killed via t.Cleanup.
func newRaftCluster(t *testing.T) *raftCluster {
	t.Helper()
	rc := &raftCluster{t: t}

	for i := 0; i < 3; i++ {
		rc.dirs[i] = t.TempDir()
		sqlPort := freePort(t)
		conn, err := grpc.NewClient(
			fmt.Sprintf("127.0.0.1:%d", sqlPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("grpc.NewClient node%d: %v", i+1, err)
		}
		t.Cleanup(func() { conn.Close() })
		rc.nodes[i] = &clusterNode{
			sqlPort: sqlPort,
			conn:    conn,
			client:  pb.NewSQLServiceClient(conn),
		}
	}

	t.Cleanup(func() {
		for id := 1; id <= 3; id++ {
			rc.stopNode(id)
		}
	})

	for id := 1; id <= 3; id++ {
		rc.startNode(id)
	}

	return rc
}

// sql sends a single SQL statement to node (1-based id).
func (rc *raftCluster) sql(ctx context.Context, id int, query string) (*pb.SQLResponse, error) {
	return rc.nodes[id-1].client.Execute(ctx, &pb.SQLRequest{Sql: query})
}

// findLeader probes all nodes with a CREATE+DROP TABLE; returns 1-based node id
// of the node that succeeds, or -1 if no leader is found within timeout.
func (rc *raftCluster) findLeader(timeout time.Duration) int {
	probe := fmt.Sprintf("__probe_%d__", time.Now().UnixNano())
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id := 1; id <= 3; id++ {
			if rc.nodes[id-1].cmd == nil {
				continue // node is down
			}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err := rc.sql(ctx, id, fmt.Sprintf("CREATE TABLE %s (x INT)", probe))
			cancel()
			if err == nil {
				// Clean up the probe table; ignore error (may not exist on followers).
				ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
				_, _ = rc.sql(ctx2, id, fmt.Sprintf("DROP TABLE %s", probe))
				cancel2()
				return id
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	return -1
}

// mustSQL fails the test if the query errors on node id.
func (rc *raftCluster) mustSQL(ctx context.Context, t *testing.T, id int, query string) *pb.SQLResponse {
	t.Helper()
	resp, err := rc.sql(ctx, id, query)
	if err != nil {
		t.Fatalf("node%d %q: %v", id, query, err)
	}
	return resp
}

// testLogger pipes process output to t.Log.
type testLogger struct {
	t      *testing.T
	prefix string
	buf    []byte
}

func (tl *testLogger) Write(p []byte) (int, error) {
	tl.buf = append(tl.buf, p...)
	for {
		idx := strings.Index(string(tl.buf), "\n")
		if idx < 0 {
			break
		}
		tl.t.Logf("[%s] %s", tl.prefix, strings.TrimRight(string(tl.buf[:idx]), "\r"))
		tl.buf = tl.buf[idx+1:]
	}
	return len(p), nil
}

// ---- S1: Cold-start leader election ----

func TestRaftCluster_S1_LeaderElection(t *testing.T) {
	rc := newRaftCluster(t)

	leader := rc.findLeader(10 * time.Second)
	if leader == -1 {
		t.Fatal("S1: no leader elected within 10s")
	}

	// Non-leaders must reject writes.
	ctx := context.Background()
	for id := 1; id <= 3; id++ {
		if id == leader {
			continue
		}
		_, err := rc.sql(ctx, id, "CREATE TABLE s1check (x INT)")
		if err == nil {
			t.Errorf("S1: node%d (non-leader) accepted DDL, expected rejection", id)
		}
	}
}

// ---- S2: Schema + data replication to followers ----

func TestRaftCluster_S2_SchemaAndDataReplication(t *testing.T) {
	rc := newRaftCluster(t)
	ctx := context.Background()

	leader := rc.findLeader(10 * time.Second)
	if leader == -1 {
		t.Fatal("S2: no initial leader")
	}

	rc.mustSQL(ctx, t, leader, "CREATE TABLE s2users (id INT, name TEXT, age INT)")
	rc.mustSQL(ctx, t, leader, "INSERT INTO s2users VALUES (1, 'alice', 30)")
	rc.mustSQL(ctx, t, leader, "INSERT INTO s2users VALUES (2, 'bob', 25)")
	rc.mustSQL(ctx, t, leader, "INSERT INTO s2users VALUES (3, 'carol', 22)")

	// Kill the current leader and find the new one.
	rc.stopNode(leader)
	time.Sleep(200 * time.Millisecond)

	newLeader := rc.findLeader(10 * time.Second)
	if newLeader == -1 {
		t.Fatal("S2: no new leader after failover")
	}

	// All three rows must be visible on the new leader.
	resp := rc.mustSQL(ctx, t, newLeader, "SELECT * FROM s2users")
	if len(resp.Rows) != 3 {
		t.Errorf("S2: expected 3 rows on new leader, got %d", len(resp.Rows))
	}
}

// ---- S4: Quorum loss — writes blocked when majority gone ----

func TestRaftCluster_S4_QuorumLoss(t *testing.T) {
	rc := newRaftCluster(t)

	leader := rc.findLeader(10 * time.Second)
	if leader == -1 {
		t.Fatal("S4: no leader elected")
	}

	rc.mustSQL(context.Background(), t, leader,
		"CREATE TABLE s4t (id INT, v INT)")

	// Kill two nodes so the remaining one has no quorum.
	killed := 0
	lone := -1
	for id := 1; id <= 3; id++ {
		if killed < 2 && id != leader {
			rc.stopNode(id)
			killed++
		} else if killed == 2 {
			lone = id
		}
	}
	if lone == -1 {
		lone = leader
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	_, err := rc.sql(ctx, lone, "INSERT INTO s4t VALUES (1, 1)")
	if err == nil {
		t.Error("S4: lone node accepted write without quorum")
	}
}

// ---- S5: Cluster heals — rejoin and catch-up ----

func TestRaftCluster_S5_ClusterHeal(t *testing.T) {
	rc := newRaftCluster(t)
	ctx := context.Background()

	leader := rc.findLeader(10 * time.Second)
	if leader == -1 {
		t.Fatal("S5: no initial leader")
	}

	rc.mustSQL(ctx, t, leader, "CREATE TABLE s5t (id INT, name TEXT)")
	rc.mustSQL(ctx, t, leader, "INSERT INTO s5t VALUES (1, 'alice')")
	rc.mustSQL(ctx, t, leader, "INSERT INTO s5t VALUES (2, 'carol')")

	// Partition: kill two followers.
	var killed []int
	for id := 1; id <= 3; id++ {
		if id != leader {
			rc.stopNode(id)
			killed = append(killed, id)
		}
	}

	// Heal: restart the killed nodes.
	for _, id := range killed {
		rc.startNode(id)
	}

	newLeader := rc.findLeader(10 * time.Second)
	if newLeader == -1 {
		t.Fatal("S5: no leader after cluster heal")
	}

	resp := rc.mustSQL(ctx, t, newLeader, "SELECT * FROM s5t WHERE id = 1")
	if len(resp.Rows) != 1 {
		t.Errorf("S5: alice not visible after heal (got %d rows)", len(resp.Rows))
	}
	resp = rc.mustSQL(ctx, t, newLeader, "SELECT * FROM s5t WHERE id = 2")
	if len(resp.Rows) != 1 {
		t.Errorf("S5: carol not visible after heal (got %d rows)", len(resp.Rows))
	}
}

// ---- S7: Rolling leader failover ----

func TestRaftCluster_S7_RollingLeaderFailover(t *testing.T) {
	rc := newRaftCluster(t)
	ctx := context.Background()

	leader := rc.findLeader(10 * time.Second)
	if leader == -1 {
		t.Fatal("S7: no initial leader")
	}

	rc.mustSQL(ctx, t, leader, "CREATE TABLE s7t (id INT, tag TEXT)")

	for round := 1; round <= 3; round++ {
		cur := rc.findLeader(10 * time.Second)
		if cur == -1 {
			t.Fatalf("S7 round%d: no leader at start of round", round)
		}
		sentinel := fmt.Sprintf("round%d", round)
		rc.mustSQL(ctx, t, cur,
			fmt.Sprintf("INSERT INTO s7t VALUES (%d, '%s')", round*100, sentinel))

		rc.stopNode(cur)
		time.Sleep(200 * time.Millisecond)

		newLeader := rc.findLeader(10 * time.Second)
		if newLeader == -1 {
			t.Fatalf("S7 round%d: no new leader after stopping node%d", round, cur)
		}

		resp, err := rc.sql(ctx, newLeader,
			fmt.Sprintf("SELECT * FROM s7t WHERE tag = '%s'", sentinel))
		if err != nil || len(resp.Rows) != 1 {
			t.Errorf("S7 round%d: sentinel %q not visible on new leader node%d (rows=%d, err=%v)",
				round, sentinel, newLeader, func() int {
					if resp == nil {
						return 0
					}
					return len(resp.Rows)
				}(), err)
		}

		// Restart the killed node so quorum is maintained for future rounds.
		rc.startNode(cur)
		time.Sleep(500 * time.Millisecond)
	}
}

// ---- S9: DROP TABLE replication ----

func TestRaftCluster_S9_DropTableReplication(t *testing.T) {
	rc := newRaftCluster(t)
	ctx := context.Background()

	leader := rc.findLeader(10 * time.Second)
	if leader == -1 {
		t.Fatal("S9: no leader elected")
	}

	rc.mustSQL(ctx, t, leader, "CREATE TABLE droptarget (id INT)")
	rc.mustSQL(ctx, t, leader, "INSERT INTO droptarget VALUES (1)")
	rc.mustSQL(ctx, t, leader, "DROP TABLE droptarget")

	// Wait for replication.
	time.Sleep(500 * time.Millisecond)

	for id := 1; id <= 3; id++ {
		if rc.nodes[id-1].cmd == nil {
			continue
		}
		_, err := rc.sql(ctx, id, "SELECT * FROM droptarget")
		if err == nil {
			t.Errorf("S9: node%d still sees droptarget after DROP", id)
		}
	}
}

// ---- S10: UPDATE replication ----

func TestRaftCluster_S10_UpdateReplication(t *testing.T) {
	rc := newRaftCluster(t)
	ctx := context.Background()

	leader := rc.findLeader(10 * time.Second)
	if leader == -1 {
		t.Fatal("S10: no leader elected")
	}

	rc.mustSQL(ctx, t, leader, "CREATE TABLE s10t (id INT, age INT)")
	rc.mustSQL(ctx, t, leader, "INSERT INTO s10t VALUES (1, 25)")
	rc.mustSQL(ctx, t, leader, "UPDATE s10t SET age = 99 WHERE id = 1")

	// Verify on leader.
	resp := rc.mustSQL(ctx, t, leader, "SELECT age FROM s10t WHERE id = 1")
	if len(resp.Rows) != 1 {
		t.Fatalf("S10: expected 1 row on leader, got %d", len(resp.Rows))
	}

	// Kill the leader and confirm the UPDATE survived on the new leader.
	rc.stopNode(leader)
	time.Sleep(200 * time.Millisecond)

	newLeader := rc.findLeader(10 * time.Second)
	if newLeader == -1 {
		t.Fatal("S10: no new leader after UPDATE failover")
	}

	resp = rc.mustSQL(ctx, t, newLeader, "SELECT age FROM s10t WHERE id = 1")
	if len(resp.Rows) != 1 {
		t.Fatalf("S10: expected 1 row on new leader, got %d", len(resp.Rows))
	}
	field := resp.Rows[0].Fields[0]
	iv, ok := field.Value.(*pb.FieldValue_IntValue)
	if !ok || iv.IntValue != 99 {
		t.Errorf("S10: expected age=99 after failover, got %v", field)
	}
}
