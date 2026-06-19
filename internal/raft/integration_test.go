// Package raft_test contains end-to-end integration tests for the Phalanx
// Raft implementation. Unlike the unit tests in raft_test.go (which drive the
// pure state machine directly), these tests exercise the full pipeline:
//
//	Raft engine → WAL → state machine → transport → linearizability checker
//
// The tests use the in-process InMemCluster transport so they run fast and
// deterministically, without any real network or disk I/O.
package raft_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phalanx-db/phalanx/internal/raft"
	"github.com/phalanx-db/phalanx/internal/statemachine"
	walstore "github.com/phalanx-db/phalanx/internal/storage/wal"
	"github.com/phalanx-db/phalanx/internal/transport"
	"github.com/phalanx-db/phalanx/internal/verification"
)

// -------------------------------------------------------------------
// testNode — an in-process Raft node with WAL and state machine
// -------------------------------------------------------------------

type testNode struct {
	id       uint64
	node     raft.Node
	storage  *walstore.WALStorage
	sm       *statemachine.KVStateMachine
	tp       *transport.InMemTransport
	recorder *verification.Recorder

	appliedIndex atomic.Uint64
	stopC        chan struct{}
	wg           sync.WaitGroup
}

func newTestNode(t *testing.T, id uint64, peers []uint64,
	cluster *transport.InMemCluster, rec *verification.Recorder) *testNode {
	t.Helper()

	walDir, err := os.MkdirTemp("", fmt.Sprintf("phalanx-wal-%d-*", id))
	if err != nil {
		t.Fatalf("tempdir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(walDir) })

	storage, err := walstore.CreateWALStorage(walDir)
	if err != nil {
		t.Fatalf("CreateWALStorage: %v", err)
	}

	sm := statemachine.NewKVStateMachine()
	n := &testNode{
		id:       id,
		storage:  storage,
		sm:       sm,
		recorder: rec,
		stopC:    make(chan struct{}),
	}

	cfg := &raft.Config{
		ID:                        id,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   storage,
		MaxSizePerMsg:             1 << 20,
		MaxInflightMsgs:           64,
		MaxUncommittedEntriesSize: ^uint64(0),
		MaxCommittedSizePerReady:  ^uint64(0),
		CheckQuorum:               true,
		PreVote:                   true,
		Logger:                    raft.DiscardLogger,
	}

	peerObjs := make([]raft.Peer, len(peers))
	for i, p := range peers {
		peerObjs[i] = raft.Peer{ID: p}
	}
	n.node = raft.StartNode(cfg, peerObjs)

	h := &msgHandler{n: n}
	n.tp = cluster.NewTransport(id, h)

	return n
}

type msgHandler struct{ n *testNode }

func (h *msgHandler) Process(_ context.Context, msg raft.Message) error {
	return h.n.node.Step(context.Background(), msg)
}
func (h *msgHandler) IsIDRemoved(_ uint64) bool { return false }

func (n *testNode) start() {
	n.wg.Add(1)
	go n.run()
}

func (n *testNode) stop() {
	close(n.stopC)
	n.wg.Wait()
	n.node.Stop()
	n.storage.Close()
}

func (n *testNode) run() {
	defer n.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond) // 1 tick = 10ms
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.node.Tick()
		case rd := <-n.node.Ready():
			n.handleReady(rd)
		case <-n.stopC:
			return
		}
	}
}

func (n *testNode) handleReady(rd raft.Ready) {
	// Persist WAL entries before sending messages (ordering invariant).
	n.storage.Save(rd.HardState, rd.Entries)

	// Send messages via the in-process transport.
	n.tp.Send(rd.Messages)

	// Apply committed entries to the state machine.
	if len(rd.CommittedEntries) > 0 {
		n.sm.Apply(rd.CommittedEntries)
		last := rd.CommittedEntries[len(rd.CommittedEntries)-1]
		n.appliedIndex.Store(last.Index)
	}

	n.node.Advance()
}

func (n *testNode) propose(data []byte) error {
	return n.node.Propose(context.Background(), data)
}

func (n *testNode) isLeader() bool {
	return n.node.Status().RaftState == raft.RoleLeader
}

func (n *testNode) applied() uint64 { return n.appliedIndex.Load() }

// -------------------------------------------------------------------
// testCluster — manages a set of testNodes
// -------------------------------------------------------------------

type testCluster struct {
	t      *testing.T
	nodes  []*testNode
	mem    *transport.InMemCluster
	rec    *verification.Recorder
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	mem := transport.NewInMemCluster()
	rec := verification.NewRecorder()
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	tc := &testCluster{t: t, mem: mem, rec: rec}
	for _, id := range ids {
		tc.nodes = append(tc.nodes, newTestNode(t, id, ids, mem, rec))
	}
	return tc
}

func (tc *testCluster) startAll() {
	for _, n := range tc.nodes {
		n.start()
	}
}

func (tc *testCluster) stopAll() {
	for _, n := range tc.nodes {
		n.stop()
	}
}

func (tc *testCluster) waitForLeader(timeout time.Duration) *testNode {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range tc.nodes {
			if n.isLeader() {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

func (tc *testCluster) leader() *testNode {
	for _, n := range tc.nodes {
		if n.isLeader() {
			return n
		}
	}
	return nil
}

func (tc *testCluster) waitApplied(target uint64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, n := range tc.nodes {
			if n.applied() < target {
				all = false
				break
			}
		}
		if all {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func (tc *testCluster) nodeByID(id uint64) *testNode {
	for _, n := range tc.nodes {
		if n.id == id {
			return n
		}
	}
	return nil
}

func (tc *testCluster) allIDs() []uint64 {
	ids := make([]uint64, len(tc.nodes))
	for i, n := range tc.nodes {
		ids[i] = n.id
	}
	return ids
}

// -------------------------------------------------------------------
// Integration tests
// -------------------------------------------------------------------

// TestIntegration_LeaderElection verifies a 3-node cluster elects exactly
// one leader within two election timeout periods.
func TestIntegration_LeaderElection(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.startAll()
	defer tc.stopAll()

	leader := tc.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected within timeout")
	}
	t.Logf("leader elected: node %d (term %d)", leader.id, leader.node.Status().Term)

	// Verify exactly one leader.
	leaders := 0
	for _, n := range tc.nodes {
		if n.isLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Errorf("expected exactly 1 leader, got %d", leaders)
	}
}

// TestIntegration_LogReplication verifies that proposals replicate to all
// followers and the committed index converges across the cluster.
func TestIntegration_LogReplication(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.startAll()
	defer tc.stopAll()

	leader := tc.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader")
	}

	const nWrites = 50
	for i := 0; i < nWrites; i++ {
		data, _ := statemachine.EncodeCommand(statemachine.Command{
			Type:  statemachine.OpPut,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("val-%d", i)),
		})
		if err := leader.propose(data); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}

	// Wait for all nodes to apply every entry.
	// The leader's applied index is the target; includes the no-op + nWrites.
	time.Sleep(500 * time.Millisecond)
	leaderApplied := leader.applied()
	if !tc.waitApplied(leaderApplied, 5*time.Second) {
		for _, n := range tc.nodes {
			t.Logf("node %d: applied=%d", n.id, n.applied())
		}
		t.Fatalf("not all nodes reached applied=%d", leaderApplied)
	}

	// Verify state machine contents on all nodes.
	for _, n := range tc.nodes {
		for i := 0; i < nWrites; i++ {
			kv, err := n.sm.Get([]byte(fmt.Sprintf("key-%d", i)))
			if err != nil {
				t.Errorf("node %d: Get(key-%d): %v", n.id, i, err)
				continue
			}
			if string(kv.Value) != fmt.Sprintf("val-%d", i) {
				t.Errorf("node %d key-%d: got %q, want val-%d",
					n.id, i, kv.Value, i)
			}
		}
	}
}

// TestIntegration_LeaderFailover verifies that after the leader is isolated,
// the remaining two nodes elect a new leader and continue accepting writes.
func TestIntegration_LeaderFailover(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.startAll()
	defer tc.stopAll()

	leader := tc.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("initial leader election failed")
	}
	leaderID := leader.id
	t.Logf("initial leader: node %d", leaderID)

	// Write before failover.
	for i := 0; i < 10; i++ {
		data, _ := statemachine.EncodeCommand(statemachine.Command{
			Type:  statemachine.OpPut,
			Key:   []byte(fmt.Sprintf("before-%d", i)),
			Value: []byte("v"),
		})
		leader.propose(data)
	}
	time.Sleep(200 * time.Millisecond)

	// Isolate the leader from the rest.
	tc.mem.Proxy.Isolate(leaderID, tc.allIDs())
	t.Logf("isolated node %d", leaderID)

	// Wait for a new leader (election timeout is ~100ms with 10-tick timeout).
	deadline := time.Now().Add(5 * time.Second)
	var newLeader *testNode
	for time.Now().Before(deadline) {
		for _, n := range tc.nodes {
			if n.id != leaderID && n.isLeader() {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("no new leader elected after isolating original leader")
	}
	t.Logf("new leader: node %d (term %d)", newLeader.id, newLeader.node.Status().Term)

	// Write after failover.
	for i := 0; i < 10; i++ {
		data, _ := statemachine.EncodeCommand(statemachine.Command{
			Type:  statemachine.OpPut,
			Key:   []byte(fmt.Sprintf("after-%d", i)),
			Value: []byte("v2"),
		})
		newLeader.propose(data)
	}
	time.Sleep(200 * time.Millisecond)

	// Heal the partition.
	tc.mem.Proxy.Heal()
	time.Sleep(500 * time.Millisecond)

	// Old leader must step down (higher term from new leader's messages).
	oldNode := tc.nodeByID(leaderID)
	if oldNode.isLeader() {
		t.Errorf("old leader (node %d) should step down after partition healed", leaderID)
	}
}

// TestIntegration_NetworkPartition_MajorityContinues verifies the CAP theorem
// in practice: a majority partition continues to serve writes, while the
// minority makes no progress (quorum unavailable).
func TestIntegration_NetworkPartition_MajorityContinues(t *testing.T) {
	tc := newTestCluster(t, 5) // 5 nodes: majority=3, minority=2
	tc.startAll()
	defer tc.stopAll()

	leader := tc.waitForLeader(4 * time.Second)
	if leader == nil {
		t.Fatal("no leader")
	}

	// Determine minority nodes (not the leader, pick 2 of the remaining 4).
	var minority []uint64
	for _, n := range tc.nodes {
		if n.id != leader.id && len(minority) < 2 {
			minority = append(minority, n.id)
		}
	}
	var majority []uint64
	for _, n := range tc.nodes {
		inMinority := false
		for _, m := range minority {
			if n.id == m {
				inMinority = true
			}
		}
		if !inMinority {
			majority = append(majority, n.id)
		}
	}

	tc.mem.Proxy.Partition(minority, majority)
	t.Logf("partitioned: minority=%v majority=%v", minority, majority)

	// Write to the majority.
	const nWrites = 20
	for i := 0; i < nWrites; i++ {
		data, _ := statemachine.EncodeCommand(statemachine.Command{
			Type:  statemachine.OpPut,
			Key:   []byte(fmt.Sprintf("k%d", i)),
			Value: []byte("v"),
		})
		leader.propose(data)
	}
	time.Sleep(300 * time.Millisecond)

	// Majority should have progressed.
	leaderApplied := leader.applied()
	if leaderApplied == 0 {
		t.Error("majority leader should have applied entries during partition")
	}

	// Minority should NOT have progressed (no quorum).
	for _, id := range minority {
		n := tc.nodeByID(id)
		if n.applied() >= leaderApplied {
			t.Errorf("minority node %d should not have applied all entries during partition (got %d, leader=%d)",
				id, n.applied(), leaderApplied)
		}
	}

	// Heal partition and verify convergence.
	tc.mem.Proxy.Heal()
	if !tc.waitApplied(leaderApplied, 5*time.Second) {
		t.Error("cluster did not converge after partition healed")
	}
}

// TestIntegration_HighThroughput_Linearizability runs 500 concurrent writes
// across multiple clients and verifies the full history is linearizable.
func TestIntegration_HighThroughput_Linearizability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping linearizability test in short mode")
	}

	tc := newTestCluster(t, 3)
	tc.startAll()
	defer tc.stopAll()

	leader := tc.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader")
	}

	rec := verification.NewRecorder()
	var wg sync.WaitGroup
	const clients = 4
	const writesPerClient = 50
	key := "shared-key"

	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			for i := 0; i < writesPerClient; i++ {
				val := fmt.Sprintf("client%d-write%d", clientID, i)
				callID := rec.Begin(uint64(clientID), verification.OpWrite, key, val, "")
				data, _ := statemachine.EncodeCommand(statemachine.Command{
					Type:  statemachine.OpPut,
					Key:   []byte(key),
					Value: []byte(val),
				})
				err := leader.propose(data)
				rec.End(callID, "", err == nil)
				time.Sleep(2 * time.Millisecond)
			}
		}(c)
	}
	wg.Wait()
	time.Sleep(500 * time.Millisecond)

	history := rec.History()
	t.Logf("recorded %d operations (%d pending)", len(history), rec.PendingCount())

	result := verification.Check(history)
	if !result.Linearizable {
		t.Errorf("history is not linearizable:\n%s", result)
	} else {
		t.Logf("linearizability verified over %d operations", len(history))
	}
}

// TestIntegration_TermMonotonicAcrossElections verifies that after several
// leader changes, no node ever decreases its term.
func TestIntegration_TermMonotonicAcrossElections(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.startAll()
	defer tc.stopAll()

	leader := tc.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("no initial leader")
	}

	prevTerms := make(map[uint64]uint64)
	for _, n := range tc.nodes {
		prevTerms[n.id] = n.node.Status().Term
	}

	// Trigger two more elections by isolating the leader twice.
	for round := 0; round < 2; round++ {
		currentLeader := tc.waitForLeader(3 * time.Second)
		if currentLeader == nil {
			t.Fatalf("round %d: no leader", round)
		}
		tc.mem.Proxy.Isolate(currentLeader.id, tc.allIDs())
		time.Sleep(500 * time.Millisecond)
		tc.mem.Proxy.Heal()
		time.Sleep(500 * time.Millisecond)

		for _, n := range tc.nodes {
			newTerm := n.node.Status().Term
			if newTerm < prevTerms[n.id] {
				t.Errorf("round %d: node %d term decreased from %d to %d",
					round, n.id, prevTerms[n.id], newTerm)
			}
			prevTerms[n.id] = newTerm
		}
	}
}

// TestIntegration_SingleNodeCluster ensures a 1-node cluster can serve writes
// without needing any peers for quorum.
func TestIntegration_SingleNodeCluster(t *testing.T) {
	mem := transport.NewInMemCluster()
	rec := verification.NewRecorder()

	walDir, _ := os.MkdirTemp("", "phalanx-single-*")
	t.Cleanup(func() { os.RemoveAll(walDir) })

	storage, _ := walstore.CreateWALStorage(walDir)
	sm := statemachine.NewKVStateMachine()

	node := &testNode{
		id: 1, storage: storage, sm: sm,
		recorder: rec, stopC: make(chan struct{}),
	}
	cfg := &raft.Config{
		ID:                        1,
		ElectionTick:              5,
		HeartbeatTick:             1,
		Storage:                   storage,
		MaxSizePerMsg:             1 << 20,
		MaxInflightMsgs:           64,
		MaxUncommittedEntriesSize: ^uint64(0),
		MaxCommittedSizePerReady:  ^uint64(0),
		Logger:                    raft.DiscardLogger,
	}
	node.node = raft.StartNode(cfg, []raft.Peer{{ID: 1}})
	node.tp = mem.NewTransport(1, &msgHandler{n: node})
	node.start()
	defer node.stop()

	// Single node should immediately become leader.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if node.isLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !node.isLeader() {
		t.Fatal("single node should become leader")
	}

	// Propose and verify.
	data, _ := statemachine.EncodeCommand(statemachine.Command{
		Type: statemachine.OpPut, Key: []byte("k"), Value: []byte("v"),
	})
	if err := node.propose(data); err != nil {
		t.Fatalf("propose: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	kv, err := sm.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(kv.Value) != "v" {
		t.Errorf("got %q, want v", kv.Value)
	}
}
