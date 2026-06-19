package chaos_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phalanx-db/phalanx/chaos"
	"github.com/phalanx-db/phalanx/chaos/scenarios"
	"github.com/phalanx-db/phalanx/internal/raft"
	"github.com/phalanx-db/phalanx/internal/statemachine"
	walstore "github.com/phalanx-db/phalanx/internal/storage/wal"
	"github.com/phalanx-db/phalanx/internal/transport"
	"github.com/phalanx-db/phalanx/internal/verification"
)

// chaosNode adapts a live Raft node to the chaos.NodeController interface.
type chaosNode struct {
	id      uint64
	peers   []uint64
	cluster *transport.InMemCluster
	rec     *verification.Recorder

	mu      sync.Mutex
	node    raft.Node
	storage *walstore.WALStorage
	sm      *statemachine.KVStateMachine
	tp      *transport.InMemTransport
	running atomic.Bool
	applied atomic.Uint64
	stopC   chan struct{}
	wg      sync.WaitGroup
	walDir  string
}

func newChaosNode(t *testing.T, id uint64, peers []uint64,
	cluster *transport.InMemCluster, rec *verification.Recorder) *chaosNode {
	t.Helper()
	walDir, err := os.MkdirTemp("", fmt.Sprintf("chaos-wal-%d-*", id))
	if err != nil {
		t.Fatalf("tempdir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(walDir) })

	cn := &chaosNode{
		id: id, peers: peers, cluster: cluster, rec: rec, walDir: walDir,
	}
	return cn
}

func (cn *chaosNode) ID() uint64 { return cn.id }

func (cn *chaosNode) Start() error {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	if cn.running.Load() {
		return nil
	}

	storage, _, err := walstore.OpenWALStorage(cn.walDir)
	if err != nil {
		// Fresh node — no WAL yet.
		storage, err = walstore.CreateWALStorage(cn.walDir)
		if err != nil {
			return err
		}
	}
	cn.storage = storage
	cn.sm = statemachine.NewKVStateMachine()

	cfg := &raft.Config{
		ID:                        cn.id,
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

	// Only bootstrap on first start; restart otherwise.
	last, _ := storage.LastIndex()
	if last == 0 {
		peerObjs := make([]raft.Peer, len(cn.peers))
		for i, p := range cn.peers {
			peerObjs[i] = raft.Peer{ID: p}
		}
		cn.node = raft.StartNode(cfg, peerObjs)
	} else {
		cn.node = raft.RestartNode(cfg)
	}

	cn.tp = cn.cluster.NewTransport(cn.id, &chaosHandler{cn: cn})
	cn.stopC = make(chan struct{})
	cn.running.Store(true)

	cn.wg.Add(1)
	go cn.run()
	return nil
}

func (cn *chaosNode) Stop() {
	if !cn.running.CompareAndSwap(true, false) {
		return
	}
	close(cn.stopC)
	cn.wg.Wait()
	cn.node.Stop()
	cn.tp.Stop()
	cn.storage.Close()
}

func (cn *chaosNode) IsRunning() bool { return cn.running.Load() }

func (cn *chaosNode) Status() raft.Status {
	if !cn.running.Load() {
		return raft.Status{}
	}
	return cn.node.Status()
}

func (cn *chaosNode) run() {
	defer cn.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			cn.node.Tick()
		case rd := <-cn.node.Ready():
			cn.storage.Save(rd.HardState, rd.Entries)
			cn.tp.Send(rd.Messages)
			if len(rd.CommittedEntries) > 0 {
				cn.sm.Apply(rd.CommittedEntries)
				last := rd.CommittedEntries[len(rd.CommittedEntries)-1]
				cn.applied.Store(last.Index)
			}
			cn.node.Advance()
		case <-cn.stopC:
			return
		}
	}
}

func (cn *chaosNode) propose(data []byte) error {
	cn.mu.Lock()
	n := cn.node
	running := cn.running.Load()
	cn.mu.Unlock()
	if !running {
		return fmt.Errorf("node %d not running", cn.id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	return n.Propose(ctx, data)
}

type chaosHandler struct{ cn *chaosNode }

func (h *chaosHandler) Process(_ context.Context, msg raft.Message) error {
	h.cn.mu.Lock()
	n := h.cn.node
	running := h.cn.running.Load()
	h.cn.mu.Unlock()
	if !running {
		return fmt.Errorf("node stopped")
	}
	return n.Step(context.Background(), msg)
}
func (h *chaosHandler) IsIDRemoved(_ uint64) bool { return false }

// buildChaosEnv constructs a chaos.Environment backed by a live cluster.
func buildChaosEnv(t *testing.T, n int) (*chaos.Environment, []*chaosNode) {
	t.Helper()
	cluster := transport.NewInMemCluster()
	rec := verification.NewRecorder()

	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}

	nodes := make([]*chaosNode, n)
	controllers := make([]chaos.NodeController, n)
	for i, id := range ids {
		nodes[i] = newChaosNode(t, id, ids, cluster, rec)
		controllers[i] = nodes[i]
	}

	// A workload function that proposes to the current leader.
	var counter atomic.Uint64
	workload := func(ctx context.Context, r *verification.Recorder) chaos.OpResult {
		var leader *chaosNode
		for _, cn := range nodes {
			if cn.IsRunning() && cn.Status().RaftState == raft.RoleLeader {
				leader = cn
				break
			}
		}
		if leader == nil {
			return chaos.OpResult{Success: false, Err: fmt.Errorf("no leader")}
		}
		i := counter.Add(1)
		key := "chaos-key"
		val := fmt.Sprintf("v%d", i)
		callID := r.Begin(leader.id, verification.OpWrite, key, val, "")
		data, _ := statemachine.EncodeCommand(statemachine.Command{
			Type:  statemachine.OpPut,
			Key:   []byte(key),
			Value: []byte(val),
		})
		err := leader.propose(data)
		r.End(callID, "", err == nil)
		return chaos.OpResult{Key: key, Write: val, Success: err == nil, Err: err}
	}

	// The chaos framework needs a NetworkController. Adapt InMemProxy.
	netCtl := &inMemNetAdapter{proxy: cluster.Proxy, allIDs: ids}

	env := &chaos.Environment{
		Nodes:    controllers,
		Net:      netCtl,
		Recorder: rec,
		Workload: workload,
		Logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
	return env, nodes
}

// inMemNetAdapter adapts transport.InMemProxy to chaos.NetworkController.
type inMemNetAdapter struct {
	proxy  *transport.InMemProxy
	allIDs []uint64
}

func (a *inMemNetAdapter) Partition(from, to []uint64) { a.proxy.Partition(from, to) }
func (a *inMemNetAdapter) Heal()                       { a.proxy.Heal() }
func (a *inMemNetAdapter) SetLossRate(p float64)       { a.proxy.SetLossRate(p) }
func (a *inMemNetAdapter) SetDelay(maxMs int)          { a.proxy.SetMaxDelay(time.Duration(maxMs) * time.Millisecond) }
func (a *inMemNetAdapter) Reset()                      { a.proxy.Reset() }

// -------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------

func TestChaos_NetworkPartition_LiveCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	env, nodes := buildChaosEnv(t, 5)
	for _, cn := range nodes {
		if err := cn.Start(); err != nil {
			t.Fatalf("start node %d: %v", cn.ID(), err)
		}
	}
	defer func() {
		for _, cn := range nodes {
			cn.Stop()
		}
	}()

	// Wait for initial leader.
	if !waitForAnyLeader(nodes, 4*time.Second) {
		t.Fatal("no initial leader elected")
	}

	scenario := scenarios.NewNetworkPartition(2, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	result := scenario.Run(ctx, env)
	t.Logf("scenario result: %s", result.String())

	if result.Err != nil {
		t.Errorf("scenario error: %v", result.Err)
	}

	// Verify linearizability over the recorded history.
	history := env.Recorder.History()
	if len(history) > 0 {
		checkResult := verification.Check(history)
		if !checkResult.Linearizable {
			t.Errorf("history not linearizable after partition:\n%s", checkResult)
		} else {
			t.Logf("linearizability verified over %d operations", len(history))
		}
	}

	if !result.ClusterRecovered {
		t.Error("cluster did not recover after partition healed")
	}
}

func TestChaos_LeaderBounce_LiveCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	env, nodes := buildChaosEnv(t, 3)
	for _, cn := range nodes {
		if err := cn.Start(); err != nil {
			t.Fatalf("start node %d: %v", cn.ID(), err)
		}
	}
	defer func() {
		for _, cn := range nodes {
			cn.Stop()
		}
	}()

	if !waitForAnyLeader(nodes, 4*time.Second) {
		t.Fatal("no initial leader")
	}

	scenario := scenarios.NewLeaderBounce(2)
	scenario.CrashDelay = 1 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	result := scenario.Run(ctx, env)
	t.Logf("leader bounce result: %s", result.String())

	// After leader bounces, the cluster must still have a working leader.
	if !waitForAnyLeader(nodes, 4*time.Second) {
		t.Error("cluster lost its leader permanently after leader bounce")
	}

	// Verify linearizability.
	history := env.Recorder.History()
	if len(history) > 0 {
		checkResult := verification.Check(history)
		if !checkResult.Linearizable {
			t.Errorf("history not linearizable after leader bounce:\n%s", checkResult)
		} else {
			t.Logf("linearizability verified over %d operations across %d leader changes",
				len(history), 2)
		}
	}
}

func TestChaos_MessageLoss_LiveCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	env, nodes := buildChaosEnv(t, 3)
	for _, cn := range nodes {
		cn.Start()
	}
	defer func() {
		for _, cn := range nodes {
			cn.Stop()
		}
	}()

	if !waitForAnyLeader(nodes, 4*time.Second) {
		t.Fatal("no initial leader")
	}

	// 20% message loss — the protocol's retry logic must handle this.
	scenario := scenarios.NewMessageLoss(0.2, 3*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	result := scenario.Run(ctx, env)
	t.Logf("message loss result: %s", result.String())

	// Cluster should survive 20% loss (Raft is designed for this).
	if !result.ClusterRecovered {
		t.Error("cluster did not recover after message loss cleared")
	}

	history := env.Recorder.History()
	if len(history) > 0 {
		checkResult := verification.Check(history)
		if !checkResult.Linearizable {
			t.Errorf("history not linearizable under message loss:\n%s", checkResult)
		}
	}
}

func waitForAnyLeader(nodes []*chaosNode, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, cn := range nodes {
			if cn.IsRunning() && cn.Status().RaftState == raft.RoleLeader {
				return true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
