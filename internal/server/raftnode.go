// Package server provides RaftNode: the single component that understands
// the required ordering between Phalanx subsystems.
//
// Ordering invariant (MUST NOT be violated):
//   1. Persist HardState+Entries to WAL        ← crash safety
//   2. Install snapshot (if rd.Snapshot set)   ← before applying entries
//   3. Send messages to peers                  ← after WAL write
//   4. Apply committed entries to state machine
//   5. Satisfy pending ReadIndex requests
//   6. Trigger snapshot if threshold exceeded
//   7. node.Advance()                          ← must be last
//
// Reference: etcd/raft README "Usage" section; CockroachDB's multiRaft.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/phalanx-db/phalanx/internal/raft"
	"github.com/phalanx-db/phalanx/internal/statemachine"
	"github.com/phalanx-db/phalanx/internal/storage/snapshot"
	"github.com/phalanx-db/phalanx/internal/storage/wal"
	"github.com/phalanx-db/phalanx/internal/transport"
)

// Config holds all RaftNode parameters.
type Config struct {
	NodeID    uint64
	PeerIDs   []uint64
	PeerAddrs map[uint64]string // id → "host:port"

	DataDir    string // parent dir; wal/ and snaps/ are created underneath
	ListenAddr string // Raft transport addr, e.g. ":2380"
	SnapAddr   string // snapshot receiver addr, e.g. ":2381"

	HeartbeatMs   int    // tick interval, default 100
	ElectionMs    int    // election timeout, default 1000
	SnapThreshold uint64 // entries between snapshots, default 10_000

	Logger *slog.Logger
}

func (c *Config) withDefaults() {
	if c.HeartbeatMs == 0 {
		c.HeartbeatMs = 100
	}
	if c.ElectionMs == 0 {
		c.ElectionMs = 1000
	}
	if c.SnapThreshold == 0 {
		c.SnapThreshold = 10_000
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type proposal struct {
	ctx    context.Context
	data   []byte
	result chan error
}

type readIndexReq struct {
	ctx    context.Context
	rctx   []byte
	result chan readIndexResult
}

type readIndexResult struct {
	index uint64
	err   error
}

// RaftNode is the complete Phalanx server. It owns the Raft engine, WAL,
// state machine, snapshot manager, and transport — and enforces the ordering
// contract between them.
type RaftNode struct {
	cfg  *Config
	id   uint64

	node    raft.Node
	storage *wal.WALStorage
	sm      *statemachine.KVStateMachine
	snapMgr *snapshot.Manager
	tp      *transport.TCPTransport

	propC  chan *proposal
	rdIdxC chan *readIndexReq

	riMu      sync.Mutex
	riPending map[string]*readIndexReq

	appliedIndex atomic.Uint64
	confState    raft.ConfState

	logger *slog.Logger
	stopC  chan struct{}
	wg     sync.WaitGroup
}

// NewRaftNode constructs a RaftNode. It opens or creates the WAL, loads
// the latest snapshot, and configures the Raft engine for the appropriate
// start mode (fresh cluster vs restart).
func NewRaftNode(cfg *Config) (*RaftNode, error) {
	cfg.withDefaults()
	lg := cfg.Logger.With("node_id", cfg.NodeID)

	walDir := fmt.Sprintf("%s/wal", cfg.DataDir)
	snapDir := fmt.Sprintf("%s/snaps", cfg.DataDir)

	snapMgr := snapshot.NewManager(snapDir, cfg.SnapThreshold, lg)

	var storage *wal.WALStorage
	var recoveredHS raft.HardState
	var err error

	if _, statErr := os.Stat(walDir); os.IsNotExist(statErr) {
		lg.Info("creating new WAL", "dir", walDir)
		storage, err = wal.CreateWALStorage(walDir)
	} else {
		lg.Info("opening existing WAL", "dir", walDir)
		storage, recoveredHS, err = wal.OpenWALStorage(walDir)
	}
	if err != nil {
		return nil, fmt.Errorf("raftnode: WAL: %w", err)
	}

	sm := statemachine.NewKVStateMachine()

	// Restore the state machine from the latest snapshot.
	var snap raft.Snapshot
	if loadedSnap, loadErr := snapMgr.Load(); loadErr == nil {
		snap = loadedSnap
		if restoreErr := sm.Restore(snap.Data); restoreErr != nil {
			return nil, fmt.Errorf("raftnode: restore SM: %w", restoreErr)
		}
		lg.Info("snapshot loaded",
			"index", snap.Metadata.Index,
			"term", snap.Metadata.Term,
			"data_bytes", len(snap.Data))
	}

	heartbeatTicks := 1
	electionTicks := cfg.ElectionMs / cfg.HeartbeatMs
	if electionTicks < 5 {
		electionTicks = 5
	}

	raftCfg := &raft.Config{
		ID:                        cfg.NodeID,
		ElectionTick:              electionTicks,
		HeartbeatTick:             heartbeatTicks,
		Storage:                   storage,
		MaxSizePerMsg:             1 << 20,  // 1 MiB per AppendEntries
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30, // 1 GiB uncommitted budget
		MaxCommittedSizePerReady:  1 << 20,
		CheckQuorum:               true,
		PreVote:                   true,
		Logger:                    raft.DiscardLogger,
	}

	var node raft.Node
	isFresh := snap.IsEmpty() && recoveredHS.IsEmpty()
	if isFresh {
		peers := make([]raft.Peer, 0, len(cfg.PeerIDs))
		for _, id := range cfg.PeerIDs {
			peers = append(peers, raft.Peer{ID: id})
		}
		node = raft.StartNode(raftCfg, peers)
	} else {
		node = raft.RestartNode(raftCfg)
	}

	// The transport handler is a thin shim; we give it a forward reference
	// to the RaftNode via a pointer that is set after construction.
	h := &raftMsgHandler{}
	tp := transport.NewTCPTransport(cfg.NodeID, h)

	for id, addr := range cfg.PeerAddrs {
		if id != cfg.NodeID {
			tp.AddPeer(id, addr)
		}
	}

	rn := &RaftNode{
		cfg:       cfg,
		id:        cfg.NodeID,
		node:      node,
		storage:   storage,
		sm:        sm,
		snapMgr:   snapMgr,
		tp:        tp,
		propC:     make(chan *proposal, 4096),
		rdIdxC:    make(chan *readIndexReq, 1024),
		riPending: make(map[string]*readIndexReq),
		logger:    lg,
		stopC:     make(chan struct{}),
	}
	h.rn = rn

	if !snap.IsEmpty() {
		rn.confState = snap.Metadata.ConfState
		rn.appliedIndex.Store(snap.Metadata.Index)
	}

	return rn, nil
}

// Start opens the network listeners and begins the Raft protocol.
func (rn *RaftNode) Start() error {
	if err := rn.tp.Start(rn.cfg.ListenAddr); err != nil {
		return fmt.Errorf("raftnode: transport: %w", err)
	}
	if rn.cfg.SnapAddr != "" {
		if err := rn.snapMgr.StartReceiver(rn.cfg.SnapAddr); err != nil {
			return fmt.Errorf("raftnode: snap receiver: %w", err)
		}
	}
	rn.wg.Add(1)
	go rn.run()
	return nil
}

// Stop shuts down the RaftNode, waits for the run loop to exit, and
// closes all resources.
func (rn *RaftNode) Stop() {
	close(rn.stopC)
	rn.wg.Wait()
	rn.node.Stop()
	rn.tp.Stop()
	if err := rn.storage.Close(); err != nil {
		rn.logger.Warn("WAL close error", "err", err)
	}
}

// Propose submits a KV command for Raft replication. It blocks until the
// Raft engine accepts the proposal (not until it is committed).
func (rn *RaftNode) Propose(ctx context.Context, cmd statemachine.Command) error {
	data, err := statemachine.EncodeCommand(cmd)
	if err != nil {
		return fmt.Errorf("raftnode: encode cmd: %w", err)
	}
	p := &proposal{ctx: ctx, data: data, result: make(chan error, 1)}
	select {
	case rn.propC <- p:
	case <-ctx.Done():
		return ctx.Err()
	case <-rn.stopC:
		return errors.New("raftnode: stopped")
	}
	select {
	case err := <-p.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-rn.stopC:
		return errors.New("raftnode: stopped")
	}
}

// ReadIndex issues a linearizable read barrier. It blocks until the Raft
// engine confirms the current commit index and the state machine has caught
// up, then returns that commit index.
func (rn *RaftNode) ReadIndex(ctx context.Context, rctx []byte) (uint64, error) {
	req := &readIndexReq{
		ctx:    ctx,
		rctx:   rctx,
		result: make(chan readIndexResult, 1),
	}
	select {
	case rn.rdIdxC <- req:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-rn.stopC:
		return 0, errors.New("raftnode: stopped")
	}
	select {
	case res := <-req.result:
		return res.index, res.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-rn.stopC:
		return 0, errors.New("raftnode: stopped")
	}
}

// AppliedIndex returns the last log index applied to the state machine.
func (rn *RaftNode) AppliedIndex() uint64 { return rn.appliedIndex.Load() }

// SM returns the underlying state machine (for direct, possibly-stale reads).
func (rn *RaftNode) SM() *statemachine.KVStateMachine { return rn.sm }

// Status returns the current Raft status (term, leader, etc.).
func (rn *RaftNode) Status() raft.Status { return rn.node.Status() }

// -------------------------------------------------------------------
// Internal run loop
// -------------------------------------------------------------------

func (rn *RaftNode) run() {
	defer rn.wg.Done()

	tickInterval := time.Duration(rn.cfg.HeartbeatMs) * time.Millisecond
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rn.node.Tick()

		case rd := <-rn.node.Ready():
			rn.handleReady(rd)

		case p := <-rn.propC:
			err := rn.node.Propose(p.ctx, p.data)
			p.result <- err

		case ri := <-rn.rdIdxC:
			rn.riMu.Lock()
			rn.riPending[string(ri.rctx)] = ri
			rn.riMu.Unlock()
			if err := rn.node.ReadIndex(ri.ctx, ri.rctx); err != nil {
				rn.riMu.Lock()
				delete(rn.riPending, string(ri.rctx))
				rn.riMu.Unlock()
				ri.result <- readIndexResult{err: err}
			}

		case incomingSnap := <-rn.snapMgr.IncomingSnapshots():
			// Snapshot arrived via chunk protocol — deliver as MsgSnap.
			if err := rn.node.Step(context.Background(), raft.Message{
				Type:     raft.MsgSnap,
				From:     raft.None,
				Snapshot: &incomingSnap,
			}); err != nil {
				rn.logger.Warn("incoming snapshot delivery failed", "err", err)
			}

		case <-rn.stopC:
			return
		}
	}
}

func (rn *RaftNode) handleReady(rd raft.Ready) {
	// ── Step 1: WAL write (MUST precede sending messages) ────────────
	if err := rn.storage.Save(rd.HardState, rd.Entries); err != nil {
		rn.logger.Error("WAL write failed — fatal", "err", err)
		os.Exit(1)
	}

	// ── Step 2: Snapshot install ───────────────────────────────────────
	if !rd.Snapshot.IsEmpty() {
		rn.doInstallSnapshot(rd.Snapshot)
	}

	// ── Step 3: Send messages (after WAL is durable) ──────────────────
	rn.tp.Send(rd.Messages)

	// ── Step 4: Apply committed entries ───────────────────────────────
	if len(rd.CommittedEntries) > 0 {
		rn.applyEntries(rd.CommittedEntries)
	}

	// ── Step 5: Satisfy pending ReadIndex ─────────────────────────────
	for _, rs := range rd.ReadStates {
		rn.satisfyReadIndex(rs)
	}

	// ── Step 6: Maybe snapshot ────────────────────────────────────────
	if rn.snapMgr.ShouldSnapshot(rn.appliedIndex.Load()) {
		go rn.doTakeSnapshot()
	}

	// ── Step 7: Advance ───────────────────────────────────────────────
	rn.node.Advance()
}

func (rn *RaftNode) applyEntries(entries []raft.Entry) {
	rn.sm.Apply(entries)
	last := entries[len(entries)-1]
	rn.appliedIndex.Store(last.Index)

	// Update conf state when a conf change is applied.
	for _, e := range entries {
		if e.Type == raft.EntryConfChange {
			var cc raft.ConfChange
			if err := decodeConfChange(e.Data, &cc); err == nil {
				cs := rn.node.ApplyConfChange(cc)
				if cs != nil {
					rn.confState = *cs
				}
			}
		}
	}
}

func (rn *RaftNode) doInstallSnapshot(snap raft.Snapshot) {
	// Persist to WAL first so a crash during SM restore doesn't lose the snapshot.
	if err := rn.storage.SaveSnapshot(snap); err != nil {
		rn.logger.Error("snapshot WAL record failed — fatal", "err", err)
		os.Exit(1)
	}
	if err := rn.snapMgr.Save(snap); err != nil {
		rn.logger.Error("snapshot disk persist failed — fatal", "err", err)
		os.Exit(1)
	}
	if err := rn.sm.Restore(snap.Data); err != nil {
		rn.logger.Error("SM restore failed — fatal", "err", err)
		os.Exit(1)
	}
	rn.confState = snap.Metadata.ConfState
	rn.appliedIndex.Store(snap.Metadata.Index)
	rn.logger.Info("snapshot installed",
		"index", snap.Metadata.Index,
		"term", snap.Metadata.Term,
		"data_bytes", len(snap.Data))
}

func (rn *RaftNode) doTakeSnapshot() {
	applied := rn.appliedIndex.Load()
	data, err := rn.sm.Snapshot()
	if err != nil {
		rn.logger.Error("SM.Snapshot() failed", "err", err)
		return
	}
	snap := raft.Snapshot{
		Data: data,
		Metadata: raft.SnapshotMetadata{
			Index:     applied,
			ConfState: rn.confState,
		},
	}
	if err := rn.snapMgr.Save(snap); err != nil {
		rn.logger.Error("doTakeSnapshot: Save failed", "err", err)
		return
	}
	if err := rn.storage.CompactBefore(applied); err != nil {
		rn.logger.Warn("WAL compaction failed",
			"err", err, "compact_index", applied)
	}
	rn.logger.Info("snapshot taken",
		"applied", applied,
		"data_bytes", len(data))
}

func (rn *RaftNode) satisfyReadIndex(rs raft.ReadState) {
	rn.riMu.Lock()
	req, ok := rn.riPending[string(rs.RequestCtx)]
	if ok {
		delete(rn.riPending, string(rs.RequestCtx))
	}
	rn.riMu.Unlock()
	if !ok {
		return
	}
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if rn.appliedIndex.Load() >= rs.Index {
				req.result <- readIndexResult{index: rs.Index}
				return
			}
			time.Sleep(500 * time.Microsecond)
		}
		req.result <- readIndexResult{err: fmt.Errorf("readindex timeout: applied=%d need=%d",
			rn.appliedIndex.Load(), rs.Index)}
	}()
}

// -------------------------------------------------------------------
// Transport message handler
// -------------------------------------------------------------------

type raftMsgHandler struct {
	rn *RaftNode
}

func (h *raftMsgHandler) Process(ctx context.Context, msg raft.Message) error {
	return h.rn.node.Step(ctx, msg)
}

func (h *raftMsgHandler) IsIDRemoved(_ uint64) bool { return false }

// -------------------------------------------------------------------
// Conf change decode helper
// -------------------------------------------------------------------

func decodeConfChange(data []byte, cc *raft.ConfChange) error {
	if len(data) < 9 {
		return fmt.Errorf("conf change too short: %d bytes", len(data))
	}
	cc.Type = raft.ConfChangeType(data[0])
	cc.NodeID = 0
	for i := 0; i < 8; i++ {
		cc.NodeID |= uint64(data[1+i]) << (8 * i)
	}
	if len(data) > 9 {
		cc.Context = data[9:]
	}
	return nil
}
