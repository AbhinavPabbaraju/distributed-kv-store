package server

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/phalanx-db/phalanx/internal/raft"
	"github.com/phalanx-db/phalanx/internal/statemachine"
)

func testConfig(t *testing.T, id uint64, dataDir string) *Config {
	t.Helper()
	return &Config{
		NodeID:        id,
		PeerIDs:       []uint64{id},
		PeerAddrs:     map[uint64]string{},
		DataDir:       dataDir,
		ListenAddr:    "127.0.0.1:0",
		SnapAddr:      "",
		HeartbeatMs:   50,
		ElectionMs:    250,
		SnapThreshold: 100,
		Logger:        slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}
}

func TestRaftNode_SingleNode_Lifecycle(t *testing.T) {
	dataDir, err := os.MkdirTemp("", "phalanx-server-*")
	if err != nil {
		t.Fatalf("tempdir: %v", err)
	}
	defer os.RemoveAll(dataDir)

	cfg := testConfig(t, 1, dataDir)
	rn, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}
	if err := rn.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer rn.Stop()

	// Single node should become leader quickly.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if rn.Status().RaftState == raft.RoleLeader {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if rn.Status().RaftState != raft.RoleLeader {
		t.Fatal("single node did not become leader")
	}
}

func TestRaftNode_SingleNode_Propose(t *testing.T) {
	dataDir, err := os.MkdirTemp("", "phalanx-server-*")
	if err != nil {
		t.Fatalf("tempdir: %v", err)
	}
	defer os.RemoveAll(dataDir)

	cfg := testConfig(t, 1, dataDir)
	rn, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}
	if err := rn.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer rn.Stop()

	// Wait for leadership.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if rn.Status().RaftState == raft.RoleLeader {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Propose writes.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < 10; i++ {
		err := rn.Propose(ctx, statemachine.Command{
			Type:  statemachine.OpPut,
			Key:   []byte(fmt.Sprintf("k%d", i)),
			Value: []byte(fmt.Sprintf("v%d", i)),
		})
		if err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
	}

	// Allow time for entries to be applied to the state machine.
	time.Sleep(300 * time.Millisecond)

	// Verify state machine contents via direct (stale) read.
	for i := 0; i < 10; i++ {
		kv, err := rn.SM().Get([]byte(fmt.Sprintf("k%d", i)))
		if err != nil {
			t.Errorf("Get k%d: %v", i, err)
			continue
		}
		if string(kv.Value) != fmt.Sprintf("v%d", i) {
			t.Errorf("k%d: got %q, want v%d", i, kv.Value, i)
		}
	}

	if rn.AppliedIndex() == 0 {
		t.Error("applied index should be > 0 after proposals")
	}
}

func TestRaftNode_CrashRecovery(t *testing.T) {
	dataDir, err := os.MkdirTemp("", "phalanx-recovery-*")
	if err != nil {
		t.Fatalf("tempdir: %v", err)
	}
	defer os.RemoveAll(dataDir)

	// Phase 1: start, write, stop (simulating a crash via clean stop).
	{
		cfg := testConfig(t, 1, dataDir)
		rn, err := NewRaftNode(cfg)
		if err != nil {
			t.Fatalf("NewRaftNode (phase 1): %v", err)
		}
		if err := rn.Start(); err != nil {
			t.Fatalf("Start (phase 1): %v", err)
		}

		// Wait for leadership.
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if rn.Status().RaftState == raft.RoleLeader {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		for i := 0; i < 20; i++ {
			rn.Propose(ctx, statemachine.Command{
				Type:  statemachine.OpPut,
				Key:   []byte(fmt.Sprintf("persistent-%d", i)),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			})
		}
		cancel()
		time.Sleep(300 * time.Millisecond)
		rn.Stop()
	}

	// Phase 2: restart from the same data dir; verify data survived.
	{
		cfg := testConfig(t, 1, dataDir)
		rn, err := NewRaftNode(cfg)
		if err != nil {
			t.Fatalf("NewRaftNode (phase 2 / recovery): %v", err)
		}
		if err := rn.Start(); err != nil {
			t.Fatalf("Start (phase 2): %v", err)
		}
		defer rn.Stop()

		// Wait for leadership (recovered node must re-elect itself).
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if rn.Status().RaftState == raft.RoleLeader {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}

		// Allow the recovered log to be re-applied to the state machine.
		time.Sleep(500 * time.Millisecond)

		// Verify all 20 keys survived the restart.
		recovered := 0
		for i := 0; i < 20; i++ {
			kv, err := rn.SM().Get([]byte(fmt.Sprintf("persistent-%d", i)))
			if err == nil && string(kv.Value) == fmt.Sprintf("value-%d", i) {
				recovered++
			}
		}
		if recovered != 20 {
			t.Errorf("crash recovery: recovered %d/20 keys from WAL", recovered)
		} else {
			t.Logf("crash recovery: all 20 keys recovered from WAL")
		}
	}
}

func TestRaftNode_DecodeConfChange(t *testing.T) {
	// Verify the conf-change decoder is the inverse of the encoder used
	// in node.go's encodeConfChange.
	original := raft.ConfChange{
		Type:    raft.ConfChangeAddNode,
		NodeID:  42,
		Context: []byte("addr=node42:2380"),
	}
	// Encode manually using the same format as encodeConfChange.
	buf := make([]byte, 9+len(original.Context))
	buf[0] = byte(original.Type)
	for i := 0; i < 8; i++ {
		buf[1+i] = byte(original.NodeID >> (8 * i))
	}
	copy(buf[9:], original.Context)

	var decoded raft.ConfChange
	if err := decodeConfChange(buf, &decoded); err != nil {
		t.Fatalf("decodeConfChange: %v", err)
	}
	if decoded.Type != original.Type {
		t.Errorf("type: got %d, want %d", decoded.Type, original.Type)
	}
	if decoded.NodeID != original.NodeID {
		t.Errorf("node id: got %d, want %d", decoded.NodeID, original.NodeID)
	}
	if string(decoded.Context) != string(original.Context) {
		t.Errorf("context: got %q, want %q", decoded.Context, original.Context)
	}
}

func TestConfig_Defaults(t *testing.T) {
	cfg := &Config{NodeID: 1}
	cfg.withDefaults()
	if cfg.HeartbeatMs != 100 {
		t.Errorf("HeartbeatMs default: got %d, want 100", cfg.HeartbeatMs)
	}
	if cfg.ElectionMs != 1000 {
		t.Errorf("ElectionMs default: got %d, want 1000", cfg.ElectionMs)
	}
	if cfg.SnapThreshold != 10_000 {
		t.Errorf("SnapThreshold default: got %d, want 10000", cfg.SnapThreshold)
	}
	if cfg.Logger == nil {
		t.Error("Logger default should not be nil")
	}
}
