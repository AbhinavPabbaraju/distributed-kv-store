package snapshot

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/phalanx-db/phalanx/internal/raft"
)

// ---------- helpers ----------

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "phalanx-snap-*")
	if err != nil {
		t.Fatalf("tempDir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func makeSnap(term, index uint64, data string) raft.Snapshot {
	return raft.Snapshot{
		Data: []byte(data),
		Metadata: raft.SnapshotMetadata{
			Term:  term,
			Index: index,
			ConfState: raft.ConfState{
				Voters:   []uint64{1, 2, 3},
				Learners: []uint64{4},
			},
		},
	}
}

// ---------- persistence ----------

func TestSave_CreatesFile(t *testing.T) {
	dir := tempDir(t)
	snap := makeSnap(2, 100, "state-at-100")

	if err := Save(dir, snap); err != nil {
		t.Fatalf("Save: %v", err)
	}

	names, _ := listSnapshotFiles(dir)
	if len(names) != 1 {
		t.Fatalf("expected 1 file, got %d: %v", len(names), names)
	}
	expected := snapFilename(2, 100)
	if names[0] != expected {
		t.Errorf("filename: got %q, want %q", names[0], expected)
	}
}

func TestSave_NoTmpFilesAfterSuccess(t *testing.T) {
	dir := tempDir(t)
	Save(dir, makeSnap(1, 50, "payload"))

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Errorf("orphan tmp file after successful Save: %q", e.Name())
		}
	}
}

func TestSave_EmptySnapshotIsNoOp(t *testing.T) {
	dir := tempDir(t)
	if err := Save(dir, raft.Snapshot{}); err != nil {
		t.Fatalf("Save empty: %v", err)
	}
	names, _ := listSnapshotFiles(dir)
	if len(names) != 0 {
		t.Errorf("empty snapshot should write no file, got %v", names)
	}
}

func TestLoad_RoundTrip(t *testing.T) {
	dir := tempDir(t)
	original := makeSnap(3, 200, "state-payload-xyz")
	Save(dir, original)

	loaded, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Metadata.Term != 3 || loaded.Metadata.Index != 200 {
		t.Errorf("metadata: got {%d %d}, want {3 200}",
			loaded.Metadata.Term, loaded.Metadata.Index)
	}
	if string(loaded.Data) != "state-payload-xyz" {
		t.Errorf("data: got %q, want %q", loaded.Data, "state-payload-xyz")
	}
	if len(loaded.Metadata.ConfState.Voters) != 3 {
		t.Errorf("voters: got %d, want 3", len(loaded.Metadata.ConfState.Voters))
	}
	if len(loaded.Metadata.ConfState.Learners) != 1 {
		t.Errorf("learners: got %d, want 1", len(loaded.Metadata.ConfState.Learners))
	}
}

func TestLoad_EmptyDir_ReturnsErrNoSnapshot(t *testing.T) {
	dir := tempDir(t)
	_, err := Load(dir)
	if err != ErrNoSnapshot {
		t.Errorf("want ErrNoSnapshot on empty dir, got %v", err)
	}
}

func TestLoad_NonexistentDir_ReturnsErrNoSnapshot(t *testing.T) {
	_, err := Load("/nonexistent/path/phalanx")
	if err != ErrNoSnapshot {
		t.Errorf("want ErrNoSnapshot for nonexistent dir, got %v", err)
	}
}

func TestLoad_ReturnsNewest(t *testing.T) {
	dir := tempDir(t)
	for _, idx := range []uint64{50, 100, 200, 300} {
		Save(dir, makeSnap(1, idx, fmt.Sprintf("state@%d", idx)))
	}
	loaded, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Metadata.Index != 300 {
		t.Errorf("expected newest snapshot (index=300), got index=%d", loaded.Metadata.Index)
	}
}

func TestLoad_SkipsCorruptFiles(t *testing.T) {
	dir := tempDir(t)
	// Write a valid snapshot.
	Save(dir, makeSnap(1, 100, "good"))
	// Write a newer file with corrupt content.
	corrupt := filepath.Join(dir, snapFilename(1, 200))
	os.WriteFile(corrupt, []byte("corrupted garbage content"), 0600)

	loaded, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Should fall back to the valid (older) snapshot.
	if loaded.Metadata.Index != 100 {
		t.Errorf("expected fallback to index=100, got %d", loaded.Metadata.Index)
	}
}

// ---------- CRC integrity ----------

func TestLoad_CRCMismatch_ReturnsError(t *testing.T) {
	dir := tempDir(t)
	Save(dir, makeSnap(1, 99, "sensitive-data"))

	names, _ := listSnapshotFiles(dir)
	path := filepath.Join(dir, names[0])
	raw, _ := os.ReadFile(path)
	// Flip a byte in the payload body (after the 20-byte header).
	raw[len(raw)-1] ^= 0xFF
	os.WriteFile(path, raw, 0600)

	_, err := Load(dir)
	// Either ErrCorrupted or ErrNoSnapshot (if it falls back to no valid file).
	if err == nil {
		t.Error("corrupt file should not load without error")
	}
}

func TestLoad_InvalidMagic(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, snapFilename(1, 77))
	// Wrong magic number.
	f, _ := os.Create(path)
	f.Write([]byte{0x00, 0x00, 0x00, 0x00}) // bad magic
	f.Close()

	_, err := Load(dir)
	if err == nil {
		t.Error("invalid magic should not load without error")
	}
}

// ---------- purge ----------

func TestPurge_KeepsNewest(t *testing.T) {
	dir := tempDir(t)
	for i := uint64(1); i <= 6; i++ {
		Save(dir, makeSnap(1, i*100, fmt.Sprintf("snap%d", i)))
	}

	if err := Purge(dir, 3); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	names, _ := listSnapshotFiles(dir)
	if len(names) != 3 {
		t.Errorf("after Purge(keep=3): got %d files, want 3: %v", len(names), names)
	}
	// Newest 3 should remain (indices 400, 500, 600).
	for _, name := range names {
		if !strings.Contains(name, "00000000000001") {
			t.Errorf("unexpected file retained: %q", name)
		}
	}
}

func TestPurge_NothingDeletedIfBelowThreshold(t *testing.T) {
	dir := tempDir(t)
	Save(dir, makeSnap(1, 100, "a"))
	Save(dir, makeSnap(1, 200, "b"))

	Purge(dir, 5)

	names, _ := listSnapshotFiles(dir)
	if len(names) != 2 {
		t.Errorf("got %d files, want 2 (no purge needed)", len(names))
	}
}

func TestPurge_RemovesOrphanTmpFiles(t *testing.T) {
	dir := tempDir(t)
	Save(dir, makeSnap(1, 100, "real"))
	// Leave a spurious .tmp file (simulates interrupted write).
	os.WriteFile(filepath.Join(dir, "orphan.snap.tmp"), []byte("partial"), 0600)

	Purge(dir, 2)

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Errorf("orphan .tmp not cleaned up: %q", e.Name())
		}
	}
}

// ---------- manager ----------

func TestManager_ShouldSnapshot_Threshold(t *testing.T) {
	dir := tempDir(t)
	mgr := NewManager(dir, 100, nil)

	if mgr.ShouldSnapshot(50) {
		t.Error("should not snapshot at index 50 with threshold 100")
	}
	if !mgr.ShouldSnapshot(100) {
		t.Error("should snapshot once threshold is reached")
	}
}

func TestManager_ShouldSnapshot_AfterSave(t *testing.T) {
	dir := tempDir(t)
	mgr := NewManager(dir, 100, nil)

	snap := makeSnap(1, 100, "s1")
	mgr.Save(snap)

	if mgr.ShouldSnapshot(150) {
		t.Error("should not snapshot 50 entries after last snapshot at index 100")
	}
	if !mgr.ShouldSnapshot(200) {
		t.Error("should snapshot once 100 entries past last snapshot index 100")
	}
}

func TestManager_Save_And_Load(t *testing.T) {
	dir := tempDir(t)
	mgr := NewManager(dir, 1000, nil)

	snap := makeSnap(2, 500, "big-state-payload")
	if err := mgr.Save(snap); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if mgr.LastIndex() != 500 {
		t.Errorf("LastIndex: got %d, want 500", mgr.LastIndex())
	}

	loaded, err := mgr.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Metadata.Index != 500 {
		t.Errorf("loaded index: got %d, want 500", loaded.Metadata.Index)
	}
	if string(loaded.Data) != "big-state-payload" {
		t.Errorf("loaded data: got %q", loaded.Data)
	}
}

func TestManager_Load_FreshCluster(t *testing.T) {
	dir := tempDir(t)
	mgr := NewManager(dir, 1000, nil)
	_, err := mgr.Load()
	if err != ErrNoSnapshot {
		t.Errorf("fresh cluster: want ErrNoSnapshot, got %v", err)
	}
}

// ---------- chunked transfer ----------

func TestChunkedTransfer_SmallPayload(t *testing.T) {
	runChunkedTransferTest(t, "hello phalanx", 10*time.Second)
}

func TestChunkedTransfer_LargePayload(t *testing.T) {
	// 4 MiB payload exercises multi-chunk path.
	payload := bytes.Repeat([]byte("phalanx-cluster-data-"), 200_000)
	runChunkedTransferTest(t, string(payload), 30*time.Second)
}

func TestChunkedTransfer_EmptyPayload(t *testing.T) {
	runChunkedTransferTest(t, "", 5*time.Second)
}

// runChunkedTransferTest sends a snapshot from a sender Manager to a
// receiver Manager over a local TCP connection and verifies the payload.
func runChunkedTransferTest(t *testing.T, payload string, timeout time.Duration) {
	t.Helper()

	sendDir := tempDir(t)
	recvDir := tempDir(t)

	recvMgr := NewManager(recvDir, 10_000, nil)

	// Find a free port for the snapshot receiver.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close() // close so the manager can re-open it

	if err := recvMgr.StartReceiver(addr); err != nil {
		t.Fatalf("StartReceiver: %v", err)
	}

	snap := raft.Snapshot{
		Data: []byte(payload),
		Metadata: raft.SnapshotMetadata{
			Term:  5,
			Index: 1000,
			ConfState: raft.ConfState{Voters: []uint64{1, 2, 3}},
		},
	}

	sendMgr := NewManager(sendDir, 10_000, nil)

	// Allow receiver to start listening.
	time.Sleep(20 * time.Millisecond)

	if err := sendMgr.SendTo(snap, 1, 2, addr); err != nil {
		t.Fatalf("SendTo: %v", err)
	}

	// Wait for the snapshot to arrive on the receiver channel.
	select {
	case received := <-recvMgr.IncomingSnapshots():
		if received.Metadata.Index != 1000 {
			t.Errorf("index: got %d, want 1000", received.Metadata.Index)
		}
		if received.Metadata.Term != 5 {
			t.Errorf("term: got %d, want 5", received.Metadata.Term)
		}
		if string(received.Data) != payload {
			if len(payload) <= 80 {
				t.Errorf("data mismatch: got %q, want %q", received.Data, payload)
			} else {
				t.Errorf("data length mismatch: got %d, want %d",
					len(received.Data), len(payload))
			}
		}
	case <-time.After(timeout):
		t.Error("timeout waiting for snapshot to arrive")
	}
}

func TestFilenameOrder_IsChronological(t *testing.T) {
	// Snapshot files must sort lexicographically by (term, index) in ascending order.
	// This ensures Load() correctly picks the newest without parsing filenames.
	cases := []struct{ term, index uint64 }{
		{1, 100},
		{1, 200},
		{1, 300},
		{2, 50},   // higher term, lower index — must still sort after all term-1 files
		{2, 400},
	}
	names := make([]string, len(cases))
	for i, c := range cases {
		names[i] = snapFilename(c.term, c.index)
	}
	for i := 1; i < len(names); i++ {
		if names[i] <= names[i-1] {
			t.Errorf("filename ordering broken: %q should be > %q", names[i], names[i-1])
		}
	}
}

// Verify that a load fallback chain works when newer files are unreadable.
func TestLoad_FallbackChain(t *testing.T) {
	dir := tempDir(t)
	Save(dir, makeSnap(1, 100, "snap-a")) // oldest, valid
	Save(dir, makeSnap(1, 200, "snap-b")) // middle, valid
	Save(dir, makeSnap(1, 300, "snap-c")) // newest

	// Corrupt the newest.
	names, _ := listSnapshotFiles(dir)
	latest := filepath.Join(dir, names[len(names)-1])
	raw, _ := io.ReadAll(mustOpen(t, latest))
	raw[len(raw)-1] ^= 0xFF
	os.WriteFile(latest, raw, 0600)

	loaded, err := Load(dir)
	if err != nil {
		t.Fatalf("should fall back to valid file: %v", err)
	}
	if loaded.Metadata.Index != 200 {
		t.Errorf("fallback: got index=%d, want 200", loaded.Metadata.Index)
	}
}

func mustOpen(t *testing.T, path string) io.Reader {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}
