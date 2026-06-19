package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/phalanx-db/phalanx/internal/raft"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "phalanx-wal-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestWALCreateAndReadBack(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, []byte("metadata"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	hs := raft.HardState{Term: 3, Vote: 2, Commit: 5}
	entries := []raft.Entry{
		{Term: 1, Index: 1, Data: []byte("first")},
		{Term: 1, Index: 2, Data: []byte("second")},
		{Term: 2, Index: 3, Data: []byte("third")},
	}
	if err := w.SaveEntries(hs, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w2.Close()

	gotHS, gotEnts, gotSnap, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if gotSnap != nil {
		t.Errorf("expected no snapshot, got %+v", gotSnap)
	}
	if gotHS != hs {
		t.Errorf("hardstate: got %+v, want %+v", gotHS, hs)
	}
	if len(gotEnts) != len(entries) {
		t.Fatalf("entries count: got %d, want %d", len(gotEnts), len(entries))
	}
	for i, e := range gotEnts {
		if e.Term != entries[i].Term || e.Index != entries[i].Index || string(e.Data) != string(entries[i].Data) {
			t.Errorf("entry[%d]: got %+v, want %+v", i, e, entries[i])
		}
	}
}

func TestWALSnapshotRoundTrip(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	snap := raft.Snapshot{
		Data: []byte("state-payload"),
		Metadata: raft.SnapshotMetadata{
			Index: 100,
			Term:  5,
			ConfState: raft.ConfState{
				Voters:   []uint64{1, 2, 3},
				Learners: []uint64{4},
			},
		},
	}
	if err := w.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w2.Close()

	_, _, gotSnap, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if gotSnap == nil {
		t.Fatal("expected snapshot, got nil")
	}
	if gotSnap.Metadata.Index != snap.Metadata.Index {
		t.Errorf("snapshot index: got %d, want %d", gotSnap.Metadata.Index, snap.Metadata.Index)
	}
	if gotSnap.Metadata.Term != snap.Metadata.Term {
		t.Errorf("snapshot term: got %d, want %d", gotSnap.Metadata.Term, snap.Metadata.Term)
	}
	if string(gotSnap.Data) != string(snap.Data) {
		t.Errorf("snapshot data: got %q, want %q", gotSnap.Data, snap.Data)
	}
	if len(gotSnap.Metadata.ConfState.Voters) != len(snap.Metadata.ConfState.Voters) {
		t.Errorf("voters count: got %d, want %d",
			len(gotSnap.Metadata.ConfState.Voters), len(snap.Metadata.ConfState.Voters))
	}
}

func TestWALCRCCorruptionDetected(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	hs := raft.HardState{Term: 1}
	entries := []raft.Entry{{Term: 1, Index: 1, Data: []byte("data")}}
	if err := w.SaveEntries(hs, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	files, err := segmentFiles(dir)
	if err != nil || len(files) == 0 {
		t.Fatalf("no segment files found: %v", err)
	}
	path := filepath.Join(dir, files[0])
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if len(raw) < 20 {
		t.Fatalf("segment too short to corrupt: %d bytes", len(raw))
	}
	// Flip a byte in the payload area (after the header)
	raw[len(raw)-2] ^= 0xFF
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatalf("write corrupted segment: %v", err)
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w2.Close()

	_, gotEnts, _, err := w2.ReadAll()
	// Either ReadAll returns ErrCRCMismatch, or it returns partial results
	// (stopping at the corrupted frame). Either is acceptable.
	if err != nil && err != ErrCRCMismatch {
		t.Logf("ReadAll returned error (acceptable): %v", err)
	} else if len(gotEnts) == len(entries) {
		t.Error("corrupted WAL should not return all entries intact")
	}
}

func TestWALTailRepairAfterCrash(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	entries := []raft.Entry{
		{Term: 1, Index: 1, Data: []byte("safe")},
	}
	if err := w.SaveEntries(raft.HardState{Term: 1}, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate crash: append garbage bytes after the last valid frame
	files, _ := segmentFiles(dir)
	path := filepath.Join(dir, files[0])
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	f.Write([]byte{0xFF, 0xFE, 0xAB, 0x00, 0x01, 0x02}) // partial frame
	f.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open after simulated crash: %v", err)
	}
	if err := w2.TailRepair(); err != nil {
		t.Fatalf("TailRepair: %v", err)
	}

	_, gotEnts, _, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after tail repair: %v", err)
	}
	w2.Close()

	if len(gotEnts) != len(entries) {
		t.Errorf("after tail repair: got %d entries, want %d", len(gotEnts), len(entries))
	}
}

func TestWALStorageRoundTrip(t *testing.T) {
	dir := tempDir(t)

	store, err := CreateWALStorage(dir)
	if err != nil {
		t.Fatalf("CreateWALStorage: %v", err)
	}

	hs := raft.HardState{Term: 2, Vote: 1, Commit: 3}
	entries := []raft.Entry{
		{Term: 2, Index: 1},
		{Term: 2, Index: 2},
		{Term: 2, Index: 3, Data: []byte("payload")},
	}
	if err := store.Save(hs, entries); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	store2, recoveredHS, err := OpenWALStorage(dir)
	if err != nil {
		t.Fatalf("OpenWALStorage: %v", err)
	}
	defer store2.Close()

	if recoveredHS != hs {
		t.Errorf("recovered hardstate: got %+v, want %+v", recoveredHS, hs)
	}
	last, _ := store2.LastIndex()
	if last != 3 {
		t.Errorf("last index: got %d, want 3", last)
	}
	got, _ := store2.Entries(1, 4, ^uint64(0))
	if len(got) != 3 {
		t.Fatalf("entries count: got %d, want 3", len(got))
	}
	if string(got[2].Data) != "payload" {
		t.Errorf("entry[2] data: got %q, want %q", got[2].Data, "payload")
	}
}

func TestWALMultipleSegments(t *testing.T) {
	dir := tempDir(t)

	w, err := Create(dir, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	payload := make([]byte, 512*1024) // 512 KiB per entry
	var allEntries []raft.Entry
	for i := uint64(1); i <= 200; i++ {
		e := raft.Entry{Term: 1, Index: i, Data: payload}
		allEntries = append(allEntries, e)
		if err := w.SaveEntries(raft.HardState{Term: 1}, []raft.Entry{e}); err != nil {
			t.Fatalf("SaveEntries[%d]: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	files, err := segmentFiles(dir)
	if err != nil {
		t.Fatalf("segmentFiles: %v", err)
	}
	if len(files) < 2 {
		t.Errorf("expected multiple segments (200 × 512 KiB > 64 MiB), got %d", len(files))
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w2.Close()

	_, gotEnts, _, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll across segments: %v", err)
	}
	if len(gotEnts) != len(allEntries) {
		t.Errorf("entry count after multi-segment replay: got %d, want %d", len(gotEnts), len(allEntries))
	}
}
