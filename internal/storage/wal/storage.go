package wal

import (
	"fmt"

	"github.com/phalanx-db/phalanx/internal/raft"
)

// WALStorage implements raft.Storage by replaying a WAL on top of an
// in-memory log. The pattern is:
//
//  1. Open/Create the WAL.
//  2. Call ReadAll() to get the last HardState, all Entries, and the
//     last Snapshot.
//  3. Feed those into a fresh MemoryStorage via Append + ApplySnapshot.
//  4. Wrap both in WALStorage.
//
// On every Ready cycle, the application must call SaveEntries before
// calling MemoryStorage.Append, so that the WAL is always ahead of
// the in-memory index.
type WALStorage struct {
	w  *WAL
	ms *raft.MemoryStorage
}

// OpenWALStorage opens an existing WAL directory and replays it into a
// fresh MemoryStorage. Returns the recovered HardState alongside the
// storage so the caller can restore it into the Raft node.
func OpenWALStorage(dir string) (*WALStorage, raft.HardState, error) {
	w, err := Open(dir)
	if err != nil {
		return nil, raft.HardState{}, fmt.Errorf("walstore: open WAL: %w", err)
	}
	if err := w.TailRepair(); err != nil {
		return nil, raft.HardState{}, fmt.Errorf("walstore: tail repair: %w", err)
	}
	hs, ents, snap, err := w.ReadAll()
	if err != nil {
		return nil, raft.HardState{}, fmt.Errorf("walstore: replay: %w", err)
	}
	ms := raft.NewMemoryStorage()
	if snap != nil {
		if err := ms.ApplySnapshot(*snap); err != nil {
			return nil, raft.HardState{}, fmt.Errorf("walstore: apply snapshot: %w", err)
		}
	}
	if len(ents) > 0 {
		if err := ms.Append(ents); err != nil {
			return nil, raft.HardState{}, fmt.Errorf("walstore: append entries: %w", err)
		}
	}
	if err := ms.SetHardState(hs); err != nil {
		return nil, raft.HardState{}, fmt.Errorf("walstore: set hardstate: %w", err)
	}
	return &WALStorage{w: w, ms: ms}, hs, nil
}

// CreateWALStorage initialises a new WAL directory and returns a fresh storage.
func CreateWALStorage(dir string) (*WALStorage, error) {
	w, err := Create(dir, nil)
	if err != nil {
		return nil, fmt.Errorf("walstore: create WAL: %w", err)
	}
	return &WALStorage{w: w, ms: raft.NewMemoryStorage()}, nil
}

// Save persists HardState and Entries to the WAL, then appends the
// entries to the in-memory storage. This MUST be called before advancing
// the Raft node so the log is never ahead of the WAL.
func (ws *WALStorage) Save(hs raft.HardState, ents []raft.Entry) error {
	if err := ws.w.SaveEntries(hs, ents); err != nil {
		return err
	}
	if !hs.IsEmpty() {
		if err := ws.ms.SetHardState(hs); err != nil {
			return err
		}
	}
	if len(ents) > 0 {
		return ws.ms.Append(ents)
	}
	return nil
}

// SaveSnapshot writes a snapshot record to the WAL and applies it to
// the in-memory storage, compacting the prefix of the log.
func (ws *WALStorage) SaveSnapshot(snap raft.Snapshot) error {
	if snap.IsEmpty() {
		return nil
	}
	if err := ws.w.SaveSnapshot(snap); err != nil {
		return err
	}
	if err := ws.ms.ApplySnapshot(snap); err != nil && err != raft.ErrSnapOutOfDate {
		return err
	}
	return nil
}

// Close flushes and syncs the active WAL segment.
func (ws *WALStorage) Close() error {
	return ws.w.Close()
}

// InitialState delegates to the in-memory storage.
func (ws *WALStorage) InitialState() (raft.HardState, raft.ConfState, error) {
	return ws.ms.InitialState()
}

// Entries delegates to the in-memory storage.
func (ws *WALStorage) Entries(lo, hi, maxSize uint64) ([]raft.Entry, error) {
	return ws.ms.Entries(lo, hi, maxSize)
}

// Term delegates to the in-memory storage.
func (ws *WALStorage) Term(i uint64) (uint64, error) {
	return ws.ms.Term(i)
}

// LastIndex delegates to the in-memory storage.
func (ws *WALStorage) LastIndex() (uint64, error) {
	return ws.ms.LastIndex()
}

// FirstIndex delegates to the in-memory storage.
func (ws *WALStorage) FirstIndex() (uint64, error) {
	return ws.ms.FirstIndex()
}

// Snapshot delegates to the in-memory storage.
func (ws *WALStorage) Snapshot() (raft.Snapshot, error) {
	return ws.ms.Snapshot()
}

var _ raft.Storage = (*WALStorage)(nil)

// CompactBefore discards log entries that are fully covered by a snapshot
// at compactIndex. It updates both the in-memory index and the on-disk segments.
// Safe to call concurrently with Save/Snapshot; uses the WAL mutex internally.
func (ws *WALStorage) CompactBefore(compactIndex uint64) error {
	if err := ws.ms.Compact(compactIndex); err != nil {
		return fmt.Errorf("walstore: memory compact at %d: %w", compactIndex, err)
	}
	return ws.w.TruncateBefore(compactIndex)
}
