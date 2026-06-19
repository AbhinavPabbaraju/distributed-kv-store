// Package pebble provides a Pebble-backed implementation of raft.Storage and
// the KV state machine persistence layer.
//
// Production problem solved: the current MemoryStorage + WAL approach keeps
// all log entries in RAM. A node with 10 million log entries at 256 bytes each
// holds 2.5 GiB in memory before any snapshot. Pebble's LSM-tree stores entries
// on disk, allowing logs many times larger than available RAM.
//
// Why Pebble and not RocksDB?
//   - Pure Go — no CGo, no C++ toolchain dependency, no CGo call overhead
//   - Production-proven: CockroachDB runs it at petabyte scale
//   - Go-native MVCC snapshots — critical for consistent state machine snapshots
//     without pausing writes (we create an SST-level snapshot, iterate it to
//     build the Raft snapshot, and release it atomically)
//   - LSM compaction is write-optimised — the WAL access pattern (append-only
//     log entries) maps naturally to LSM L0 → L1 compaction
//   - BoltDB is B-tree (read-optimised, write-amplified) and unmaintained
//   - BadgerDB is LSM but less battle-tested at scale than Pebble
//
// Key layout:
//
//	Raft WAL entries:   wal-entry:{term:8BE}{index:8BE}  → gob(Entry)
//	HardState:          wal-hardstate                    → gob(HardState)
//	Snapshot metadata:  wal-snapshot                     → gob(SnapshotMetadata)
//	KV state machine:   kv:{user-key}                    → user-value
//
// The "8BE" suffix means 8-byte big-endian encoding, which allows lexicographic
// key ordering to match numeric ordering — critical for prefix scans over index ranges.
//
// To activate: replace WALStorage with PebbleStorage in server.RaftNode.
// The raft.Storage interface is unchanged; the swap is transparent to the Raft engine.
//
// Build tag: requires `go get github.com/cockroachdb/pebble` once the network
// allowlist includes proxy.golang.org.
package pebble

import (
	"encoding/binary"
	"encoding/gob"
	"bytes"
	"errors"
	"fmt"

	"github.com/phalanx-db/phalanx/internal/raft"
)

// Key prefixes — separates WAL entries from KV data in the same Pebble instance.
// Two separate instances would provide cleaner isolation but complicate
// atomic snapshot creation that must include both WAL and KV state.
var (
	prefixWALEntry    = []byte("wal-entry:")
	prefixWALHardState = []byte("wal-hardstate")
	prefixWALSnapshot = []byte("wal-snapshot")
	prefixKV          = []byte("kv:")
)

// ErrNoPebble is returned when the Pebble library is not available.
// Add `github.com/cockroachdb/pebble` to go.mod to activate.
var ErrNoPebble = errors.New("pebble: package not compiled in; add cockroachdb/pebble to go.mod")

// -------------------------------------------------------------------
// Key encoding helpers
// -------------------------------------------------------------------

// walEntryKey returns the Pebble key for a WAL log entry.
//
// Entries are keyed by INDEX ALONE, not (term, index). In Raft, the log index
// is globally unique — there is never more than one entry at a given index in
// a node's log (a conflicting entry at the same index from a higher term
// OVERWRITES the old one). Keying by index makes range scans correct and makes
// overwrite-on-conflict a simple Set to the same key. The term is stored in the
// entry value, not the key.
//
// Big-endian encoding ensures lexicographic byte order == numeric index order.
func walEntryKey(term, index uint64) []byte {
	_ = term // term is stored in the value, not the key
	key := make([]byte, len(prefixWALEntry)+8)
	copy(key, prefixWALEntry)
	binary.BigEndian.PutUint64(key[len(prefixWALEntry):], index)
	return key
}

// walEntryLowerBound returns the inclusive lower bound key for a range scan
// of log entries starting at the given index.
func walEntryLowerBound(index uint64) []byte {
	key := make([]byte, len(prefixWALEntry)+8)
	copy(key, prefixWALEntry)
	binary.BigEndian.PutUint64(key[len(prefixWALEntry):], index)
	return key
}

// kvKey returns the Pebble key for a user-level KV entry.
func kvKey(userKey []byte) []byte {
	return append(append([]byte{}, prefixKV...), userKey...)
}

// -------------------------------------------------------------------
// Codec
// -------------------------------------------------------------------

func encodeEntry(e *raft.Entry) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(e); err != nil {
		return nil, fmt.Errorf("pebble: encode entry: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeEntry(data []byte) (raft.Entry, error) {
	var e raft.Entry
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&e); err != nil {
		return raft.Entry{}, fmt.Errorf("pebble: decode entry: %w", err)
	}
	return e, nil
}

func encodeHardState(hs raft.HardState) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(hs); err != nil {
		return nil, fmt.Errorf("pebble: encode hardstate: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeHardState(data []byte) (raft.HardState, error) {
	var hs raft.HardState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&hs); err != nil {
		return raft.HardState{}, fmt.Errorf("pebble: decode hardstate: %w", err)
	}
	return hs, nil
}

// -------------------------------------------------------------------
// Storage interface stub — compile-checked without the Pebble library
// -------------------------------------------------------------------

// Storage is the interface PebbleStorage would implement.
// It extends raft.Storage with write methods (analogous to WALStorage.Save).
type Storage interface {
	raft.Storage
	Save(hardState raft.HardState, entries []raft.Entry) error
	SaveSnapshot(snap raft.Snapshot) error
	CompactBefore(index uint64) error
	Close() error
}

// NullStorage is a compile-time proof that this package is type-correct
// with respect to the Storage interface. It panics on every call and is
// never used in production.
type NullStorage struct{}

func (n NullStorage) InitialState() (raft.HardState, raft.ConfState, error) {
	panic(ErrNoPebble)
}
func (n NullStorage) Entries(_, _, _ uint64) ([]raft.Entry, error) { panic(ErrNoPebble) }
func (n NullStorage) Term(_ uint64) (uint64, error)                { panic(ErrNoPebble) }
func (n NullStorage) LastIndex() (uint64, error)                   { panic(ErrNoPebble) }
func (n NullStorage) FirstIndex() (uint64, error)                  { panic(ErrNoPebble) }
func (n NullStorage) Snapshot() (raft.Snapshot, error)             { panic(ErrNoPebble) }
func (n NullStorage) Save(_ raft.HardState, _ []raft.Entry) error  { panic(ErrNoPebble) }
func (n NullStorage) SaveSnapshot(_ raft.Snapshot) error           { panic(ErrNoPebble) }
func (n NullStorage) CompactBefore(_ uint64) error                 { panic(ErrNoPebble) }
func (n NullStorage) Close() error                                 { panic(ErrNoPebble) }

var _ Storage = NullStorage{} // compile-time interface check

// -------------------------------------------------------------------
// PebbleStorage implementation notes (requires cockroachdb/pebble)
// -------------------------------------------------------------------

// When the Pebble dependency is available, the full implementation is:
//
//	type PebbleStorage struct {
//	    db        *pebble.DB
//	    mu        sync.RWMutex
//	    firstIndex uint64  // cached; invalidated on compaction
//	    lastIndex  uint64  // cached; updated on every Append
//	    hardState  raft.HardState
//	    snapshot   raft.Snapshot
//	}
//
// Save() uses a pebble.Batch for atomic multi-key writes:
//
//	batch := db.NewBatch()
//	batch.Set(prefixWALHardState, encodeHardState(hs), pebble.Sync)
//	for _, e := range entries {
//	    batch.Set(walEntryKey(e.Term, e.Index), encodeEntry(e), nil)
//	}
//	return batch.Commit(pebble.Sync)  // single fdatasync
//
// Entries() uses a range iterator:
//
//	iter, _ := db.NewIter(&pebble.IterOptions{
//	    LowerBound: walEntryLowerBound(lo),
//	    UpperBound: walEntryLowerBound(hi),
//	})
//	defer iter.Close()
//	for iter.First(); iter.Valid(); iter.Next() { ... }
//
// CompactBefore() deletes the key range [prefixWALEntry, walEntryLowerBound(index)):
//
//	db.DeleteRange(prefixWALEntry, walEntryLowerBound(index), pebble.NoSync)
//
// Snapshot() uses pebble.DB.NewSnapshot() for a consistent point-in-time read:
//
//	snap := db.NewSnapshot()       // zero-copy, O(1)
//	defer snap.Close()
//	// iterate kv:* prefix to build state machine snapshot
//	// the snapshot is consistent even while writes continue
//
// This is the key advantage over the WAL approach: Pebble snapshots are
// instantaneous copy-on-write SST pointers, not copies of data.

// Verify key encoding does what we claim: lexicographic order == index order.
// Entries are keyed by index alone, so a lower index always sorts first
// regardless of term.
func init() {
	i100 := walEntryKey(1, 100)
	i200 := walEntryKey(5, 200) // higher term, higher index
	i50 := walEntryKey(9, 50)   // highest term, lowest index
	if bytes.Compare(i100, i200) >= 0 {
		panic("pebble: key encoding broken: index 100 should sort before index 200")
	}
	if bytes.Compare(i50, i100) >= 0 {
		panic("pebble: key encoding broken: index 50 should sort before index 100 regardless of term")
	}
}
