// Package snapshot handles snapshot persistence, recovery, and chunked
// transmission for the Phalanx distributed KV store.
//
// Production problem solved: Without snapshots, a restarting node must replay
// the entire WAL from genesis. A 30-day-old cluster at 10K ops/day has 300K
// entries — seconds of startup time that grows unboundedly.
//
// Design: atomic file rename ensures crash safety. Only fsync'd files are
// ever renamed into the snap directory, so a partial write is never visible
// as a complete snapshot.
package snapshot

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/phalanx-db/phalanx/internal/raft"
)

const (
	fileMagic  uint32 = 0x504C4E58 // "PLNX"
	fileVer    uint32 = 1
	snapSuffix        = ".snap"
	crcTable          = crc32.Castagnoli
)

var (
	ErrNoSnapshot   = errors.New("snapshot: no snapshot found")
	ErrCorrupted    = errors.New("snapshot: CRC mismatch — data corrupted")
	ErrInvalidMagic = errors.New("snapshot: unrecognised file header")
)

// snapBody is the gob-encoded body written after the 20-byte file header.
type snapBody struct {
	Term     uint64
	Index    uint64
	Voters   []uint64
	Learners []uint64
	Data     []byte
}

// Save writes snap to snapdir atomically. The write is:
//
//	write to {name}.tmp → fdatasync → rename to {name}.snap
//
// If the process crashes mid-write, the .tmp file is left behind (harmless)
// and the .snap file is never created, so Load will not see a corrupt file.
func Save(snapdir string, snap raft.Snapshot) error {
	if snap.IsEmpty() {
		return nil
	}
	if err := os.MkdirAll(snapdir, 0750); err != nil {
		return fmt.Errorf("snapshot: mkdir %s: %w", snapdir, err)
	}

	body := snapBody{
		Term:     snap.Metadata.Term,
		Index:    snap.Metadata.Index,
		Voters:   snap.Metadata.ConfState.Voters,
		Learners: snap.Metadata.ConfState.Learners,
		Data:     snap.Data,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(body); err != nil {
		return fmt.Errorf("snapshot: encode: %w", err)
	}
	payload := buf.Bytes()
	checksum := crc32.Checksum(payload, crc32.MakeTable(crcTable))

	name := snapFilename(snap.Metadata.Term, snap.Metadata.Index)
	tmpPath := filepath.Join(snapdir, name+".tmp")
	dstPath := filepath.Join(snapdir, name)

	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("snapshot: create tmp: %w", err)
	}

	hdr := make([]byte, 20)
	binary.BigEndian.PutUint32(hdr[0:4], fileMagic)
	binary.BigEndian.PutUint32(hdr[4:8], fileVer)
	binary.BigEndian.PutUint64(hdr[8:16], uint64(len(payload)))
	binary.BigEndian.PutUint32(hdr[16:20], checksum)

	if _, err := f.Write(hdr); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("snapshot: write header: %w", err)
	}
	if _, err := f.Write(payload); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("snapshot: write body: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("snapshot: fsync: %w", err)
	}
	f.Close()

	if err := os.Rename(tmpPath, dstPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("snapshot: rename: %w", err)
	}
	return nil
}

// Load returns the most recent valid snapshot from snapdir.
// Returns ErrNoSnapshot if the directory is empty or all files are corrupt.
func Load(snapdir string) (raft.Snapshot, error) {
	names, err := listSnapshotFiles(snapdir)
	if err != nil {
		return raft.Snapshot{}, err
	}
	if len(names) == 0 {
		return raft.Snapshot{}, ErrNoSnapshot
	}
	// newest-first
	for i := len(names) - 1; i >= 0; i-- {
		snap, err := readFile(filepath.Join(snapdir, names[i]))
		if err == nil {
			return snap, nil
		}
	}
	return raft.Snapshot{}, ErrNoSnapshot
}

// Purge removes all but the newest keepCount snapshot files.
// Called after log compaction to reclaim disk space.
func Purge(snapdir string, keepCount int) error {
	names, err := listSnapshotFiles(snapdir)
	if err != nil {
		return err
	}
	for i := 0; i < len(names)-keepCount; i++ {
		path := filepath.Join(snapdir, names[i])
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("snapshot: purge %s: %w", path, err)
		}
	}
	// Also remove any orphan .tmp files
	entries, _ := os.ReadDir(snapdir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			os.Remove(filepath.Join(snapdir, e.Name()))
		}
	}
	return nil
}

func readFile(path string) (raft.Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return raft.Snapshot{}, err
	}
	defer f.Close()

	hdr := make([]byte, 20)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return raft.Snapshot{}, fmt.Errorf("snapshot: read header: %w", err)
	}

	magic := binary.BigEndian.Uint32(hdr[0:4])
	if magic != fileMagic {
		return raft.Snapshot{}, ErrInvalidMagic
	}
	payloadLen := binary.BigEndian.Uint64(hdr[8:16])
	expectedCRC := binary.BigEndian.Uint32(hdr[16:20])

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(f, payload); err != nil {
		return raft.Snapshot{}, fmt.Errorf("snapshot: read body: %w", err)
	}
	if crc32.Checksum(payload, crc32.MakeTable(crcTable)) != expectedCRC {
		return raft.Snapshot{}, ErrCorrupted
	}

	var body snapBody
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&body); err != nil {
		return raft.Snapshot{}, fmt.Errorf("snapshot: decode: %w", err)
	}

	return raft.Snapshot{
		Data: body.Data,
		Metadata: raft.SnapshotMetadata{
			Term:  body.Term,
			Index: body.Index,
			ConfState: raft.ConfState{
				Voters:   body.Voters,
				Learners: body.Learners,
			},
		},
	}, nil
}

func listSnapshotFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("snapshot: readdir %s: %w", dir, err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), snapSuffix) &&
			!strings.HasSuffix(e.Name(), ".tmp") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names) // lexicographic order = chronological order given the filename format
	return names, nil
}

// snapFilename returns the filename for a snapshot at (term, index).
// The format is: {term:016x}-{index:016x}.snap
// Lexicographic sort of these names is equivalent to chronological order.
func snapFilename(term, index uint64) string {
	return fmt.Sprintf("%016x-%016x%s", term, index, snapSuffix)
}
