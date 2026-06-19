package wal

import (
	"fmt"
	"os"
)

// TruncateBefore removes WAL segment files whose entries are entirely
// superseded by a snapshot at compactIndex.
//
// Production problem solved: without truncation, the WAL grows indefinitely.
// After a snapshot at index N, all entries ≤ N are redundant — they are
// encoded in the snapshot data. We can safely delete any segment file whose
// entire index range is ≤ compactIndex.
//
// Safety invariant: we NEVER delete the active segment. We also NEVER delete
// a segment whose firstIdx > compactIndex (those entries are still live). We
// only delete a segment S when the NEXT segment S' has firstIdx ≤ compactIndex,
// meaning every entry in S is earlier than the snapshot and is therefore
// captured by the snapshot.
//
// CockroachDB reference: RocksDB handles this at the SSTable level via
// compaction filters. Our simpler approach mirrors etcd's wal package.
func (w *WAL) TruncateBefore(compactIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.segments) <= 1 {
		// Only the active segment exists — nothing to truncate.
		return nil
	}

	var toDelete []*segment
	var keep []*segment

	for i, seg := range w.segments {
		if seg == w.active {
			keep = append(keep, seg)
			continue
		}
		// A sealed segment (not active) can be deleted if the segment that
		// follows it has firstIdx ≤ compactIndex, meaning every entry in
		// this segment predates the snapshot.
		if i+1 < len(w.segments) && w.segments[i+1].firstIdx <= compactIndex {
			toDelete = append(toDelete, seg)
		} else {
			keep = append(keep, seg)
		}
	}

	for _, seg := range toDelete {
		if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("wal: compaction: remove %s: %w", seg.path, err)
		}
	}

	w.segments = keep
	return nil
}

// SegmentCount returns the number of WAL segments currently on disk.
// Used by metrics and for compaction threshold decisions.
func (w *WAL) SegmentCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.segments)
}

// FirstIndex returns the lowest log index still present in the WAL after
// compaction. Entries before this index have been covered by a snapshot.
func (w *WAL) FirstIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.segments) == 0 {
		return 1
	}
	return w.segments[0].firstIdx
}
