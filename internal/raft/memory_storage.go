package raft

import (
	"errors"
	"sync"
)

type MemoryStorage struct {
	sync.Mutex
	hardState HardState
	snapshot  Snapshot
	ents      []Entry
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		ents: make([]Entry, 1),
	}
}

func (ms *MemoryStorage) InitialState() (HardState, ConfState, error) {
	return ms.hardState, ms.snapshot.Metadata.ConfState, nil
}

func (ms *MemoryStorage) SetHardState(st HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		DefaultLogger.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}
	if len(ms.ents) == 1 {
		return nil, ErrUnavailable
	}
	ents := ms.ents[lo-offset : hi-offset]
	return limitSize(ents, maxSize), nil
}

func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

func (ms *MemoryStorage) Snapshot() (Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

func (ms *MemoryStorage) ApplySnapshot(snap Snapshot) error {
	ms.Lock()
	defer ms.Unlock()
	msIndex := ms.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index
	if msIndex >= snapIndex {
		return ErrSnapOutOfDate
	}
	ms.snapshot = snap
	ms.ents = []Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *ConfState, data []byte) (Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	if i <= ms.snapshot.Metadata.Index {
		return Snapshot{}, ErrSnapOutOfDate
	}
	offset := ms.ents[0].Index
	if i > ms.lastIndex() {
		DefaultLogger.Panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
	}
	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if cs != nil {
		ms.snapshot.Metadata.ConfState = *cs
	}
	ms.snapshot.Data = data
	return ms.snapshot, nil
}

func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		DefaultLogger.Panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}
	i := compactIndex - offset
	ents := make([]Entry, 1, uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

func (ms *MemoryStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	ms.Lock()
	defer ms.Unlock()
	first := ms.firstIndex()
	last := entries[0].Index + uint64(len(entries)) - 1
	if last < first {
		return nil
	}
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}
	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		ms.ents = append([]Entry{}, ms.ents[:offset]...)
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset:
		ms.ents = append(ms.ents, entries...)
	default:
		DefaultLogger.Panicf("missing log entry [last: %d, append at: %d]", ms.lastIndex(), entries[0].Index)
	}
	return nil
}

var _ Storage = (*MemoryStorage)(nil)
var _ = errors.New
