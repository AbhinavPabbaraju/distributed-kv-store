package raft

import (
	"errors"
	"fmt"
)

var (
	ErrCompacted  = errors.New("requested index is unavailable due to compaction")
	ErrSnapOutOfDate = errors.New("requested index is older than existing snapshot")
	ErrUnavailable   = errors.New("requested log entries unavailable")
	ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")
)

type Storage interface {
	InitialState() (HardState, ConfState, error)
	Entries(lo, hi, maxSize uint64) ([]Entry, error)
	Term(i uint64) (uint64, error)
	LastIndex() (uint64, error)
	FirstIndex() (uint64, error)
	Snapshot() (Snapshot, error)
}

type unstable struct {
	snapshot *Snapshot
	entries  []Entry
	offset   uint64
	logger   Logger
}

func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index, true
	}
	return 0, false
}

func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}
	last, ok := u.maybeLastIndex()
	if !ok || i > last {
		return 0, false
	}
	return u.entries[i-u.offset].Term, true
}

func (u *unstable) stableTo(i, t uint64) {
	gt, ok := u.maybeTerm(i)
	if !ok || gt != t || i < u.offset {
		return
	}
	u.entries = u.entries[i+1-u.offset:]
	u.offset = i + 1
	u.shrinkEntriesArray()
}

func (u *unstable) shrinkEntriesArray() {
	const lenMultiple = 2
	if len(u.entries) == 0 {
		u.entries = nil
	} else if len(u.entries)*lenMultiple < cap(u.entries) {
		newEntries := make([]Entry, len(u.entries))
		copy(newEntries, u.entries)
		u.entries = newEntries
	}
}

func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
	}
}

func (u *unstable) restore(s Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.entries = nil
	u.snapshot = &s
}

func (u *unstable) truncateAndAppend(ents []Entry) {
	after := ents[0].Index
	switch {
	case after == u.offset+uint64(len(u.entries)):
		u.entries = append(u.entries, ents...)
	case after <= u.offset:
		u.logger.Infof("replace the unstable entries from index %d", after)
		u.offset = after
		u.entries = ents
	default:
		u.logger.Infof("truncate the unstable entries before index %d", after)
		u.entries = append([]Entry{}, u.slice(u.offset, after)...)
		u.entries = append(u.entries, ents...)
	}
}

func (u *unstable) slice(lo, hi uint64) []Entry {
	u.mustCheckOutOfBounds(lo, hi)
	return u.entries[lo-u.offset : hi-u.offset]
}

func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}

type raftLog struct {
	storage  Storage
	unstable unstable
	committed uint64
	applying  uint64
	applied   uint64
	maxNextEntsSize uint64
	logger    Logger
}

func newRaftLog(storage Storage, logger Logger, maxNextEntsSize uint64) *raftLog {
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	rl := &raftLog{
		storage:         storage,
		logger:          logger,
		maxNextEntsSize: maxNextEntsSize,
	}
	rl.unstable.offset = lastIndex + 1
	rl.unstable.logger = logger
	rl.committed = firstIndex - 1
	rl.applying = firstIndex - 1
	rl.applied = firstIndex - 1
	return rl
}

func (l *raftLog) String() string {
	return fmt.Sprintf("committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d",
		l.committed, l.applied, l.unstable.offset, len(l.unstable.entries))
}

func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) {
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			l.logger.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			offset := index + 1
			if ci-offset > uint64(len(ents)) {
				l.logger.Panicf("index, %d, is out of range [%d]", ci-offset, len(ents))
			}
			l.append(ents[ci-offset:]...)
		}
		l.commitTo(min64(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

func (l *raftLog) append(ents ...Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		l.logger.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	l.unstable.truncateAndAppend(ents)
	return l.lastIndex()
}

func (l *raftLog) findConflict(ents []Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.lastIndex() {
				l.logger.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

func (l *raftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	for ; index > 0; index-- {
		if ourTerm, err := l.term(index); err != nil {
			break
		} else if ourTerm <= term {
			break
		}
	}
	return index
}

func (l *raftLog) unstableEntries() []Entry {
	if len(l.unstable.entries) == 0 {
		return nil
	}
	return l.unstable.entries
}

func (l *raftLog) nextCommittedEnts(allowUnstable bool) []Entry {
	of := l.maxAppliableIndex(allowUnstable)
	if l.applying >= of {
		return nil
	}
	ents, err := l.slice(l.applying+1, of+1, l.maxNextEntsSize)
	if err != nil {
		l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
	}
	return ents
}

func (l *raftLog) hasNextCommittedEnts(allowUnstable bool) bool {
	return l.maxAppliableIndex(allowUnstable) > l.applying
}

func (l *raftLog) maxAppliableIndex(allowUnstable bool) uint64 {
	hi := l.committed
	if !allowUnstable {
		if ui := l.unstable.offset - 1; ui < hi {
			hi = ui
		}
	}
	return hi
}

func (l *raftLog) nextUnstableEnts() []Entry {
	return l.unstableEntries()
}

func (l *raftLog) hasNextUnstableEnts() bool {
	return len(l.unstableEntries()) > 0
}

func (l *raftLog) hasNextOrInProgressUnstableEnts() bool {
	return len(l.unstable.entries) > 0
}

func (l *raftLog) snapshot() (Snapshot, error) {
	if l.unstable.snapshot != nil {
		return *l.unstable.snapshot, nil
	}
	return l.storage.Snapshot()
}

func (l *raftLog) firstIndex() uint64 {
	if i, ok := l.unstable.maybeFirstIndex(); ok {
		return i
	}
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return index
}

func (l *raftLog) lastIndex() uint64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *raftLog) commitTo(tocommit uint64) {
	if l.committed < tocommit {
		if l.lastIndex() < tocommit {
			l.logger.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?",
				tocommit, l.lastIndex())
		}
		l.committed = tocommit
	}
}

func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		l.logger.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
	l.applying = i
}

func (l *raftLog) acceptApplying(i uint64, allowUnstable bool) {
	if l.committed < i {
		l.logger.Panicf("applying(%d) is out of range [committed(%d)]", i, l.committed)
	}
	if !allowUnstable && i > l.unstable.offset-1 {
		l.logger.Panicf("applying(%d) is unstable, but allowUnstable is false", i)
	}
	l.applying = i
}

func (l *raftLog) stableTo(i, t uint64) { l.unstable.stableTo(i, t) }
func (l *raftLog) stableSnapTo(i uint64) { l.unstable.stableSnapTo(i) }

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		l.logger.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

func (l *raftLog) term(i uint64) (uint64, error) {
	dummyIndex := l.firstIndex() - 1
	if i < dummyIndex || i > l.lastIndex() {
		return 0, nil
	}
	if t, ok := l.unstable.maybeTerm(i); ok {
		return t, nil
	}
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if errors.Is(err, ErrCompacted) || errors.Is(err, ErrUnavailable) {
		return 0, err
	}
	panic(err)
}

func (l *raftLog) entries(i, maxSize uint64) ([]Entry, error) {
	if i > l.lastIndex() {
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxSize)
}

func (l *raftLog) allEntries() []Entry {
	ents, err := l.entries(l.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	if errors.Is(err, ErrCompacted) {
		return l.allEntries()
	}
	panic(err)
}

func (l *raftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

func (l *raftLog) matchTerm(i, term uint64) bool {
	t, err := l.term(i)
	if err != nil {
		return false
	}
	return t == term
}

func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *raftLog) restore(s Snapshot) {
	l.logger.Infof("log [%s] starts to restore snapshot [index: %d, term: %d]",
		l, s.Metadata.Index, s.Metadata.Term)
	l.committed = s.Metadata.Index
	l.unstable.restore(s)
}

func (l *raftLog) slice(lo, hi, maxSize uint64) ([]Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var ents []Entry
	if lo < l.unstable.offset {
		storedEnts, err := l.storage.Entries(lo, min64(hi, l.unstable.offset), maxSize)
		if err != nil {
			if errors.Is(err, ErrCompacted) {
				return nil, err
			} else if errors.Is(err, ErrUnavailable) {
				l.logger.Panicf("entries[%d:%d) is unavailable from storage", lo, min64(hi, l.unstable.offset))
			} else {
				panic(err)
			}
		}
		if uint64(len(storedEnts)) < min64(hi, l.unstable.offset)-lo {
			return storedEnts, nil
		}
		ents = storedEnts
	}
	if hi > l.unstable.offset {
		unstable := l.unstable.slice(max64(lo, l.unstable.offset), hi)
		if len(ents) > 0 {
			combined := make([]Entry, len(ents), len(ents)+len(unstable))
			copy(combined, ents)
			ents = append(combined, unstable...)
		} else {
			ents = unstable
		}
	}
	return limitSize(ents, maxSize), nil
}

func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}
	length := l.lastIndex() + 1 - fi
	if hi > fi+length {
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.lastIndex())
	}
	return nil
}

func (l *raftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if errors.Is(err, ErrCompacted) {
		return 0
	}
	l.logger.Panicf("unexpected error (%v)", err)
	return 0
}

const noLimit = ^uint64(0)

func limitSize(ents []Entry, maxSize uint64) []Entry {
	if len(ents) == 0 || maxSize == noLimit {
		return ents
	}
	var size uint64
	for i := range ents {
		size += uint64(len(ents[i].Data)) + 16
		if size > maxSize {
			return ents[:i]
		}
	}
	return ents
}
