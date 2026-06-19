package statemachine

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"

	"github.com/phalanx-db/phalanx/internal/raft"
)

var (
	ErrKeyNotFound      = errors.New("statemachine: key not found")
	ErrCASFailed        = errors.New("statemachine: compare-and-swap condition not met")
	ErrInvalidOperation = errors.New("statemachine: invalid operation")
)

type OpType uint8

const (
	OpPut    OpType = iota
	OpDelete OpType = iota
	OpCAS    OpType = iota
	OpBatch  OpType = iota
)

type KVEntry struct {
	Key            []byte
	Value          []byte
	CreateRevision int64
	ModRevision    int64
	Version        int64
}

type Command struct {
	Type OpType

	Key      []byte
	Value    []byte
	TTLMs    int64
	Expected []byte

	Puts    []PutOp
	Deletes []DeleteOp
}

type PutOp struct {
	Key   []byte
	Value []byte
}

type DeleteOp struct {
	Key []byte
}

type Result struct {
	Err      error
	PrevKV   *KVEntry
	KV       *KVEntry
	Succeeded bool
	Revision  int64
}

type WatchEvent struct {
	Type     EventType
	KV       KVEntry
	PrevKV   *KVEntry
	Revision int64
}

type EventType uint8

const (
	EventPut    EventType = iota
	EventDelete EventType = iota
)

type Watcher struct {
	Key    []byte
	EndKey []byte
	Ch     chan WatchEvent
}

// KVStateMachine is the deterministic application state machine.
// It must produce identical results on every replica for the same
// sequence of applied log entries.
type KVStateMachine struct {
	mu       sync.RWMutex
	data     map[string]*KVEntry
	revision int64

	watchMu  sync.RWMutex
	watchers []*Watcher
}

func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{
		data: make(map[string]*KVEntry),
	}
}

// Apply executes committed Raft log entries against the state machine.
// Each call is idempotent for a given (term, index) pair.
func (sm *KVStateMachine) Apply(entries []raft.Entry) []Result {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	results := make([]Result, 0, len(entries))
	for _, e := range entries {
		if e.Type == raft.EntryConfChange {
			results = append(results, Result{})
			continue
		}
		if len(e.Data) == 0 {
			results = append(results, Result{})
			continue
		}
		cmd, err := decodeCommand(e.Data)
		if err != nil {
			results = append(results, Result{Err: fmt.Errorf("statemachine: decode command: %w", err)})
			continue
		}
		sm.revision++
		r := sm.applyCommand(cmd)
		r.Revision = sm.revision
		results = append(results, r)
	}
	return results
}

func (sm *KVStateMachine) applyCommand(cmd Command) Result {
	switch cmd.Type {
	case OpPut:
		return sm.applyPut(cmd.Key, cmd.Value)
	case OpDelete:
		return sm.applyDelete(cmd.Key)
	case OpCAS:
		return sm.applyCAS(cmd.Key, cmd.Expected, cmd.Value)
	case OpBatch:
		return sm.applyBatch(cmd.Puts, cmd.Deletes)
	default:
		return Result{Err: ErrInvalidOperation}
	}
}

func (sm *KVStateMachine) applyPut(key, value []byte) Result {
	k := string(key)
	prev := sm.data[k]
	var prevKV *KVEntry
	if prev != nil {
		cp := *prev
		prevKV = &cp
	}
	var entry *KVEntry
	if prev != nil {
		entry = &KVEntry{
			Key:            key,
			Value:          value,
			CreateRevision: prev.CreateRevision,
			ModRevision:    sm.revision,
			Version:        prev.Version + 1,
		}
	} else {
		entry = &KVEntry{
			Key:            key,
			Value:          value,
			CreateRevision: sm.revision,
			ModRevision:    sm.revision,
			Version:        1,
		}
	}
	sm.data[k] = entry
	sm.notifyWatchers(WatchEvent{
		Type:     EventPut,
		KV:       *entry,
		PrevKV:   prevKV,
		Revision: sm.revision,
	})
	kvcopy := *entry
	return Result{PrevKV: prevKV, KV: &kvcopy}
}

func (sm *KVStateMachine) applyDelete(key []byte) Result {
	k := string(key)
	prev := sm.data[k]
	if prev == nil {
		return Result{}
	}
	delete(sm.data, k)
	prevCopy := *prev
	sm.notifyWatchers(WatchEvent{
		Type:     EventDelete,
		KV:       prevCopy,
		Revision: sm.revision,
	})
	return Result{PrevKV: &prevCopy}
}

func (sm *KVStateMachine) applyCAS(key, expected, newValue []byte) Result {
	k := string(key)
	current := sm.data[k]
	if current == nil {
		if expected != nil {
			return Result{Err: ErrCASFailed, Succeeded: false}
		}
		r := sm.applyPut(key, newValue)
		r.Succeeded = true
		return r
	}
	if !bytes.Equal(current.Value, expected) {
		kvcopy := *current
		return Result{Err: ErrCASFailed, Succeeded: false, KV: &kvcopy}
	}
	r := sm.applyPut(key, newValue)
	r.Succeeded = true
	return r
}

func (sm *KVStateMachine) applyBatch(puts []PutOp, deletes []DeleteOp) Result {
	for _, p := range puts {
		sm.applyPut(p.Key, p.Value)
	}
	for _, d := range deletes {
		sm.applyDelete(d.Key)
	}
	return Result{Succeeded: true}
}

// Get reads a value directly from the state machine. Only safe for
// stale reads; linearizable reads must go through the ReadIndex protocol.
func (sm *KVStateMachine) Get(key []byte) (*KVEntry, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	e := sm.data[string(key)]
	if e == nil {
		return nil, ErrKeyNotFound
	}
	cp := *e
	return &cp, nil
}

// Range returns all entries in [start, end). If end is nil, returns
// entries matching the exact start key only.
func (sm *KVStateMachine) Range(start, end []byte, limit int64) ([]KVEntry, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	var results []KVEntry
	for k, v := range sm.data {
		kb := []byte(k)
		if bytes.Compare(kb, start) < 0 {
			continue
		}
		if end != nil && bytes.Compare(kb, end) >= 0 {
			continue
		}
		results = append(results, *v)
		if limit > 0 && int64(len(results)) >= limit {
			break
		}
	}
	return results, nil
}

// Revision returns the current revision (number of mutations applied).
func (sm *KVStateMachine) Revision() int64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.revision
}

// Watch registers a watcher for keys in [key, endKey). If endKey is nil,
// only exact matches on key are reported.
func (sm *KVStateMachine) Watch(key, endKey []byte, ch chan WatchEvent) *Watcher {
	w := &Watcher{Key: key, EndKey: endKey, Ch: ch}
	sm.watchMu.Lock()
	sm.watchers = append(sm.watchers, w)
	sm.watchMu.Unlock()
	return w
}

// CancelWatch removes a previously registered watcher.
func (sm *KVStateMachine) CancelWatch(w *Watcher) {
	sm.watchMu.Lock()
	defer sm.watchMu.Unlock()
	for i, watcher := range sm.watchers {
		if watcher == w {
			sm.watchers = append(sm.watchers[:i], sm.watchers[i+1:]...)
			return
		}
	}
}

func (sm *KVStateMachine) notifyWatchers(event WatchEvent) {
	sm.watchMu.RLock()
	ws := make([]*Watcher, len(sm.watchers))
	copy(ws, sm.watchers)
	sm.watchMu.RUnlock()

	for _, w := range ws {
		if !watcherMatches(w, event.KV.Key) {
			continue
		}
		select {
		case w.Ch <- event:
		default:
		}
	}
}

func watcherMatches(w *Watcher, key []byte) bool {
	if bytes.Compare(key, w.Key) < 0 {
		return false
	}
	if w.EndKey != nil && bytes.Compare(key, w.EndKey) >= 0 {
		return false
	}
	if w.EndKey == nil && !bytes.Equal(key, w.Key) {
		return false
	}
	return true
}

// snapshotData is the serialisable form of the entire state machine.
type snapshotData struct {
	Data     map[string]*KVEntry
	Revision int64
}

// Snapshot serialises the current state machine contents for Raft snapshotting.
func (sm *KVStateMachine) Snapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	snap := snapshotData{
		Data:     sm.data,
		Revision: sm.revision,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snap); err != nil {
		return nil, fmt.Errorf("statemachine: snapshot encode: %w", err)
	}
	return buf.Bytes(), nil
}

// Restore replaces the state machine contents from a snapshot.
func (sm *KVStateMachine) Restore(data []byte) error {
	var snap snapshotData
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&snap); err != nil {
		return fmt.Errorf("statemachine: snapshot decode: %w", err)
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.data = snap.Data
	sm.revision = snap.Revision
	return nil
}

// EncodeCommand serialises a command for inclusion in a Raft log entry.
func EncodeCommand(cmd Command) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(byte(cmd.Type))
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		return nil, fmt.Errorf("statemachine: encode command: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeCommand(data []byte) (Command, error) {
	if len(data) < 1 {
		return Command{}, fmt.Errorf("statemachine: empty command data")
	}
	var cmd Command
	if err := gob.NewDecoder(bytes.NewReader(data[1:])).Decode(&cmd); err != nil {
		return cmd, fmt.Errorf("statemachine: decode command: %w", err)
	}
	return cmd, nil
}

// encodeUint64 / decodeUint64 are helpers for key encoding.
func encodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
