// Package verification implements linearizability checking for the Phalanx KV store.
//
// Linearizability (Herlihy & Wing 1990): a history is linearizable if there exists
// a sequential permutation of completed operations that:
//   1. Respects real-time ordering (if op A completes before op B starts, A precedes B)
//   2. Is consistent with the sequential specification of the object (KV semantics)
//
// Why it matters: a cluster that claims strong consistency but allows a client to
// observe value V2 followed by V1 (where V1 was written before V2) is not linearizable.
// This is the bug that haunts every distributed system that hasn't been verified.
//
// Common implementation mistakes:
//   - Testing with synchronized clocks only (hides real-time ordering violations)
//   - Not recording concurrent operations (misses interleaving bugs)
//   - Using eventual-consistency semantics and calling it "linearizable"
//   - Not testing read-after-write in the presence of leader failover
package verification

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// OpType classifies the KV operation in a history entry.
type OpType uint8

const (
	OpWrite OpType = iota // PUT or DELETE
	OpRead  OpType = iota // GET
	OpCAS   OpType = iota // compare-and-swap
)

func (o OpType) String() string {
	switch o {
	case OpWrite:
		return "Write"
	case OpRead:
		return "Read"
	case OpCAS:
		return "CAS"
	}
	return "Unknown"
}

// CallEvent records the start of an operation.
type CallEvent struct {
	ID        uint64    // unique operation ID
	ClientID  uint64    // which client issued this
	Op        OpType
	Key       string
	WriteVal  string    // for PUT: value being written; for CAS: new value
	ExpectVal string    // for CAS: expected current value
	StartTime time.Time
}

// ReturnEvent records the completion of an operation.
type ReturnEvent struct {
	ID       uint64    // matches CallEvent.ID
	ReadVal  string    // value returned by GET (empty for writes)
	Success  bool      // false for CAS that failed, or error return
	EndTime  time.Time
}

// Operation is a matched call+return pair representing a completed operation.
// Concurrent operations may have overlapping [Start, End] intervals.
type Operation struct {
	ID        uint64
	ClientID  uint64
	Op        OpType
	Key       string
	WriteVal  string
	ExpectVal string
	ReadVal   string
	Success   bool
	Start     time.Time
	End       time.Time
}

func (o Operation) String() string {
	switch o.Op {
	case OpWrite:
		return fmt.Sprintf("op#%d Write(%q, %q) [%s]",
			o.ID, o.Key, o.WriteVal, o.End.Sub(o.Start).Round(time.Microsecond))
	case OpRead:
		return fmt.Sprintf("op#%d Read(%q)→%q [%s]",
			o.ID, o.Key, o.ReadVal, o.End.Sub(o.Start).Round(time.Microsecond))
	case OpCAS:
		return fmt.Sprintf("op#%d CAS(%q, expect=%q, new=%q)→ok=%v [%s]",
			o.ID, o.Key, o.ExpectVal, o.WriteVal, o.Success,
			o.End.Sub(o.Start).Round(time.Microsecond))
	}
	return fmt.Sprintf("op#%d Unknown", o.ID)
}

// Recorder captures concurrent KV operations with timestamps.
// It is safe for use from multiple goroutines simultaneously.
type Recorder struct {
	mu      sync.Mutex
	calls   map[uint64]CallEvent
	history []Operation
	nextID  atomic.Uint64
}

// NewRecorder creates a new empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{calls: make(map[uint64]CallEvent)}
}

// Begin records the start of an operation and returns a unique call ID.
func (r *Recorder) Begin(clientID uint64, op OpType, key, writeVal, expectVal string) uint64 {
	id := r.nextID.Add(1)
	r.mu.Lock()
	r.calls[id] = CallEvent{
		ID:        id,
		ClientID:  clientID,
		Op:        op,
		Key:       key,
		WriteVal:  writeVal,
		ExpectVal: expectVal,
		StartTime: time.Now(),
	}
	r.mu.Unlock()
	return id
}

// End records the completion of an operation identified by callID.
func (r *Recorder) End(callID uint64, readVal string, success bool) {
	endTime := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	call, ok := r.calls[callID]
	if !ok {
		return
	}
	delete(r.calls, callID)
	r.history = append(r.history, Operation{
		ID:        call.ID,
		ClientID:  call.ClientID,
		Op:        call.Op,
		Key:       call.Key,
		WriteVal:  call.WriteVal,
		ExpectVal: call.ExpectVal,
		ReadVal:   readVal,
		Success:   success,
		Start:     call.StartTime,
		End:       endTime,
	})
}

// History returns a copy of all completed operations, sorted by end time.
func (r *Recorder) History() []Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := make([]Operation, len(r.history))
	copy(h, r.history)
	sort.Slice(h, func(i, j int) bool {
		return h[i].End.Before(h[j].End)
	})
	return h
}

// PendingCount returns how many operations have been started but not finished.
func (r *Recorder) PendingCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

// HistoryByKey returns all operations on a specific key, sorted by end time.
func (r *Recorder) HistoryByKey(key string) []Operation {
	h := r.History()
	var out []Operation
	for _, op := range h {
		if op.Key == key {
			out = append(out, op)
		}
	}
	return out
}
