package verification

import (
	"fmt"
	"strings"
)

// CheckResult summarises whether a history is linearizable.
type CheckResult struct {
	Linearizable bool
	// When not linearizable, Witness contains the problematic operations.
	Witness []Operation
	// Explanation is a human-readable description of the violation.
	Explanation string
}

func (cr CheckResult) String() string {
	if cr.Linearizable {
		return "LINEARIZABLE ✓"
	}
	var sb strings.Builder
	sb.WriteString("NOT LINEARIZABLE ✗\n")
	sb.WriteString(cr.Explanation)
	sb.WriteString("\nWitness operations:\n")
	for _, op := range cr.Witness {
		sb.WriteString("  " + op.String() + "\n")
	}
	return sb.String()
}

// Check verifies whether history is linearizable under KV semantics.
//
// Algorithm: WGL (Wing, Gong, Lam 1993), extended for CAS operations.
// The algorithm is exponential in the worst case but runs in O(n²) on
// typical test histories because real-time order prunes the search space.
//
// The KV specification:
//   - Write(k, v): always succeeds; updates state[k] = v
//   - Read(k) → v: succeeds iff state[k] == v at the linearization point
//   - CAS(k, expected, new) → ok: ok iff state[k] == expected at the point;
//     if ok, updates state[k] = new
//
// Safety: this checker only considers completed operations. In-flight
// operations at the time of a check are ignored (they may or may not
// have taken effect on the server).
func Check(history []Operation) CheckResult {
	if len(history) == 0 {
		return CheckResult{Linearizable: true}
	}

	// Per-key checking: since keys are independent in a KV store, a history
	// is linearizable iff each per-key sub-history is linearizable.
	// This reduces complexity from O(n!) to O(k × (n/k)!) where k = key count.
	byKey := groupByKey(history)
	for key, ops := range byKey {
		result := checkPerKey(ops)
		if !result.Linearizable {
			result.Explanation = fmt.Sprintf("key %q: %s", key, result.Explanation)
			return result
		}
	}
	return CheckResult{Linearizable: true}
}

// kvState is the sequential state used during the linearization search.
type kvState struct {
	values map[string]string // current value of each key
}

func newKVState() kvState {
	return kvState{values: make(map[string]string)}
}

func (s kvState) clone() kvState {
	c := kvState{values: make(map[string]string, len(s.values))}
	for k, v := range s.values {
		c.values[k] = v
	}
	return c
}

// apply checks whether op is consistent with state s, and if so, returns
// the new state after applying op.
func (s kvState) apply(op Operation) (kvState, bool) {
	switch op.Op {
	case OpWrite:
		next := s.clone()
		next.values[op.Key] = op.WriteVal
		return next, true

	case OpRead:
		current := s.values[op.Key]
		if current != op.ReadVal {
			return s, false
		}
		return s, true

	case OpCAS:
		current := s.values[op.Key]
		if op.Success {
			// CAS succeeded: current must have equalled expected, and now == new.
			if current != op.ExpectVal {
				return s, false
			}
			next := s.clone()
			next.values[op.Key] = op.WriteVal
			return next, true
		}
		// CAS failed: current must not equal expected.
		if current == op.ExpectVal {
			return s, false
		}
		return s, true
	}
	return s, true
}

// checkPerKey runs the WGL checker on a single-key operation history.
// It uses backtracking with real-time pruning to bound the search.
func checkPerKey(ops []Operation) CheckResult {
	state := newKVState()
	ok, witness := wgl(ops, state, nil)
	if ok {
		return CheckResult{Linearizable: true}
	}
	return CheckResult{
		Linearizable: false,
		Witness:      witness,
		Explanation:  "no valid linearization exists",
	}
}

// wgl is the recursive WGL linearizability checker.
// remaining: operations not yet placed in the linearization.
// state:     current sequential KV state.
// prefix:    operations placed so far (for witness reporting).
func wgl(remaining []Operation, state kvState, prefix []Operation) (bool, []Operation) {
	if len(remaining) == 0 {
		return true, nil
	}

	// Find all "minimal" operations — those with no completed operation that
	// ends before they start. These are the candidates for the next position
	// in the linearization.
	for i, candidate := range remaining {
		if !isMinimal(candidate, remaining) {
			continue
		}
		// Try placing candidate at the current position.
		newState, consistent := state.apply(candidate)
		if !consistent {
			continue
		}
		// Recurse with candidate removed from remaining.
		rest := removeAt(remaining, i)
		if ok, _ := wgl(rest, newState, append(prefix, candidate)); ok {
			return true, nil
		}
		// Recursive failure: this candidate doesn't lead to a valid
		// linearization. Continue and try the next minimal candidate.
	}

	// All minimal candidates exhausted without finding a valid linearization.
	return false, append(prefix, remaining...)
}

// isMinimal returns true if no other operation in ops ends strictly before
// candidate starts. "Minimal" operations are candidates for being first in
// a linearization.
func isMinimal(candidate Operation, ops []Operation) bool {
	for _, other := range ops {
		if other.ID == candidate.ID {
			continue
		}
		// other ends before candidate starts → other MUST precede candidate
		if other.End.Before(candidate.Start) {
			return false
		}
	}
	return true
}

// removeAt returns a new slice with the element at index i removed.
func removeAt(ops []Operation, i int) []Operation {
	result := make([]Operation, 0, len(ops)-1)
	result = append(result, ops[:i]...)
	result = append(result, ops[i+1:]...)
	return result
}

func groupByKey(ops []Operation) map[string][]Operation {
	m := make(map[string][]Operation)
	for _, op := range ops {
		m[op.Key] = append(m[op.Key], op)
	}
	return m
}

// Report generates a human-readable linearizability summary for a history.
func Report(history []Operation) string {
	var sb strings.Builder
	result := Check(history)
	sb.WriteString(fmt.Sprintf("=== Linearizability Report (%d operations) ===\n", len(history)))
	sb.WriteString(result.String())
	sb.WriteString("\n")
	byKey := groupByKey(history)
	sb.WriteString(fmt.Sprintf("Keys touched: %d\n", len(byKey)))
	for key, ops := range byKey {
		writes, reads, cas := 0, 0, 0
		for _, op := range ops {
			switch op.Op {
			case OpWrite:
				writes++
			case OpRead:
				reads++
			case OpCAS:
				cas++
			}
		}
		sb.WriteString(fmt.Sprintf("  %q: %d writes %d reads %d cas\n", key, writes, reads, cas))
	}
	return sb.String()
}
