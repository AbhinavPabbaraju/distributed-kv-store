package verification

import (
	"fmt"
	"testing"
	"time"
)

func makeOp(id uint64, op OpType, key, writeVal, readVal, expectVal string, start, end time.Time, success bool) Operation {
	return Operation{
		ID:        id,
		ClientID:  1,
		Op:        op,
		Key:       key,
		WriteVal:  writeVal,
		ExpectVal: expectVal,
		ReadVal:   readVal,
		Success:   success,
		Start:     start,
		End:       end,
	}
}

func t0(offset time.Duration) time.Time {
	return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(offset)
}

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

// TestLinearizable_SimpleWriteRead verifies the trivial linearizable history:
//   1. Write(k, "v1")          [0ms, 10ms]
//   2. Read(k) → "v1"          [12ms, 20ms]  (sequential, after write)
func TestLinearizable_SimpleWriteRead(t *testing.T) {
	history := []Operation{
		makeOp(1, OpWrite, "k", "v1", "", "", t0(0), t0(ms(10)), true),
		makeOp(2, OpRead, "k", "", "v1", "", t0(ms(12)), t0(ms(20)), true),
	}
	result := Check(history)
	if !result.Linearizable {
		t.Errorf("simple write-then-read must be linearizable: %s", result)
	}
}

// TestLinearizable_ConcurrentReads verifies that reads concurrent with a write
// may return either the old or new value.
func TestLinearizable_ConcurrentReads(t *testing.T) {
	// Write v2 is concurrent with both reads.
	// Read1 returns "" (old), Read2 returns "v2" (new). Both are valid.
	history := []Operation{
		makeOp(1, OpWrite, "k", "v2", "", "", t0(0), t0(ms(30)), true),
		makeOp(2, OpRead, "k", "", "", "", t0(ms(5)), t0(ms(15)), true),  // saw old value (empty)
		makeOp(3, OpRead, "k", "", "v2", "", t0(ms(20)), t0(ms(35)), true), // saw new value
	}
	result := Check(history)
	if !result.Linearizable {
		t.Errorf("concurrent reads with mixed results must be linearizable: %s", result)
	}
}

// TestNotLinearizable_StaleRead is the canonical non-linearizable history:
//   1. Write(k, "v1") completes at t=10
//   2. Read(k) starts at t=15 (after write completed) but returns ""
//
// Since the write completed strictly before the read started, the linearization
// must place Write before Read — but Read returns the pre-write value. Violation.
func TestNotLinearizable_StaleRead(t *testing.T) {
	history := []Operation{
		makeOp(1, OpWrite, "k", "v1", "", "", t0(0), t0(ms(10)), true),
		// Read starts AFTER write completes but returns empty — impossible.
		makeOp(2, OpRead, "k", "", "", "", t0(ms(15)), t0(ms(25)), true),
	}
	result := Check(history)
	if result.Linearizable {
		t.Error("stale read after completed write must NOT be linearizable")
	}
}

// TestNotLinearizable_TimeTravel is the most famous non-linearizable history:
//   C1: Write(k, "v2")                    [0ms, 20ms]
//   C2:          Read(k) → "v2"     [5ms, 15ms]   (saw new value mid-write)
//   C3:                   Read(k) → "v1"   [25ms, 35ms]  (went back in time!)
//
// C3's read starts after BOTH C1 and C2 finished. C2 already observed "v2".
// C3 returning "v1" means it observed a state BEFORE C2's observation — impossible.
func TestNotLinearizable_TimeTravel(t *testing.T) {
	history := []Operation{
		makeOp(1, OpWrite, "k", "v2", "", "", t0(0), t0(ms(20)), true),
		makeOp(2, OpRead, "k", "", "v2", "", t0(ms(5)), t0(ms(15)), true),
		// Starts after everything completed, but reads old value
		makeOp(3, OpRead, "k", "", "v1", "", t0(ms(25)), t0(ms(35)), true),
	}
	result := Check(history)
	if result.Linearizable {
		t.Error("time-travel read must NOT be linearizable")
	}
}

// TestLinearizable_CAS_Success verifies a successful CAS in a linearizable history.
func TestLinearizable_CAS_Success(t *testing.T) {
	history := []Operation{
		makeOp(1, OpWrite, "k", "v1", "", "", t0(0), t0(ms(10)), true),
		makeOp(2, OpCAS, "k", "v2", "", "v1", t0(ms(15)), t0(ms(25)), true), // CAS v1→v2, succeeded
		makeOp(3, OpRead, "k", "", "v2", "", t0(ms(30)), t0(ms(40)), true),
	}
	result := Check(history)
	if !result.Linearizable {
		t.Errorf("sequential write-CAS-read must be linearizable: %s", result)
	}
}

// TestLinearizable_CAS_Failure verifies a failed CAS in a linearizable history.
func TestLinearizable_CAS_Failure(t *testing.T) {
	history := []Operation{
		makeOp(1, OpWrite, "k", "v1", "", "", t0(0), t0(ms(10)), true),
		// CAS expecting "wrong" — must fail because current is "v1"
		makeOp(2, OpCAS, "k", "v2", "", "wrong", t0(ms(15)), t0(ms(25)), false),
		// Read must still see "v1"
		makeOp(3, OpRead, "k", "", "v1", "", t0(ms(30)), t0(ms(40)), true),
	}
	result := Check(history)
	if !result.Linearizable {
		t.Errorf("write-failedCAS-read must be linearizable: %s", result)
	}
}

// TestLinearizable_MultiKey verifies that independent keys are checked separately.
func TestLinearizable_MultiKey(t *testing.T) {
	history := []Operation{
		makeOp(1, OpWrite, "a", "1", "", "", t0(0), t0(ms(10)), true),
		makeOp(2, OpWrite, "b", "2", "", "", t0(5), t0(ms(15)), true),
		makeOp(3, OpRead, "a", "", "1", "", t0(ms(20)), t0(ms(30)), true),
		makeOp(4, OpRead, "b", "", "2", "", t0(ms(20)), t0(ms(30)), true),
	}
	result := Check(history)
	if !result.Linearizable {
		t.Errorf("independent-key history must be linearizable: %s", result)
	}
}

// TestLinearizable_ConcurrentWrites checks that concurrent writes where reads
// observe a consistent ordering are linearizable.
func TestLinearizable_ConcurrentWrites(t *testing.T) {
	// Both writes are concurrent; reader sees v2. This is valid if we place
	// Write(v1) before Write(v2) in the linearization.
	history := []Operation{
		makeOp(1, OpWrite, "k", "v1", "", "", t0(0), t0(ms(20)), true),
		makeOp(2, OpWrite, "k", "v2", "", "", t0(5), t0(ms(25)), true),
		makeOp(3, OpRead, "k", "", "v2", "", t0(ms(30)), t0(ms(40)), true),
	}
	result := Check(history)
	if !result.Linearizable {
		t.Errorf("concurrent-writes-then-consistent-read must be linearizable: %s", result)
	}
}

// TestNotLinearizable_ConcurrentWrites_Disagreement is non-linearizable.
//
// Construction: Write(v1) ends at 5ms, Read3 starts at 8ms.
// Therefore Write(v1) MUST precede Read3 in any valid linearization (real-time).
// For Read3 to return "v2", Write(v2) must also precede Read3.
// So both writes must precede Read3. Only two orderings:
//   (a) v1 → v2 → Read3(v2) → Read4:  after v2 state=v2, Read4(v1) fails.
//   (b) v2 → v1 → Read3(v2):          after v1 state=v1, Read3(v2) fails.
// No valid linearization exists — verified non-linearizable.
func TestNotLinearizable_ConcurrentWrites_Disagreement(t *testing.T) {
	history := []Operation{
		// Write(v1) ends before Read3 starts → real-time constraint: v1 before Read3.
		makeOp(1, OpWrite, "k", "v1", "", "", t0(0), t0(ms(5)), true),
		// Write(v2) is concurrent with Read3 so its position is flexible.
		makeOp(2, OpWrite, "k", "v2", "", "", t0(0), t0(ms(20)), true),
		// Read3 starts at 8ms (after v1 ends) and must see v2.
		makeOp(3, OpRead, "k", "", "v2", "", t0(ms(8)), t0(ms(15)), true),
		// Read4 starts after everything and must see v1 — impossible given above.
		makeOp(4, OpRead, "k", "", "v1", "", t0(ms(25)), t0(ms(35)), true),
	}
	result := Check(history)
	if result.Linearizable {
		t.Error("history must be non-linearizable: Write(v1) ends before Read3 starts, " +
			"so both writes precede Read3, but then Read4 cannot regress to v1")
	}
}

// TestRecorder_BasicWorkflow tests the Recorder API.
func TestRecorder_BasicWorkflow(t *testing.T) {
	r := NewRecorder()

	id1 := r.Begin(1, OpWrite, "foo", "bar", "")
	id2 := r.Begin(1, OpRead, "foo", "", "")
	r.End(id1, "", true)
	r.End(id2, "bar", true)

	h := r.History()
	if len(h) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(h))
	}
}

// TestRecorder_PendingOps tests that pending (unfinished) ops are not in History.
func TestRecorder_PendingOps(t *testing.T) {
	r := NewRecorder()
	r.Begin(1, OpWrite, "k", "v", "")
	id2 := r.Begin(1, OpRead, "k", "", "")
	r.End(id2, "v", true)

	if r.PendingCount() != 1 {
		t.Errorf("expected 1 pending op, got %d", r.PendingCount())
	}
	h := r.History()
	if len(h) != 1 {
		t.Errorf("expected 1 completed op, got %d", len(h))
	}
}

// TestLargeLinearizableHistory stress-tests the checker with many operations.
func TestLargeLinearizableHistory(t *testing.T) {
	var history []Operation
	currentVal := ""
	for i := 0; i < 50; i++ {
		newVal := fmt.Sprintf("v%d", i)
		writeStart := t0(time.Duration(i*20) * time.Millisecond)
		writeEnd := writeStart.Add(5 * time.Millisecond)
		history = append(history, makeOp(
			uint64(i*2+1), OpWrite, "k", newVal, "", "",
			writeStart, writeEnd, true,
		))
		readStart := writeEnd.Add(2 * time.Millisecond)
		readEnd := readStart.Add(3 * time.Millisecond)
		history = append(history, makeOp(
			uint64(i*2+2), OpRead, "k", "", newVal, "",
			readStart, readEnd, true,
		))
		currentVal = newVal
	}
	_ = currentVal

	result := Check(history)
	if !result.Linearizable {
		t.Errorf("sequential write-read chain must be linearizable: %s", result)
	}
}

// TestReport verifies the report formatter doesn't panic.
func TestReport(t *testing.T) {
	history := []Operation{
		makeOp(1, OpWrite, "k", "v1", "", "", t0(0), t0(ms(10)), true),
		makeOp(2, OpRead, "k", "", "v1", "", t0(ms(15)), t0(ms(25)), true),
	}
	report := Report(history)
	if report == "" {
		t.Error("report should not be empty")
	}
}
