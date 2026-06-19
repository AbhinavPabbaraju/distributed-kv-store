package pebble

import (
	"bytes"
	"testing"

	"github.com/phalanx-db/phalanx/internal/raft"
)

func TestWALEntryKey_LexicographicOrdering(t *testing.T) {
	// The whole point of big-endian encoding: byte-wise comparison must
	// match numeric (term, index) ordering. This is what makes Pebble
	// range scans over log indices correct.
	// Entries are keyed by index alone; term is irrelevant to ordering.
	cases := []struct {
		name       string
		aT, aI     uint64
		bT, bI     uint64
		aBeforeB   bool
	}{
		{"ascending index", 1, 100, 1, 200, true},
		{"descending index", 1, 200, 1, 100, false},
		{"lower index wins regardless of term", 9, 50, 1, 100, true},
		{"index 0 vs 1", 1, 0, 1, 1, true},
		{"max index", 1, 1 << 62, 1, (1 << 62) + 1, true},
		{"equal index", 5, 50, 9, 50, false}, // same index, different term → equal key
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := walEntryKey(c.aT, c.aI)
			b := walEntryKey(c.bT, c.bI)
			got := bytes.Compare(a, b) < 0
			if got != c.aBeforeB {
				t.Errorf("walEntryKey(%d,%d) < walEntryKey(%d,%d): got %v, want %v",
					c.aT, c.aI, c.bT, c.bI, got, c.aBeforeB)
			}
		})
	}
}

func TestWALEntryKey_HasPrefix(t *testing.T) {
	key := walEntryKey(1, 1)
	if !bytes.HasPrefix(key, prefixWALEntry) {
		t.Errorf("WAL entry key missing prefix: %q", key)
	}
}

func TestKVKey_HasPrefix(t *testing.T) {
	key := kvKey([]byte("user-key"))
	if !bytes.HasPrefix(key, prefixKV) {
		t.Errorf("KV key missing prefix: %q", key)
	}
	// The user key must be recoverable.
	suffix := key[len(prefixKV):]
	if string(suffix) != "user-key" {
		t.Errorf("user key not preserved: got %q, want %q", suffix, "user-key")
	}
}

func TestWALEntryLowerBound_BoundsRange(t *testing.T) {
	// LowerBound(100) must be ≤ any entry key at index 100 across all terms,
	// and the range [LowerBound(100), LowerBound(200)) must contain exactly
	// the entries with index in [100, 200).
	lb100 := walEntryLowerBound(100)
	lb200 := walEntryLowerBound(200)

	// An entry at index 150, any term, should fall in [lb100, lb200).
	entry150 := walEntryKey(1, 150)
	if bytes.Compare(entry150, lb100) < 0 {
		t.Errorf("entry(1,150) should be >= lowerBound(100)")
	}
	if bytes.Compare(entry150, lb200) >= 0 {
		t.Errorf("entry(1,150) should be < lowerBound(200)")
	}
}

func TestEncodeDecodeEntry_RoundTrip(t *testing.T) {
	original := raft.Entry{
		Term:  5,
		Index: 1000,
		Type:  raft.EntryNormal,
		Data:  []byte("test-payload-data"),
	}
	encoded, err := encodeEntry(&original)
	if err != nil {
		t.Fatalf("encodeEntry: %v", err)
	}
	decoded, err := decodeEntry(encoded)
	if err != nil {
		t.Fatalf("decodeEntry: %v", err)
	}
	if decoded.Term != original.Term ||
		decoded.Index != original.Index ||
		decoded.Type != original.Type ||
		string(decoded.Data) != string(original.Data) {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", decoded, original)
	}
}

func TestEncodeDecodeEntry_ConfChange(t *testing.T) {
	original := raft.Entry{
		Term:  3,
		Index: 7,
		Type:  raft.EntryConfChange,
		Data:  []byte{0x01, 0x02, 0x03},
	}
	encoded, _ := encodeEntry(&original)
	decoded, err := decodeEntry(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Type != raft.EntryConfChange {
		t.Errorf("entry type not preserved: got %d, want %d",
			decoded.Type, raft.EntryConfChange)
	}
}

func TestEncodeDecodeHardState_RoundTrip(t *testing.T) {
	original := raft.HardState{Term: 10, Vote: 3, Commit: 500}
	encoded, err := encodeHardState(original)
	if err != nil {
		t.Fatalf("encodeHardState: %v", err)
	}
	decoded, err := decodeHardState(encoded)
	if err != nil {
		t.Fatalf("decodeHardState: %v", err)
	}
	if decoded != original {
		t.Errorf("hardstate round-trip: got %+v, want %+v", decoded, original)
	}
}

func TestNullStorage_ImplementsInterface(t *testing.T) {
	// Compile-time check is in pebble.go; this is a runtime smoke test that
	// the panic message is correct (so a misconfigured deployment fails loudly).
	defer func() {
		r := recover()
		if r == nil {
			t.Error("NullStorage.LastIndex should panic with ErrNoPebble")
		}
		if r != ErrNoPebble {
			t.Errorf("panic value: got %v, want ErrNoPebble", r)
		}
	}()
	var s Storage = NullStorage{}
	s.LastIndex()
}

func TestKeyEncoding_InitCheck(t *testing.T) {
	// The init() function in pebble.go panics if key encoding is broken.
	// If this test runs at all, init() passed. This test documents that
	// dependency explicitly.
	a := walEntryKey(9, 50)  // high term, low index
	b := walEntryKey(1, 100) // low term, high index
	if bytes.Compare(a, b) >= 0 {
		t.Fatal("init() invariant violated: index 50 must sort before index 100 regardless of term")
	}
}
