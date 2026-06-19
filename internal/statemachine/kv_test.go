package statemachine

import (
	"testing"

	"github.com/phalanx-db/phalanx/internal/raft"
)

func applyCmd(t *testing.T, sm *KVStateMachine, cmd Command) Result {
	t.Helper()
	data, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	results := sm.Apply([]raft.Entry{{Type: raft.EntryNormal, Data: data}})
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	return results[0]
}

func TestKVPutAndGet(t *testing.T) {
	sm := NewKVStateMachine()

	r := applyCmd(t, sm, Command{Type: OpPut, Key: []byte("foo"), Value: []byte("bar")})
	if r.Err != nil {
		t.Fatalf("PUT failed: %v", r.Err)
	}
	if r.PrevKV != nil {
		t.Errorf("first PUT should have nil PrevKV, got %+v", r.PrevKV)
	}
	if r.Revision != 1 {
		t.Errorf("first PUT revision: got %d, want 1", r.Revision)
	}

	got, err := sm.Get([]byte("foo"))
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if string(got.Value) != "bar" {
		t.Errorf("GET value: got %q, want %q", got.Value, "bar")
	}
	if got.Version != 1 {
		t.Errorf("version after first PUT: got %d, want 1", got.Version)
	}
	if got.CreateRevision != 1 {
		t.Errorf("create_revision: got %d, want 1", got.CreateRevision)
	}
}

func TestKVPutOverwrite(t *testing.T) {
	sm := NewKVStateMachine()

	applyCmd(t, sm, Command{Type: OpPut, Key: []byte("k"), Value: []byte("v1")})
	r := applyCmd(t, sm, Command{Type: OpPut, Key: []byte("k"), Value: []byte("v2")})

	if r.PrevKV == nil || string(r.PrevKV.Value) != "v1" {
		t.Errorf("overwrite should return prev value v1, got %+v", r.PrevKV)
	}

	got, _ := sm.Get([]byte("k"))
	if string(got.Value) != "v2" {
		t.Errorf("after overwrite: got %q, want %q", got.Value, "v2")
	}
	if got.Version != 2 {
		t.Errorf("version after overwrite: got %d, want 2", got.Version)
	}
	if got.CreateRevision != 1 {
		t.Errorf("create_revision unchanged: got %d, want 1", got.CreateRevision)
	}
	if got.ModRevision != 2 {
		t.Errorf("mod_revision after update: got %d, want 2", got.ModRevision)
	}
}

func TestKVDelete(t *testing.T) {
	sm := NewKVStateMachine()

	applyCmd(t, sm, Command{Type: OpPut, Key: []byte("k"), Value: []byte("v")})
	r := applyCmd(t, sm, Command{Type: OpDelete, Key: []byte("k")})

	if r.PrevKV == nil || string(r.PrevKV.Value) != "v" {
		t.Errorf("delete should return prev KV, got %+v", r.PrevKV)
	}

	_, err := sm.Get([]byte("k"))
	if err != ErrKeyNotFound {
		t.Errorf("after delete: want ErrKeyNotFound, got %v", err)
	}
}

func TestKVDeleteNonExistent(t *testing.T) {
	sm := NewKVStateMachine()
	r := applyCmd(t, sm, Command{Type: OpDelete, Key: []byte("ghost")})
	if r.Err != nil {
		t.Errorf("deleting non-existent key should not error, got %v", r.Err)
	}
	if r.PrevKV != nil {
		t.Errorf("deleting non-existent key should return nil PrevKV")
	}
}

func TestKVCompareAndSwapSuccess(t *testing.T) {
	sm := NewKVStateMachine()

	applyCmd(t, sm, Command{Type: OpPut, Key: []byte("k"), Value: []byte("old")})
	r := applyCmd(t, sm, Command{
		Type:     OpCAS,
		Key:      []byte("k"),
		Expected: []byte("old"),
		Value:    []byte("new"),
	})

	if r.Err != nil && r.Err != ErrCASFailed {
		t.Fatalf("CAS error: %v", r.Err)
	}
	if !r.Succeeded {
		t.Error("CAS should succeed when expected value matches")
	}

	got, _ := sm.Get([]byte("k"))
	if string(got.Value) != "new" {
		t.Errorf("after successful CAS: got %q, want %q", got.Value, "new")
	}
}

func TestKVCompareAndSwapFailure(t *testing.T) {
	sm := NewKVStateMachine()

	applyCmd(t, sm, Command{Type: OpPut, Key: []byte("k"), Value: []byte("current")})
	r := applyCmd(t, sm, Command{
		Type:     OpCAS,
		Key:      []byte("k"),
		Expected: []byte("wrong"),
		Value:    []byte("new"),
	})

	if r.Succeeded {
		t.Error("CAS should fail when expected value does not match")
	}
	if r.Err != ErrCASFailed {
		t.Errorf("CAS failure should return ErrCASFailed, got %v", r.Err)
	}

	got, _ := sm.Get([]byte("k"))
	if string(got.Value) != "current" {
		t.Errorf("value should be unchanged after failed CAS: got %q", got.Value)
	}
}

func TestKVCASCreateIfAbsent(t *testing.T) {
	sm := NewKVStateMachine()

	r := applyCmd(t, sm, Command{
		Type:     OpCAS,
		Key:      []byte("new-key"),
		Expected: nil,
		Value:    []byte("initial"),
	})

	if !r.Succeeded {
		t.Error("CAS with nil expected on absent key should succeed (create-if-absent)")
	}
	got, _ := sm.Get([]byte("new-key"))
	if string(got.Value) != "initial" {
		t.Errorf("after create-if-absent CAS: got %q, want %q", got.Value, "initial")
	}
}

func TestKVBatchWrite(t *testing.T) {
	sm := NewKVStateMachine()

	applyCmd(t, sm, Command{Type: OpPut, Key: []byte("existing"), Value: []byte("to-delete")})

	r := applyCmd(t, sm, Command{
		Type: OpBatch,
		Puts: []PutOp{
			{Key: []byte("a"), Value: []byte("1")},
			{Key: []byte("b"), Value: []byte("2")},
		},
		Deletes: []DeleteOp{
			{Key: []byte("existing")},
		},
	})

	if r.Err != nil {
		t.Fatalf("batch write failed: %v", r.Err)
	}

	if v, _ := sm.Get([]byte("a")); string(v.Value) != "1" {
		t.Errorf("batch PUT a: got %q, want 1", v.Value)
	}
	if v, _ := sm.Get([]byte("b")); string(v.Value) != "2" {
		t.Errorf("batch PUT b: got %q, want 2", v.Value)
	}
	if _, err := sm.Get([]byte("existing")); err != ErrKeyNotFound {
		t.Error("batch DELETE should have removed 'existing'")
	}
}

func TestKVRevisionMonotonicallyIncreases(t *testing.T) {
	sm := NewKVStateMachine()
	var lastRev int64
	for i := 0; i < 10; i++ {
		r := applyCmd(t, sm, Command{
			Type:  OpPut,
			Key:   []byte("k"),
			Value: []byte("v"),
		})
		if r.Revision <= lastRev {
			t.Errorf("revision must increase: got %d after %d", r.Revision, lastRev)
		}
		lastRev = r.Revision
	}
}

func TestKVWatchNotifiesPut(t *testing.T) {
	sm := NewKVStateMachine()
	ch := make(chan WatchEvent, 4)
	w := sm.Watch([]byte("watched-key"), nil, ch)
	defer sm.CancelWatch(w)

	applyCmd(t, sm, Command{Type: OpPut, Key: []byte("watched-key"), Value: []byte("v1")})
	applyCmd(t, sm, Command{Type: OpPut, Key: []byte("other-key"), Value: []byte("ignored")})
	applyCmd(t, sm, Command{Type: OpPut, Key: []byte("watched-key"), Value: []byte("v2")})

	if len(ch) != 2 {
		t.Errorf("expected 2 watch events (2 puts on watched-key), got %d", len(ch))
	}
	ev1 := <-ch
	if ev1.Type != EventPut || string(ev1.KV.Value) != "v1" {
		t.Errorf("first event: got type=%d value=%q, want PUT v1", ev1.Type, ev1.KV.Value)
	}
}

func TestKVWatchRangeNotifies(t *testing.T) {
	sm := NewKVStateMachine()
	ch := make(chan WatchEvent, 8)
	w := sm.Watch([]byte("prefix/"), []byte("prefix0"), ch)
	defer sm.CancelWatch(w)

	applyCmd(t, sm, Command{Type: OpPut, Key: []byte("prefix/a"), Value: []byte("1")})
	applyCmd(t, sm, Command{Type: OpPut, Key: []byte("prefix/b"), Value: []byte("2")})
	applyCmd(t, sm, Command{Type: OpPut, Key: []byte("other"), Value: []byte("3")})

	if len(ch) != 2 {
		t.Errorf("range watch: expected 2 events, got %d", len(ch))
	}
}

func TestKVSnapshotAndRestore(t *testing.T) {
	sm := NewKVStateMachine()

	for i := 0; i < 100; i++ {
		applyCmd(t, sm, Command{
			Type:  OpPut,
			Key:   []byte{byte(i)},
			Value: []byte{byte(i * 2)},
		})
	}

	snapData, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snapData) == 0 {
		t.Fatal("snapshot data should not be empty")
	}

	sm2 := NewKVStateMachine()
	if err := sm2.Restore(snapData); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if sm2.Revision() != sm.Revision() {
		t.Errorf("revision after restore: got %d, want %d", sm2.Revision(), sm.Revision())
	}

	for i := 0; i < 100; i++ {
		orig, _ := sm.Get([]byte{byte(i)})
		restored, err := sm2.Get([]byte{byte(i)})
		if err != nil {
			t.Errorf("key %d missing after restore", i)
			continue
		}
		if string(orig.Value) != string(restored.Value) {
			t.Errorf("key %d: orig %q, restored %q", i, orig.Value, restored.Value)
		}
	}
}

func TestKVEmptyEntriesSkipped(t *testing.T) {
	sm := NewKVStateMachine()
	results := sm.Apply([]raft.Entry{
		{Type: raft.EntryNormal, Data: nil},
		{Type: raft.EntryConfChange, Data: []byte("ignored")},
	})
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Err != nil {
			t.Errorf("empty/conf entries should not error: %v", r.Err)
		}
	}
	if sm.Revision() != 0 {
		t.Errorf("revision should not change for no-op entries, got %d", sm.Revision())
	}
}
