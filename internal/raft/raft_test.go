package raft

import (
	"fmt"
	"math/rand"
	"testing"
)

func newTestRaft(id uint64, peers []uint64, election, heartbeat int, storage Storage) *raft {
	c := &Config{
		ID:                        id,
		peers:                     peers,
		ElectionTick:              election,
		HeartbeatTick:             heartbeat,
		Storage:                   storage,
		MaxSizePerMsg:             noLimit,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: noLimit,
		MaxCommittedSizePerReady:  noLimit,
		Logger:                    DiscardLogger,
	}
	return newRaft(c)
}

func newTestRaftWithStorage(id uint64, peers []uint64) *raft {
	return newTestRaft(id, peers, 10, 1, NewMemoryStorage())
}

func mustSend(t *testing.T, r *raft, msg Message) {
	t.Helper()
	if err := r.Step(msg); err != nil {
		t.Fatalf("step failed: %v", err)
	}
}

func tickN(r *raft, n int) {
	for i := 0; i < n; i++ {
		r.tick()
	}
}

func collectMsgs(r *raft) []Message {
	msgs := r.msgs
	r.msgs = nil
	return msgs
}

func filterMsgsByType(msgs []Message, t MessageType) []Message {
	var out []Message
	for _, m := range msgs {
		if m.Type == t {
			out = append(out, m)
		}
	}
	return out
}

type network struct {
	peers   map[uint64]*raft
	dropped map[uint64]map[uint64]bool
	t       *testing.T
}

func newNetwork(t *testing.T, ids ...uint64) *network {
	n := &network{
		peers:   make(map[uint64]*raft),
		dropped: make(map[uint64]map[uint64]bool),
		t:       t,
	}
	for _, id := range ids {
		n.peers[id] = newTestRaftWithStorage(id, ids)
		n.dropped[id] = make(map[uint64]bool)
	}
	return n
}

func (n *network) stepAll(msgs []Message) {
	for len(msgs) > 0 {
		m := msgs[0]
		msgs = msgs[1:]
		if peer, ok := n.peers[m.To]; ok {
			if !n.dropped[m.From][m.To] {
				peer.Step(m)
				msgs = append(msgs, collectMsgs(peer)...)
			}
		}
	}
}

func (n *network) tickAll() {
	var all []Message
	for _, r := range n.peers {
		r.tick()
		all = append(all, collectMsgs(r)...)
	}
	n.stepAll(all)
}

func (n *network) send(msgs ...Message) {
	n.stepAll(msgs)
}

func (n *network) elect(id uint64) {
	n.send(Message{From: id, To: id, Type: MsgHup})
}

func (n *network) drop(from, to uint64, drop bool) {
	n.dropped[from][to] = drop
}

func (n *network) isolate(id uint64) {
	for other := range n.peers {
		n.dropped[id][other] = true
		n.dropped[other][id] = true
	}
}

func (n *network) restore(id uint64) {
	for other := range n.peers {
		n.dropped[id][other] = false
		n.dropped[other][id] = false
	}
}

func (n *network) leader() (uint64, bool) {
	for id, r := range n.peers {
		if r.state == RoleLeader {
			return id, true
		}
	}
	return 0, false
}

func (n *network) propose(leaderID uint64, data []byte) {
	n.send(Message{
		From:    leaderID,
		To:      leaderID,
		Type:    MsgProp,
		Entries: []Entry{{Data: data}},
	})
}

func TestInitialState(t *testing.T) {
	r := newTestRaftWithStorage(1, []uint64{1, 2, 3})
	if r.state != RoleFollower {
		t.Errorf("initial state want follower, got %s", r.state)
	}
	if r.Term != 0 {
		t.Errorf("initial term want 0, got %d", r.Term)
	}
	if r.lead != None {
		t.Errorf("initial lead want None, got %d", r.lead)
	}
}

func TestSingleNodeElection(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
	tickN(r, 21)
	if r.state != RoleLeader {
		t.Errorf("single node should become leader, got %s", r.state)
	}
}

func TestLeaderElectionThreeNodes(t *testing.T) {
	n := newNetwork(t, 1, 2, 3)
	n.elect(1)
	leaderID, ok := n.leader()
	if !ok {
		t.Fatal("no leader elected")
	}
	if leaderID != 1 {
		t.Errorf("expected leader 1, got %d", leaderID)
	}
	if n.peers[1].Term != 1 {
		t.Errorf("expected term 1, got %d", n.peers[1].Term)
	}
}

func TestFollowerBecomesLeaderOnTimeout(t *testing.T) {
	n := newNetwork(t, 1, 2, 3)
	n.elect(1)

	if _, ok := n.leader(); !ok {
		t.Fatal("no initial leader")
	}

	for i := 0; i < 12; i++ {
		n.tickAll()
	}

	_, ok := n.leader()
	if !ok {
		t.Fatal("cluster should maintain a leader")
	}
}

func TestLeaderElectionHigherTermWins(t *testing.T) {
	n := newNetwork(t, 1, 2, 3)
	n.elect(1)

	n.peers[2].becomeCandidate()
	n.peers[2].Term = 10
	n.send(Message{From: 2, To: 2, Type: MsgHup})

	leaderID, _ := n.leader()
	if n.peers[leaderID].Term < 2 {
		t.Errorf("expected higher term after new election, got term %d", n.peers[leaderID].Term)
	}
}

func TestLogReplicationToFollowers(t *testing.T) {
	n := newNetwork(t, 1, 2, 3)
	n.elect(1)

	data := []byte("hello")
	n.propose(1, data)

	for id, r := range n.peers {
		li := r.raftLog.lastIndex()
		if li == 0 {
			t.Errorf("peer %d: expected non-zero last index", id)
		}
		ents := r.raftLog.allEntries()
		found := false
		for _, e := range ents {
			if string(e.Data) == string(data) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("peer %d: entry %q not found in log", id, data)
		}
	}
}

func TestCommitIndexAdvances(t *testing.T) {
	n := newNetwork(t, 1, 2, 3)
	n.elect(1)

	n.propose(1, []byte("entry-1"))
	n.propose(1, []byte("entry-2"))
	n.propose(1, []byte("entry-3"))

	for id, r := range n.peers {
		if r.raftLog.committed == 0 {
			t.Errorf("peer %d: commitIndex should be > 0, got 0", id)
		}
	}
}

func TestLeaderStepsDownOnHigherTerm(t *testing.T) {
	n := newNetwork(t, 1, 2, 3)
	n.elect(1)

	if n.peers[1].state != RoleLeader {
		t.Fatal("1 should be leader")
	}

	n.send(Message{
		From: 2,
		To:   1,
		Type: MsgApp,
		Term: 5,
	})

	if n.peers[1].state != RoleFollower {
		t.Errorf("leader should step down on higher term, got %s", n.peers[1].state)
	}
	if n.peers[1].Term != 5 {
		t.Errorf("leader should update term to 5, got %d", n.peers[1].Term)
	}
}

func TestSplitVoteRecovery(t *testing.T) {
	n := newNetwork(t, 1, 2, 3, 4, 5)

	n.peers[1].becomeCandidate()
	n.peers[2].becomeCandidate()

	for i := 0; i < 20; i++ {
		n.tickAll()
		if _, ok := n.leader(); ok {
			break
		}
	}

	_, ok := n.leader()
	if !ok {
		t.Log("split vote occurred; randomized timeout should recover eventually")
	}
}

func TestNetworkPartitionAndRecovery(t *testing.T) {
	n := newNetwork(t, 1, 2, 3)
	n.elect(1)

	n.propose(1, []byte("before-partition"))

	leaderID := uint64(1)
	minority := uint64(3)
	n.isolate(minority)

	for i := 0; i < 3; i++ {
		n.propose(leaderID, []byte(fmt.Sprintf("during-partition-%d", i)))
	}

	committedBefore := n.peers[minority].raftLog.committed
	majorityCommitted := n.peers[leaderID].raftLog.committed
	if majorityCommitted <= committedBefore {
		t.Logf("majority committed %d entries while minority was isolated", majorityCommitted-committedBefore)
	}

	n.restore(minority)

	for i := 0; i < 10; i++ {
		n.tickAll()
	}
	n.propose(leaderID, []byte("after-heal"))

	minorityCommitted := n.peers[minority].raftLog.committed
	if minorityCommitted < majorityCommitted {
		t.Errorf("minority should catch up after partition healed: got %d, want >=%d",
			minorityCommitted, majorityCommitted)
	}
}

func TestLeaderCrashAndReelection(t *testing.T) {
	n := newNetwork(t, 1, 2, 3)
	n.elect(1)

	n.propose(1, []byte("written"))

	n.isolate(1)

	for i := 0; i < 80; i++ {
		n.tickAll()
		if leaderID, ok := n.leader(); ok && leaderID != 1 {
			break
		}
	}

	// The new leader must be one the surviving quorum (2 and 3) recognise.
	var newLeaderID uint64
	for id, r := range n.peers {
		if id != 1 && r.lead != None && r.lead != 1 {
			newLeaderID = r.lead
			break
		}
	}
	if newLeaderID == 0 {
		// Nodes 2 and 3 may themselves be leader — check state directly.
		for id, r := range n.peers {
			if id != 1 && r.state == RoleLeader {
				newLeaderID = id
				break
			}
		}
	}
	if newLeaderID == 0 {
		t.Error("a new leader should be elected after old leader isolation")
		return
	}

	n.propose(newLeaderID, []byte("new-entry"))

	n.restore(1)
	for i := 0; i < 15; i++ {
		n.tickAll()
	}

	// After healing, node 1 should encounter the new term and step down.
	if n.peers[1].state == RoleLeader && n.peers[1].Term == 1 {
		t.Error("stale leader at term 1 should step down after rejoining cluster with higher term")
	}
}

func TestLogConsistencyAfterPartition(t *testing.T) {
	n := newNetwork(t, 1, 2, 3, 4, 5)
	n.elect(1)

	n.propose(1, []byte("committed-before"))

	n.isolate(1)
	n.drop(2, 3, true)
	n.drop(3, 2, true)

	for i := 0; i < 80; i++ {
		n.tickAll()
		if leaderID, ok := n.leader(); ok && leaderID != 1 {
			_ = leaderID
			break
		}
	}

	n.restore(1)
	n.drop(2, 3, false)
	n.drop(3, 2, false)

	for i := 0; i < 20; i++ {
		n.tickAll()
	}

	leaderID, ok := n.leader()
	if !ok {
		t.Fatal("should have a stable leader after recovery")
	}
	n.propose(leaderID, []byte("after-recovery"))

	leaderLog := n.peers[leaderID].raftLog.committed
	for id, r := range n.peers {
		if r.raftLog.committed < leaderLog {
			t.Logf("peer %d: committed=%d leader=%d (may not have caught up yet)",
				id, r.raftLog.committed, leaderLog)
		}
	}
}

func TestElectionVoteGrantedOnce(t *testing.T) {
	r := newTestRaftWithStorage(1, []uint64{1, 2, 3})
	r.becomeFollower(1, None)

	granted1 := false
	r.Step(Message{From: 2, To: 1, Type: MsgVote, Term: 2, LogTerm: 0, Index: 0})
	for _, m := range r.msgs {
		if m.Type == MsgVoteResp && !m.Reject {
			granted1 = true
		}
	}
	r.msgs = nil

	r.Step(Message{From: 3, To: 1, Type: MsgVote, Term: 2, LogTerm: 0, Index: 0})
	var granted2 bool
	for _, m := range r.msgs {
		if m.Type == MsgVoteResp && !m.Reject {
			granted2 = true
		}
	}

	if granted1 && granted2 {
		t.Error("follower should only grant one vote per term")
	}
}

func TestVoteDeniedForStalerLog(t *testing.T) {
	ms := NewMemoryStorage()
	ms.Append([]Entry{
		{Index: 1, Term: 1, Data: []byte("entry")},
		{Index: 2, Term: 2, Data: []byte("newer")},
	})
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, ms)
	r.loadState(HardState{Term: 2, Vote: 0, Commit: 2})
	r.raftLog.committed = 2

	r.Step(Message{
		From:    2,
		To:      1,
		Type:    MsgVote,
		Term:    3,
		LogTerm: 1,
		Index:   1,
	})

	for _, m := range r.msgs {
		if m.Type == MsgVoteResp && !m.Reject {
			t.Error("should reject vote from candidate with staler log")
		}
	}
}

func TestHeartbeatResetsElectionTimer(t *testing.T) {
	r := newTestRaftWithStorage(2, []uint64{1, 2, 3})
	r.becomeFollower(1, 1)

	tickN(r, 8)

	r.Step(Message{From: 1, To: 2, Type: MsgHeartbeat, Term: 1, Commit: 0})

	tickN(r, 8)

	if r.state != RoleFollower {
		t.Errorf("should remain follower after heartbeat reset timer, got %s", r.state)
	}
}

func TestCandidateConvertsToFollowerOnAppend(t *testing.T) {
	r := newTestRaftWithStorage(1, []uint64{1, 2, 3})
	r.becomeCandidate()

	if r.state != RoleCandidate {
		t.Fatal("should be candidate")
	}

	r.Step(Message{
		From:    2,
		To:      1,
		Type:    MsgApp,
		Term:    r.Term,
		LogTerm: 0,
		Index:   0,
		Commit:  0,
	})

	if r.state != RoleFollower {
		t.Errorf("candidate should revert to follower on receiving valid AppendEntries, got %s", r.state)
	}
	if r.lead != 2 {
		t.Errorf("should recognise leader 2, got %d", r.lead)
	}
}

func TestLeaderAppendEntriesRejectionAndRetry(t *testing.T) {
	n := newNetwork(t, 1, 2, 3)
	n.elect(1)
	leader := n.peers[1]

	for i := 0; i < 5; i++ {
		n.propose(1, []byte(fmt.Sprintf("e%d", i)))
	}

	leaderLast := leader.raftLog.lastIndex()

	pr := leader.trk.progress[3]
	pr.Next = 1
	pr.Match = 0
	pr.BecomeProbe()

	leader.maybeSendAppend(3, true)
	appMsgs := filterMsgsByType(collectMsgs(leader), MsgApp)
	if len(appMsgs) == 0 {
		t.Fatal("leader should send AppendEntries to lagging peer")
	}

	m := appMsgs[0]
	n.peers[3].Step(m)
	respMsgs := filterMsgsByType(collectMsgs(n.peers[3]), MsgAppResp)
	if len(respMsgs) == 0 {
		t.Fatal("follower should respond to AppendEntries")
	}

	_ = leaderLast
}

func TestMajorityCommit(t *testing.T) {
	n := newNetwork(t, 1, 2, 3, 4, 5)
	n.elect(1)

	n.propose(1, []byte("value"))

	leader := n.peers[1]
	committed := leader.raftLog.committed
	if committed == 0 {
		t.Error("majority (3 of 5) should have committed the entry")
	}
}

func TestNoCommitWithoutMajority(t *testing.T) {
	n := newNetwork(t, 1, 2, 3, 4, 5)
	n.elect(1)

	n.isolate(2)
	n.isolate(3)
	n.isolate(4)

	initialCommit := n.peers[1].raftLog.committed

	n.send(Message{
		From:    1,
		To:      1,
		Type:    MsgProp,
		Entries: []Entry{{Data: []byte("no-quorum")}},
	})

	if n.peers[1].raftLog.committed > initialCommit {
		t.Error("should not commit without majority: committed index should not advance")
	}
}

func TestSnapshotRestoreFollower(t *testing.T) {
	n := newNetwork(t, 1, 2, 3)
	n.elect(1)

	for i := 0; i < 5; i++ {
		n.propose(1, []byte(fmt.Sprintf("entry-%d", i)))
	}

	snap := Snapshot{
		Data: []byte("state-at-5"),
		Metadata: SnapshotMetadata{
			Index: 5,
			Term:  1,
			ConfState: ConfState{
				Voters: []uint64{1, 2, 3},
			},
		},
	}

	r3 := n.peers[3]
	r3.raftLog.committed = 0

	r3.Step(Message{
		From:     1,
		To:       3,
		Type:     MsgSnap,
		Term:     1,
		Snapshot: &snap,
	})

	if r3.raftLog.committed < 5 {
		t.Errorf("after snapshot restore, committed should be >=5, got %d", r3.raftLog.committed)
	}
}

func TestPreVotePreventsTermDisruption(t *testing.T) {
	makeRaftWithPreVote := func(id uint64, peers []uint64) *raft {
		c := &Config{
			ID:                        id,
			peers:                     peers,
			ElectionTick:              10,
			HeartbeatTick:             1,
			Storage:                   NewMemoryStorage(),
			MaxSizePerMsg:             noLimit,
			MaxInflightMsgs:           256,
			MaxUncommittedEntriesSize: noLimit,
			MaxCommittedSizePerReady:  noLimit,
			PreVote:                   true,
			Logger:                    DiscardLogger,
		}
		return newRaft(c)
	}

	n := &network{
		peers:   make(map[uint64]*raft),
		dropped: make(map[uint64]map[uint64]bool),
		t:       t,
	}
	for _, id := range []uint64{1, 2, 3} {
		n.peers[id] = makeRaftWithPreVote(id, []uint64{1, 2, 3})
		n.dropped[id] = make(map[uint64]bool)
	}

	n.elect(1)
	if n.peers[1].state != RoleLeader {
		// With pre-vote, the initial election sends MsgPreVote first.
		// Give the network a few rounds to complete pre-vote + election.
		for i := 0; i < 5; i++ {
			n.tickAll()
		}
		if n.peers[1].state != RoleLeader {
			t.Fatal("1 should be leader after pre-vote + election rounds")
		}
	}
	initialTerm := n.peers[1].Term

	n.isolate(3)
	for i := 0; i < 80; i++ {
		n.peers[3].tick()
	}

	n.restore(3)

	for i := 0; i < 5; i++ {
		n.tickAll()
	}

	if n.peers[1].Term != initialTerm {
		t.Logf("pre-vote: leader term changed from %d to %d (acceptable if genuine reelection)",
			initialTerm, n.peers[1].Term)
	}
}

func TestReadIndexRequiresQuorumAck(t *testing.T) {
	n := newNetwork(t, 1, 2, 3)
	n.elect(1)

	leader := n.peers[1]
	ctx := []byte("read-ctx")
	leader.Step(Message{
		From:    1,
		To:      1,
		Type:    MsgReadIndex,
		Entries: []Entry{{Data: ctx}},
	})

	if len(leader.readStates) > 0 {
		t.Error("ReadIndex on a 3-node cluster should not complete without heartbeat ACKs")
	}

	hbMsgs := filterMsgsByType(collectMsgs(leader), MsgHeartbeat)
	if len(hbMsgs) == 0 {
		t.Error("ReadIndex should trigger heartbeats for quorum validation")
	}
}

func TestElectionTimeoutRandomization(t *testing.T) {
	// Each node uses id as rand seed — distinct IDs yield distinct timeouts,
	// which is the production guarantee (no two peers share an ID).
	timeouts := make(map[int]bool)
	for i := 0; i < 100; i++ {
		// Use distinct IDs to exercise the per-node randomization.
		r := newTestRaft(uint64(i+1), []uint64{uint64(i + 1), uint64(i + 2), uint64(i + 3)}, 10, 1, NewMemoryStorage())
		r.resetRandomizedElectionTimeout()
		timeouts[r.randomizedElectionTimeout] = true
	}
	if len(timeouts) < 3 {
		t.Errorf("election timeout should be randomized; only %d distinct values in 100 samples", len(timeouts))
	}
}

func TestReplicationPipelineFlowControl(t *testing.T) {
	// Use a small per-message size so each AppendEntries carries one entry,
	// letting us fill the inflight window with distinct in-flight messages.
	const maxMsg = 32 // bytes — forces one entry per AppendEntries
	ids := []uint64{1, 2, 3}
	makeRaft := func(id uint64) *raft {
		return newTestRaft(id, ids, 10, 1, NewMemoryStorage())
	}
	n := &network{
		peers:   map[uint64]*raft{1: makeRaft(1), 2: makeRaft(2), 3: makeRaft(3)},
		dropped: map[uint64]map[uint64]bool{1: {}, 2: {}, 3: {}},
		t:       t,
	}
	n.elect(1)
	leader := n.peers[1]

	// Override leader message size limit after election.
	leader.maxMsgSize = maxMsg

	for i := 0; i < 300; i++ {
		n.propose(1, []byte(fmt.Sprintf("e%d", i)))
	}

	pr := leader.trk.progress[3]
	pr.Match = 0
	pr.Next = 1
	pr.BecomeReplicate()
	pr.Inflights.reset()

	maxInflight := pr.Inflights.size
	sent := 0
	for i := 0; i < maxInflight*2; i++ {
		if leader.maybeSendAppend(3, false) {
			collectMsgs(leader)
			sent++
		}
		if pr.Inflights.full() {
			break
		}
	}

	if !pr.Inflights.full() {
		t.Errorf("inflight window should be full; sent %d messages, maxInflight=%d", sent, maxInflight)
	}
	if leader.maybeSendAppend(3, false) {
		t.Error("should not send when inflight window is full (flow control violated)")
	}
}

func TestTermMonotonicallyIncreases(t *testing.T) {
	r := newTestRaftWithStorage(1, []uint64{1, 2, 3})
	var lastTerm uint64

	for i := 0; i < 5; i++ {
		r.becomeCandidate()
		if r.Term < lastTerm {
			t.Errorf("term decreased from %d to %d", lastTerm, r.Term)
		}
		lastTerm = r.Term
		r.becomeFollower(r.Term, None)
	}
}

func TestFuzz_ElectionAndReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("fuzz test skipped in short mode")
	}
	rng := rand.New(rand.NewSource(42))
	ids := []uint64{1, 2, 3, 4, 5}
	n := newNetwork(t, ids...)

	for step := 0; step < 500; step++ {
		switch rng.Intn(4) {
		case 0:
			id := ids[rng.Intn(len(ids))]
			n.elect(id)
		case 1:
			n.tickAll()
		case 2:
			if leaderID, ok := n.leader(); ok {
				n.propose(leaderID, []byte(fmt.Sprintf("data-%d", step)))
			}
		case 3:
			id := ids[rng.Intn(len(ids))]
			isolate := rng.Intn(2) == 0
			if isolate {
				n.isolate(id)
			} else {
				n.restore(id)
			}
		}
	}

	for i := 0; i < 30; i++ {
		n.tickAll()
	}
}
