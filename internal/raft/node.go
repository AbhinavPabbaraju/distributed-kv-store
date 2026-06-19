package raft

import (
	"context"
	"errors"
)

var ErrStopped = errors.New("raft: stopped")

type SnapshotStatus int

const (
	SnapshotFinish  SnapshotStatus = iota
	SnapshotFailure SnapshotStatus = iota
)

type Ready struct {
	SoftState       *SoftState
	HardState       HardState
	ReadStates      []ReadState
	Entries         []Entry
	Snapshot        Snapshot
	CommittedEntries []Entry
	Messages        []Message
	MustSync        bool
}

func (rd Ready) containsUpdates() bool {
	return rd.SoftState != nil ||
		!rd.HardState.IsEmpty() ||
		!rd.Snapshot.IsEmpty() ||
		len(rd.Entries) > 0 ||
		len(rd.CommittedEntries) > 0 ||
		len(rd.Messages) > 0 ||
		len(rd.ReadStates) > 0
}

func newReady(r *raft, prevSoftSt *SoftState, prevHardSt HardState) Ready {
	rd := Ready{
		Entries:          r.raftLog.unstableEntries(),
		CommittedEntries: r.raftLog.nextCommittedEnts(true),
		Messages:         r.msgs,
	}
	if softSt := r.softState(); !softStatesEqual(softSt, *prevSoftSt) {
		rd.SoftState = &softSt
	}
	if hardSt := r.hardState(); !hardStatesEqual(hardSt, prevHardSt) {
		rd.HardState = hardSt
	}
	if r.raftLog.unstable.snapshot != nil {
		rd.Snapshot = *r.raftLog.unstable.snapshot
	}
	if len(r.readStates) != 0 {
		rd.ReadStates = r.readStates
	}
	rd.MustSync = mustSync(r.hardState(), prevHardSt, len(rd.Entries))
	return rd
}

func mustSync(st, prevst HardState, entsnum int) bool {
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}

func softStatesEqual(a, b SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

func hardStatesEqual(a, b HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

type msgWithResult struct {
	m      Message
	result chan error
}

type Node interface {
	Tick()
	Campaign(ctx context.Context) error
	Propose(ctx context.Context, data []byte) error
	ProposeConfChange(ctx context.Context, cc ConfChange) error
	Step(ctx context.Context, msg Message) error
	Ready() <-chan Ready
	Advance()
	ApplyConfChange(cc ConfChange) *ConfState
	TransferLeadership(ctx context.Context, lead, transferee uint64)
	ReadIndex(ctx context.Context, rctx []byte) error
	Status() Status
	ReportUnreachable(id uint64)
	ReportSnapshot(id uint64, status SnapshotStatus)
	Stop()
}

type Status struct {
	ID uint64
	HardState
	SoftState
	Applied  uint64
	Progress map[uint64]Progress
}

type node struct {
	propc      chan msgWithResult
	recvc      chan Message
	confc      chan ConfChange
	confstatec chan ConfState
	readyc     chan Ready
	advancec   chan struct{}
	tickc      chan struct{}
	done       chan struct{}
	stop       chan struct{}
	status     chan chan Status
	raft       *raft
	prevSoftSt SoftState
	prevHardSt HardState
	logger     Logger
}

func StartNode(c *Config, peers []Peer) Node {
	if len(peers) == 0 {
		panic("no peers given; use RestartNode instead")
	}
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}
	err = rn.Bootstrap(peers)
	if err != nil {
		c.Logger.Warningf("error occurred during starting a new node: %v", err)
	}
	n := newNode(rn)
	go n.run()
	return n
}

func RestartNode(c *Config) Node {
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}
	n := newNode(rn)
	go n.run()
	return n
}

func newNode(rn *RawNode) *node {
	return &node{
		propc:      make(chan msgWithResult),
		recvc:      make(chan Message),
		confc:      make(chan ConfChange),
		confstatec: make(chan ConfState),
		readyc:     make(chan Ready),
		advancec:   make(chan struct{}),
		tickc:      make(chan struct{}, 128),
		done:       make(chan struct{}),
		stop:       make(chan struct{}),
		status:     make(chan chan Status),
		raft:       rn.raft,
		logger:     rn.raft.logger,
	}
}

func (n *node) Stop() {
	select {
	case n.stop <- struct{}{}:
	case <-n.done:
	}
	<-n.done
}

func (n *node) run() {
	var propc chan msgWithResult
	var readyc chan Ready
	var advancec chan struct{}
	var rd Ready

	r := n.raft
	lead := None

	prevSoftSt := r.softState()
	prevHardSt := emptyHardState

	for {
		if advancec != nil {
			readyc = nil
		} else if n.hasReady() {
			rd = n.readyWithoutAccept()
			readyc = n.readyc
		}

		if lead != r.lead {
			if r.hasLeader() {
				if lead == None {
					r.logger.Infof("raft.node: %x elected leader %x at term %d", r.id, r.lead, r.Term)
				} else {
					r.logger.Infof("raft.node: %x changed leader from %x to %x at term %d", r.id, lead, r.lead, r.Term)
				}
				propc = n.propc
			} else {
				r.logger.Infof("raft.node: %x lost leader %x at term %d", r.id, lead, r.Term)
				propc = nil
			}
			lead = r.lead
		}

		select {
		case pm := <-propc:
			m := pm.m
			m.From = r.id
			err := r.Step(m)
			if pm.result != nil {
				pm.result <- err
				close(pm.result)
			}
		case m := <-n.recvc:
			if isResponseMsg(m.Type) && !isLocalMsg(m.Type) {
				if pr := r.trk.progress[m.From]; pr != nil {
					pr.RecentActive = true
				}
			}
			r.Step(m)
		case cc := <-n.confc:
			_, okBefore := r.trk.progress[r.id]
			cs := r.applyConfChange(cc)
			if _, okAfter := r.trk.progress[r.id]; okBefore && !okAfter {
				var found bool
				for _, id := range cs.Voters {
					if id == r.id {
						found = true
						break
					}
				}
				if !found {
					propc = nil
				}
			}
			select {
			case n.confstatec <- cs:
			case <-n.done:
			}
		case <-n.tickc:
			n.raft.tick()
		case readyc <- rd:
			n.acceptReady(rd)
			advancec = n.advancec
		case <-advancec:
			n.advance(rd)
			rd = Ready{}
			advancec = nil
		case c := <-n.status:
			c <- n.getStatus()
		case <-n.stop:
			close(n.done)
			return
		}

		prevSoftSt = r.softState()
		prevHardSt = r.hardState()
		_, _ = prevSoftSt, prevHardSt
	}
}

func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		n.logger.Warningf("%x (leader %v) A tick missed to fire. Node blocks too long!", n.raft.id, n.raft.id == n.raft.lead)
	}
}

func (n *node) Campaign(ctx context.Context) error {
	return n.step(ctx, Message{Type: MsgHup})
}

func (n *node) Propose(ctx context.Context, data []byte) error {
	return n.stepWait(ctx, Message{Type: MsgProp, Entries: []Entry{{Data: data}}})
}

func (n *node) Step(ctx context.Context, m Message) error {
	if isLocalMsg(m.Type) {
		return nil
	}
	return n.step(ctx, m)
}

func (n *node) ProposeConfChange(ctx context.Context, cc ConfChange) error {
	data, err := encodeConfChange(cc)
	if err != nil {
		return err
	}
	return n.stepWait(ctx, Message{Type: MsgProp, Entries: []Entry{{Type: EntryConfChange, Data: data}}})
}

func (n *node) step(ctx context.Context, m Message) error {
	return n.stepWithWaitOption(ctx, m, false)
}

func (n *node) stepWait(ctx context.Context, m Message) error {
	return n.stepWithWaitOption(ctx, m, true)
}

func (n *node) stepWithWaitOption(ctx context.Context, m Message, wait bool) error {
	if m.Type != MsgProp {
		select {
		case n.recvc <- m:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-n.done:
			return ErrStopped
		}
	}
	ch := n.propc
	pm := msgWithResult{m: m}
	if wait {
		pm.result = make(chan error, 1)
	}
	select {
	case ch <- pm:
		if !wait {
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	select {
	case err := <-pm.result:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	return nil
}

func (n *node) Ready() <-chan Ready {
	return n.readyc
}

func (n *node) Advance() {
	select {
	case n.advancec <- struct{}{}:
	case <-n.done:
	}
}

func (n *node) ApplyConfChange(cc ConfChange) *ConfState {
	var cs ConfState
	select {
	case n.confc <- cc:
	case <-n.done:
	}
	select {
	case cs = <-n.confstatec:
	case <-n.done:
	}
	return &cs
}

func (n *node) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	select {
	case n.recvc <- Message{Type: MsgTransferLeader, From: transferee, To: lead}:
	case <-ctx.Done():
	case <-n.done:
	}
}

func (n *node) ReadIndex(ctx context.Context, rctx []byte) error {
	return n.step(ctx, Message{Type: MsgReadIndex, Entries: []Entry{{Data: rctx}}})
}

func (n *node) Status() Status {
	c := make(chan Status)
	select {
	case n.status <- c:
	case <-n.done:
		return Status{}
	}
	select {
	case s := <-c:
		return s
	case <-n.done:
		return Status{}
	}
}

func (n *node) ReportUnreachable(id uint64) {
	select {
	case n.recvc <- Message{Type: MsgUnreachable, From: id}:
	case <-n.done:
	}
}

func (n *node) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure
	select {
	case n.recvc <- Message{Type: MsgSnapStatus, From: id, Reject: rej}:
	case <-n.done:
	}
}

func (n *node) hasReady() bool {
	r := n.raft
	if softSt := r.softState(); !softStatesEqual(softSt, n.prevSoftSt) {
		return true
	}
	if hardSt := r.hardState(); !hardStatesEqual(hardSt, n.prevHardSt) && !hardSt.IsEmpty() {
		return true
	}
	if r.raftLog.unstable.snapshot != nil && !r.raftLog.unstable.snapshot.IsEmpty() {
		return true
	}
	if len(r.msgs) > 0 || len(r.raftLog.unstableEntries()) > 0 || r.raftLog.hasNextCommittedEnts(true) {
		return true
	}
	if len(r.readStates) != 0 {
		return true
	}
	return false
}

func (n *node) readyWithoutAccept() Ready {
	return newReady(n.raft, &n.prevSoftSt, n.prevHardSt)
}

func (n *node) acceptReady(rd Ready) {
	if rd.SoftState != nil {
		n.prevSoftSt = *rd.SoftState
	}
	if len(rd.ReadStates) != 0 {
		n.raft.readStates = nil
	}
	n.raft.msgs = nil
}

func (n *node) advance(rd Ready) {
	n.raft.advance(rd)
	n.prevHardSt = n.raft.hardState()
}

func (n *node) getStatus() Status {
	r := n.raft
	s := Status{
		ID:        r.id,
		HardState: r.hardState(),
		SoftState: r.softState(),
		Applied:   r.raftLog.applied,
	}
	if s.RaftState == RoleLeader {
		s.Progress = make(map[uint64]Progress, len(r.trk.progress))
		for id, p := range r.trk.progress {
			s.Progress[id] = *p
		}
	}
	return s
}

func isResponseMsg(msgt MessageType) bool {
	return msgt == MsgAppResp || msgt == MsgVoteResp || msgt == MsgHeartbeatResp ||
		msgt == MsgUnreachable || msgt == MsgPreVoteResp
}

func isLocalMsg(msgt MessageType) bool {
	return msgt == MsgHup || msgt == MsgBeat || msgt == MsgUnreachable ||
		msgt == MsgSnapStatus || msgt == MsgCheckQuorum
}

func (r *raft) hasLeader() bool { return r.lead != None }

var emptyHardState = HardState{}

type Peer struct {
	ID      uint64
	Context []byte
}

type RawNode struct {
	raft *raft
}

func NewRawNode(config *Config) (*RawNode, error) {
	r := newRaft(config)
	return &RawNode{raft: r}, nil
}

func (rn *RawNode) Bootstrap(peers []Peer) error {
	if len(peers) == 0 {
		return errors.New("must provide at least one peer on bootstrap")
	}
	lastIndex, err := rn.raft.raftLog.storage.LastIndex()
	if err != nil {
		return err
	}
	if lastIndex != 0 {
		return nil
	}
	ents := make([]Entry, len(peers))
	for i, peer := range peers {
		ents[i] = Entry{Type: EntryConfChange, Term: 1, Index: uint64(i + 1), Data: peer.Context}
	}
	rn.raft.raftLog.append(ents...)
	rn.raft.raftLog.committed = uint64(len(ents))
	for _, peer := range peers {
		rn.raft.applyConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: peer.ID})
	}
	return nil
}

func (r *raft) applyConfChange(cc ConfChange) ConfState {
	if cc.NodeID == None {
		return r.confState()
	}
	switch cc.Type {
	case ConfChangeAddNode, ConfChangeAddLearnerNode:
		isLearner := cc.Type == ConfChangeAddLearnerNode
		if pr, ok := r.trk.progress[cc.NodeID]; ok {
			if isLearner {
				pr.IsLearner = true
			}
			return r.confState()
		}
		inflightSz := r.maxInflightMsgs
		if inflightSz <= 0 {
			inflightSz = 256
		}
		r.trk.progress[cc.NodeID] = &Progress{
			Next:      r.raftLog.lastIndex() + 1,
			Inflights: newInflights(inflightSz),
			IsLearner: isLearner,
		}
		if isLearner {
			r.trk.learners = append(r.trk.learners, cc.NodeID)
		} else {
			r.trk.voters = append(r.trk.voters, cc.NodeID)
		}
	case ConfChangeRemoveNode:
		delete(r.trk.progress, cc.NodeID)
		r.trk.voters = removeID(r.trk.voters, cc.NodeID)
		r.trk.learners = removeID(r.trk.learners, cc.NodeID)
	case ConfChangeUpdateNode:
	}
	if r.id == cc.NodeID {
		if cc.Type == ConfChangeRemoveNode {
			r.isLearner = false
		}
	}
	return r.confState()
}

func (r *raft) confState() ConfState {
	return ConfState{
		Voters:   r.trk.voters,
		Learners: r.trk.learners,
	}
}

func removeID(ids []uint64, id uint64) []uint64 {
	for i, v := range ids {
		if v == id {
			return append(ids[:i], ids[i+1:]...)
		}
	}
	return ids
}

func encodeConfChange(cc ConfChange) ([]byte, error) {
	buf := make([]byte, 16)
	buf[0] = byte(cc.Type)
	for i := 0; i < 8; i++ {
		buf[1+i] = byte(cc.NodeID >> (8 * i))
	}
	return append(buf[:9], cc.Context...), nil
}
