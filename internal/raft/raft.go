package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
)

type campaignType string

const (
	campaignPreElection campaignType = "CampaignPreElection"
	campaignElection    campaignType = "CampaignElection"
	campaignTransfer    campaignType = "CampaignTransfer"
)

var errProposalDropped = errors.New("raft proposal dropped")

type Config struct {
	ID    uint64
	peers []uint64

	ElectionTick  int
	HeartbeatTick int

	Storage Storage
	Applied uint64

	MaxSizePerMsg        uint64
	MaxCommittedSizePerReady uint64
	MaxUncommittedEntriesSize uint64
	MaxInflightMsgs      int
	MaxInflightBytes     uint64

	CheckQuorum bool
	PreVote     bool

	ReadOnlyOption ReadOnlyOption

	Logger Logger

	DisableProposalForwarding bool
	StepDownOnRemoval         bool
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}
	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}
	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}
	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}
	if c.MaxUncommittedEntriesSize == 0 {
		c.MaxUncommittedEntriesSize = noLimit
	}
	if c.MaxCommittedSizePerReady == 0 {
		c.MaxCommittedSizePerReady = noLimit
	}
	if c.MaxInflightMsgs <= 0 {
		c.MaxInflightMsgs = 256
	}
	if c.Logger == nil {
		c.Logger = DefaultLogger
	}
	if c.ReadOnlyOption == ReadOnlyLeaseBased && !c.CheckQuorum {
		return errors.New("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased")
	}
	return nil
}

type ReadOnlyOption int

const (
	ReadOnlySafe      ReadOnlyOption = iota
	ReadOnlyLeaseBased ReadOnlyOption = iota
)

type readIndexStatus struct {
	req   Message
	index uint64
	acks  map[uint64]Message
}

type readOnly struct {
	option           ReadOnlyOption
	pendingReadIndex map[string]*readIndexStatus
	readIndexQueue   []string
}

func newReadOnly(option ReadOnlyOption) *readOnly {
	return &readOnly{
		option:           option,
		pendingReadIndex: make(map[string]*readIndexStatus),
	}
}

func (ro *readOnly) addRequest(index uint64, m Message) {
	s := string(m.Entries[0].Data)
	if _, ok := ro.pendingReadIndex[s]; ok {
		return
	}
	ro.pendingReadIndex[s] = &readIndexStatus{index: index, req: m, acks: make(map[uint64]Message)}
	ro.readIndexQueue = append(ro.readIndexQueue, s)
}

func (ro *readOnly) recvAck(id uint64, context []byte) map[uint64]Message {
	rs, ok := ro.pendingReadIndex[string(context)]
	if !ok {
		return nil
	}
	rs.acks[id] = Message{}
	return rs.acks
}

func (ro *readOnly) advance(m Message) []*readIndexStatus {
	var (
		i     int
		found bool
	)
	ctx := string(m.Context)
	rss := []*readIndexStatus{}
	for _, okctx := range ro.readIndexQueue {
		i++
		rs, ok := ro.pendingReadIndex[okctx]
		if !ok {
			panic("cannot find corresponding read state from pending map")
		}
		rss = append(rss, rs)
		if okctx == ctx {
			found = true
			break
		}
	}
	if found {
		ro.readIndexQueue = ro.readIndexQueue[i:]
		for _, rs := range rss {
			delete(ro.pendingReadIndex, string(rs.req.Entries[0].Data))
		}
		return rss
	}
	return nil
}

func (ro *readOnly) lastPendingRequestCtx() string {
	if len(ro.readIndexQueue) == 0 {
		return ""
	}
	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}

type raft struct {
	id   uint64
	Term uint64
	Vote uint64

	readStates []ReadState

	raftLog *raftLog

	maxMsgSize         uint64
	maxUncommittedSize uint64

	trk *ProgressTracker

	state StateRole

	isLearner bool

	msgs []Message

	lead            uint64
	leadTransferee  uint64
	pendingConfIndex uint64
	uncommittedSize  uint64

	readOnly *readOnly

	electionElapsed           int
	heartbeatElapsed          int
	checkQuorum               bool
	preVote                   bool
	heartbeatTimeout          int
	electionTimeout           int
	randomizedElectionTimeout int
	disableProposalForwarding bool
	stepDownOnRemoval         bool
	maxInflightMsgs           int

	tick func()
	step stepFunc

	logger Logger
	rng    *rand.Rand
}

type stepFunc func(r *raft, m Message) error

func newRaft(c *Config) *raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newRaftLog(c.Storage, c.Logger, c.MaxCommittedSizePerReady)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	r := &raft{
		id:                        c.ID,
		lead:                      None,
		isLearner:                 false,
		raftLog:                   raftlog,
		maxMsgSize:                c.MaxSizePerMsg,
		maxUncommittedSize:        c.MaxUncommittedEntriesSize,
		checkQuorum:               c.CheckQuorum,
		preVote:                   c.PreVote,
		readOnly:                  newReadOnly(c.ReadOnlyOption),
		disableProposalForwarding: c.DisableProposalForwarding,
		stepDownOnRemoval:         c.StepDownOnRemoval,
		heartbeatTimeout:          c.HeartbeatTick,
		electionTimeout:           c.ElectionTick,
		logger:                    c.Logger,
	}

	voters := cs.Voters
	if len(voters) == 0 && len(c.peers) > 0 {
		voters = c.peers
	}
	r.trk = newProgressTracker(voters, cs.Learners)
	for id := range r.trk.progress {
		isLearner := r.trk.progress[id].IsLearner
		if id == r.id {
			r.isLearner = isLearner
		}
		r.trk.progress[id] = &Progress{
			Next:      raftlog.lastIndex() + 1,
			Match:     0,
			Inflights: newInflights(c.MaxInflightMsgs),
			IsLearner: isLearner,
		}
	}

	if !hs.IsEmpty() {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	r.maxInflightMsgs = c.MaxInflightMsgs
	r.rng = rand.New(rand.NewSource(int64(c.ID)))
	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range r.trk.voters {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}
	sort.Strings(nodesStrs)
	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return r
}

func (r *raft) softState() SoftState {
	return SoftState{Lead: r.lead, RaftState: r.state}
}

func (r *raft) hardState() HardState {
	return HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.raftLog.committed,
	}
}

func (r *raft) quorum() int { return quorum(len(r.trk.voters)) }

func (r *raft) send(m Message) {
	if m.From == None {
		m.From = r.id
	}
	if m.Type == MsgVote || m.Type == MsgVoteResp || m.Type == MsgPreVote || m.Type == MsgPreVoteResp {
		if m.Term == 0 {
			panic(fmt.Sprintf("term should be set when sending %s", m.Type))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.Type, m.Term))
		}
		if m.Type != MsgProp && m.Type != MsgReadIndex {
			m.Term = r.Term
		}
	}
	if m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap {
		m.Term = r.Term
	}
	r.msgs = append(r.msgs, m)
}

func (r *raft) sendAppend(to uint64) {
	r.maybeSendAppend(to, true)
}

func (r *raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.trk.progress[to]
	if pr.IsPaused() {
		return false
	}
	m := Message{To: to}
	term, errt := r.raftLog.term(pr.Next - 1)
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}
	if errt != nil || erre != nil {
		if !pr.RecentActive {
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return false
		}
		m.Type = MsgSnap
		snapshot, err := r.raftLog.snapshot()
		if err != nil {
			if errors.Is(err, ErrSnapshotTemporarilyUnavailable) {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return false
			}
			panic(err)
		}
		if snapshot.IsEmpty() {
			panic("need non-empty snapshot")
		}
		m.Snapshot = &snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
			r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		pr.BecomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
	} else {
		m.Type = MsgApp
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ents
		m.Commit = r.raftLog.committed
		if n := len(m.Entries); n != 0 {
			switch pr.State {
			case StateReplicate:
				last := m.Entries[n-1].Index
				pr.SentEntries(last)
			case StateProbe:
				pr.MsgAppFlowPaused = true
			default:
				r.logger.Panicf("%x is sending append in unhandled state %s", r.id, pr.State)
			}
		}
	}
	r.send(m)
	return true
}

func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	commit := min64(r.trk.progress[to].Match, r.raftLog.committed)
	m := Message{
		To:      to,
		Type:    MsgHeartbeat,
		Commit:  commit,
		Context: ctx,
	}
	r.send(m)
}

func (r *raft) bcastAppend() {
	r.trk.Visit(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}
		r.sendAppend(id)
	})
}

func (r *raft) bcastHeartbeat() {
	lastCtx := r.readOnly.lastPendingRequestCtx()
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}

func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	r.trk.Visit(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}
		r.sendHeartbeat(id, ctx)
	})
}

func (r *raft) advance(rd Ready) {
	if n := len(rd.Entries); n > 0 {
		e := rd.Entries[n-1]
		r.raftLog.stableTo(e.Index, e.Term)
	}
	if !rd.Snapshot.IsEmpty() {
		r.raftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
	if n := len(rd.CommittedEntries); n > 0 {
		index := rd.CommittedEntries[n-1].Index
		r.raftLog.appliedTo(index)
		r.reduceUncommittedSize(rd.CommittedEntries)
	}
}

func (r *raft) maybeCommit() bool {
	mci := r.trk.CommittedIndex()
	return r.raftLog.maybeCommit(mci, r.Term)
}

func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.abortLeaderTransfer()
	r.trk.ResetVotes()
	r.trk.Visit(func(id uint64, pr *Progress) {
		*pr = Progress{
			Match:     0,
			Next:      r.raftLog.lastIndex() + 1,
			Inflights: newInflights(pr.Inflights.size),
			IsLearner: pr.IsLearner,
		}
		if id == r.id {
			pr.Match = r.raftLog.lastIndex()
		}
	})
	r.pendingConfIndex = 0
	r.uncommittedSize = 0
	r.readOnly = newReadOnly(r.readOnly.option)
}

func (r *raft) appendEntry(es ...Entry) (accepted bool) {
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	if !r.increaseUncommittedSize(es) {
		r.logger.Debugf("%x appending new entries to log would exceed uncommitted entry size limit; dropping proposal", r.id)
		return false
	}
	li = r.raftLog.append(es...)
	pr := r.trk.progress[r.id]
	pr.MaybeUpdate(li)
	r.maybeCommit()
	return true
}

func (r *raft) tickElection() {
	r.electionElapsed++
	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		if err := r.Step(Message{From: r.id, Type: MsgHup}); err != nil {
			r.logger.Debugf("error occurred during checking whether should campaign after election timeout: %v", err)
		}
	}
}

func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if r.checkQuorum {
			if err := r.Step(Message{From: r.id, Type: MsgCheckQuorum}); err != nil {
				r.logger.Debugf("error occurred during checking quorum: %v", err)
			}
		}
		if r.state == RoleLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}

	if r.state != RoleLeader {
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		if err := r.Step(Message{From: r.id, Type: MsgBeat}); err != nil {
			r.logger.Debugf("error occurred during checking heartbeat: %v", err)
		}
	}
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = RoleFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

func (r *raft) becomeCandidate() {
	if r.state == RoleLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.Vote = r.id
	r.state = RoleCandidate
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

func (r *raft) becomePreCandidate() {
	if r.state == RoleLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	r.step = stepCandidate
	r.trk.ResetVotes()
	r.tick = r.tickElection
	r.lead = None
	r.state = RolePreCandidate
	r.logger.Infof("%x became pre-candidate at term %d", r.id, r.Term)
}

func (r *raft) becomeLeader() {
	if r.state == RoleFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = RoleLeader
	r.pendingConfIndex = r.raftLog.lastIndex()

	uncommittedEntries := r.raftLog.allEntries()
	var nconf int
	for _, e := range uncommittedEntries {
		if e.Index <= r.raftLog.committed {
			continue
		}
		if e.Type == EntryConfChange {
			nconf++
		}
	}
	if nconf > 1 {
		panic("unexpected multiple uncommitted config entry")
	}

	emptyEnt := Entry{Data: nil}
	if !r.appendEntry(emptyEnt) {
		panic("empty entry was dropped")
	}
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}

func (r *raft) hup(t campaignType) {
	if r.state == RoleLeader {
		r.logger.Debugf("%x ignoring MsgHup because already leader", r.id)
		return
	}
	if !r.promotable() {
		r.logger.Warningf("%x is unpromotable and can not campaign", r.id)
		return
	}
	ents, err := r.raftLog.slice(r.raftLog.applied+1, r.raftLog.committed+1, noLimit)
	if err != nil {
		r.logger.Panicf("unexpected error getting unapplied entries (%v)", err)
	}
	if n := numOfPendingConf(ents); n != 0 && r.raftLog.committed > r.raftLog.applied {
		r.logger.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
		return
	}
	r.logger.Infof("%x is starting a new election at term %d", r.id, r.Term)
	r.campaign(t)
}

func (r *raft) campaign(t campaignType) {
	if !r.promotable() {
		r.logger.Warningf("%x is unpromotable; campaign() should have been called", r.id)
	}
	var term uint64
	var voteMsg MessageType
	if t == campaignPreElection {
		r.becomePreCandidate()
		voteMsg = MsgPreVote
		term = r.Term + 1
	} else {
		r.becomeCandidate()
		voteMsg = MsgVote
		term = r.Term
	}
	r.trk.RecordVote(r.id, true)
	if granted, _, _ := r.trk.TallyVotes(); granted >= quorum(len(r.trk.voters)) {
		if t == campaignPreElection {
			r.campaign(campaignElection)
		} else {
			r.becomeLeader()
		}
		return
	}
	var ids []uint64
	for id := range r.trk.progress {
		if r.trk.progress[id].IsLearner {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if id == r.id {
			continue
		}
		r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

		var ctx []byte
		if t == campaignTransfer {
			ctx = []byte(t)
		}
		r.send(Message{
			Term:    term,
			To:      id,
			Type:    voteMsg,
			Index:   r.raftLog.lastIndex(),
			LogTerm: r.raftLog.lastTerm(),
			Context: ctx,
		})
	}
}

func (r *raft) poll(id uint64, t MessageType, v bool) (granted int, rejected int, result VoteResult) {
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	r.trk.RecordVote(id, v)
	granted, rejected, _ = r.trk.TallyVotes()
	result = r.trk.VoteResult()
	return
}

func (r *raft) Step(m Message) error {
	switch {
	case m.Term == 0:
	case m.Term > r.Term:
		if m.Type == MsgVote || m.Type == MsgPreVote {
			force := strings.EqualFold(string(m.Context), string(campaignTransfer))
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
			if !force && inLease {
				r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
				return nil
			}
		}
		switch {
		case m.Type == MsgPreVote:
		case m.Type == MsgPreVoteResp && !m.Reject:
		default:
			r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
			if m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap {
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, None)
			}
		}
	case m.Term < r.Term:
		if (r.checkQuorum || r.preVote) && (m.Type == MsgHeartbeat || m.Type == MsgApp) {
			r.send(Message{To: m.From, Type: MsgAppResp})
		} else if m.Type == MsgPreVote {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(Message{To: m.From, Term: r.Term, Type: MsgPreVoteResp, Reject: true})
		} else {
			r.logger.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
		}
		return nil
	}

	switch m.Type {
	case MsgHup:
		if r.preVote {
			r.hup(campaignPreElection)
		} else {
			r.hup(campaignElection)
		}
	case MsgVote, MsgPreVote:
		canVote := r.Vote == m.From ||
			(r.Vote == None && r.lead == None) ||
			(m.Type == MsgPreVote && m.Term > r.Term)
		if canVote && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(Message{To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type)})
			if m.Type == MsgVote {
				r.electionElapsed = 0
				r.Vote = m.From
			}
		} else {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}
	default:
		err := r.step(r, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func stepLeader(r *raft, m Message) error {
	switch m.Type {
	case MsgBeat:
		r.bcastHeartbeat()
		return nil
	case MsgCheckQuorum:
		if !r.trk.HasQuorum(func(id uint64) bool {
			if id == r.id {
				return true
			}
			return r.trk.progress[id].RecentActive
		}) {
			r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
			r.becomeFollower(r.Term, None)
		}
		r.trk.Visit(func(id uint64, pr *Progress) { pr.RecentActive = false })
		return nil
	case MsgProp:
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MsgProp", r.id)
		}
		if r.trk.progress[r.id] == nil {
			return errProposalDropped
		}
		if r.leadTransferee != None {
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return errProposalDropped
		}
		for i := range m.Entries {
			e := &m.Entries[i]
			if e.Type == EntryConfChange {
				if r.pendingConfIndex > r.raftLog.applied {
					r.logger.Infof("propose conf %s ignored since pending unapplied configuration [index %d, applied %d]",
						e, r.pendingConfIndex, r.raftLog.applied)
					m.Entries[i] = Entry{Type: EntryNormal}
					e = &m.Entries[i]
				} else {
					r.pendingConfIndex = r.raftLog.lastIndex() + 1 + uint64(i)
				}
			}
		}
		if !r.appendEntry(m.Entries...) {
			return errProposalDropped
		}
		r.bcastAppend()
		return nil
	case MsgReadIndex:
		if r.quorum() > 1 {
			if r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) != r.Term {
				return nil
			}
			switch r.readOnly.option {
			case ReadOnlySafe:
				r.readOnly.addRequest(r.raftLog.committed, m)
				r.bcastHeartbeatWithCtx(m.Entries[0].Data)
			case ReadOnlyLeaseBased:
				ri := r.raftLog.committed
				if m.From == None || m.From == r.id {
					r.readStates = append(r.readStates, ReadState{Index: ri, RequestCtx: m.Entries[0].Data})
				} else {
					r.send(Message{To: m.From, Type: MsgReadIndexResp, Index: ri, Entries: m.Entries})
				}
			}
		} else {
			r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
		}
		return nil
	}

	pr := r.trk.progress[m.From]
	if pr == nil {
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return nil
	}

	switch m.Type {
	case MsgAppResp:
		pr.RecentActive = true
		if m.Reject {
			r.logger.Debugf("%x received MsgAppResp(MsgApp was rejected, lastindex: %d) from %x for index %d",
				r.id, m.RejectHint, m.From, m.Index)
			nextProbeIdx := m.RejectHint
			if m.LogTerm > 0 {
				nextProbeIdx = r.raftLog.findConflictByTerm(m.RejectHint, m.LogTerm)
			}
			if pr.MaybeDecrTo(m.Index, nextProbeIdx) {
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				if pr.State == StateReplicate {
					pr.BecomeProbe()
				}
				r.sendAppend(m.From)
			}
		} else {
			oldPaused := pr.IsPaused()
			if pr.MaybeUpdate(m.Index) {
				switch {
				case pr.State == StateProbe:
					pr.BecomeReplicate()
				case pr.State == StateSnapshot && pr.Match >= pr.PendingSnapshot:
					r.logger.Debugf("%x recovered from needing snapshot, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					pr.BecomeProbe()
					pr.BecomeReplicate()
				case pr.State == StateReplicate:
					pr.Inflights.freeTo(m.Index)
				}
				if r.maybeCommit() {
					r.bcastAppend()
				} else if oldPaused {
					r.sendAppend(m.From)
				}
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case MsgHeartbeatResp:
		pr.RecentActive = true
		pr.MsgAppFlowPaused = false
		if pr.State == StateReplicate && pr.Inflights.full() {
			pr.Inflights.freeTo(pr.Inflights.buffer[pr.Inflights.start])
		}
		if pr.Match < r.raftLog.lastIndex() {
			r.sendAppend(m.From)
		}
		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return nil
		}
		if r.trk.HasQuorum(func(id uint64) bool {
			if id == r.id {
				return true
			}
			_, ok := r.readOnly.recvAck(id, m.Context)[id]
			return ok
		}) {
			rss := r.readOnly.advance(m)
			for _, rs := range rss {
				req := rs.req
				if req.From == None || req.From == r.id {
					r.readStates = append(r.readStates, ReadState{Index: rs.index, RequestCtx: req.Entries[0].Data})
				} else {
					r.send(Message{To: req.From, Type: MsgReadIndexResp, Index: rs.index, Entries: req.Entries})
				}
			}
		}
	case MsgSnapStatus:
		if pr.State != StateSnapshot {
			return nil
		}
		if !m.Reject {
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		} else {
			pr.PendingSnapshot = 0
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
			pr.BecomeProbe()
		}
	case MsgUnreachable:
		if pr.State == StateReplicate {
			pr.BecomeProbe()
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
	case MsgTransferLeader:
		if pr.IsLearner {
			r.logger.Debugf("%x is learner. Ignored transferring leadership", r.id)
			return nil
		}
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, leadTransferee, leadTransferee)
				return nil
			}
			r.abortLeaderTransfer()
			r.logger.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
		}
		if leadTransferee == r.id {
			r.logger.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
			return nil
		}
		r.logger.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.raftLog.lastIndex() {
			r.sendTimeoutNow(leadTransferee)
			r.logger.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else {
			r.sendAppend(leadTransferee)
		}
	}
	return nil
}

func stepCandidate(r *raft, m Message) error {
	var myVoteRespType MessageType
	if r.state == RolePreCandidate {
		myVoteRespType = MsgPreVoteResp
	} else {
		myVoteRespType = MsgVoteResp
	}
	switch m.Type {
	case MsgProp:
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return errProposalDropped
	case MsgApp:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case MsgSnap:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case myVoteRespType:
		gr, rj, res := r.poll(m.From, m.Type, !m.Reject)
		r.logger.Infof("%x has received %d %s votes and %d vote rejections", r.id, gr, m.Type, rj)
		switch res {
		case VoteWon:
			if r.state == RoleCandidate {
				r.becomeLeader()
				r.bcastAppend()
			} else {
				r.campaign(campaignElection)
			}
		case VoteLost:
			r.becomeFollower(r.Term, None)
		}
	case MsgTimeoutNow:
		r.logger.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.state, m.From)
	}
	return nil
}

func stepFollower(r *raft, m Message) error {
	switch m.Type {
	case MsgProp:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return errProposalDropped
		} else if r.disableProposalForwarding {
			r.logger.Infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.Term)
			return errProposalDropped
		}
		m.To = r.lead
		r.send(m)
	case MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppendEntries(m)
	case MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case MsgTransferLeader:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return nil
		}
		m.To = r.lead
		r.send(m)
	case MsgTimeoutNow:
		r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership", r.id, r.Term, m.From)
		r.hup(campaignTransfer)
	case MsgReadIndex:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return nil
		}
		m.To = r.lead
		r.send(m)
	case MsgReadIndexResp:
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return nil
		}
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
	return nil
}

func (r *raft) handleAppendEntries(m Message) {
	if m.Index < r.raftLog.committed {
		r.send(Message{To: m.From, Type: MsgAppResp, Index: r.raftLog.committed})
		return
	}
	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		r.send(Message{To: m.From, Type: MsgAppResp, Index: mlastIndex})
	} else {
		r.logger.Debugf("%x [logterm: %d, index: %d] rejected MsgApp [logterm: %d, index: %d] from %x",
			r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		hintIndex := min64(m.Index, r.raftLog.lastIndex())
		hintIndex = r.raftLog.findConflictByTerm(hintIndex, m.LogTerm)
		hintTerm, err := r.raftLog.term(hintIndex)
		if err != nil {
			panic(fmt.Sprintf("term(%d) must be valid, but got %v", hintIndex, err))
		}
		r.send(Message{
			To:         m.From,
			Type:       MsgAppResp,
			Index:      m.Index,
			Reject:     true,
			RejectHint: hintIndex,
			LogTerm:    hintTerm,
		})
	}
}

func (r *raft) handleHeartbeat(m Message) {
	r.raftLog.commitTo(m.Commit)
	r.send(Message{To: m.From, Type: MsgHeartbeatResp, Context: m.Context})
}

func (r *raft) handleSnapshot(m Message) {
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.restore(*m.Snapshot) {
		r.logger.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(Message{To: m.From, Type: MsgAppResp, Index: r.raftLog.lastIndex()})
	} else {
		r.logger.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(Message{To: m.From, Type: MsgAppResp, Index: r.raftLog.committed})
	}
}

func (r *raft) restore(s Snapshot) bool {
	if s.Metadata.Index <= r.raftLog.committed {
		return false
	}
	if r.state != RoleFollower {
		r.logger.Warningf("%x attempted to restore snapshot as leader; should never happen", r.id)
		r.becomeFollower(r.Term+1, None)
		return false
	}
	found := false
	cs := s.Metadata.ConfState
	for _, set := range [][]uint64{cs.Voters, cs.VotersOutgoing, cs.Learners, cs.LearnersNext} {
		for _, id := range set {
			if id == r.id {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		r.logger.Warningf("%x attempted to restore snapshot but it is not in the ConfState %v; should never happen", r.id, cs)
		return false
	}
	if r.raftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
		r.raftLog.commitTo(s.Metadata.Index)
		return false
	}
	r.raftLog.restore(s)
	r.trk = newProgressTracker(cs.Voters, cs.Learners)
	for id := range r.trk.progress {
		pr := r.trk.progress[id]
		pr.Next = r.raftLog.lastIndex() + 1
		pr.Inflights = newInflights(r.trk.progress[id].Inflights.size)
		pr.Match = 0
		if id == r.id {
			pr.Match = r.raftLog.lastIndex()
		}
	}
	r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] restored snapshot [index: %d, term: %d]",
		r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
	return true
}

func (r *raft) promotable() bool {
	pr := r.trk.progress[r.id]
	return pr != nil && !pr.IsLearner
}

func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + r.rng.Intn(r.electionTimeout)
}

func (r *raft) sendTimeoutNow(to uint64) {
	r.send(Message{To: to, Type: MsgTimeoutNow})
}

func (r *raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

func (r *raft) increaseUncommittedSize(ents []Entry) bool {
	s := payloadSize(ents)
	if r.uncommittedSize > 0 && s > 0 && r.uncommittedSize+s > r.maxUncommittedSize {
		return false
	}
	r.uncommittedSize += s
	return true
}

func (r *raft) reduceUncommittedSize(ents []Entry) {
	if r.uncommittedSize == 0 {
		return
	}
	s := payloadSize(ents)
	if s > r.uncommittedSize {
		r.uncommittedSize = 0
	} else {
		r.uncommittedSize -= s
	}
}

func (r *raft) loadState(state HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

func (r *raft) nodes() []uint64 {
	nodes := make([]uint64, 0, len(r.trk.voters))
	for _, id := range r.trk.voters {
		nodes = append(nodes, id)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}

func numOfPendingConf(ents []Entry) int {
	n := 0
	for i := range ents {
		if ents[i].Type == EntryConfChange {
			n++
		}
	}
	return n
}

func payloadSize(ents []Entry) uint64 {
	s := uint64(0)
	for i := range ents {
		s += uint64(len(ents[i].Data))
	}
	return s
}

func voteRespMsgType(msgt MessageType) MessageType {
	switch msgt {
	case MsgVote:
		return MsgVoteResp
	case MsgPreVote:
		return MsgPreVoteResp
	default:
		panic(fmt.Sprintf("not a vote message: %s", msgt))
	}
}



func (r *raft) String() string {
	return fmt.Sprintf("id=%x term=%d state=%v", r.id, r.Term, r.state)
}


