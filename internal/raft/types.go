package raft

import "fmt"

type StateRole uint8

const (
	RoleFollower     StateRole = iota
	RolePreCandidate StateRole = iota
	RoleCandidate    StateRole = iota
	RoleLeader       StateRole = iota
)

func (r StateRole) String() string {
	switch r {
	case RoleFollower:
		return "follower"
	case RolePreCandidate:
		return "pre-candidate"
	case RoleCandidate:
		return "candidate"
	case RoleLeader:
		return "leader"
	default:
		return "unknown"
	}
}

type EntryType uint8

const (
	EntryNormal     EntryType = iota
	EntryConfChange EntryType = iota
)

type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

func (e Entry) String() string {
	return fmt.Sprintf("Entry{term=%d index=%d type=%d len=%d}", e.Term, e.Index, e.Type, len(e.Data))
}

type MessageType uint8

const (
	MsgHup            MessageType = iota
	MsgBeat           MessageType = iota
	MsgProp           MessageType = iota
	MsgApp            MessageType = iota
	MsgAppResp        MessageType = iota
	MsgVote           MessageType = iota
	MsgVoteResp       MessageType = iota
	MsgPreVote        MessageType = iota
	MsgPreVoteResp    MessageType = iota
	MsgSnap           MessageType = iota
	MsgHeartbeat      MessageType = iota
	MsgHeartbeatResp  MessageType = iota
	MsgUnreachable    MessageType = iota
	MsgSnapStatus     MessageType = iota
	MsgCheckQuorum    MessageType = iota
	MsgTransferLeader MessageType = iota
	MsgTimeoutNow     MessageType = iota
	MsgReadIndex      MessageType = iota
	MsgReadIndexResp  MessageType = iota
)

func (t MessageType) String() string {
	names := [...]string{
		"Hup", "Beat", "Prop", "App", "AppResp",
		"Vote", "VoteResp", "PreVote", "PreVoteResp",
		"Snap", "Heartbeat", "HeartbeatResp",
		"Unreachable", "SnapStatus", "CheckQuorum",
		"TransferLeader", "TimeoutNow",
		"ReadIndex", "ReadIndexResp",
	}
	if int(t) < len(names) {
		return names[t]
	}
	return fmt.Sprintf("MessageType(%d)", t)
}

type Message struct {
	Type       MessageType
	To         uint64
	From       uint64
	Term       uint64
	LogTerm    uint64
	Index      uint64
	Entries    []Entry
	Commit     uint64
	Reject     bool
	RejectHint uint64
	Context    []byte
	Snapshot   *Snapshot
}

func (m Message) IsLocal() bool {
	return m.Type == MsgHup || m.Type == MsgBeat || m.Type == MsgCheckQuorum ||
		m.Type == MsgProp || m.Type == MsgUnreachable || m.Type == MsgSnapStatus
}

type HardState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

func (hs HardState) IsEmpty() bool {
	return hs.Term == 0 && hs.Vote == 0 && hs.Commit == 0
}

type SoftState struct {
	Lead      uint64
	RaftState StateRole
}

type ConfState struct {
	Voters         []uint64
	Learners       []uint64
	VotersOutgoing []uint64
	LearnersNext   []uint64
	AutoLeave      bool
}

type SnapshotMetadata struct {
	ConfState ConfState
	Index     uint64
	Term      uint64
}

type Snapshot struct {
	Data     []byte
	Metadata SnapshotMetadata
}

func (s *Snapshot) IsEmpty() bool {
	return s == nil || s.Metadata.Index == 0
}


const (
)

type ConfChangeType uint8

const (
	ConfChangeAddNode        ConfChangeType = iota
	ConfChangeRemoveNode     ConfChangeType = iota
	ConfChangeUpdateNode     ConfChangeType = iota
	ConfChangeAddLearnerNode ConfChangeType = iota
)

type ConfChange struct {
	Type    ConfChangeType
	NodeID  uint64
	Context []byte
	ID      uint64
}

type ReadState struct {
	Index      uint64
	RequestCtx []byte
}

const None uint64 = 0
const LocalAppendThread uint64 = ^uint64(0)
