package raft

import "sort"

type VoteResult uint8

const (
	VotePending VoteResult = iota
	VoteWon     VoteResult = iota
	VoteLost    VoteResult = iota
)

type QuorumTracker struct {
	votes    map[uint64]bool
	voters   map[uint64]struct{}
	learners map[uint64]struct{}
}

func newQuorumTracker(voters []uint64, learners []uint64) *QuorumTracker {
	qt := &QuorumTracker{
		votes:    make(map[uint64]bool),
		voters:   make(map[uint64]struct{}, len(voters)),
		learners: make(map[uint64]struct{}, len(learners)),
	}
	for _, id := range voters {
		qt.voters[id] = struct{}{}
	}
	for _, id := range learners {
		qt.learners[id] = struct{}{}
	}
	return qt
}

func (qt *QuorumTracker) RecordVote(id uint64, v bool) {
	if _, ok := qt.votes[id]; !ok {
		qt.votes[id] = v
	}
}

func (qt *QuorumTracker) TallyVotes() VoteResult {
	granted, rejected, missing := 0, 0, 0
	for id := range qt.voters {
		v, ok := qt.votes[id]
		if !ok {
			missing++
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	q := quorum(len(qt.voters))
	if granted >= q {
		return VoteWon
	}
	if rejected+1 > len(qt.voters)-q {
		return VoteLost
	}
	_ = missing
	return VotePending
}

func quorum(n int) int {
	return n/2 + 1
}

type ProgressTracker struct {
	progress map[uint64]*Progress
	voters   []uint64
	learners []uint64

	votes    map[uint64]bool
}

func newProgressTracker(voters []uint64, learners []uint64) *ProgressTracker {
	pt := &ProgressTracker{
		progress: make(map[uint64]*Progress),
		voters:   voters,
		learners: learners,
		votes:    make(map[uint64]bool),
	}
	for _, id := range voters {
		pt.progress[id] = &Progress{}
	}
	for _, id := range learners {
		pt.progress[id] = &Progress{IsLearner: true}
	}
	return pt
}

func (pt *ProgressTracker) Visit(fn func(id uint64, p *Progress)) {
	for id, p := range pt.progress {
		fn(id, p)
	}
}

func (pt *ProgressTracker) CommittedIndex() uint64 {
	indices := make([]uint64, 0, len(pt.voters))
	for _, id := range pt.voters {
		if p, ok := pt.progress[id]; ok {
			indices = append(indices, p.Match)
		}
	}
	if len(indices) == 0 {
		return 0
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] > indices[j] })
	q := quorum(len(pt.voters))
	return indices[q-1]
}

func (pt *ProgressTracker) ResetVotes() {
	pt.votes = make(map[uint64]bool)
}

func (pt *ProgressTracker) RecordVote(id uint64, v bool) {
	if _, ok := pt.votes[id]; !ok {
		pt.votes[id] = v
	}
}

func (pt *ProgressTracker) TallyVotes() (granted, rejected, missing int) {
	for _, id := range pt.voters {
		v, ok := pt.votes[id]
		if !ok {
			missing++
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	return
}

func (pt *ProgressTracker) VoteResult() VoteResult {
	granted, rejected, _ := pt.TallyVotes()
	q := quorum(len(pt.voters))
	if granted >= q {
		return VoteWon
	}
	if rejected+1 > len(pt.voters)-q {
		return VoteLost
	}
	return VotePending
}

func (pt *ProgressTracker) HasQuorum(fn func(id uint64) bool) bool {
	count := 0
	for _, id := range pt.voters {
		if fn(id) {
			count++
		}
	}
	return count >= quorum(len(pt.voters))
}
