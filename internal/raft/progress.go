package raft

type ProgressStateType uint8

const (
	StateProbe     ProgressStateType = iota
	StateReplicate ProgressStateType = iota
	StateSnapshot  ProgressStateType = iota
)

func (s ProgressStateType) String() string {
	switch s {
	case StateProbe:
		return "StateProbe"
	case StateReplicate:
		return "StateReplicate"
	case StateSnapshot:
		return "StateSnapshot"
	default:
		return "unknown"
	}
}

type inflights struct {
	start  int
	count  int
	size   int
	buffer []uint64
}

func newInflights(size int) *inflights {
	return &inflights{size: size, buffer: make([]uint64, size)}
}

func (i *inflights) add(inflight uint64) {
	next := i.start + i.count
	if next >= i.size {
		next -= i.size
	}
	i.buffer[next] = inflight
	i.count++
}

func (i *inflights) freeTo(to uint64) {
	if i.count == 0 || to < i.buffer[i.start] {
		return
	}
	idx := i.start
	var j int
	for j = 0; j < i.count; j++ {
		if to < i.buffer[idx] {
			break
		}
		idx++
		if idx >= i.size {
			idx -= i.size
		}
	}
	i.count -= j
	i.start = idx
}

func (i *inflights) full() bool {
	return i.count == i.size
}

func (i *inflights) reset() {
	i.count = 0
	i.start = 0
}

type Progress struct {
	Match     uint64
	Next      uint64
	State     ProgressStateType
	PendingSnapshot uint64
	RecentActive    bool
	MsgAppFlowPaused bool
	IsLearner bool
	Inflights *inflights
}

func (p *Progress) ResetState(state ProgressStateType) {
	p.MsgAppFlowPaused = false
	p.PendingSnapshot = 0
	p.State = state
	p.Inflights.reset()
}

func (p *Progress) BecomeProbe() {
	if p.State == StateSnapshot {
		pendingSnapshot := p.PendingSnapshot
		p.ResetState(StateProbe)
		p.Next = max64(p.Match+1, pendingSnapshot+1)
	} else {
		p.ResetState(StateProbe)
		p.Next = p.Match + 1
	}
}

func (p *Progress) BecomeReplicate() {
	p.ResetState(StateReplicate)
	p.Next = p.Match + 1
}

func (p *Progress) BecomeSnapshot(snapshotIndex uint64) {
	p.ResetState(StateSnapshot)
	p.PendingSnapshot = snapshotIndex
}

func (p *Progress) MaybeUpdate(n uint64) bool {
	updated := false
	if p.Match < n {
		p.Match = n
		updated = true
		p.MsgAppFlowPaused = false
	}
	p.Next = max64(p.Next, n+1)
	return updated
}

func (p *Progress) MaybeDecrTo(rejected, matchHint uint64) bool {
	if p.State == StateReplicate {
		if rejected <= p.Match {
			return false
		}
		p.Next = p.Match + 1
		return true
	}
	if p.Next-1 != rejected {
		return false
	}
	p.Next = max64(min64(rejected, matchHint+1), 1)
	p.MsgAppFlowPaused = false
	return true
}

func (p *Progress) IsPaused() bool {
	switch p.State {
	case StateProbe:
		return p.MsgAppFlowPaused
	case StateReplicate:
		return p.Inflights.full()
	case StateSnapshot:
		return true
	}
	return false
}

func (p *Progress) SentEntries(lastIdx uint64) {
	switch p.State {
	case StateReplicate:
		p.Next = lastIdx + 1
		p.Inflights.add(lastIdx)
	case StateProbe:
		p.MsgAppFlowPaused = true
	}
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
