package transport

import (
	"context"
	"sync"
	"time"

	"github.com/phalanx-db/phalanx/internal/raft"
)

// InMemCluster is a test double for a real network: it routes Raft messages
// between in-process nodes via channels. A NetworkFaultProxy can intercept
// every message to simulate packet loss, reordering, and partitions.
//
// Usage:
//
//	cluster := NewInMemCluster()
//	t1 := cluster.NewTransport(1, handler1)
//	t2 := cluster.NewTransport(2, handler2)
//	cluster.Proxy.Partition([]uint64{1}, []uint64{2, 3})
//	// ... run Raft ...
//	cluster.Proxy.Heal()
type InMemCluster struct {
	mu     sync.RWMutex
	nodes  map[uint64]*InMemTransport
	Proxy  *InMemProxy
}

// NewInMemCluster creates a cluster with no initial nodes or faults.
func NewInMemCluster() *InMemCluster {
	return &InMemCluster{
		nodes: make(map[uint64]*InMemTransport),
		Proxy: &InMemProxy{dropped: make(map[inMemPair]bool)},
	}
}

// NewTransport creates and registers an InMemTransport for nodeID.
func (c *InMemCluster) NewTransport(nodeID uint64, handler MessageHandler) *InMemTransport {
	t := &InMemTransport{
		id:      nodeID,
		cluster: c,
		handler: handler,
		stopC:   make(chan struct{}),
	}
	c.mu.Lock()
	c.nodes[nodeID] = t
	c.mu.Unlock()
	return t
}

// deliver routes a message to its destination node.
func (c *InMemCluster) deliver(msg raft.Message) {
	c.mu.RLock()
	dest, ok := c.nodes[msg.To]
	c.mu.RUnlock()
	if !ok {
		return
	}
	if err := dest.handler.Process(context.Background(), msg); err != nil {
		// Delivery errors (e.g. node stopped) are silently dropped,
		// which is how real networks behave.
		return
	}
}

// InMemTransport is a node-local transport backed by InMemCluster.
type InMemTransport struct {
	id      uint64
	cluster *InMemCluster
	handler MessageHandler
	stopC   chan struct{}
}

func (t *InMemTransport) Send(msgs []raft.Message) {
	for _, m := range msgs {
		if m.To == raft.None {
			continue
		}
		// Check fault proxy.
		if t.cluster.Proxy.ShouldDrop(m.From, m.To) {
			continue
		}
		delay := t.cluster.Proxy.DelayFor()
		msg := m
		msg.From = t.id
		if delay > 0 {
			go func() {
				select {
				case <-time.After(delay):
					t.cluster.deliver(msg)
				case <-t.stopC:
				}
			}()
		} else {
			t.cluster.deliver(msg)
		}
	}
}

func (t *InMemTransport) AddPeer(_ uint64, _ string) {}
func (t *InMemTransport) RemovePeer(_ uint64)         {}
func (t *InMemTransport) ActiveSince(_ uint64) time.Time { return time.Time{} }
func (t *InMemTransport) Start(_ string) error           { return nil }
func (t *InMemTransport) Stop()                          { close(t.stopC) }

// -------------------------------------------------------------------
// InMemProxy — fault injection for the in-process cluster
// -------------------------------------------------------------------

type inMemPair struct{ from, to uint64 }

// InMemProxy intercepts messages between in-process nodes to simulate
// network faults: partitions, random loss, and artificial delay.
type InMemProxy struct {
	mu       sync.RWMutex
	dropped  map[inMemPair]bool
	lossRate float64
	maxDelay time.Duration
	rng      rngSource
}

type rngSource interface {
	Float64() float64
	Intn(n int) int
}

// Partition makes all messages between every node in 'a' and every node in 'b'
// be silently dropped in both directions.
func (p *InMemProxy) Partition(a, b []uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, from := range a {
		for _, to := range b {
			p.dropped[inMemPair{from, to}] = true
			p.dropped[inMemPair{to, from}] = true
		}
	}
}

// Isolate is a convenience wrapper that isolates one node from all others.
func (p *InMemProxy) Isolate(id uint64, all []uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, other := range all {
		if other == id {
			continue
		}
		p.dropped[inMemPair{id, other}] = true
		p.dropped[inMemPair{other, id}] = true
	}
}

// Heal restores normal message delivery by clearing all partition rules.
func (p *InMemProxy) Heal() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropped = make(map[inMemPair]bool)
}

// SetLossRate sets the probability [0.0, 1.0] that any individual message is dropped.
func (p *InMemProxy) SetLossRate(r float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lossRate = r
}

// SetMaxDelay sets the maximum artificial delay added to each message.
func (p *InMemProxy) SetMaxDelay(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maxDelay = d
}

// Reset clears all faults.
func (p *InMemProxy) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropped = make(map[inMemPair]bool)
	p.lossRate = 0
	p.maxDelay = 0
}

// ShouldDrop returns true if the from→to message should be silently dropped.
func (p *InMemProxy) ShouldDrop(from, to uint64) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.dropped[inMemPair{from, to}] {
		return true
	}
	if p.lossRate > 0 && p.rng != nil {
		return p.rng.Float64() < p.lossRate
	}
	return false
}

// DelayFor returns the artificial delay to apply to a message.
func (p *InMemProxy) DelayFor() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.maxDelay <= 0 || p.rng == nil {
		return 0
	}
	ms := time.Duration(p.rng.Intn(int(p.maxDelay/time.Millisecond))) * time.Millisecond
	return ms
}
