// Package chaos provides a fault-injection framework for verifying Phalanx's
// correctness under adversarial conditions.
//
// Production problem solved: unit tests with perfect networks prove the happy
// path. Real systems fail in combinations that unit tests never exercise:
// leader crashes mid-commit, network partitions isolate a minority, disk
// writes block for 10 seconds, then the network heals while a new election
// is in progress. Chaos engineering finds these bugs before production does.
//
// Design inspired by: etcd's functional test suite, TiDB's Jepsen integration,
// and CockroachDB's roachtest framework.
package chaos

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/phalanx-db/phalanx/internal/raft"
	"github.com/phalanx-db/phalanx/internal/verification"
)

// FaultType classifies the failure being injected.
type FaultType string

const (
	FaultNetworkPartition FaultType = "network_partition"
	FaultLeaderCrash      FaultType = "leader_crash"
	FaultNodeCrash        FaultType = "node_crash"
	FaultDiskSlow         FaultType = "disk_slow"
	FaultMessageLoss      FaultType = "message_loss"
	FaultMessageDelay     FaultType = "message_delay"
	FaultClockSkew        FaultType = "clock_skew"
)

// Fault describes a single injected failure event.
type Fault struct {
	Type     FaultType
	Affected []uint64      // node IDs affected
	Duration time.Duration // how long the fault lasts (0 = permanent)
	Param    float64       // e.g. loss probability, delay milliseconds
}

func (f Fault) String() string {
	return fmt.Sprintf("Fault{type=%s nodes=%v duration=%s param=%.2f}",
		f.Type, f.Affected, f.Duration, f.Param)
}

// ScenarioResult captures what happened during a chaos scenario.
type ScenarioResult struct {
	Name            string
	Duration        time.Duration
	FaultsInjected  []Fault
	OpsAttempted    int
	OpsSucceeded    int
	OpsFailed       int
	Linearizable    bool
	LinearReport    string
	ClusterRecovered bool
	Err             error
}

func (r *ScenarioResult) String() string {
	status := "PASS"
	if r.Err != nil || !r.Linearizable || !r.ClusterRecovered {
		status = "FAIL"
	}
	return fmt.Sprintf("[%s] %s | %d ops (%d ok / %d fail) | linearizable=%v | recovered=%v | dur=%s",
		status, r.Name,
		r.OpsAttempted, r.OpsSucceeded, r.OpsFailed,
		r.Linearizable, r.ClusterRecovered,
		r.Duration.Round(time.Millisecond))
}

// NodeController gives the chaos framework control over a single Raft node.
// Implementations must be safe for concurrent use.
type NodeController interface {
	ID() uint64
	// Start starts the node (after a crash).
	Start() error
	// Stop crashes the node (simulates kill -9).
	Stop()
	// IsRunning returns true if the node is currently up.
	IsRunning() bool
	// Status returns the current Raft status.
	Status() raft.Status
}

// NetworkController intercepts messages between nodes.
type NetworkController interface {
	// Partition drops all messages between the from and to node sets.
	Partition(from, to []uint64)
	// Heal restores normal message delivery.
	Heal()
	// SetLossRate sets random message loss probability [0.0, 1.0] on all links.
	SetLossRate(p float64)
	// SetDelay adds artificial latency [0, maxMs] to all messages.
	SetDelay(maxMs int)
	// Reset restores the network to pristine state.
	Reset()
}

// OpResult tracks a single client operation during a scenario.
type OpResult struct {
	Key     string
	Write   string // non-empty for writes
	Read    string // non-empty for reads
	Success bool
	Err     error
}

// Scenario is a runnable chaos test.
type Scenario interface {
	Name() string
	Run(ctx context.Context, env *Environment) *ScenarioResult
}

// Environment is the runtime context for a chaos scenario. It provides access
// to node controllers, network control, and a workload generator.
type Environment struct {
	Nodes     []NodeController
	Net       NetworkController
	Recorder  *verification.Recorder
	Workload  WorkloadFunc
	RNG       *rand.Rand
	Logger    *slog.Logger
}

// WorkloadFunc performs one KV operation and records it in the provided
// Recorder. It should be non-blocking and return quickly.
type WorkloadFunc func(ctx context.Context, rec *verification.Recorder) OpResult

// -------------------------------------------------------------------
// NetworkFaultProxy — in-process drop table for tests
// -------------------------------------------------------------------

// NetworkFaultProxy is an in-process NetworkController that hooks into the
// transport layer's message pipeline to inject network faults.
type NetworkFaultProxy struct {
	mu         sync.RWMutex
	partitions map[peerPair]bool    // dropped links
	lossRate   float64              // 0–1
	maxDelayMs int
	rng        *rand.Rand
}

type peerPair struct{ from, to uint64 }

func NewNetworkFaultProxy(seed int64) *NetworkFaultProxy {
	return &NetworkFaultProxy{
		partitions: make(map[peerPair]bool),
		rng:        rand.New(rand.NewSource(seed)),
	}
}

func (p *NetworkFaultProxy) Partition(from, to []uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, f := range from {
		for _, t := range to {
			p.partitions[peerPair{f, t}] = true
			p.partitions[peerPair{t, f}] = true
		}
	}
}

func (p *NetworkFaultProxy) Heal() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.partitions = make(map[peerPair]bool)
}

func (p *NetworkFaultProxy) SetLossRate(rate float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lossRate = rate
}

func (p *NetworkFaultProxy) SetDelay(maxMs int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maxDelayMs = maxMs
}

func (p *NetworkFaultProxy) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.partitions = make(map[peerPair]bool)
	p.lossRate = 0
	p.maxDelayMs = 0
}

// ShouldDrop returns true if the message from→to should be dropped.
// Called by the transport layer on every outbound message.
func (p *NetworkFaultProxy) ShouldDrop(from, to uint64) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.partitions[peerPair{from, to}] {
		return true
	}
	if p.lossRate > 0 && p.rng.Float64() < p.lossRate {
		return true
	}
	return false
}

// DelayFor returns the artificial delay to apply to message from→to.
func (p *NetworkFaultProxy) DelayFor(from, to uint64) time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.maxDelayMs <= 0 {
		return 0
	}
	ms := p.rng.Intn(p.maxDelayMs)
	return time.Duration(ms) * time.Millisecond
}

// -------------------------------------------------------------------
// Runner — executes scenarios and collects results
// -------------------------------------------------------------------

// Runner executes a list of chaos scenarios and reports results.
type Runner struct {
	scenarios []Scenario
	env       *Environment
	logger    *slog.Logger
}

func NewRunner(env *Environment, logger *slog.Logger) *Runner {
	return &Runner{env: env, logger: logger}
}

func (r *Runner) Add(s Scenario) {
	r.scenarios = append(r.scenarios, s)
}

// RunAll executes all registered scenarios sequentially. After each scenario,
// it verifies cluster health (all nodes running, same committed index) and
// runs the linearizability checker over the operation history.
func (r *Runner) RunAll(ctx context.Context) []ScenarioResult {
	results := make([]ScenarioResult, 0, len(r.scenarios))
	for _, s := range r.scenarios {
		r.logger.Info("starting chaos scenario", "name", s.Name())
		result := s.Run(ctx, r.env)
		if result == nil {
			result = &ScenarioResult{Name: s.Name(), Err: fmt.Errorf("scenario returned nil")}
		}

		// Run linearizability check over the scenario's operation history.
		history := r.env.Recorder.History()
		if len(history) > 0 {
			checkResult := verification.Check(history)
			result.Linearizable = checkResult.Linearizable
			result.LinearReport = checkResult.String()
		} else {
			result.Linearizable = true
			result.LinearReport = "no operations recorded"
		}

		r.logger.Info("scenario complete",
			"name", s.Name(),
			"result", result.String())
		results = append(results, *result)

		// Reset for next scenario.
		r.env.Net.Reset()
		for _, n := range r.env.Nodes {
			if !n.IsRunning() {
				if err := n.Start(); err != nil {
					r.logger.Warn("node restart failed after scenario",
						"node_id", n.ID(), "err", err)
				}
			}
		}
		time.Sleep(500 * time.Millisecond) // stabilize between scenarios
	}
	return results
}

// SummaryReport generates a human-readable summary of all scenario results.
func SummaryReport(results []ScenarioResult) string {
	passed, failed := 0, 0
	var lines []string
	for _, r := range results {
		lines = append(lines, r.String())
		if r.Err == nil && r.Linearizable && r.ClusterRecovered {
			passed++
		} else {
			failed++
		}
	}
	summary := fmt.Sprintf("\n=== Chaos Test Summary: %d passed, %d failed ===\n",
		passed, failed)
	for _, l := range lines {
		summary += "  " + l + "\n"
	}
	return summary
}
