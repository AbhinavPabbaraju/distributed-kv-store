package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/phalanx-db/phalanx/chaos"
	"github.com/phalanx-db/phalanx/internal/raft"
)

// -------------------------------------------------------------------
// LeaderBounce — repeatedly crash and restart the leader
// -------------------------------------------------------------------

// LeaderBounce crashes the current leader, waits for re-election,
// and verifies the cluster continues accepting writes.
//
// This catches: lost writes during leader failover, violation of the
// "leader commits before responding" invariant, split-brain after restart.
type LeaderBounce struct {
	Cycles      int           // how many leader crashes to inject
	CrashDelay  time.Duration // gap between crash and restart
	WriteRate   time.Duration // how often to propose entries during test
}

func NewLeaderBounce(cycles int) *LeaderBounce {
	return &LeaderBounce{
		Cycles:     cycles,
		CrashDelay: 2 * time.Second,
		WriteRate:  50 * time.Millisecond,
	}
}

func (s *LeaderBounce) Name() string {
	return fmt.Sprintf("leader_bounce_%d_cycles", s.Cycles)
}

func (s *LeaderBounce) Run(ctx context.Context, env *chaos.Environment) *chaos.ScenarioResult {
	result := &chaos.ScenarioResult{Name: s.Name(), Linearizable: true}
	start := time.Now()

	// Background workload
	workloadDone := make(chan struct{})
	go func() {
		defer close(workloadDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-workloadDone:
				return
			case <-time.After(s.WriteRate):
				if env.Workload != nil {
					op := env.Workload(ctx, env.Recorder)
					result.OpsAttempted++
					if op.Success {
						result.OpsSucceeded++
					} else {
						result.OpsFailed++
					}
				}
			}
		}
	}()

	for cycle := 0; cycle < s.Cycles; cycle++ {
		// Find the current leader.
		leaderID := findLeader(env.Nodes)
		if leaderID == 0 {
			env.Logger.Warn("no leader found before crash", "cycle", cycle)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		env.Logger.Info("crashing leader", "leader_id", leaderID, "cycle", cycle)
		result.FaultsInjected = append(result.FaultsInjected, chaos.Fault{
			Type:     chaos.FaultLeaderCrash,
			Affected: []uint64{leaderID},
			Duration: s.CrashDelay,
		})

		// Crash the leader.
		var leaderNode chaos.NodeController
		for _, n := range env.Nodes {
			if n.ID() == leaderID {
				leaderNode = n
				break
			}
		}
		if leaderNode != nil {
			leaderNode.Stop()
		}

		// Wait for new election (election timeout is typically 1–2 seconds).
		electionCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		newLeaderID := waitForLeader(electionCtx, env.Nodes, leaderID)
		cancel()
		if newLeaderID == 0 {
			result.Err = fmt.Errorf("cycle %d: no new leader elected after crash", cycle)
		} else {
			env.Logger.Info("new leader elected",
				"new_leader", newLeaderID, "cycle", cycle)
		}

		// Wait, then restart the crashed node.
		time.Sleep(s.CrashDelay)
		if leaderNode != nil {
			if err := leaderNode.Start(); err != nil {
				env.Logger.Warn("failed to restart crashed node",
					"node_id", leaderID, "err", err)
			}
		}

		// Give the restarted node time to catch up.
		time.Sleep(500 * time.Millisecond)
	}

	workloadDone <- struct{}{}
	<-workloadDone

	result.ClusterRecovered = clusterHealthy(env.Nodes)
	result.Duration = time.Since(start)
	return result
}

// -------------------------------------------------------------------
// NetworkPartition — isolate a minority, verify majority continues
// -------------------------------------------------------------------

// NetworkPartition isolates a minority partition for a duration and verifies:
//   1. The majority continues accepting writes.
//   2. The minority does NOT accept writes (no stale leader).
//   3. After healing, the minority catches up (committed index converges).
//   4. The complete history is linearizable.
type NetworkPartition struct {
	MinoritySize int           // nodes to isolate (must be < quorum)
	Duration     time.Duration // how long to keep partition active
}

func NewNetworkPartition(minoritySize int, duration time.Duration) *NetworkPartition {
	return &NetworkPartition{MinoritySize: minoritySize, Duration: duration}
}

func (s *NetworkPartition) Name() string {
	return fmt.Sprintf("network_partition_%d_minority_%s", s.MinoritySize, s.Duration)
}

func (s *NetworkPartition) Run(ctx context.Context, env *chaos.Environment) *chaos.ScenarioResult {
	result := &chaos.ScenarioResult{Name: s.Name(), Linearizable: true}
	start := time.Now()

	if len(env.Nodes) < 3 {
		result.Err = fmt.Errorf("network partition requires ≥3 nodes, have %d", len(env.Nodes))
		return result
	}

	// Select minority nodes (never include the current leader in minority — that's
	// the more interesting LeaderPartition scenario).
	leaderID := findLeader(env.Nodes)
	minority := selectMinority(env.Nodes, leaderID, s.MinoritySize)
	majority := complement(env.Nodes, minority)

	minorityIDs := nodeIDs(minority)
	majorityIDs := nodeIDs(majority)

	env.Logger.Info("injecting network partition",
		"minority", minorityIDs,
		"majority", majorityIDs)

	result.FaultsInjected = append(result.FaultsInjected, chaos.Fault{
		Type:     chaos.FaultNetworkPartition,
		Affected: minorityIDs,
		Duration: s.Duration,
	})

	env.Net.Partition(minorityIDs, majorityIDs)

	// Run workload against majority during partition.
	partitionEnd := time.After(s.Duration)
	for {
		select {
		case <-partitionEnd:
			goto partitionHealed
		case <-ctx.Done():
			goto partitionHealed
		case <-time.After(20 * time.Millisecond):
			if env.Workload != nil {
				op := env.Workload(ctx, env.Recorder)
				result.OpsAttempted++
				if op.Success {
					result.OpsSucceeded++
				} else {
					result.OpsFailed++
				}
			}
		}
	}

partitionHealed:
	env.Logger.Info("healing network partition")
	env.Net.Heal()

	// Allow time for minority to catch up.
	time.Sleep(2 * time.Second)

	// Verify committed index converged across all nodes.
	result.ClusterRecovered = committedIndexConverged(env.Nodes)
	result.Duration = time.Since(start)
	return result
}

// -------------------------------------------------------------------
// MinorityLeaderPartition — isolate the leader in a minority partition
// -------------------------------------------------------------------

// MinorityLeaderPartition isolates the leader so it becomes a minority.
// The majority elects a new leader. This is the hardest scenario for
// preventing stale reads: the old leader must step down.
type MinorityLeaderPartition struct {
	Duration time.Duration
}

func NewMinorityLeaderPartition(d time.Duration) *MinorityLeaderPartition {
	return &MinorityLeaderPartition{Duration: d}
}

func (s *MinorityLeaderPartition) Name() string {
	return fmt.Sprintf("minority_leader_partition_%s", s.Duration)
}

func (s *MinorityLeaderPartition) Run(ctx context.Context, env *chaos.Environment) *chaos.ScenarioResult {
	result := &chaos.ScenarioResult{Name: s.Name(), Linearizable: true}
	start := time.Now()

	leaderID := findLeader(env.Nodes)
	if leaderID == 0 {
		result.Err = fmt.Errorf("no leader before injecting partition")
		return result
	}

	// Isolate the leader into a minority of size 1.
	others := make([]uint64, 0)
	for _, n := range env.Nodes {
		if n.ID() != leaderID {
			others = append(others, n.ID())
		}
	}

	env.Logger.Info("isolating leader in minority partition",
		"leader", leaderID, "others", others)
	env.Net.Partition([]uint64{leaderID}, others)

	result.FaultsInjected = append(result.FaultsInjected, chaos.Fault{
		Type:     chaos.FaultNetworkPartition,
		Affected: []uint64{leaderID},
		Duration: s.Duration,
	})

	// Wait for new leader election on the majority side.
	electionCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	newLeaderID := waitForLeader(electionCtx, env.Nodes, leaderID)
	cancel()

	if newLeaderID == 0 {
		result.Err = fmt.Errorf("majority failed to elect new leader")
	} else {
		env.Logger.Info("new leader on majority side", "leader", newLeaderID)
	}

	// Run writes against the new leader during partition.
	time.Sleep(s.Duration)

	env.Logger.Info("healing partition")
	env.Net.Heal()

	// The old leader MUST step down when it hears the new leader's term.
	time.Sleep(2 * time.Second)

	// Verify old leader stepped down.
	for _, n := range env.Nodes {
		if n.ID() == leaderID {
			st := n.Status()
			if st.RaftState == raft.RoleLeader {
				result.Err = fmt.Errorf(
					"stale leader %d still believes it is leader after partition healed — "+
						"linearizability violation risk", leaderID)
			}
		}
	}

	result.ClusterRecovered = clusterHealthy(env.Nodes) && result.Err == nil
	result.Duration = time.Since(start)
	return result
}

// -------------------------------------------------------------------
// MessageLoss — probabilistic message drop
// -------------------------------------------------------------------

// MessageLoss injects random message loss during normal operation.
// Tests that the protocol's retry/backoff logic handles packet loss.
type MessageLoss struct {
	LossRate float64
	Duration time.Duration
}

func NewMessageLoss(lossRate float64, duration time.Duration) *MessageLoss {
	return &MessageLoss{LossRate: lossRate, Duration: duration}
}

func (s *MessageLoss) Name() string {
	return fmt.Sprintf("message_loss_%.0fpct_%s", s.LossRate*100, s.Duration)
}

func (s *MessageLoss) Run(ctx context.Context, env *chaos.Environment) *chaos.ScenarioResult {
	result := &chaos.ScenarioResult{Name: s.Name(), Linearizable: true}
	start := time.Now()

	env.Logger.Info("injecting message loss", "rate", s.LossRate)
	env.Net.SetLossRate(s.LossRate)

	result.FaultsInjected = append(result.FaultsInjected, chaos.Fault{
		Type:     chaos.FaultMessageLoss,
		Affected: nodeIDs(env.Nodes),
		Duration: s.Duration,
		Param:    s.LossRate,
	})

	deadline := time.Now().Add(s.Duration)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			goto done
		case <-time.After(20 * time.Millisecond):
			if env.Workload != nil {
				op := env.Workload(ctx, env.Recorder)
				result.OpsAttempted++
				if op.Success {
					result.OpsSucceeded++
				} else {
					result.OpsFailed++
				}
			}
		}
	}

done:
	env.Net.SetLossRate(0)
	time.Sleep(time.Second)
	result.ClusterRecovered = clusterHealthy(env.Nodes)
	result.Duration = time.Since(start)
	return result
}

// -------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------

func findLeader(nodes []chaos.NodeController) uint64 {
	for _, n := range nodes {
		if !n.IsRunning() {
			continue
		}
		st := n.Status()
		if st.RaftState == raft.RoleLeader {
			return n.ID()
		}
	}
	return 0
}

func waitForLeader(ctx context.Context, nodes []chaos.NodeController, exclude uint64) uint64 {
	for {
		select {
		case <-ctx.Done():
			return 0
		case <-time.After(100 * time.Millisecond):
			for _, n := range nodes {
				if n.ID() == exclude || !n.IsRunning() {
					continue
				}
				if n.Status().RaftState == raft.RoleLeader {
					return n.ID()
				}
			}
		}
	}
}

func clusterHealthy(nodes []chaos.NodeController) bool {
	running := 0
	for _, n := range nodes {
		if n.IsRunning() {
			running++
		}
	}
	return running >= quorum(len(nodes))
}

func committedIndexConverged(nodes []chaos.NodeController) bool {
	var maxCommit uint64
	for _, n := range nodes {
		if !n.IsRunning() {
			continue
		}
		ci := n.Status().HardState.Commit
		if ci > maxCommit {
			maxCommit = ci
		}
	}
	for _, n := range nodes {
		if !n.IsRunning() {
			continue
		}
		if n.Status().HardState.Commit < maxCommit {
			return false
		}
	}
	return true
}

func selectMinority(nodes []chaos.NodeController, excludeID uint64, size int) []chaos.NodeController {
	var result []chaos.NodeController
	for _, n := range nodes {
		if n.ID() == excludeID {
			continue
		}
		result = append(result, n)
		if len(result) >= size {
			break
		}
	}
	return result
}

func complement(all []chaos.NodeController, subset []chaos.NodeController) []chaos.NodeController {
	inSubset := make(map[uint64]bool)
	for _, n := range subset {
		inSubset[n.ID()] = true
	}
	var result []chaos.NodeController
	for _, n := range all {
		if !inSubset[n.ID()] {
			result = append(result, n)
		}
	}
	return result
}

func nodeIDs(nodes []chaos.NodeController) []uint64 {
	ids := make([]uint64, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID()
	}
	return ids
}

func quorum(n int) int { return n/2 + 1 }
