package metrics

import (
	"fmt"
	"net/http"
	"time"

	"github.com/phalanx-db/phalanx/internal/raft"
)

// RaftMetrics is the canonical set of metrics every production Raft system
// should export. These metrics tell the story of cluster health at a glance.
//
// Dashboard layout (Grafana):
//
//	Row 1: is_leader, current_term, leader_changes_total
//	Row 2: committed_index, applied_index, applied_lag (= committed - applied)
//	Row 3: proposal_duration p50/p95/p99, proposal_failures_total
//	Row 4: replication_lag_bytes by peer, heartbeat_duration p99
//	Row 5: wal_fsync_duration p99, snapshot_size_bytes, wal_segments
//	Row 6: election_duration p99, election_attempts_total
type RaftMetrics struct {
	reg *Registry

	// Cluster identity
	IsLeader    *Gauge // 1 if this node is the current leader, 0 otherwise
	CurrentTerm *Gauge // Raft term number
	NodeID      *Gauge // This node's ID (constant; useful for dashboard filtering)

	// Log progress
	CommitIndex   *Gauge // Index of the last committed log entry
	AppliedIndex  *Gauge // Index of the last entry applied to the state machine
	LastLogIndex  *Gauge // Index of the last log entry (committed or not)
	AppliedLag    *Gauge // CommitIndex - AppliedIndex (>0 = SM behind)

	// Leader tracking
	LeaderChanges *Counter // total leader elections observed by this node
	LeaderID      *Gauge   // current leader's node ID (0 if unknown)

	// Proposal pipeline
	ProposalDuration *Histogram // time from Propose() to committed (seconds)
	ProposalTotal    *Counter   // proposals accepted by Raft
	ProposalFailed   *Counter   // proposals rejected (not leader, dropped, etc.)
	ProposalPending  *Gauge     // proposals in flight right now

	// Replication
	ReplicationLag *CounterVec // bytes lag per peer — WithLabel("peer_id")
	HeartbeatDur   *Histogram  // duration of heartbeat round-trips (seconds)

	// Storage
	WALFsyncDur   *Histogram // WAL fdatasync latency (seconds)
	WALSegments   *Gauge     // number of WAL segment files on disk
	WALBytes      *Gauge     // estimated total WAL bytes on disk
	SnapSizeBytes *Gauge     // size of the latest snapshot file (bytes)
	SnapIndex     *Gauge     // Raft index at which the latest snapshot was taken

	// Elections
	ElectionDuration *Histogram // time from election start to new leader (seconds)
	ElectionAttempts *Counter   // elections started by this node

	// Reads
	ReadIndexDuration *Histogram // LinearizableGet latency (seconds)
	StaleReadTotal    *Counter   // reads served without going through ReadIndex (dangerous)

	// Error budget
	ReceiveErrors *Counter // inbound message decode/process errors
	SendErrors    *Counter // outbound message send errors
}

// NewRaftMetrics registers all Raft metrics with the given registry.
func NewRaftMetrics(reg *Registry, nodeID uint64) *RaftMetrics {
	m := &RaftMetrics{reg: reg}

	m.IsLeader = reg.NewGauge("phalanx_raft_is_leader",
		"1 if this node is the current Raft leader, 0 otherwise.")
	m.CurrentTerm = reg.NewGauge("phalanx_raft_term",
		"Current Raft term number.")
	m.NodeID = reg.NewGauge("phalanx_raft_node_id",
		"This node's Raft node ID.")
	m.NodeID.SetUint(nodeID)

	m.CommitIndex = reg.NewGauge("phalanx_raft_commit_index",
		"Index of the most recent committed log entry.")
	m.AppliedIndex = reg.NewGauge("phalanx_raft_applied_index",
		"Index of the most recent log entry applied to the state machine.")
	m.LastLogIndex = reg.NewGauge("phalanx_raft_last_log_index",
		"Index of the last log entry (may be uncommitted).")
	m.AppliedLag = reg.NewGauge("phalanx_raft_applied_lag",
		"Entries committed but not yet applied to the state machine (commit_index - applied_index).")

	m.LeaderChanges = reg.NewCounter("phalanx_raft_leader_changes_total",
		"Total number of leader changes observed by this node.")
	m.LeaderID = reg.NewGauge("phalanx_raft_leader_id",
		"Node ID of the current Raft leader (0 if unknown).")

	m.ProposalDuration = reg.NewHistogram("phalanx_raft_proposal_duration_seconds",
		"Time from Propose() call to log entry committed, in seconds.",
		DefaultLatencyBuckets)
	m.ProposalTotal = reg.NewCounter("phalanx_raft_proposals_total",
		"Total proposals accepted by the Raft engine.")
	m.ProposalFailed = reg.NewCounter("phalanx_raft_proposals_failed_total",
		"Total proposals rejected (node not leader, uncommitted budget exceeded, etc.).")
	m.ProposalPending = reg.NewGauge("phalanx_raft_proposals_pending",
		"Number of proposals currently in-flight (accepted but not yet committed).")

	m.ReplicationLag = reg.NewCounterVec("phalanx_raft_replication_lag_bytes",
		"Bytes of log entries not yet replicated to each peer.", []string{"peer_id"})
	m.HeartbeatDur = reg.NewHistogram("phalanx_raft_heartbeat_duration_seconds",
		"Duration of leader heartbeat round-trips.", DefaultLatencyBuckets)

	m.WALFsyncDur = reg.NewHistogram("phalanx_wal_fsync_duration_seconds",
		"Time to fsync the WAL active segment after a batch write.", DefaultLatencyBuckets)
	m.WALSegments = reg.NewGauge("phalanx_wal_segments",
		"Number of WAL segment files currently on disk.")
	m.WALBytes = reg.NewGauge("phalanx_wal_bytes",
		"Estimated total bytes in all WAL segments.")
	m.SnapSizeBytes = reg.NewGauge("phalanx_snapshot_size_bytes",
		"Size of the latest snapshot file in bytes.")
	m.SnapIndex = reg.NewGauge("phalanx_snapshot_index",
		"Raft log index at which the latest snapshot was taken.")

	m.ElectionDuration = reg.NewHistogram("phalanx_raft_election_duration_seconds",
		"Time from election start to successful leader election.",
		[]float64{0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0})
	m.ElectionAttempts = reg.NewCounter("phalanx_raft_election_attempts_total",
		"Total number of elections started by this node.")

	m.ReadIndexDuration = reg.NewHistogram("phalanx_raft_readindex_duration_seconds",
		"End-to-end latency of linearizable reads (ReadIndex protocol).",
		DefaultLatencyBuckets)
	m.StaleReadTotal = reg.NewCounter("phalanx_raft_stale_reads_total",
		"Reads served from local state without ReadIndex (potentially stale).")

	m.ReceiveErrors = reg.NewCounter("phalanx_transport_receive_errors_total",
		"Errors processing inbound Raft messages.")
	m.SendErrors = reg.NewCounter("phalanx_transport_send_errors_total",
		"Errors sending outbound Raft messages.")

	return m
}

// Update refreshes the gauge-type metrics from the current Raft status.
// Call this on every tick or Ready cycle.
func (m *RaftMetrics) Update(status raft.Status, appliedIndex uint64) {
	isLeader := 0.0
	if status.RaftState == raft.RoleLeader {
		isLeader = 1.0
	}
	m.IsLeader.Set(isLeader)
	m.CurrentTerm.SetUint(status.Term)
	m.CommitIndex.SetUint(status.Commit)
	m.AppliedIndex.SetUint(appliedIndex)
	m.LeaderID.SetUint(status.Lead)

	lag := int64(status.Commit) - int64(appliedIndex)
	if lag < 0 {
		lag = 0
	}
	m.AppliedLag.SetInt(lag)
}

// RecordProposal records the outcome of a proposal.
func (m *RaftMetrics) RecordProposal(start time.Time, success bool) {
	dur := time.Since(start).Seconds()
	if success {
		m.ProposalTotal.Inc()
		m.ProposalDuration.Observe(dur)
	} else {
		m.ProposalFailed.Inc()
	}
}

// RecordElection records a completed election. start is when the election began.
func (m *RaftMetrics) RecordElection(start time.Time) {
	m.ElectionAttempts.Inc()
	m.ElectionDuration.Observe(time.Since(start).Seconds())
}

// RecordReadIndex records the outcome of a linearizable read.
func (m *RaftMetrics) RecordReadIndex(start time.Time) {
	m.ReadIndexDuration.Observe(time.Since(start).Seconds())
}

// RecordWALFsync records the duration of a WAL fdatasync call.
func (m *RaftMetrics) RecordWALFsync(start time.Time) {
	m.WALFsyncDur.Observe(time.Since(start).Seconds())
}

// StartMetricsServer starts an HTTP server on addr exposing /metrics.
// It also exposes /debug/raft which prints current Raft status as plain text.
func StartMetricsServer(addr string, reg *Registry, statusFn func() string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", reg.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	if statusFn != nil {
		mux.HandleFunc("/debug/raft", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprintln(w, statusFn())
		})
	}
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go srv.ListenAndServe()
	return nil
}
