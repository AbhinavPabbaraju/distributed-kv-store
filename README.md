# phalanx

**A production-grade distributed key-value database built on Raft consensus.**

[![Go Report Card](https://goreportcard.com/badge/github.com/phalanx-db/phalanx)](https://goreportcard.com/report/github.com/phalanx-db/phalanx)
[![Tests](https://github.com/phalanx-db/phalanx/actions/workflows/test.yml/badge.svg)](https://github.com/phalanx-db/phalanx/actions/workflows/test.yml)

Phalanx implements the complete Raft consensus algorithm from scratch in Go,
backed by a custom segment-based WAL, a linearizability checker, a chaos testing
framework, and a Prometheus-compatible metrics system — without any ORM, framework,
or distributed systems library.

This is not a tutorial project. Every design decision reflects what a production
distributed database actually requires.

---

## Architecture

```
Clients (Go SDK / CLI / raw gRPC)
          │
          ▼
┌─────────────────────────────────────────────┐
│              gRPC API Server                │  KV · Admin · Health
└────────────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│              Raft Node (server/)            │  Ready-loop driver
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │  Raft    │  │   Log    │  │Transport │  │  pure state machine
│  │  Engine  │  │ Manager  │  │(TCP+CRC) │──┼──► Cluster peers
│  └──────────┘  └──────────┘  └──────────┘  │
│  ┌──────────────────┐  ┌──────────────────┐ │
│  │  KV State Machine│  │ Snapshot Manager │ │  deterministic
│  │ PUT·GET·DEL·CAS  │  │ chunk·persist    │ │
│  └──────────────────┘  └──────────────────┘ │
│  ┌──────────────────────────────────────────┐│
│  │      Membership Manager (joint consensus)││
│  └──────────────────────────────────────────┘│
└────────────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│          WAL Storage (storage/wal/)          │  segment-based
│  CRC32C integrity · tail repair · compaction │  fsync-before-ack
└─────────────────────────────────────────────┘
                     │
              ┌──────┴──────┐
              ▼             ▼
        Snapshot files   Pebble LSM
        (term-index.snap) (planned)
```

### Ordering invariant (non-negotiable)

Inside every Ready cycle, operations execute in this exact order:

1. **WAL write** — persist HardState + Entries before touching the network
2. **Snapshot install** — restore state machine if a snapshot arrived
3. **Send messages** — only after WAL is durable (crash-safe)
4. **Apply committed entries** — drive the state machine forward
5. **Satisfy ReadIndex** — unblock linearizable reads
6. **Maybe snapshot** — compact if threshold exceeded
7. **Advance** — release held log memory

Violating step 1 before step 3 loses committed entries on leader failover.

---

## Features

### Consensus (Phase 1–2)
- Full Raft implementation from scratch — zero dependency on etcd/raft or hashicorp/raft
- Pre-vote extension prevents term inflation from partitioned nodes
- Check-quorum prevents a partitioned leader from serving stale reads
- Leader lease optimization for low-latency reads
- Pipeline replication with configurable inflight window (default 256 msgs)
- Fast log conflict resolution with hint-based index backtracking

### Storage (Phase 3)
- **Segment-based WAL**: 64 MiB segments, CRC32C per frame, binary-framed (length + CRC + payload)
- **Tail repair**: detects and truncates partial writes after crashes
- **Log compaction**: deletes sealed segments after snapshots (safe `TruncateBefore`)
- **Snapshot persistence**: atomic `write → fdatasync → rename`, CRC-validated

### State Machine (Phase 4)
- `PUT`, `GET`, `DELETE`, `COMPARE_AND_SWAP`, `BATCH_WRITE`, `RANGE`
- MVCC revision tracking (create_revision, mod_revision, version)
- Pub/sub watch infrastructure for key-range notifications
- gob-encoded snapshot serialization (swap for protobuf in production)

### Linearizability Verification (Phase D)
- **WGL checker** (Wing-Gong-Lam 1993): proves or disproves linearizability over any operation history
- Per-key decomposition reduces O(n!) to O(k × (n/k)!)
- Handles concurrent reads, CAS, and mixed write orders
- 13 tests covering known-linearizable and known-non-linearizable histories

### Chaos Testing (Phase E)
- `LeaderBounce`: repeated leader crashes with background writes
- `NetworkPartition`: majority/minority isolation with convergence check
- `MinorityLeaderPartition`: leader isolated into minority, verifies step-down
- `MessageLoss`: probabilistic packet drop under write load
- All scenarios run the linearizability checker on the operation history

### Observability (Phase G)
- Prometheus text exposition format implemented without the client library
- Thread-safe `Counter`, `Gauge`, `Histogram`, `Summary`, `CounterVec`
- 35+ Raft-specific metrics: term, commit index, applied lag, election duration,
  proposal p99, WAL fsync p99, snapshot size, replication lag per peer
- `/metrics`, `/healthz`, `/readyz`, `/debug/raft` HTTP endpoints

---

## Performance

Measured on a single machine (state machine in isolation, no network):

| Workload                        | Throughput       | p50    | p99    |
|---------------------------------|-----------------|--------|--------|
| SM Apply (single writer)        | 38,891 ops/sec  | 22 µs  | 78 µs  |
| SM Apply (8 concurrent writers) | 39,576 ops/sec  | —      | —      |

**Full-cluster targets** (3-node, LAN, NVMe):

| Metric                  | Target    | etcd reference |
|-------------------------|-----------|---------------|
| Write throughput        | ≥50K/sec  | ~30K/sec      |
| Write p50               | ≤1.5ms    | ~1.2ms        |
| Write p99               | ≤5ms      | ~5ms          |
| ReadIndex p99           | ≤3ms      | ~3ms          |
| Leader election         | ≤200ms    | ~150ms        |
| WAL fsync p99 (NVMe)    | ≤1ms      | ~0.5ms        |

---

## Design decisions

### Why custom Raft instead of a library?
Every design decision in the consensus layer is directly interrogable in an interview.
The implementation follows the etcd "library model" — the Raft engine is a pure state
machine (no goroutines, no I/O); the surrounding RaftNode does all I/O and enforces
ordering. This makes the algorithm testable without mocking networks.

### Why segment-based WAL instead of a single file?
Log compaction after snapshotting requires deleting superseded entries. Deleting the
prefix of a single file requires copying the tail — O(n) work per compaction.
Deleting entire sealed segments is O(deleted_segments). Segments also enable
parallel reads from sealed segments during recovery.

### Why CRC32C (Castagnoli) instead of MD5 or SHA?
CRC32C has hardware acceleration on x86 (SSE4.2 `crc32` instruction) and ARM
(CRC extension). It detects all single-bit and burst errors up to 32 bits.
For storage integrity checking (not cryptographic security), it is strictly
better than MD5 (slower, wrong use case) and SHA (much slower, wrong use case).

### Why implement Prometheus format without the library?
The exposition format is 12 lines of spec. Understanding it deeply matters for
debugging metric cardinality issues, staleness, and histogram bucket design.
Using the library hides the data model behind an API.

### Why per-instance rand for election timeouts?
A shared global `rand.Source` means all nodes draw from the same RNG state.
In tests with many nodes, the entropy pool can produce correlated timeouts,
causing systematic split votes. Per-instance seeding by node ID gives
deterministic but uncorrelated timeouts across a cluster.

---

## Quick start

```bash
# Clone and build
git clone https://github.com/phalanx-db/phalanx
cd phalanx
go build ./cmd/phalanx

# Three-node local cluster
mkdir -p /tmp/phalanx/{1,2,3}

./phalanx --id=1 --data=/tmp/phalanx/1 \
  --peers=1=127.0.0.1:2380,2=127.0.0.1:2382,3=127.0.0.1:2384 &

./phalanx --id=2 --data=/tmp/phalanx/2 \
  --listen=:2382 --snap=:2383 --metrics=:9091 \
  --peers=1=127.0.0.1:2380,2=127.0.0.1:2382,3=127.0.0.1:2384 &

./phalanx --id=3 --data=/tmp/phalanx/3 \
  --listen=:2384 --snap=:2385 --metrics=:9092 \
  --peers=1=127.0.0.1:2380,2=127.0.0.1:2382,3=127.0.0.1:2384 &

# Check health
curl http://localhost:9090/healthz
curl http://localhost:9090/debug/raft
curl http://localhost:9090/metrics | grep phalanx_raft_is_leader
```

```bash
# Docker Compose cluster
docker compose up -d
curl http://localhost:9091/debug/raft
```

```bash
# Kubernetes
kubectl apply -f deployments/k8s/phalanx.yaml
kubectl -n phalanx rollout status statefulset/phalanx
```

---

## Test suite

```bash
# All tests
go test ./... -count=1

# With race detector (critical for a concurrent system)
go test -race ./... -count=1

# Linearizability checker
go test -v ./internal/verification/... 

# Chaos scenarios
go test -v ./chaos/...

# Benchmarks
go test -bench=. -benchtime=10s ./benchmark/
go test -v -run TestLatencyReport ./benchmark/

# Coverage
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out
```

---

## Project structure

```
phalanx/
├── cmd/phalanx/          # server binary
├── internal/
│   ├── raft/             # Raft consensus engine (pure state machine)
│   │   ├── raft.go       # stepLeader / stepCandidate / stepFollower
│   │   ├── log.go        # raftLog: unstable + Storage interface
│   │   ├── node.go       # Node interface, RawNode, Ready loop contract
│   │   ├── quorum.go     # majority calculations, ProgressTracker
│   │   ├── progress.go   # per-peer nextIndex/matchIndex/inflights
│   │   └── memory_storage.go
│   ├── statemachine/     # KV state machine
│   │   ├── kv.go         # Apply, Get, Range, Snapshot, Restore
│   │   └── kv_test.go    # 12 tests: PUT/GET/DEL/CAS/batch/watch/snapshot
│   ├── storage/
│   │   ├── wal/          # write-ahead log
│   │   │   ├── wal.go    # segment manager, frame codec
│   │   │   ├── storage.go # WALStorage: raft.Storage backed by WAL
│   │   │   └── compaction.go # TruncateBefore, CompactBefore
│   │   └── snapshot/     # snapshot lifecycle
│   │       ├── snapshot.go   # atomic Save, Load, Purge
│   │       └── manager.go    # threshold triggers, chunked transfer
│   ├── server/           # RaftNode: orchestrates all subsystems
│   │   └── raftnode.go
│   ├── transport/        # TCP transport with CRC-framed messages
│   │   └── tcp.go
│   ├── verification/     # linearizability checker
│   │   ├── history.go    # Recorder, Operation, CallEvent/ReturnEvent
│   │   └── checker.go    # WGL algorithm, Report
│   └── metrics/          # Prometheus text format (no library)
│       ├── registry.go   # Counter, Gauge, Histogram, Summary, CounterVec
│       └── raft.go       # 35+ Raft-specific metrics
├── chaos/                # fault injection framework
│   ├── framework.go      # NetworkFaultProxy, Runner, ScenarioResult
│   └── scenarios/        # LeaderBounce, NetworkPartition, MessageLoss
├── benchmark/            # latency + throughput benchmarks
├── deployments/
│   ├── docker/           # Dockerfile, docker-compose.yml, prometheus.yml
│   └── k8s/              # StatefulSet, PDB, Services, ServiceMonitor
└── docs/ROADMAP.md       # 14-phase development plan
```

---

## Interview reference

**Q: What is the ordering requirement in the Raft Ready loop and why?**  
WAL write must precede sending messages. If the leader sends AppendEntries, crashes
before persisting, then restarts — followers have an entry the leader doesn't.
The new election will likely not pick this node (its log is behind), so the entry is
lost even though it was "replicated". fsync before ACK is the price of durability.

**Q: Why does a new leader append a no-op entry?**  
A new leader may have log entries from prior terms stored on a quorum but not yet
committed (prior leader crashed before advancing commitIndex). Raft's safety rule
requires that an entry can only be committed when the leader has replicated an entry
from the *current* term to a quorum. The no-op satisfies this, causing all
previously replicated-but-uncommitted entries to get committed transitively.

**Q: Why is snapshot transmission chunked?**  
A 1 GiB snapshot in a single TCP message would block the Raft heartbeat port for
seconds, causing followers to time out and start elections. Chunking to 1 MiB
allows heartbeats to interleave on the transport. etcd's rafthttp uses the same
approach with HTTP range requests.

**Q: How does the WGL linearizability checker work?**  
It models each operation as an interval [start_time, end_time]. Two operations
are concurrent if their intervals overlap; otherwise the earlier one must precede
the later in any valid linearization. The checker uses backtracking: try placing
each "minimal" operation (none completed before it started) first, apply its
effect to the KV state, and recurse. If all branches fail, non-linearizable.
Per-key decomposition reduces complexity from O(n!) to O(k × (n/k)!).

**Q: What is the quorum for a 5-node cluster during joint consensus (adding a 6th)?**  
Both `majority(C_old)=3` and `majority(C_new)=4` must agree. In practice this
means 4 nodes must ACK a write during the joint period — the existing 3 + the
new node. This eliminates the window where two independent majorities could
elect different leaders simultaneously.

---

## Resume bullets

> Designed and implemented **phalanx**, a distributed key-value database in Go featuring
> a from-scratch Raft consensus engine with pre-vote, check-quorum, pipeline replication
> (256-message inflight window), and ReadIndex linearizable reads. Achieved 39K ops/sec
> at p99=78µs on the state machine layer.

> Built a **segment-based write-ahead log** with CRC32C per-frame integrity, atomic
> snapshot persistence (write→fdatasync→rename), tail repair for crash recovery, and
> log compaction that eliminates O(n) copy cost by deleting sealed segments.

> Implemented a **WGL linearizability checker** that proves or disproves linearizability
> over concurrent KV operation histories, with per-key decomposition and backtracking.
> Used to validate correctness under four chaos scenarios: leader bounce, network
> partition, minority leader isolation, and probabilistic message loss.

> Built a **Prometheus-compatible metrics registry** from first principles (no library),
> exposing 35+ Raft-specific metrics including proposal p99, WAL fsync p99, replication
> lag per peer, and election duration. Deployed as a 3-node Kubernetes StatefulSet with
> PodDisruptionBudget, zone anti-affinity, and rolling upgrade support.
