package snapshot

import (
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/phalanx-db/phalanx/internal/raft"
)

const (
	defaultThreshold  = 10_000 // entries between snapshots
	defaultKeepCount  = 3      // number of snapshots to retain
	chunkSize         = 1 << 20 // 1 MiB per chunk
	snapDialTimeout   = 10 * time.Second
	snapPortOffset    = 1 // listen on main_port+1 for snapshot transfer
)

// ErrSnapTransfer indicates a failure during chunked snapshot transmission.
var ErrSnapTransfer = errors.New("snapshot: transfer failed")

// Manager owns the full snapshot lifecycle:
//   - Deciding when to trigger snapshots (threshold-based)
//   - Persisting snapshots to disk
//   - Loading the latest snapshot on startup
//   - Sending snapshots to lagging followers (chunked TCP)
//   - Receiving and installing snapshots from the leader
//   - Purging old snapshot files
type Manager struct {
	snapdir   string
	threshold uint64
	keepCount int

	mu        sync.Mutex
	lastIndex uint64 // index of the last snapshot taken

	logger *slog.Logger

	// Receive-side: channel that delivers assembled snapshots to the Raft node
	incomingC chan raft.Snapshot
}

// NewManager creates a snapshot manager. snapdir is the directory where
// snapshot files are stored (created on first use). threshold is the number
// of log entries to accumulate before triggering a new snapshot.
func NewManager(snapdir string, threshold uint64, logger *slog.Logger) *Manager {
	if threshold == 0 {
		threshold = defaultThreshold
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		snapdir:   snapdir,
		threshold: threshold,
		keepCount: defaultKeepCount,
		incomingC: make(chan raft.Snapshot, 4),
		logger:    logger,
	}
}

// ShouldSnapshot returns true when the applied index has advanced far enough
// beyond the last snapshot that a new one should be taken.
func (m *Manager) ShouldSnapshot(appliedIndex uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return appliedIndex > m.lastIndex && appliedIndex-m.lastIndex >= m.threshold
}

// Save persists snap to disk and records it as the new latest snapshot.
// This MUST be called after the WAL has recorded the snapshot so that
// crash recovery sees a consistent state.
func (m *Manager) Save(snap raft.Snapshot) error {
	if snap.IsEmpty() {
		return nil
	}
	if err := Save(m.snapdir, snap); err != nil {
		return err
	}
	if err := Purge(m.snapdir, m.keepCount); err != nil {
		m.logger.Warn("snapshot purge failed", "err", err)
	}
	m.mu.Lock()
	m.lastIndex = snap.Metadata.Index
	m.mu.Unlock()
	m.logger.Info("snapshot saved",
		"term", snap.Metadata.Term,
		"index", snap.Metadata.Index,
		"size_bytes", len(snap.Data))
	return nil
}

// Load returns the most recent snapshot from disk.
// Returns ErrNoSnapshot when no snapshot exists (normal for a fresh cluster).
func (m *Manager) Load() (raft.Snapshot, error) {
	snap, err := Load(m.snapdir)
	if err != nil {
		return snap, err
	}
	m.mu.Lock()
	m.lastIndex = snap.Metadata.Index
	m.mu.Unlock()
	return snap, nil
}

// LastIndex returns the index of the most recently persisted snapshot.
func (m *Manager) LastIndex() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastIndex
}

// IncomingSnapshots returns the channel on which fully received (chunked)
// snapshots are delivered. The Raft node reads from this channel.
func (m *Manager) IncomingSnapshots() <-chan raft.Snapshot {
	return m.incomingC
}

// -------------------------------------------------------------------
// Chunked snapshot sender
// -------------------------------------------------------------------

// SnapshotChunk is the wire format for one chunk of a snapshot transfer.
type SnapshotChunk struct {
	SnapID [8]byte // unique per-transfer ID (from||index as 4+4 bytes)
	From   uint64
	To     uint64
	Term   uint64
	Index  uint64
	Offset uint64  // byte offset within the full payload
	Total  uint64  // total snapshot payload bytes
	Data   []byte
	Done   bool    // true on the final chunk
	CRC    uint32  // CRC32C of this chunk's Data
}

// SendTo sends snap to the peer at addr using a dedicated TCP connection.
// The connection is separate from the main Raft message transport to avoid
// head-of-line blocking during large snapshot transfers.
//
// Protocol: each chunk is gob-encoded, length-prefixed (4 bytes big-endian).
func (m *Manager) SendTo(snap raft.Snapshot, fromID, toID uint64, addr string) error {
	conn, err := net.DialTimeout("tcp", addr, snapDialTimeout)
	if err != nil {
		return fmt.Errorf("snapshot: dial %s: %w", addr, err)
	}
	defer conn.Close()

	// Announce ourselves as a snapshot sender: magic byte 0x53 ('S')
	if _, err := conn.Write([]byte{0x53}); err != nil {
		return fmt.Errorf("snapshot: handshake: %w", err)
	}

	payload := snap.Data
	total := uint64(len(payload))
	chunkID := m.makeChunkID(fromID, snap.Metadata.Index)
	sent := uint64(0)
	offset := uint64(0)

	m.logger.Info("sending snapshot",
		"to", toID,
		"index", snap.Metadata.Index,
		"total_bytes", total)

	for {
		end := offset + uint64(chunkSize)
		if end > total {
			end = total
		}
		chunk := SnapshotChunk{
			SnapID: chunkID,
			From:   fromID,
			To:     toID,
			Term:   snap.Metadata.Term,
			Index:  snap.Metadata.Index,
			Offset: offset,
			Total:  total,
			Data:   payload[offset:end],
			Done:   end == total,
			CRC:    crc32.Checksum(payload[offset:end], crc32.MakeTable(crc32.Castagnoli)),
		}
		if err := sendChunk(conn, chunk); err != nil {
			return fmt.Errorf("snapshot: send chunk @%d: %w", offset, err)
		}
		sent += uint64(end - offset)
		offset = end
		if chunk.Done {
			break
		}
	}

	m.logger.Info("snapshot sent", "to", toID, "bytes", sent)
	return nil
}

// StartReceiver listens on addr for incoming snapshot chunks.
// Completed snapshots are forwarded to IncomingSnapshots().
func (m *Manager) StartReceiver(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("snapshot: listen %s: %w", addr, err)
	}
	go m.acceptLoop(ln)
	return nil
}

func (m *Manager) acceptLoop(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go m.handleConn(conn)
	}
}

func (m *Manager) handleConn(conn net.Conn) {
	defer conn.Close()

	// Read handshake byte
	hdr := make([]byte, 1)
	if _, err := io.ReadFull(conn, hdr); err != nil || hdr[0] != 0x53 {
		return
	}

	// Accumulate chunks for one snapshot transfer
	var assembled []byte
	var meta SnapshotChunk

	for {
		chunk, err := recvChunk(conn)
		if err != nil {
			m.logger.Warn("snapshot receive error", "err", err)
			return
		}
		if crc32.Checksum(chunk.Data, crc32.MakeTable(crc32.Castagnoli)) != chunk.CRC {
			m.logger.Error("snapshot chunk CRC mismatch", "offset", chunk.Offset)
			return
		}
		if assembled == nil {
			assembled = make([]byte, 0, chunk.Total)
			meta = chunk
		}
		assembled = append(assembled, chunk.Data...)
		if chunk.Done {
			break
		}
	}

	snap := raft.Snapshot{
		Data: assembled,
		Metadata: raft.SnapshotMetadata{
			Term:  meta.Term,
			Index: meta.Index,
		},
	}
	select {
	case m.incomingC <- snap:
		m.logger.Info("snapshot received",
			"from", meta.From,
			"index", meta.Index,
			"bytes", len(assembled))
	default:
		m.logger.Warn("snapshot receive buffer full — dropping")
	}
}

func sendChunk(w io.Writer, c SnapshotChunk) error {
	var buf boundedBuf
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return err
	}
	data := buf.b
	lenHdr := make([]byte, 4)
	binary.BigEndian.PutUint32(lenHdr, uint32(len(data)))
	if _, err := w.Write(lenHdr); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func recvChunk(r io.Reader) (SnapshotChunk, error) {
	lenHdr := make([]byte, 4)
	if _, err := io.ReadFull(r, lenHdr); err != nil {
		return SnapshotChunk{}, err
	}
	length := binary.BigEndian.Uint32(lenHdr)
	if length > 10*1024*1024 {
		return SnapshotChunk{}, fmt.Errorf("snapshot chunk too large: %d", length)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return SnapshotChunk{}, err
	}
	var c SnapshotChunk
	if err := gob.NewDecoder(bytesReader(data)).Decode(&c); err != nil {
		return SnapshotChunk{}, err
	}
	return c, nil
}

func (m *Manager) makeChunkID(fromID, index uint64) [8]byte {
	var id [8]byte
	binary.BigEndian.PutUint32(id[0:4], uint32(fromID))
	binary.BigEndian.PutUint32(id[4:8], uint32(index))
	return id
}

type boundedBuf struct{ b []byte }

func (bb *boundedBuf) Write(p []byte) (int, error) {
	bb.b = append(bb.b, p...)
	return len(p), nil
}

type byteRdr struct{ d []byte; p int }

func bytesReader(d []byte) io.Reader { return &byteRdr{d: d} }

func (b *byteRdr) Read(p []byte) (int, error) {
	if b.p >= len(b.d) {
		return 0, io.EOF
	}
	n := copy(p, b.d[b.p:])
	b.p += n
	return n, nil
}
