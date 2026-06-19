package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/phalanx-db/phalanx/internal/raft"
)

const (
	segmentSizeBytes = 64 * 1024 * 1024
	frameLenBytes    = 4
	frameCRCBytes    = 4
	frameHeaderBytes = frameLenBytes + frameCRCBytes
	walFileSuffix    = ".wal"
	tempFileSuffix   = ".tmp"
)

var (
	ErrNotFound     = errors.New("wal: record not found")
	ErrCRCMismatch  = errors.New("wal: crc mismatch")
	ErrFileClosed   = errors.New("wal: file already closed")
	ErrOutOfOrder   = errors.New("wal: out of order write")
	crcTable        = crc32.MakeTable(crc32.Castagnoli)
)

type recordType uint8

const (
	recordEntry     recordType = iota
	recordHardState recordType = iota
	recordSnapshot  recordType = iota
)

type Record struct {
	Type recordType
	Data []byte
}

type WAL struct {
	dir      string
	mu       sync.Mutex
	segments []*segment
	active   *segment
}

type segment struct {
	path      string
	firstIdx  uint64
	file      *os.File
	bw        *bufio.Writer
	size      int64
	closed    bool
}

func Create(dir string, metadata []byte) (*WAL, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("wal: create dir: %w", err)
	}
	existing, err := segmentFiles(dir)
	if err != nil {
		return nil, err
	}
	if len(existing) > 0 {
		return nil, fmt.Errorf("wal: directory already contains WAL files; use Open instead")
	}
	seg, err := createSegment(dir, 1)
	if err != nil {
		return nil, err
	}
	w := &WAL{
		dir:      dir,
		segments: []*segment{seg},
		active:   seg,
	}
	if err := w.writeRecord(recordEntry, metadata); err != nil {
		return nil, err
	}
	return w, nil
}

func Open(dir string) (*WAL, error) {
	files, err := segmentFiles(dir)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("wal: no WAL files found in %s", dir)
	}
	segs := make([]*segment, 0, len(files))
	for _, f := range files {
		idx, err := parseSegmentIndex(f)
		if err != nil {
			return nil, err
		}
		segs = append(segs, &segment{path: filepath.Join(dir, f), firstIdx: idx})
	}
	last := segs[len(segs)-1]
	f, err := os.OpenFile(last.path, os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("wal: open active segment: %w", err)
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	last.file = f
	last.bw = bufio.NewWriterSize(f, 64*1024)
	last.size = size
	return &WAL{dir: dir, segments: segs, active: last}, nil
}

func (w *WAL) SaveEntries(hardState raft.HardState, entries []raft.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !hardState.IsEmpty() {
		data, err := encodeHardState(hardState)
		if err != nil {
			return fmt.Errorf("wal: encode hardstate: %w", err)
		}
		if err := w.writeRecord(recordHardState, data); err != nil {
			return err
		}
	}
	for i := range entries {
		data, err := encodeEntry(&entries[i])
		if err != nil {
			return fmt.Errorf("wal: encode entry: %w", err)
		}
		if err := w.writeRecord(recordEntry, data); err != nil {
			return err
		}
	}
	return w.sync()
}

func (w *WAL) SaveSnapshot(snap raft.Snapshot) error {
	if snap.IsEmpty() {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	data, err := encodeSnapshot(&snap)
	if err != nil {
		return fmt.Errorf("wal: encode snapshot: %w", err)
	}
	if err := w.writeRecord(recordSnapshot, data); err != nil {
		return err
	}
	return w.sync()
}

func (w *WAL) ReadAll() (raft.HardState, []raft.Entry, *raft.Snapshot, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var hs raft.HardState
	var ents []raft.Entry
	var snap *raft.Snapshot

	for _, seg := range w.segments {
		f, err := os.Open(seg.path)
		if err != nil {
			return hs, nil, nil, fmt.Errorf("wal: open segment %s: %w", seg.path, err)
		}
		recs, err := readSegment(f)
		f.Close()
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
			return hs, nil, nil, fmt.Errorf("wal: read segment %s: %w", seg.path, err)
		}
		for _, rec := range recs {
			switch rec.Type {
			case recordHardState:
				decoded, err := decodeHardState(rec.Data)
				if err != nil {
					return hs, nil, nil, err
				}
				hs = decoded
			case recordEntry:
				if len(rec.Data) < 17 {
					continue
				}
				e, err := decodeEntry(rec.Data)
				if err != nil {
					return hs, nil, nil, err
				}
				ents = append(ents, e)
			case recordSnapshot:
				s, err := decodeSnapshot(rec.Data)
				if err != nil {
					return hs, nil, nil, err
				}
				snap = &s
			}
		}
	}
	return hs, ents, snap, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active == nil {
		return ErrFileClosed
	}
	if err := w.active.bw.Flush(); err != nil {
		return err
	}
	if err := w.active.file.Sync(); err != nil {
		return err
	}
	return w.active.file.Close()
}

func (w *WAL) writeRecord(t recordType, data []byte) error {
	if w.active.size >= segmentSizeBytes {
		if err := w.rotateSegment(); err != nil {
			return err
		}
	}
	return writeFrame(w.active.bw, t, data, &w.active.size)
}

func (w *WAL) rotateSegment() error {
	if err := w.active.bw.Flush(); err != nil {
		return err
	}
	if err := w.active.file.Sync(); err != nil {
		return err
	}
	w.active.file.Close()
	w.active.closed = true

	nextIdx := w.active.firstIdx
	for _, seg := range w.segments {
		if seg.firstIdx > nextIdx {
			nextIdx = seg.firstIdx
		}
	}
	nextIdx++

	seg, err := createSegment(w.dir, nextIdx)
	if err != nil {
		return fmt.Errorf("wal: create segment: %w", err)
	}
	w.segments = append(w.segments, seg)
	w.active = seg
	return nil
}

func (w *WAL) sync() error {
	if err := w.active.bw.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	return w.active.file.Sync()
}

func (w *WAL) TailRepair() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	seg := w.active
	if seg.file == nil {
		return nil
	}
	if err := seg.bw.Flush(); err != nil {
		return err
	}
	size, err := seg.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	validEnd, err := findValidTail(seg.file)
	if err != nil {
		return err
	}
	if validEnd < size {
		if err := seg.file.Truncate(validEnd); err != nil {
			return fmt.Errorf("wal: truncate tail: %w", err)
		}
		seg.size = validEnd
	}
	return nil
}

func createSegment(dir string, firstIdx uint64) (*segment, error) {
	name := fmt.Sprintf("%016x%s", firstIdx, walFileSuffix)
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, fmt.Errorf("wal: create segment file: %w", err)
	}
	return &segment{
		path:     path,
		firstIdx: firstIdx,
		file:     f,
		bw:       bufio.NewWriterSize(f, 64*1024),
	}, nil
}

func writeFrame(w io.Writer, t recordType, data []byte, sizeTracker *int64) error {
	payload := make([]byte, 1+len(data))
	payload[0] = byte(t)
	copy(payload[1:], data)

	checksum := crc32.Checksum(payload, crcTable)
	header := make([]byte, frameHeaderBytes)
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:8], checksum)

	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("wal: write frame header: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("wal: write frame payload: %w", err)
	}
	*sizeTracker += int64(frameHeaderBytes + len(payload))
	return nil
}

func readSegment(r io.Reader) ([]Record, error) {
	var records []Record
	buf := make([]byte, frameHeaderBytes)
	for {
		_, err := io.ReadFull(r, buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return records, fmt.Errorf("wal: read frame header: %w", err)
		}
		length := binary.BigEndian.Uint32(buf[:4])
		expectedCRC := binary.BigEndian.Uint32(buf[4:8])
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			return records, fmt.Errorf("wal: read frame payload: %w", err)
		}
		actualCRC := crc32.Checksum(payload, crcTable)
		if actualCRC != expectedCRC {
			return records, ErrCRCMismatch
		}
		records = append(records, Record{Type: recordType(payload[0]), Data: payload[1:]})
	}
	return records, nil
}

func findValidTail(f *os.File) (int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	buf := make([]byte, frameHeaderBytes)
	var pos int64
	for {
		n, err := io.ReadFull(f, buf)
		if errors.Is(err, io.EOF) || (errors.Is(err, io.ErrUnexpectedEOF) && n == 0) {
			break
		}
		if err != nil {
			break
		}
		length := binary.BigEndian.Uint32(buf[:4])
		expectedCRC := binary.BigEndian.Uint32(buf[4:8])
		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			break
		}
		if crc32.Checksum(payload, crcTable) != expectedCRC {
			break
		}
		pos += int64(frameHeaderBytes) + int64(length)
	}
	return pos, nil
}

func segmentFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: read dir: %w", err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), walFileSuffix) {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	return files, nil
}

func parseSegmentIndex(name string) (uint64, error) {
	base := strings.TrimSuffix(name, walFileSuffix)
	idx, err := strconv.ParseUint(base, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("wal: parse segment index from %q: %w", name, err)
	}
	return idx, nil
}

func encodeHardState(hs raft.HardState) ([]byte, error) {
	buf := make([]byte, 24)
	binary.BigEndian.PutUint64(buf[0:8], hs.Term)
	binary.BigEndian.PutUint64(buf[8:16], hs.Vote)
	binary.BigEndian.PutUint64(buf[16:24], hs.Commit)
	return buf, nil
}

func decodeHardState(b []byte) (raft.HardState, error) {
	if len(b) < 24 {
		return raft.HardState{}, fmt.Errorf("wal: hardstate too short")
	}
	return raft.HardState{
		Term:   binary.BigEndian.Uint64(b[0:8]),
		Vote:   binary.BigEndian.Uint64(b[8:16]),
		Commit: binary.BigEndian.Uint64(b[16:24]),
	}, nil
}

func encodeEntry(e *raft.Entry) ([]byte, error) {
	buf := make([]byte, 17+len(e.Data))
	binary.BigEndian.PutUint64(buf[0:8], e.Term)
	binary.BigEndian.PutUint64(buf[8:16], e.Index)
	buf[16] = byte(e.Type)
	copy(buf[17:], e.Data)
	return buf, nil
}

func decodeEntry(b []byte) (raft.Entry, error) {
	if len(b) < 17 {
		return raft.Entry{}, fmt.Errorf("wal: entry too short")
	}
	e := raft.Entry{
		Term:  binary.BigEndian.Uint64(b[0:8]),
		Index: binary.BigEndian.Uint64(b[8:16]),
		Type:  raft.EntryType(b[16]),
	}
	if len(b) > 17 {
		e.Data = make([]byte, len(b)-17)
		copy(e.Data, b[17:])
	}
	return e, nil
}

func encodeSnapshot(s *raft.Snapshot) ([]byte, error) {
	meta := s.Metadata
	voterCount := len(meta.ConfState.Voters)
	learnerCount := len(meta.ConfState.Learners)
	headerSize := 8 + 8 + 8 + 4 + voterCount*8 + 4 + learnerCount*8
	buf := make([]byte, headerSize+len(s.Data))
	off := 0
	binary.BigEndian.PutUint64(buf[off:], meta.Index)
	off += 8
	binary.BigEndian.PutUint64(buf[off:], meta.Term)
	off += 8
	binary.BigEndian.PutUint64(buf[off:], 0)
	off += 8
	binary.BigEndian.PutUint32(buf[off:], uint32(voterCount))
	off += 4
	for _, v := range meta.ConfState.Voters {
		binary.BigEndian.PutUint64(buf[off:], v)
		off += 8
	}
	binary.BigEndian.PutUint32(buf[off:], uint32(learnerCount))
	off += 4
	for _, l := range meta.ConfState.Learners {
		binary.BigEndian.PutUint64(buf[off:], l)
		off += 8
	}
	copy(buf[off:], s.Data)
	return buf, nil
}

func decodeSnapshot(b []byte) (raft.Snapshot, error) {
	if len(b) < 24 {
		return raft.Snapshot{}, fmt.Errorf("wal: snapshot too short")
	}
	var s raft.Snapshot
	off := 0
	s.Metadata.Index = binary.BigEndian.Uint64(b[off:])
	off += 8
	s.Metadata.Term = binary.BigEndian.Uint64(b[off:])
	off += 8
	off += 8
	if off+4 > len(b) {
		return s, nil
	}
	voterCount := int(binary.BigEndian.Uint32(b[off:]))
	off += 4
	for i := 0; i < voterCount && off+8 <= len(b); i++ {
		s.Metadata.ConfState.Voters = append(s.Metadata.ConfState.Voters, binary.BigEndian.Uint64(b[off:]))
		off += 8
	}
	if off+4 <= len(b) {
		learnerCount := int(binary.BigEndian.Uint32(b[off:]))
		off += 4
		for i := 0; i < learnerCount && off+8 <= len(b); i++ {
			s.Metadata.ConfState.Learners = append(s.Metadata.ConfState.Learners, binary.BigEndian.Uint64(b[off:]))
			off += 8
		}
	}
	if off < len(b) {
		s.Data = make([]byte, len(b)-off)
		copy(s.Data, b[off:])
	}
	return s, nil
}
