package transport

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"sync"
	"time"

	"github.com/phalanx-db/phalanx/internal/raft"
)

const (
	dialTimeout        = 5 * time.Second
	writeTimeout       = 2 * time.Second
	keepAliveInterval  = 15 * time.Second
	maxMsgBufSize      = 65536
	reconnectBaseDelay = 100 * time.Millisecond
	reconnectMaxDelay  = 10 * time.Second
	pipelineCap        = 512
)

var (
	ErrTransportStopped = errors.New("transport: stopped")
	ErrPeerNotFound     = errors.New("transport: peer not found")
	crcTable            = crc32.MakeTable(crc32.Castagnoli)
)

type MessageHandler interface {
	Process(ctx context.Context, msg raft.Message) error
	IsIDRemoved(id uint64) bool
}

type Transporter interface {
	Start(addr string) error
	Send(msgs []raft.Message)
	AddPeer(id uint64, addr string)
	RemovePeer(id uint64)
	ActiveSince(id uint64) time.Time
	Stop()
}

type wireMsg struct {
	Type    raft.MessageType
	To      uint64
	From    uint64
	Term    uint64
	LogTerm uint64
	Index   uint64
	Commit  uint64
	Reject  bool
	RejectHint uint64
	Context []byte
	Entries []raft.Entry
}

func toWire(m raft.Message) wireMsg {
	return wireMsg{
		Type: m.Type, To: m.To, From: m.From,
		Term: m.Term, LogTerm: m.LogTerm, Index: m.Index,
		Commit: m.Commit, Reject: m.Reject, RejectHint: m.RejectHint,
		Context: m.Context, Entries: m.Entries,
	}
}

func fromWire(w wireMsg) raft.Message {
	return raft.Message{
		Type: w.Type, To: w.To, From: w.From,
		Term: w.Term, LogTerm: w.LogTerm, Index: w.Index,
		Commit: w.Commit, Reject: w.Reject, RejectHint: w.RejectHint,
		Context: w.Context, Entries: w.Entries,
	}
}

type TCPTransport struct {
	id      uint64
	handler MessageHandler

	peers   sync.Map
	mu      sync.RWMutex

	listener net.Listener
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func NewTCPTransport(id uint64, handler MessageHandler) *TCPTransport {
	return &TCPTransport{
		id:      id,
		handler: handler,
		stopCh:  make(chan struct{}),
	}
}

func (t *TCPTransport) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("transport: listen on %s: %w", addr, err)
	}
	t.listener = ln
	t.wg.Add(1)
	go t.accept()
	return nil
}

func (t *TCPTransport) accept() {
	defer t.wg.Done()
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.stopCh:
				return
			default:
				continue
			}
		}
		t.wg.Add(1)
		go t.serveConn(conn)
	}
}

func (t *TCPTransport) serveConn(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close()
	br := bufio.NewReaderSize(conn, maxMsgBufSize)
	for {
		select {
		case <-t.stopCh:
			return
		default:
		}
		msg, err := readMessage(br)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				_ = err
			}
			return
		}
		if t.handler.IsIDRemoved(msg.From) {
			return
		}
		if err := t.handler.Process(context.Background(), msg); err != nil {
			return
		}
	}
}

func (t *TCPTransport) Send(msgs []raft.Message) {
	for _, m := range msgs {
		if m.To == raft.None {
			continue
		}
		if v, ok := t.peers.Load(m.To); ok {
			p := v.(*peer)
			p.send(m)
		}
	}
}

func (t *TCPTransport) AddPeer(id uint64, addr string) {
	p := newPeer(id, addr, t.id, t.stopCh)
	if _, loaded := t.peers.LoadOrStore(id, p); !loaded {
		go p.run()
	}
}

func (t *TCPTransport) RemovePeer(id uint64) {
	if v, ok := t.peers.LoadAndDelete(id); ok {
		p := v.(*peer)
		p.stop()
	}
}

func (t *TCPTransport) ActiveSince(id uint64) time.Time {
	if v, ok := t.peers.Load(id); ok {
		return v.(*peer).activeSince()
	}
	return time.Time{}
}

func (t *TCPTransport) Stop() {
	close(t.stopCh)
	if t.listener != nil {
		t.listener.Close()
	}
	t.peers.Range(func(k, v interface{}) bool {
		v.(*peer).stop()
		return true
	})
	t.wg.Wait()
}

type peer struct {
	id       uint64
	addr     string
	localID  uint64
	pipeline chan raft.Message
	stopCh   <-chan struct{}
	peerStop chan struct{}
	active   time.Time
	mu       sync.RWMutex
}

func newPeer(id uint64, addr string, localID uint64, stopCh <-chan struct{}) *peer {
	return &peer{
		id:       id,
		addr:     addr,
		localID:  localID,
		pipeline: make(chan raft.Message, pipelineCap),
		stopCh:   stopCh,
		peerStop: make(chan struct{}),
	}
}

func (p *peer) send(m raft.Message) {
	select {
	case p.pipeline <- m:
	default:
	}
}

func (p *peer) stop() {
	close(p.peerStop)
}

func (p *peer) activeSince() time.Time {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.active
}

func (p *peer) run() {
	delay := reconnectBaseDelay
	for {
		conn, err := dialPeer(p.addr)
		if err != nil {
			select {
			case <-p.peerStop:
				return
			case <-p.stopCh:
				return
			case <-time.After(delay):
				delay = backoff(delay, reconnectMaxDelay)
				continue
			}
		}
		delay = reconnectBaseDelay
		p.mu.Lock()
		p.active = time.Now()
		p.mu.Unlock()
		if err := p.sendLoop(conn); err != nil {
			conn.Close()
			p.mu.Lock()
			p.active = time.Time{}
			p.mu.Unlock()
		}
		select {
		case <-p.peerStop:
			return
		case <-p.stopCh:
			return
		default:
		}
	}
}

func (p *peer) sendLoop(conn net.Conn) error {
	bw := bufio.NewWriterSize(conn, maxMsgBufSize)
	for {
		select {
		case m := <-p.pipeline:
			if err := writeMessage(bw, m); err != nil {
				return err
			}
			for len(p.pipeline) > 0 {
				m = <-p.pipeline
				if err := writeMessage(bw, m); err != nil {
					return err
				}
			}
			if err := bw.Flush(); err != nil {
				return err
			}
		case <-p.peerStop:
			return nil
		case <-p.stopCh:
			return nil
		}
	}
}

func dialPeer(addr string) (net.Conn, error) {
	d := net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: keepAliveInterval,
	}
	return d.Dial("tcp", addr)
}

func writeMessage(w io.Writer, m raft.Message) error {
	var buf [8192]byte
	enc := gob.NewEncoder(&fixedWriter{buf: buf[:0]})

	wm := toWire(m)
	var bb boundedBuffer
	enc2 := gob.NewEncoder(&bb)
	if err := enc2.Encode(wm); err != nil {
		return fmt.Errorf("transport: encode message: %w", err)
	}

	data := bb.bytes()
	checksum := crc32.Checksum(data, crcTable)
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(data)))
	binary.BigEndian.PutUint32(header[4:8], checksum)
	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("transport: write header: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("transport: write body: %w", err)
	}
	_ = enc
	return nil
}

func readMessage(r io.Reader) (raft.Message, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return raft.Message{}, err
	}
	length := binary.BigEndian.Uint32(header[0:4])
	expectedCRC := binary.BigEndian.Uint32(header[4:8])
	if length > 32*1024*1024 {
		return raft.Message{}, fmt.Errorf("transport: message too large: %d bytes", length)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return raft.Message{}, fmt.Errorf("transport: read body: %w", err)
	}
	if crc32.Checksum(data, crcTable) != expectedCRC {
		return raft.Message{}, fmt.Errorf("transport: CRC mismatch")
	}
	var wm wireMsg
	dec := gob.NewDecoder(bytesReader(data))
	if err := dec.Decode(&wm); err != nil {
		return raft.Message{}, fmt.Errorf("transport: decode message: %w", err)
	}
	return fromWire(wm), nil
}

func backoff(delay, max time.Duration) time.Duration {
	delay *= 2
	if delay > max {
		return max
	}
	return delay
}

type boundedBuffer struct {
	data []byte
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *boundedBuffer) bytes() []byte { return b.data }

type fixedWriter struct {
	buf []byte
}

func (f *fixedWriter) Write(p []byte) (int, error) {
	f.buf = append(f.buf, p...)
	return len(p), nil
}

type byteReadCloser struct {
	data []byte
	pos  int
}

func bytesReader(data []byte) io.Reader {
	return &byteReadCloser{data: data}
}

func (b *byteReadCloser) Read(p []byte) (int, error) {
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}
