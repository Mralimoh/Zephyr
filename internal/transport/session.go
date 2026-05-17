package transport

import (
	"context"
	"sync"
	"time"
	"io"
)

type Direction string

const (
	DirReq Direction = "req"
	DirRes Direction = "res"

	FlushThresholdBytes = 256 * 1024
)

type Session struct {
	ID           string
	mu           sync.Mutex
	txBuf        []byte
	txSeq        uint64
	rxSeq        uint64
	rxQueue      map[uint64]*Envelope
	lastActivity time.Time
	txBufAge     time.Time
	closed       bool
	rxClosed     bool
	TargetAddr   string
	ClientID     string

	txCond *sync.Cond
	rxBuf  []byte
	rxCond *sync.Cond

	Ctx    context.Context
	cancel context.CancelFunc

	rxStallTime time.Time
}

func NewSession(ctx context.Context, id string) *Session {
	sessionCtx, cancel := context.WithCancel(ctx)
	s := &Session{
		ID:           id,
		rxQueue:      make(map[uint64]*Envelope),
		lastActivity: time.Now(),
		txBuf:        make([]byte, 0, 4096),
		rxBuf:        make([]byte, 0, 4096),
		Ctx:          sessionCtx,
		cancel:       cancel,
	}
	s.txCond = sync.NewCond(&s.mu)
	s.rxCond = sync.NewCond(&s.mu)

	go func() {
		<-sessionCtx.Done()
		s.mu.Lock()
		s.closed = true
		s.rxCond.Broadcast()
		s.txCond.Broadcast()
		s.mu.Unlock()
	}()

	return s
}

func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}

	s.closed = true
	s.cancel()
	s.txCond.Broadcast()
	s.rxCond.Broadcast()
}

func (s *Session) EnqueueTx(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for len(s.txBuf) > 512*1024 && !s.closed {
		s.txCond.Wait()
	}

	if s.closed {
		return io.EOF
	}

	if len(s.txBuf) == 0 {
		s.txBufAge = time.Now()
	}
	
	s.txBuf = append(s.txBuf, data...)
	s.lastActivity = time.Now()
	return nil
}

func (s *Session) ProcessRx(env *Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActivity = time.Now()

	if s.rxClosed || s.closed {
		return
	}

	if !s.rxStallTime.IsZero() && time.Since(s.rxStallTime) > 25*time.Second {
		s.rxClosed = true
		s.closed = true
		s.cancel()
		s.rxCond.Broadcast()
		s.txCond.Broadcast()
		return
	}

	if s.closed {
		return
	}

	for len(s.rxBuf) > 5*1024*1024 && !s.closed && !s.rxClosed {
		s.rxCond.Wait()
	}

	if s.closed || s.rxClosed {
		return
	}

	if env.Seq == s.rxSeq {
		if len(env.Payload) > 0 {
			s.rxBuf = append(s.rxBuf, env.Payload...)
			s.rxCond.Broadcast()
		}
		s.rxSeq++
		if env.Close {
			s.rxClosed = true
			s.closed = true
			s.cancel()
			s.rxCond.Broadcast()
			return
		}

		for {
			if nextEnv, ok := s.rxQueue[s.rxSeq]; ok {
				if s.closed {
					return
				}

				if len(nextEnv.Payload) > 0 {
					s.rxBuf = append(s.rxBuf, nextEnv.Payload...)
					s.rxCond.Broadcast()
				}
				delete(s.rxQueue, s.rxSeq)
				s.rxSeq++
				if nextEnv.Close {
					s.rxClosed = true
					s.closed = true
					s.cancel()
					s.rxCond.Broadcast()
					return
				}
			} else {
				break
			}
		}

		if len(s.rxQueue) == 0 {
			s.rxStallTime = time.Time{}
		}

	} else if env.Seq > s.rxSeq {
		if env.Seq-s.rxSeq > 32 {
			s.rxClosed = true
			s.closed = true
			s.cancel()
			s.rxCond.Broadcast()
			s.txCond.Broadcast()
			return
		}
		s.rxQueue[env.Seq] = env

		if s.rxStallTime.IsZero() {
			s.rxStallTime = time.Now()
		}
	}
}