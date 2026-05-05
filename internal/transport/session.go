package transport

import (
	"context"
	"sync"
	"time"
)

type Direction string

const (
	DirReq Direction = "req"
	DirRes Direction = "res"

	FlushThresholdBytes = 128 * 1024
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
}

func NewSession(id string) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{
		ID:           id,
		rxQueue:      make(map[uint64]*Envelope),
		lastActivity: time.Now(),
		txBuf:        make([]byte, 0, FlushThresholdBytes*2),
		rxBuf:        make([]byte, 0, FlushThresholdBytes*2),
		Ctx:          ctx,
		cancel:       cancel,
	}
	s.txCond = sync.NewCond(&s.mu)
	s.rxCond = sync.NewCond(&s.mu)

	go func() {
		<-ctx.Done()
		s.mu.Lock()
		s.closed = true
		s.rxCond.Broadcast()
		s.txCond.Broadcast()
		s.mu.Unlock()
	}()

	return s
}

func (s *Session) EnqueueTx(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for len(s.txBuf) > 2*1024*1024 && !s.closed {
		s.txCond.Wait()
	}

	if len(s.txBuf) == 0 {
		s.txBufAge = time.Now()
	}
	
	s.txBuf = append(s.txBuf, data...)
	s.lastActivity = time.Now()
}

func (s *Session) ClearTx() {
	s.mu.Lock()
	s.txBuf = make([]byte, 0, FlushThresholdBytes*2)
	s.txCond.Broadcast()
	s.mu.Unlock()
}

func (s *Session) ProcessRx(env *Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActivity = time.Now()

	if s.rxClosed || s.closed {
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
	} else if env.Seq > s.rxSeq {
		s.rxQueue[env.Seq] = env
	}
}
