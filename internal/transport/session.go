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

	FlushThresholdBytes = 1024 * 1024
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
	rxChunks [][]byte
	rxCond *sync.Cond

	Ctx    context.Context
	cancel context.CancelFunc

	rxStallTime time.Time
}

func NewSession(ctx context.Context, id string, engine *Engine) *Session {
	sessionCtx, cancel := context.WithCancel(ctx)

	qPtr := engine.rxQueuePool.Get().(map[uint64]*Envelope)
	chunks := engine.rxChunksPool.Get().([][]byte)

	s := &Session{
		ID:           id,
		rxQueue:      qPtr,
		lastActivity: time.Now(),
		txBuf:        make([]byte, 0, FlushThresholdBytes),
		rxChunks:     chunks[:0],
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

		if len(s.txBuf) > 0 {
			engine.txBufPool.Put(s.txBuf[:0])
			s.txBuf = nil
		}

		for k, env := range s.rxQueue {
			if len(env.Payload) > 0 {
				engine.payloadPool.Put(env.Payload[:0])
			}
			delete(s.rxQueue, k)
		}
		engine.rxQueuePool.Put(s.rxQueue)

		for i := range s.rxChunks {
			if len(s.rxChunks[i]) > 0 {
				engine.payloadPool.Put(s.rxChunks[i][:0])
			}
			s.rxChunks[i] = nil
		}
		
		engine.rxChunksPool.Put(s.rxChunks[:0])
		s.rxChunks = nil

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

func (s *Session) ProcessRx(env *Envelope) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActivity = time.Now()

	if s.rxClosed || s.closed {
		return false
	}

	if !s.rxStallTime.IsZero() && time.Since(s.rxStallTime) > 25*time.Second {
		s.rxClosed = true
		s.closed = true
		s.cancel()
		s.rxCond.Broadcast()
		s.txCond.Broadcast()
		return false
	}

	if env.Seq == s.rxSeq {
		if len(env.Payload) > 0 {
			s.rxChunks = append(s.rxChunks, env.Payload)
			s.rxCond.Signal()
		}
		s.rxSeq++
		if env.Close {
			s.rxClosed = true
			s.closed = true
			s.cancel()
			s.rxCond.Broadcast()
			return true
		}

		for {
			if nextEnv, ok := s.rxQueue[s.rxSeq]; ok {
				if s.closed {
					return true
				}
				if len(nextEnv.Payload) > 0 {
					s.rxChunks = append(s.rxChunks, nextEnv.Payload)
					s.rxCond.Signal()
				}
				delete(s.rxQueue, s.rxSeq)
				s.rxSeq++
				if nextEnv.Close {
					s.rxClosed = true
					s.closed = true
					s.cancel()
					s.rxCond.Broadcast()
					return true
				}
			} else {
				break
			}
		}

		if len(s.rxQueue) == 0 {
			s.rxStallTime = time.Time{}
		}
		return true

	} else if env.Seq > s.rxSeq {
		if env.Seq-s.rxSeq > 32 {
			s.rxClosed = true
			s.closed = true
			s.cancel()
			s.rxCond.Broadcast()
			s.txCond.Broadcast()
			return false
		}
		s.rxQueue[env.Seq] = env

		if s.rxStallTime.IsZero() {
			s.rxStallTime = time.Now()
		}
		return true
	}

	return false
}