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
	lastActivity time.Time
	TargetAddr   string
	ClientID     string
	closed       bool

	Ctx    context.Context
	cancel context.CancelFunc

	txIn  chan []byte
	txOut chan Envelope

	rxIn  chan *Envelope
	rxOut chan []byte
}

func NewSession(id string) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{
		ID:           id,
		lastActivity: time.Now(),
		Ctx:          ctx,
		cancel:       cancel,
		txIn:         make(chan []byte, 128),
		txOut:        make(chan Envelope, 64),
		rxIn:         make(chan *Envelope, 128),
		rxOut:        make(chan []byte, 64),
	}

	go s.txWorker()
	go s.rxWorker()

	go func() {
		<-ctx.Done()
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
	}()

	return s
}

func (s *Session) EnqueueTx(data []byte) {
	buf := make([]byte, len(data))
	copy(buf, data)
	select {
	case s.txIn <- buf:
		s.updateActivity()
	case <-s.Ctx.Done():
	}
}

func (s *Session) ProcessRx(env *Envelope) {
	select {
	case s.rxIn <- env:
		s.updateActivity()
	case <-s.Ctx.Done():
	}
}

func (s *Session) updateActivity() {
	s.mu.Lock()
	s.lastActivity = time.Now()
	s.mu.Unlock()
}

func (s *Session) txWorker() {
	var txBuf []byte
	var txSeq uint64
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() { <-timer.C }

	send := func(isClose bool) {
		s.mu.Lock()
		addr := s.TargetAddr
		s.mu.Unlock()
		
		select {
		case s.txOut <- Envelope{SessionID: s.ID, Seq: txSeq, Payload: txBuf, TargetAddr: addr, Close: isClose}:
			txSeq++
			txBuf = nil
			timer.Stop()
		case <-s.Ctx.Done():
		}
	}

	for {
		select {
		case data := <-s.txIn:
			if len(txBuf) == 0 { timer.Reset(50 * time.Millisecond) }
			txBuf = append(txBuf, data...)
			if len(txBuf) >= FlushThresholdBytes { send(false) }
		case <-timer.C:
			if len(txBuf) > 0 { send(false) }
		case <-s.Ctx.Done():
			send(true)
			return
		}
	}
}

func (s *Session) rxWorker() {
	rxSeq := uint64(0)
	queue := make(map[uint64]*Envelope)
	const maxQueueSize = 1024

	for {
		select {
		case env := <-s.rxIn:
			if env.Seq == rxSeq {
				s.deliver(env)
				rxSeq++
				for {
					if next, ok := queue[rxSeq]; ok {
						s.deliver(next)
						delete(queue, rxSeq)
						rxSeq++
						if next.Close {
							return
						}
					} else {
						break
					}
				}
				if env.Close {
					return
				}
			} else if env.Seq > rxSeq {
				if len(queue) >= maxQueueSize {
					s.cancel()
					return
				}
				queue[env.Seq] = env
			}
		case <-s.Ctx.Done():
			return
		}
	}
}

func (s *Session) deliver(env *Envelope) {
	if len(env.Payload) > 0 {
		select {
		case s.rxOut <- env.Payload:
		case <-s.Ctx.Done():
		}
	}
	if env.Close {
		s.cancel()
	}
}