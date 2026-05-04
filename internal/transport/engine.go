package transport

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	"bytes"

	"Zephyr/internal/storage"
	"github.com/klauspost/compress/zstd"
)

type Engine struct {
	backend storage.Backend
	myDir   Direction
	peerDir Direction
	id      string

	sessions  map[string]*Session
	sessionMu sync.RWMutex

	closedSessions   map[string]time.Time
	closedSessionsMu sync.Mutex

	pollTicker  time.Duration
	flushTicker time.Duration

	OnNewSession func(sessionID, targetAddr string, s *Session)

	sem chan struct{}

	processed   map[string]bool
	processedMu sync.Mutex
}

func NewEngine(backend storage.Backend, isClient bool, clientID string) *Engine {
	e := &Engine{
		backend:        backend,
		id:             clientID,
		sessions:       make(map[string]*Session),
		closedSessions: make(map[string]time.Time),
		processed:      make(map[string]bool),
		pollTicker:  500 * time.Millisecond,
		flushTicker: 300 * time.Millisecond,
	}
	if isClient {
		e.myDir = DirReq
		e.peerDir = DirRes
	} else {
		e.myDir = DirRes
		e.peerDir = DirReq
	}
	e.sem = make(chan struct{}, 8)
	return e
}

func (e *Engine) SetRefreshRate(ms int) {
	if ms > 0 {
		e.pollTicker = time.Duration(ms) * time.Millisecond
		if e.flushTicker == 300*time.Millisecond {
			e.flushTicker = time.Duration(ms) * time.Millisecond
		}
	}
}

func (e *Engine) SetPollRate(ms int) {
	if ms > 0 {
		e.pollTicker = time.Duration(ms) * time.Millisecond
	}
}

func (e *Engine) SetFlushRate(ms int) {
	if ms > 0 {
		e.flushTicker = time.Duration(ms) * time.Millisecond
	}
}

func (e *Engine) Start(ctx context.Context) {
	go e.flushLoop(ctx)
	go e.pollLoop(ctx)
	go e.cleanupLoop(ctx)
}

func (e *Engine) GetSession(id string) *Session {
	e.sessionMu.RLock()
	defer e.sessionMu.RUnlock()
	return e.sessions[id]
}

func (e *Engine) AddSession(s *Session) {
	e.sessionMu.Lock()
	defer e.sessionMu.Unlock()
	e.sessions[s.ID] = s
	log.Printf("Engine.AddSession: Added session %s (Total now: %d)", s.ID, len(e.sessions))
}

func (e *Engine) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(e.flushTicker)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.flushAll(ctx)
		}
	}
}

func (e *Engine) flushAll(ctx context.Context) {
	e.sessionMu.Lock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessionMu.Unlock()

	muxes := make(map[string][]Envelope)
	var closedSessionIDs []string

	for _, s := range sessions {
		s.mu.Lock()

		if time.Since(s.lastActivity) > 10*time.Second {
			s.closed = true
		}

		shouldSend := s.closed || (s.txSeq == 0 && e.myDir == DirReq) || 
			len(s.txBuf) >= FlushThresholdBytes || 
			(len(s.txBuf) > 0 && time.Since(s.lastActivity) > 250*time.Millisecond)

		if !shouldSend {
			s.mu.Unlock()
			continue
		}

		payload := s.txBuf
		s.txBuf = make([]byte, 0, FlushThresholdBytes*2)
		s.txCond.Broadcast()

		env := Envelope{
			SessionID:  s.ID,
			Seq:        s.txSeq,
			Payload:    payload,
			Close:      s.closed,
			TargetAddr: s.TargetAddr,
		}

		s.txSeq++
		if s.closed {
			closedSessionIDs = append(closedSessionIDs, s.ID)
		}

		cid := s.ClientID
		if cid == "" && e.myDir == DirReq {
			cid = e.id
		}

		muxes[cid] = append(muxes[cid], env)
		s.mu.Unlock()
	}

	for cid, mux := range muxes {
		fnameCID := cid
		if fnameCID == "" {
			fnameCID = "unknown"
		}
		filename := fmt.Sprintf("%s-%s-mux-%d.bin", e.myDir, fnameCID, time.Now().UnixNano())

		go func(fname string, m []Envelope) {
			e.sem <- struct{}{}
			defer func() { <-e.sem }()

			var buf bytes.Buffer
			zw, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(zstd.SpeedFastest))
			if err != nil {
				log.Printf("zstd writer error: %v", err)
				return
			}
			
			for _, env := range m {
				if err := env.Encode(zw); err != nil {
					break
				}
			}
			zw.Close()

			payload := buf.Bytes()
			maxRetries := 3
			for attempt := 1; attempt <= maxRetries; attempt++ {
				err := e.backend.Upload(ctx, fname, bytes.NewReader(payload))
				if err == nil {
					return
				}
				
				log.Printf("upload retry %d/%d for %s: %v", attempt, maxRetries, fname, err)
				if attempt < maxRetries {
					time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
				}
			}
		}(filename, mux)
	}

	for _, id := range closedSessionIDs {
		e.RemoveSession(id)
	}
}

func (e *Engine) pollLoop(ctx context.Context) {
	currentPollInterval := e.pollTicker
	maxPollInterval := 5 * time.Second
	timer := time.NewTimer(currentPollInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		pollAgain:
			if e.myDir == DirReq {
				e.sessionMu.RLock()
				count := len(e.sessions)
				e.sessionMu.RUnlock()
				if count == 0 {
					timer.Reset(currentPollInterval)
					continue
				}
			}

			prefix := string(e.peerDir) + "-"
			if e.myDir == DirReq {
				prefix += e.id + "-mux-"
			}

			files, err := e.backend.ListQuery(ctx, prefix)
			if err != nil {
				log.Printf("poll list error: %v", err)
				timer.Reset(currentPollInterval)
				continue
			}

			if len(files) == 0 {
				if e.myDir == DirRes {
					e.sessionMu.RLock()
					activeSessions := len(e.sessions)
					e.sessionMu.RUnlock()

					if activeSessions == 0 {
						currentPollInterval += 500 * time.Millisecond
						if currentPollInterval > maxPollInterval {
							currentPollInterval = maxPollInterval
						}
					} else {
						currentPollInterval = e.pollTicker
					}
				}
				timer.Reset(currentPollInterval)
				continue
			}

			currentPollInterval = e.pollTicker

			var wg sync.WaitGroup
			for _, f := range files {
				parts := strings.Split(f, "-")
				if len(parts) >= 3 {
					tsStr := parts[len(parts)-1]
					tsStr = strings.TrimSuffix(tsStr, ".bin")
					ts, _ := strconv.ParseInt(tsStr, 10, 64)
					if ts > 0 && time.Since(time.Unix(0, ts)) > 5*time.Minute {
						e.backend.Delete(ctx, f)
						continue
					}
				}

				e.processedMu.Lock()
				already := e.processed[f]
				if !already {
					e.processed[f] = true
				}
				e.processedMu.Unlock()

				if already {
					continue
				}

				wg.Add(1)
				go func(fname string) {
					defer wg.Done()

					e.sem <- struct{}{}
					defer func() { <-e.sem }()

					rc, err := e.backend.Download(ctx, fname)
					if err != nil {
						log.Printf("download error %s: %v", fname, err)
						e.processedMu.Lock()
						delete(e.processed, fname)
						e.processedMu.Unlock()
						return
					}
					defer rc.Close()

					zr, err := zstd.NewReader(rc)
					if err != nil {
						log.Printf("zstd reader init error for %s: %v", fname, err)
						return
					}
					defer zr.Close()

					var fileClientID string
					parts := strings.Split(fname, "-")
					if len(parts) >= 4 && parts[2] == "mux" {
						fileClientID = parts[1]
					}

					for {
						var env Envelope
						if err := env.Decode(zr); err != nil {
							if err != io.EOF && err != io.ErrUnexpectedEOF {
								log.Printf("mux decode error %s: %v", fname, err)
							}
							break
						}

						e.closedSessionsMu.Lock()
						if _, exists := e.closedSessions[env.SessionID]; exists {
							e.closedSessionsMu.Unlock()
							continue
						}
						e.closedSessionsMu.Unlock()

						e.sessionMu.Lock()
						s, exists := e.sessions[env.SessionID]
						if !exists && e.myDir == DirRes && e.OnNewSession != nil {
							s = NewSession(env.SessionID)
							s.ClientID = fileClientID
							e.sessions[env.SessionID] = s
							e.sessionMu.Unlock()
							log.Printf("Engine: Triggering new session %s for Client %s", env.SessionID, fileClientID)
							e.OnNewSession(env.SessionID, env.TargetAddr, s)
						} else {
							e.sessionMu.Unlock()
						}

						if s != nil {
							s.ProcessRx(&env)
						}
					}

					e.backend.Delete(ctx, fname)
				}(f)
			}

			wg.Wait()

			time.Sleep(100 * time.Millisecond)
			goto pollAgain
		}
	}
}

func (e *Engine) RemoveSession(id string) {
	e.sessionMu.Lock()
	delete(e.sessions, id)
	e.sessionMu.Unlock()

	e.closedSessionsMu.Lock()
	e.closedSessions[id] = time.Now()
	e.closedSessionsMu.Unlock()
}

func (e *Engine) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.closedSessionsMu.Lock()
			for id, t := range e.closedSessions {
				if time.Since(t) > 30*time.Second {
					delete(e.closedSessions, id)
				}
			}
			e.closedSessionsMu.Unlock()

			e.processedMu.Lock()
			if len(e.processed) > 5000 {
				e.processed = make(map[string]bool)
			}
			e.processedMu.Unlock()

			if e.myDir == DirReq {
				e.sessionMu.RLock()
				count := len(e.sessions)
				e.sessionMu.RUnlock()
				if count == 0 {
					continue
				}
			}

			files, _ := e.backend.ListQuery(ctx, string(e.myDir)+"-")
			for _, f := range files {
				parts := strings.Split(f, "-")
				if len(parts) >= 3 {
					tsStr := parts[len(parts)-1]
					tsStr = strings.TrimSuffix(tsStr, ".json")
					tsStr = strings.TrimSuffix(tsStr, ".bin")
					ts, err := strconv.ParseInt(tsStr, 10, 64)
					if err == nil {
						t := time.Unix(0, ts)
						if time.Since(t) > 10*time.Second {
							e.backend.Delete(ctx, f)
						}
					}
				}
			}
		}
	}
}
