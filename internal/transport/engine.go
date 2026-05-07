package transport

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	"io"

	"github.com/klauspost/compress/zstd"
)

type Datastore interface {
	Upload(ctx context.Context, filename string, data io.Reader) error
	ListQuery(ctx context.Context, prefix string) ([]string, error)
	Download(ctx context.Context, filename string) (io.ReadCloser, error)
	Delete(ctx context.Context, filename string) error
}

type Engine struct {
	store   Datastore
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

	txSem chan struct{}
	rxSem chan struct{}

	processed   map[string]bool
	processedMu sync.Mutex

	lastTxTime time.Time

	fileRetries   map[string]int
	fileRetriesMu sync.Mutex

	zstdWriterPool sync.Pool
}

func NewEngine(store Datastore, isClient bool, clientID string) *Engine {
	e := &Engine{
		store:          store,
		id:             clientID,
		sessions:       make(map[string]*Session),
		closedSessions: make(map[string]time.Time),
		processed:      make(map[string]bool),
		pollTicker:     100 * time.Millisecond,
		flushTicker:    50 * time.Millisecond,
		txSem:          make(chan struct{}, 16),
		rxSem:          make(chan struct{}, 32),
		fileRetries:    make(map[string]int),
	}

	e.zstdWriterPool.New = func() any {
		zw, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			log.Fatalf("Critical: failed to initialize zstd writer: %v", err)
		}
		return zw
	}

	if isClient {
		e.myDir = DirReq
		e.peerDir = DirRes
	} else {
		e.myDir = DirRes
		e.peerDir = DirReq
	}

	return e
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

func (e *Engine) makeBaseline(ctx context.Context) {
	prefixes := []string{string(DirReq) + "-", string(DirRes) + "-"}

	for _, pref := range prefixes {
		files, err := e.store.ListQuery(ctx, pref)
		if err != nil {
			continue
		}

		e.processedMu.Lock()
		for _, f := range files {
			e.processed[f] = true
		}
		e.processedMu.Unlock()
	}
}

func (e *Engine) Start(ctx context.Context) {
	e.makeBaseline(ctx)
	go e.flushLoop(ctx)
	go e.pollLoop(ctx)
	go e.cleanupLoop(ctx)
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
	e.sessionMu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessionMu.RUnlock()

	muxes := make(map[string][]Envelope)
	var closedIDs []string

	for _, s := range sessions {
		collecting := true
		for collecting {
			select {
			case env, ok := <-s.txOut:
				if !ok {
					collecting = false
					closedIDs = append(closedIDs, s.ID)
					continue
				}
				
				cid := s.ClientID
				if cid == "" && e.myDir == DirReq {
					cid = e.id
				}
				muxes[cid] = append(muxes[cid], env)
				
				if env.Close {
					closedIDs = append(closedIDs, s.ID)
				}
			default:
				collecting = false
			}
		}
	}

	for cid, envelopes := range muxes {
		if len(envelopes) == 0 {
			continue
		}

		filename := fmt.Sprintf("%s-%s-mux-%d.bin", e.myDir, cid, time.Now().UnixNano())
		
		select {
		case e.txSem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		go func(fname string, envs []Envelope) {
			defer func() { <-e.txSem }()
			pr, pw := io.Pipe()

			go func() {
				zw := e.zstdWriterPool.Get().(*zstd.Encoder)
				zw.Reset(pw)
				var encErr error
				for _, env := range envs {
					if err := env.Encode(zw); err != nil {
						encErr = err
						break
					}
				}
				zw.Close()
				e.zstdWriterPool.Put(zw)
				pw.CloseWithError(encErr)
			}()

			if err := e.store.Upload(ctx, fname, pr); err == nil {
				e.lastTxTime = time.Now()
			}
			pr.Close()
		}(filename, envelopes)
	}

	for _, id := range closedIDs {
		e.RemoveSession(id)
	}
}

func (e *Engine) pollLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			isAggressiveMode := time.Since(e.lastTxTime) < 2*time.Second

			prefix := string(e.peerDir) + "-"
			if e.myDir == DirReq {
				prefix += e.id + "-mux-"
			}

			files, err := e.store.ListQuery(ctx, prefix)
			if err != nil {
				log.Printf("[Engine] List error in poll: %v", err)
				select {
				case <-time.After(e.pollTicker):
				case <-ctx.Done():
					return
				}
				continue
			}

			foundNewData := false
			if len(files) > 0 {
				var newFiles []string
				e.processedMu.Lock()
				for _, f := range files {
					if !e.processed[f] {
						e.processed[f] = true
						newFiles = append(newFiles, f)
						foundNewData = true
					}
				}
				e.processedMu.Unlock()

				if len(newFiles) > 0 {
					var wg sync.WaitGroup
					for _, f := range newFiles {
						wg.Add(1)
						go func(fname string) {
							defer wg.Done()
							e.processFile(ctx, fname)
						}(f)
					}
					wg.Wait()
				}
			}

			if foundNewData {
				continue
			}

			e.sessionMu.RLock()
			activeSessions := len(e.sessions)
			e.sessionMu.RUnlock()

			var sleepDur time.Duration
			if activeSessions > 0 {
				if isAggressiveMode {
					sleepDur = 20 * time.Millisecond
				} else {
					sleepDur = 50 * time.Millisecond
				}
			} else {
				sleepDur = 1 * time.Second
			}

			select {
			case <-time.After(sleepDur):
			case <-ctx.Done():
				return
			}
		}
	}
}

func (e *Engine) processFile(ctx context.Context, fname string) {
	select {
	case e.rxSem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-e.rxSem }()

	rc, err := e.store.Download(ctx, fname)
	if err != nil {
		log.Printf("[Engine] Warning: download failed for %s (Retrying): %v", fname, err)
		
		e.fileRetriesMu.Lock()
		e.fileRetries[fname]++
		retryCount := e.fileRetries[fname]
		e.fileRetriesMu.Unlock()

		if retryCount < 3 {
			e.processedMu.Lock()
			delete(e.processed, fname)
			e.processedMu.Unlock()
		} else {
			log.Printf("[Engine] Error: giving up on %s after 3 attempts, deleting.", fname)
			e.store.Delete(ctx, fname)
			e.fileRetriesMu.Lock()
			delete(e.fileRetries, fname)
			e.fileRetriesMu.Unlock()
		}
		return
	}
	defer rc.Close()

	e.fileRetriesMu.Lock()
	delete(e.fileRetries, fname)
	e.fileRetriesMu.Unlock()

	zr, err := zstd.NewReader(rc)
	if err != nil {
		log.Printf("[Engine] Error: failed to create zstd reader for %s: %v", fname, err)
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
				log.Printf("[Engine] Warning: decode error in file %s: %v", fname, err)
			}
			break
		}

		e.closedSessionsMu.Lock()
		_, isClosed := e.closedSessions[env.SessionID]
		e.closedSessionsMu.Unlock()
		if isClosed {
			continue
		}

		e.sessionMu.Lock()
		s, exists := e.sessions[env.SessionID]
		if !exists && e.myDir == DirRes && e.OnNewSession != nil {
			s = NewSession(env.SessionID)
			s.ClientID = fileClientID
			e.sessions[env.SessionID] = s
			e.sessionMu.Unlock()
			e.OnNewSession(env.SessionID, env.TargetAddr, s)
		} else {
			e.sessionMu.Unlock()
		}

		if s != nil {
			s.ProcessRx(&env)
		}
	}

	e.store.Delete(ctx, fname)
}

func (e *Engine) RemoveSession(id string) {
	e.sessionMu.Lock()
	s, exists := e.sessions[id]
	delete(e.sessions, id)
	e.sessionMu.Unlock()

	if exists && s != nil {
		s.cancel()
	}

	e.closedSessionsMu.Lock()
	e.closedSessions[id] = time.Now()
	e.closedSessionsMu.Unlock()
}

func (e *Engine) cleanupLoop(ctx context.Context) {
	doCleanup := func() {
		e.sessionMu.Lock()
		for id, s := range e.sessions {
			s.mu.Lock()
			last := s.lastActivity
			s.mu.Unlock()

			if time.Since(last) > 60*time.Second {
				delete(e.sessions, id)
				s.cancel() 
			}
		}
		e.sessionMu.Unlock()

		e.processedMu.Lock()
		if len(e.processed) > 2000 {
			e.processed = make(map[string]bool)
		}
		e.processedMu.Unlock()

		e.closedSessionsMu.Lock()
		for id, t := range e.closedSessions {
			if time.Since(t) > 1*time.Minute {
				delete(e.closedSessions, id)
			}
		}
		e.closedSessionsMu.Unlock()

		prefixes := []string{string(DirReq) + "-", string(DirRes) + "-"}
		for _, pref := range prefixes {
			files, err := e.store.ListQuery(ctx, pref)
			if err != nil {
				continue
			}

			for _, f := range files {
				parts := strings.Split(strings.TrimSuffix(f, ".bin"), "-")
				if len(parts) < 4 {
					continue 
				}

				nanoStr := parts[len(parts)-1]
				nanos, err := strconv.ParseInt(nanoStr, 10, 64)
				if err != nil {
					continue
				}

				if time.Since(time.Unix(0, nanos)) > 2*time.Minute {
					e.store.Delete(ctx, f)
				}
			}
		}
	}

	doCleanup()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			doCleanup()
		}
	}
}
