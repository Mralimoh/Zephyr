package transport

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

	mode       string
	gasIDs     []string
	gasKey     string
	gasRRIndex uint32
	ctx        context.Context

	sessions  map[string]*Session
	sessionMu sync.RWMutex

	closedSessions   map[string]time.Time
	closedSessionsMu sync.Mutex

	OnNewSession func(sessionID, targetAddr string, s *Session)

	txSem chan struct{}
	rxSem chan struct{}

	processed     map[string]bool
	processedRing []string
	processedIdx  int
	processedMu   sync.Mutex

	lastTxTime time.Time

	zstdWriterPool sync.Pool
	zstdReaderPool sync.Pool
}

func NewEngine(store Datastore, isClient bool, clientID string, mode string, gasIDs []string, gasKey string) *Engine {
	e := &Engine{
		store:          store,
		id:             clientID,
		mode:           mode,
		gasIDs:         gasIDs,
		gasKey:         gasKey,
		sessions:       make(map[string]*Session),
		closedSessions: make(map[string]time.Time),
		processed:      make(map[string]bool),
		processedRing:  make([]string, 120),
		txSem:          make(chan struct{}, 16),
		rxSem:          make(chan struct{}, 32),
	}

	e.zstdWriterPool.New = func() any {
		zw, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			log.Fatalf("Critical: failed to initialize zstd writer: %v", err)
		}
		return zw
	}

	e.zstdReaderPool.New = func() any {
		zr, err := zstd.NewReader(nil)
		if err != nil {
			log.Fatalf("Critical: failed to initialize zstd reader: %v", err)
		}
		return zr
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
	e.ctx = ctx
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
	interval := 100 * time.Millisecond
	if e.mode == "script" {
		interval = 150 * time.Millisecond
	}
	
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.flushAll(context.Background())
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
	var closedIDs []string

	for _, s := range sessions {
		s.mu.Lock()
		if time.Since(s.lastActivity) > 60*time.Second {
			s.closed = true
		}
		
		shouldSend := s.closed || 
		              len(s.txBuf) >= FlushThresholdBytes || 
		              (len(s.txBuf) > 0 && time.Since(s.txBufAge) >= 100*time.Millisecond)

		if e.mode == "script" && s.txSeq == 0 && len(s.txBuf) == 0 && !s.closed {
			shouldSend = false
		}

		if !shouldSend {
			s.mu.Unlock()
			continue
		}

		env := Envelope{
			SessionID:  s.ID,
			Seq:        s.txSeq,
			Payload:    s.txBuf,
			Close:      s.closed,
			TargetAddr: s.TargetAddr,
		}
		
		if s.closed {
			closedIDs = append(closedIDs, s.ID)
		}

		cid := s.ClientID
		if cid == "" && e.myDir == DirReq {
			cid = e.id
		}
		muxes[cid] = append(muxes[cid], env)

		s.txBuf = make([]byte, 0, FlushThresholdBytes*2)
		s.txSeq++
		s.txCond.Broadcast()
		s.mu.Unlock()
	}

	for cid, mux := range muxes {
		filename := fmt.Sprintf("%s-%s-mux-%d.bin", e.myDir, cid, time.Now().UnixNano())
		
		select {
		case e.txSem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		go func(fname string, envelopes []Envelope, targetCID string) {
			defer func() { <-e.txSem }()
			delays := []time.Duration{0, 100 * time.Millisecond, 250 * time.Millisecond}

			for i := 0; i < len(delays); i++ {
				if delays[i] > 0 {
					select {
					case <-time.After(delays[i]):
					case <-ctx.Done():
						return
					}
				}

				pr, pw := io.Pipe()
				go func() {
					zw := e.zstdWriterPool.Get().(*zstd.Encoder)
					zw.Reset(pw)
					var encErr error
					for _, env := range envelopes {
						if err := env.Encode(zw); err != nil {
							encErr = err
							break
						}
					}
					zw.Close()
					e.zstdWriterPool.Put(zw)
					pw.CloseWithError(encErr)
				}()

				var err error
				if e.mode == "script" && len(e.gasIDs) > 0 && e.myDir == DirReq {
					idx := atomic.AddUint32(&e.gasRRIndex, 1)
					selectedID := e.gasIDs[idx%uint32(len(e.gasIDs))]
					selectedURL := fmt.Sprintf("https://script.google.com/macros/s/%s/exec", selectedID)

					if gbe, ok := e.store.(interface {
						UploadViaGAS(ctx context.Context, gasURL, gasKey, clientID string, data io.Reader) error
					}); ok {
						err = gbe.UploadViaGAS(ctx, selectedURL, e.gasKey, e.id, pr)
					} else {
						err = e.store.Upload(ctx, fname, pr)
					}
				} else {
					err = e.store.Upload(ctx, fname, pr)
				}
				
				pr.Close()

				if err == nil {
					e.lastTxTime = time.Now()
					return
				}

				if i == len(delays)-1 {
					log.Printf("[Engine] Upload permanently failed for %s after %d attempts.", fname, len(delays), len(envelopes), err)
					for _, env := range envelopes {
						e.RemoveSession(env.SessionID)
					}
				}
			}
		}(filename, mux, cid)
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
			prefix := string(e.peerDir) + "-"
			if e.myDir == DirReq {
				prefix += e.id + "-mux-"
			}

			files, _ := e.store.ListQuery(ctx, prefix)
			foundNewData := false
			if len(files) > 0 {
				var newFiles []string
				e.processedMu.Lock()
				for _, f := range files {
					if !e.processed[f] {
						oldest := e.processedRing[e.processedIdx]
						if oldest != "" {
							delete(e.processed, oldest)
						}

						e.processed[f] = true
						e.processedRing[e.processedIdx] = f
						e.processedIdx = (e.processedIdx + 1) % 120

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

			if foundNewData && e.mode == "script" {
				continue
			}

			e.sessionMu.RLock()
			activeSessions := len(e.sessions)
			e.sessionMu.RUnlock()

			var sleepDur time.Duration
			if activeSessions > 0 {
				if e.mode == "script" {
					sleepDur = 20 * time.Millisecond
				} else {
					isAggressiveMode := time.Since(e.lastTxTime) < 2*time.Second
					if isAggressiveMode {
						sleepDur = 20 * time.Millisecond
					} else {
						sleepDur = 50 * time.Millisecond
					}
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

	delays := []time.Duration{0, 100 * time.Millisecond, 250 * time.Millisecond}
	var rc io.ReadCloser
	var err error

	for i := 0; i < len(delays); i++ {
		if delays[i] > 0 {
			select {
			case <-time.After(delays[i]):
			case <-ctx.Done():
				return
			}
		}

		rc, err = e.store.Download(ctx, fname)
		if err == nil {
			break
		}
	}

	if err != nil {
		return
	}
	defer rc.Close()

	var fileClientID string
	parts := strings.Split(fname, "-")
	if len(parts) >= 4 && parts[2] == "mux" {
		fileClientID = parts[1]
	}

	e.ProcessRawStream(rc, fileClientID)
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
	seenFiles := make(map[string]time.Time)

	doCleanup := func() {
		e.closedSessionsMu.Lock()
		for id, t := range e.closedSessions {
			if time.Since(t) > 1*time.Minute {
				delete(e.closedSessions, id)
			}
		}
		e.closedSessionsMu.Unlock()

		prefixes :=[]string{string(e.myDir) + "-"}
		currentSeenInStore := make(map[string]bool)

		for _, pref := range prefixes {
			select {
			case <-ctx.Done():
				return
			default:
			}

			files, err := e.store.ListQuery(ctx, pref)
			if err != nil {
				continue
			}

			for _, f := range files {
				select {
				case <-ctx.Done():
					return
				default:
				}

				parts := strings.Split(strings.TrimSuffix(f, ".bin"), "-")
				if len(parts) < 4 {
					continue
				}

				currentSeenInStore[f] = true

				if firstSeen, exists := seenFiles[f]; !exists {
					seenFiles[f] = time.Now()
				} else if time.Since(firstSeen) > 2*time.Minute {
					if err := e.store.Delete(ctx, f); err != nil {
						log.Printf("[Engine] Cleanup: failed to delete old file %s: %v", f, err)
					} else {
						delete(seenFiles, f)
					}
				}
			}
		}

		for f := range seenFiles {
			if !currentSeenInStore[f] {
				delete(seenFiles, f)
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

func (e *Engine) ProcessRawStream(r io.Reader, fileClientID string) {
	zr := e.zstdReaderPool.Get().(*zstd.Decoder)
	if err := zr.Reset(r); err != nil {
		e.zstdReaderPool.Put(zr)
		return
	}
	defer e.zstdReaderPool.Put(zr)

	for {
		select {
		case <-e.ctx.Done():
			return
		default:
		}

		var env Envelope
		if err := env.Decode(zr); err != nil {
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
			s = NewSession(e.ctx, env.SessionID)
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
}