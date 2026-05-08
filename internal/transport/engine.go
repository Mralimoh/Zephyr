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

	"github.com/klauspost/compress/zstd"
)

type Datastore interface {
	Upload(ctx context.Context, filename string, data io.Reader) error
	ListQuery(ctx context.Context, prefix string) ([]string, error)
	Download(ctx context.Context, filename string) (io.ReadCloser, error)
	Delete(ctx context.Context, filename string) error
}

type opKind int

const (
	opAdd opKind = iota
	opRemove
	opList
	opGet
	opCheckFile
	opMarkFile
	opUnmarkFile
	opGetRetry
	opIncRetry
	opResetFiles
	opSetLastTx
	opGetLastTx
	opCheckClosed
)

type engineOp struct {
	kind     opKind
	session  *Session
	sid      string
	filename string
	txTime   time.Time
	resp     chan *Session
	respList chan []*Session
	respBool chan bool
	respInt  chan int
	respTime chan time.Time
}

type Engine struct {
	store   Datastore
	myDir   Direction
	peerDir Direction
	id      string

	sessions       map[string]*Session
	processed      map[string]bool
	fileRetries    map[string]int
	closedSessions map[string]time.Time
	lastTxTime     time.Time
	managerChan    chan engineOp

	pollTicker  time.Duration
	flushTicker time.Duration

	OnNewSession func(sessionID, targetAddr string, s *Session)

	txSem chan struct{}
	rxSem chan struct{}

	chanPool sync.Pool 

	zstdWriterPool sync.Pool
}

func NewEngine(store Datastore, isClient bool, clientID string) *Engine {
	e := &Engine{
		store:          store,
		id:             clientID,
		sessions:       make(map[string]*Session),
		processed:      make(map[string]bool),
		fileRetries:    make(map[string]int),
		closedSessions: make(map[string]time.Time),
		managerChan:    make(chan engineOp, 256),
		pollTicker:     100 * time.Millisecond,
		flushTicker:    50 * time.Millisecond,
		txSem:          make(chan struct{}, 16),
		rxSem:          make(chan struct{}, 32),
	}

	e.chanPool.New = func() any {
		return make(chan []*Session, 1)
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

		for _, f := range files {
			e.processed[f] = true
		}
	}
}

func (e *Engine) Start(ctx context.Context) {
	e.makeBaseline(ctx)
	go e.runManager(ctx)
	go e.flushLoop(ctx)
	go e.pollLoop(ctx)
	go e.cleanupLoop(ctx)
}

func (e *Engine) AddSession(s *Session) {
	e.managerChan <- engineOp{
		kind:    opAdd,
		session: s,
	}
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
	respChan := e.chanPool.Get().(chan []*Session)
	e.managerChan <- engineOp{kind: opList, respList: respChan}
	sessions := <-respChan
	e.chanPool.Put(respChan)

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
				if cid == "" && e.myDir == DirReq { cid = e.id }
				muxes[cid] = append(muxes[cid], env)
				if env.Close { closedIDs = append(closedIDs, s.ID) }
			default:
				collecting = false
			}
		}
	}

	for cid, envelopes := range muxes {
		if len(envelopes) == 0 { continue }
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
				e.managerChan <- engineOp{kind: opSetLastTx, txTime: time.Now()}
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
			prefix := string(e.peerDir) + "-"
			if e.myDir == DirReq {
				prefix += e.id + "-mux-"
			}

			files, err := e.store.ListQuery(ctx, prefix)
			if err != nil {
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
				for _, f := range files {
					resCh := make(chan bool, 1)
					e.managerChan <- engineOp{kind: opCheckFile, filename: f, respBool: resCh}
					if isProcessed := <-resCh; !isProcessed {
						e.managerChan <- engineOp{kind: opMarkFile, filename: f}
						newFiles = append(newFiles, f)
						foundNewData = true
					}
				}

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

			select {
			case <-time.After(e.pollTicker):
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
		e.managerChan <- engineOp{kind: opIncRetry, filename: fname}
		retCh := make(chan int, 1)
		e.managerChan <- engineOp{kind: opGetRetry, filename: fname, respInt: retCh}
		retryCount := <-retCh

		if retryCount < 3 {
			e.managerChan <- engineOp{kind: opUnmarkFile, filename: fname}
		} else {
			e.store.Delete(ctx, fname)
		}
		return
	}
	defer rc.Close()

	zr, err := zstd.NewReader(rc)
	if err != nil {
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
			break
		}

		resp := make(chan *Session, 1)
		e.managerChan <- engineOp{kind: opGet, sid: env.SessionID, resp: resp}
		s := <-resp

		if s == nil && e.myDir == DirRes && e.OnNewSession != nil {
			s = NewSession(env.SessionID)
			s.ClientID = fileClientID
			e.AddSession(s)
			e.OnNewSession(env.SessionID, env.TargetAddr, s)
		}

		if s != nil {
			s.ProcessRx(&env)
		}
	}

	e.store.Delete(ctx, fname)
}

func (e *Engine) RemoveSession(id string) {
	e.managerChan <- engineOp{
		kind: opRemove,
		sid:  id,
	}
}

func (e *Engine) cleanupLoop(ctx context.Context) {
	doCleanup := func() {
		respChan := e.chanPool.Get().(chan []*Session)
		e.managerChan <- engineOp{kind: opList, respList: respChan}
		sessions := <-respChan
		e.chanPool.Put(respChan)

		for _, s := range sessions {
			s.mu.Lock()
			last := s.lastActivity
			s.mu.Unlock()

			if time.Since(last) > 60*time.Second {
				e.RemoveSession(s.ID)
			}
		}

		e.managerChan <- engineOp{kind: opResetFiles}

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

func (e *Engine) runManager(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case op := <-e.managerChan:
			switch op.kind {
			case opAdd:
				e.sessions[op.session.ID] = op.session

			case opRemove:
				if s, ok := e.sessions[op.sid]; ok {
					delete(e.sessions, op.sid)
					s.cancel()
					e.closedSessions[op.sid] = time.Now()
				}

			case opList:
				list := make([]*Session, 0, len(e.sessions))
				for _, s := range e.sessions {
					list = append(list, s)
				}
				op.respList <- list

			case opGet:
				op.resp <- e.sessions[op.sid]

			case opCheckFile:
				op.respBool <- e.processed[op.filename]

			case opMarkFile:
				e.processed[op.filename] = true
				delete(e.fileRetries, op.filename)

			case opUnmarkFile:
				delete(e.processed, op.filename)

			case opGetRetry:
				op.respInt <- e.fileRetries[op.filename]

			case opIncRetry:
				e.fileRetries[op.filename]++

			case opResetFiles:
				if len(e.processed) > 500 {
					e.processed = make(map[string]bool)
				}
				for id, t := range e.closedSessions {
					if time.Since(t) > 1*time.Minute {
						delete(e.closedSessions, id)
					}
				}

			case opSetLastTx:
				e.lastTxTime = op.txTime

			case opGetLastTx:
				op.respTime <- e.lastTxTime

			case opCheckClosed:
				_, closed := e.closedSessions[op.sid]
				op.respBool <- closed
			}
		}
	}
}
