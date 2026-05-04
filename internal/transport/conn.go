package transport

import (
	"io"
	"net"
	"sync"
	"time"
)

type VirtualConn struct {
	session *Session
	engine  *Engine
	readBuf []byte
	mu      sync.Mutex
}

func NewVirtualConn(s *Session, e *Engine) *VirtualConn {
	return &VirtualConn{
		session: s,
		engine:  e,
	}
}

func (v *VirtualConn) Read(b []byte) (n int, err error) {
	for {
		v.mu.Lock()
		if len(v.readBuf) > 0 {
			n = copy(b, v.readBuf)
			v.readBuf = v.readBuf[n:]
			v.mu.Unlock()
			return n, nil
		}
		v.mu.Unlock()

		select {
		case data, ok := <-v.session.RxChan:
			if !ok {
				return 0, io.EOF
			}

			if len(data) > 0 {
				v.mu.Lock()
				n = copy(b, data)
				if n < len(data) {
					v.readBuf = data[n:]
				}
				v.mu.Unlock()
				return n, nil
			}
		case <-v.session.Ctx.Done():
			return 0, io.EOF
		}

		v.session.mu.Lock()
		closed := v.session.closed
		v.session.mu.Unlock()

		if closed {
			return 0, io.EOF
		}
	}
}

func (v *VirtualConn) Write(b []byte) (n int, err error) {
	if len(b) > 0 {
		v.session.EnqueueTx(b)
	}
	return len(b), nil
}

func (v *VirtualConn) Close() error {
	v.session.mu.Lock()
	v.session.closed = true
	v.session.txCond.Broadcast()
	v.session.cancel()
	v.session.mu.Unlock()
	
	return nil
}

func (v *VirtualConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 65535}
}
func (v *VirtualConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 65535}
}
func (v *VirtualConn) SetDeadline(t time.Time) error      { return nil }
func (v *VirtualConn) SetReadDeadline(t time.Time) error  { return nil }
func (v *VirtualConn) SetWriteDeadline(t time.Time) error { return nil }
