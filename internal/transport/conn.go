package transport

import (
	"io"
	"net"
	"time"
)

type VirtualConn struct {
	session *Session
	engine  *Engine
}

func NewVirtualConn(s *Session, e *Engine) *VirtualConn {
	return &VirtualConn{
		session: s,
		engine:  e,
	}
}

func (v *VirtualConn) Read(b []byte) (n int, err error) {
	v.session.mu.Lock()
	defer v.session.mu.Unlock()

	for len(v.session.rxChunks) == 0 {
		if v.session.closed || v.session.rxClosed {
			return 0, io.EOF
		}
		v.session.rxCond.Wait()
	}

	chunk := v.session.rxChunks[0]
	n = copy(b, chunk)

	if n == len(chunk) {
		v.session.rxChunks[0] = nil 
		v.session.rxChunks = v.session.rxChunks[1:]
		
		if len(v.session.rxChunks) == 0 && cap(v.session.rxChunks) > 64 {
			v.session.rxChunks = make([][]byte, 0, 16)
		}
	} else {
		v.session.rxChunks[0] = chunk[n:]
	}

	return n, nil
}

func (v *VirtualConn) Write(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}

	if err := v.session.EnqueueTx(b); err != nil {
		return 0, err
	}

	return len(b), nil
}

func (v *VirtualConn) Close() error {
	v.session.mu.Lock()
	v.session.closed = true
	v.session.txCond.Broadcast()
	v.session.rxCond.Broadcast()
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
