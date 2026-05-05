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

	for len(v.session.rxBuf) == 0 && !v.session.closed && !v.session.rxClosed {
		v.session.rxCond.Wait()
	}

	if len(v.session.rxBuf) > 0 {
		n = copy(b, v.session.rxBuf)
		v.session.rxBuf = v.session.rxBuf[n:]
		
		v.session.rxCond.Broadcast()
		return n, nil
	}

	return 0, io.EOF
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
