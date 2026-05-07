package transport

import (
	"io"
	"net"
	"time"
)

type VirtualConn struct {
	session *Session
	engine  *Engine
	readBuf []byte
}

func NewVirtualConn(s *Session, e *Engine) *VirtualConn {
	return &VirtualConn{
		session: s,
		engine:  e,
	}
}

func (v *VirtualConn) Read(b []byte) (n int, err error) {
	if len(v.readBuf) > 0 {
		n = copy(b, v.readBuf)
		v.readBuf = v.readBuf[n:]
		return n, nil
	}

	select {
	case data, ok := <-v.session.rxOut:
		if !ok {
			return 0, io.EOF
		}
		
		n = copy(b, data)
		if n < len(data) {
			v.readBuf = data[n:]
		}
		return n, nil

	case <-v.session.Ctx.Done():
		return 0, io.EOF
	}
}

func (v *VirtualConn) Write(b []byte) (n int, err error) {
	v.session.mu.Lock()
	if v.session.closed {
		v.session.mu.Unlock()
		return 0, io.EOF
	}
	v.session.mu.Unlock()

	if len(b) > 0 {
		v.session.EnqueueTx(b)
	}
	return len(b), nil
}

func (v *VirtualConn) Close() error {
	v.session.cancel()
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
