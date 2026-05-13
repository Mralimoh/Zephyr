package transport

import (
	"encoding/binary"
	"fmt"
	"io"
)

type Envelope struct {
	SessionID  string `json:"session_id"`
	Seq        uint64 `json:"seq"`
	TargetAddr string `json:"target_addr,omitempty"`
	Payload    []byte `json:"payload,omitempty"`
	Close      bool   `json:"close,omitempty"`
}

const (
	MagicByte = 0x1F
	MaxSessionIDLen = 64
	MaxAddrLen      = 255
	MaxPayloadLen   = 5 * 1024 * 1024
)

func (e *Envelope) Encode(w io.Writer) error {
	if len(e.SessionID) > MaxSessionIDLen {
		return fmt.Errorf("session ID too long: %d", len(e.SessionID))
	}
	if len(e.TargetAddr) > MaxAddrLen {
		return fmt.Errorf("target address too long: %d", len(e.TargetAddr))
	}
	if len(e.Payload) > MaxPayloadLen {
		return fmt.Errorf("payload too large: %d", len(e.Payload))
	}

	headerSize := 1 + 1 + len(e.SessionID) + 8 + 1 + len(e.TargetAddr) + 1 + 4
	var hdrBuf [512]byte
	hdr := hdrBuf[:headerSize]
	
	hdr[0] = MagicByte
	hdr[1] = uint8(len(e.SessionID))
	offset := 2
	copy(hdr[offset:], e.SessionID)
	offset += len(e.SessionID)

	binary.BigEndian.PutUint64(hdr[offset:], e.Seq)
	offset += 8

	hdr[offset] = uint8(len(e.TargetAddr))
	offset++
	copy(hdr[offset:], e.TargetAddr)
	offset += len(e.TargetAddr)

	if e.Close {
		hdr[offset] = 1
	} else {
		hdr[offset] = 0
	}
	offset++

	binary.BigEndian.PutUint32(hdr[offset:], uint32(len(e.Payload)))
	
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	if len(e.Payload) > 0 {
		_, err := w.Write(e.Payload)
		return err
	}
	return nil
}

func (e *Envelope) Decode(r io.Reader) error {
	var prefix [2]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return err
	}
	if prefix[0] != MagicByte {
		return fmt.Errorf("invalid magic byte: 0x%X", prefix[0])
	}

	sidLen := int(prefix[1])
	if sidLen > MaxSessionIDLen {
		return fmt.Errorf("malicious session ID length: %d", sidLen)
	}

	sidBuf := make([]byte, sidLen)
	if _, err := io.ReadFull(r, sidBuf); err != nil {
		return err
	}
	e.SessionID = string(sidBuf)

	var seqBuf [8]byte
	if _, err := io.ReadFull(r, seqBuf[:]); err != nil {
		return err
	}
	e.Seq = binary.BigEndian.Uint64(seqBuf[:])

	var addrLenBuf [1]byte
	if _, err := io.ReadFull(r, addrLenBuf[:]); err != nil {
		return err
	}
	addrLen := int(addrLenBuf[0])
	if addrLen > MaxAddrLen {
		return fmt.Errorf("malicious address length: %d", addrLen)
	}
	
	if addrLen > 0 {
		addrBuf := make([]byte, addrLen)
		if _, err := io.ReadFull(r, addrBuf); err != nil {
			return err
		}
		e.TargetAddr = string(addrBuf)
	}

	var footer [5]byte
	if _, err := io.ReadFull(r, footer[:]); err != nil {
		return err
	}
	e.Close = footer[0] == 1
	payLen := binary.BigEndian.Uint32(footer[1:])

	if payLen > MaxPayloadLen {
		return fmt.Errorf("packet too large: %d", payLen)
	}

	if payLen > 0 {
		e.Payload = make([]byte, payLen)
		if _, err := io.ReadFull(r, e.Payload); err != nil {
			return err
		}
	} else {
		e.Payload = nil
	}
	return nil
}