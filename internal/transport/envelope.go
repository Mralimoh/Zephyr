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
)

func (e *Envelope) Encode(w io.Writer) error {
	var hdr [512]byte
	hdr[0] = MagicByte
	hdr[1] = uint8(len(e.SessionID))
	copy(hdr[2:], e.SessionID)
	offset := 2 + len(e.SessionID)

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
	offset += 4

	if _, err := w.Write(hdr[:offset]); err != nil {
		return err
	}
	if len(e.Payload) > 0 {
		_, err := w.Write(e.Payload)
		return err
	}
	return nil
}

func (e *Envelope) Decode(r io.Reader) error {
	var hdr [2]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err
	}
	if hdr[0] != MagicByte {
		return fmt.Errorf("invalid magic byte: 0x%X", hdr[0])
	}
	sidLen := int(hdr[1])
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
	addrBuf := make([]byte, addrLen)
	if _, err := io.ReadFull(r, addrBuf); err != nil {
		return err
	}
	e.TargetAddr = string(addrBuf)

	var closeBuf [1]byte
	if _, err := io.ReadFull(r, closeBuf[:]); err != nil {
		return err
	}
	e.Close = closeBuf[0] == 1

	var payLenBuf [4]byte
	if _, err := io.ReadFull(r, payLenBuf[:]); err != nil {
		return err
	}
	payLen := binary.BigEndian.Uint32(payLenBuf[:])
	if payLen > 10*1024*1024 {
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
