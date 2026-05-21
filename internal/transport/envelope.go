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
	MaxPayloadLen   = 10 * 1024 * 1024
)

func (e *Envelope) Encode(w io.Writer) error {
	metaSize := 1 + 1 + len(e.SessionID) + 8 + 1 + len(e.TargetAddr) + 1 + 4
	
	buf := make([]byte, 2 + metaSize)
	binary.BigEndian.PutUint16(buf[0:2], uint16(metaSize))
	
	offset := 2
	buf[offset] = MagicByte
	buf[offset+1] = uint8(len(e.SessionID))
	offset += 2
	copy(buf[offset:], e.SessionID)
	offset += len(e.SessionID)

	binary.BigEndian.PutUint64(buf[offset:], e.Seq)
	offset += 8

	buf[offset] = uint8(len(e.TargetAddr))
	offset++
	copy(buf[offset:], e.TargetAddr)
	offset += len(e.TargetAddr)

	if e.Close {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}
	offset++

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(e.Payload)))
	
	if _, err := w.Write(buf); err != nil {
		return err
	}
	if len(e.Payload) > 0 {
		_, err := w.Write(e.Payload)
	}
	return nil
}

func (e *Envelope) Decode(r io.Reader) error {
	var sizeBuf [2]byte
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return err
	}
	metaSize := int(binary.BigEndian.Uint16(sizeBuf[:]))

	buf := make([]byte, metaSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}

	if buf[0] != MagicByte {
		return fmt.Errorf("invalid magic byte")
	}

	sidLen := int(buf[1])
	e.SessionID = string(buf[2 : 2+sidLen])
	
	offset := 2 + sidLen
	e.Seq = binary.BigEndian.Uint64(buf[offset : offset+8])
	offset += 8
	
	addrLen := int(buf[offset])
	offset++
	if addrLen > 0 {
		e.TargetAddr = string(buf[offset : offset+addrLen])
		offset += addrLen
	}

	e.Close = buf[offset] == 1
	payLen := binary.BigEndian.Uint32(buf[offset+1:])

	if payLen > 0 {
		if payLen > MaxPayloadLen {
			return fmt.Errorf("packet too large: %d", payLen)
		}
		e.Payload = make([]byte, payLen)
		if _, err := io.ReadFull(r, e.Payload); err != nil {
			return err
		}
	}
	return nil
}