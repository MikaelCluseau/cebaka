package log

// Mostly borrowed from Kafka.
// See http://kafka.apache.org/documentation.html#messageformat.

import (
	"hash/crc32"
	"io"
	"time"
)

// On-disk format of a message
//
// offset         : 8 bytes
// message length : 4 bytes (value: 4 + 1 + 1 + 8(if magic value > 0) + 4 + K + 4 + V)
// crc            : 4 bytes
// magic value    : 1 byte
// attributes     : 1 byte
// timestamp      : 8 bytes (Only exists when magic value is greater than zero)
// key length     : 4 bytes
// key            : K bytes
// value length   : 4 bytes
// value          : V bytes
type Message struct {
	// 4 byte CRC32 of the message
	CRC uint32
	// 1 byte "magic" identifier to allow format changes, value is 0 or 1
	Format byte
	// 1 byte "attributes" identifier to allow annotations on the message independent
	//   bit 0 ~ 2 : Compression codec.
	//      0 : no compression
	//      1 : gzip
	//      2 : snappy
	//      3 : lz4
	//    bit 3 : Timestamp type
	//      0 : create time
	//      1 : log append time
	//    bit 4 ~ 7 : reserved
	Attributes byte
	// (Optional) 8 byte timestamp only if "magic" identifier is greater than 0
	Timestamp uint64
	// K byte key
	Key []byte
	// V byte payload
	Payload []byte
}

func Timestamp(t time.Time) uint64 {
	// FIXME is it Unix, UnixNano, something else?
	return uint64(t.Unix())
}

func NewMessage(timestamp uint64, key, data []byte) *Message {
	l := &Message{
		Format:     1,
		Attributes: 0,
		Timestamp:  timestamp,
		Key:        key,
		Payload:    data,
	}
	l.UpdateCRC()
	return l
}

func (l *Message) Len() uint32 {
	x := uint32(4 + 1 + 1 + 4 + 4 + len(l.Key) + len(l.Payload))
	if l.Format > 0 {
		x += 8
	}
	return x
}

func (l *Message) UpdateCRC() {
    l.CRC = l.ComputeCRC()
}

func (l *Message) ComputeCRC() uint32 {
	h := crc32.NewIEEE()
	l.writePostCRCTo(NewBinaryWriter(h))
	return h.Sum32()
}

func (l *Message) ReadFrom(reader io.Reader) error {
	r := BinaryReader{reader, nil}
	l.CRC = r.ReadUint32()
    l.Format = r.ReadByte()
    l.Attributes = r.ReadByte()
	if l.Format > 0 {
        l.Timestamp = r.ReadUint64()
	}
    l.Key = r.ReadBytes()
    l.Payload = r.ReadBytes()
	return r.err
}

func (l *Message) WriteTo(writer *BinaryWriter) {
    writer.WriteUint32(l.CRC)
	l.writePostCRCTo(writer)
}

func (l *Message) writePostCRCTo(w *BinaryWriter) {
	w.WriteByte(l.Format)
	w.WriteByte(l.Attributes)
	if l.Format > 0 {
		w.WriteUint64(l.Timestamp)
	}
	w.WriteBytes(l.Key)
	w.WriteBytes(l.Payload)
}
