package log

import (
	"encoding/binary"
	"io"
)

var byteOrder = binary.BigEndian

// Performs reads from a reader, unless an error occur.
// When an error occurs, it is recorded in the err property, and future calls to read methods are no-op.
type BinaryReader struct {
	io.Reader
	err error
}

func (br *BinaryReader) Err() error {
	return br.err
}

func (br *BinaryReader) ReadUint32() uint32 {
	b := [4]byte{}
	if !br.read(b[:]) {
		return 0
	}
	return byteOrder.Uint32(b[:])
}

func (br *BinaryReader) ReadUint64() uint64 {
	b := [8]byte{}
	if !br.read(b[:]) {
		return 0
	}
	return byteOrder.Uint64(b[:])
}

func (br *BinaryReader) read(b []byte) bool {
	if br.err != nil {
		return false
	}
	if _, err := br.Read(b[:]); err != nil {
		br.err = err
		return false
    }
	return true
}

func (br *BinaryReader) ReadByte() byte {
	b := [1]byte{}
	br.read(b[:])
	return b[0]
}

func (br *BinaryReader) ReadBytes() []byte {
	size := br.ReadUint32()
	if br.err != nil || size == nilBytesSize {
		return nil
	}
	buf := make([]byte, size)
    if !br.read(buf) {
        return nil
    }
	return buf
}

// Performs writes to a writer, unless an error occur.
// When an error occurs, it is recorded in the err property, and future calls to write methods are no-op.
type BinaryWriter struct {
	io.Writer
	err error

    b4,
    b8 []byte
}

func NewBinaryWriter(writer io.Writer) *BinaryWriter {
    return &BinaryWriter{writer, nil, make([]byte, 4), make([]byte, 8)}
}

func (bw *BinaryWriter) Err() error {
	return bw.err
}

var (
    nilBytesSize uint32 = 0xffffffff
)

func (bw *BinaryWriter) WriteUint32(v uint32) {
    if bw.err != nil {
        return
    }
    byteOrder.PutUint32(bw.b4, v)
    _, bw.err = bw.Write(bw.b4)
}


func (bw *BinaryWriter) WriteUint64(v uint64) {
    if bw.err != nil {
        return
    }
    byteOrder.PutUint64(bw.b8, v)
    _, bw.err = bw.Write(bw.b8)
}

func (bw *BinaryWriter) WriteByte(v byte) {
    if bw.err != nil {
        return
    }
	b := [1]byte{v}
    _, bw.err = bw.Write(b[:])
}

func (bw *BinaryWriter) WriteBytes(b []byte) {
    if bw.err != nil {
        return
    }
	if b == nil {
		bw.WriteUint32(0xffffffff)
		return
	}
	bw.WriteUint32(uint32(len(b)))
	if bw.err != nil {
		return
	}
	_, bw.err = bw.Write(b)
}
