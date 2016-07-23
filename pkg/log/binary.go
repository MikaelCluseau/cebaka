package log

import (
	"encoding/binary"
	"io"
)

var byteOrder = binary.BigEndian

type binaryReader struct {
	io.Reader
	err    error
}

func (br binaryReader) ReadValue(data interface{}) {
	if br.err != nil {
		return
	}
	br.err = binary.Read(br, byteOrder, data)
}

func (br binaryReader) ReadBytes() []byte {
    var size int32
    br.ReadValue(&size)
	if br.err != nil {
		return nil
	}
    if size < 0 {
        return nil
    }
	buf := make([]byte, size)
	_, br.err = br.Read(buf)
	if br.err != nil {
		return nil
	}
	return buf
}

type binaryWriter struct {
	io.Writer
	err    error
}

func (bw binaryWriter) WriteValue(data interface{}) {
	if bw.err != nil {
		return
	}
	bw.err = binary.Write(bw, byteOrder, data)
}

func (bw binaryWriter) WriteBytes(b []byte) {
    if b == nil {
        bw.WriteValue(int32(-1))
        return
    }
    bw.WriteValue(int32(len(b)))
	if bw.err != nil {
		return
	}
	_, bw.err = bw.Write(b)
}
