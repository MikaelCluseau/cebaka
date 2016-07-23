package log

import (
	"errors"
	"io"
)

var BadCRC = errors.New("Bad CRC in message")

type Reader struct {
	reader      io.ReadSeeker
	startOffset uint64
	position    int64
}

func (lr *Reader) Read() (uint64, *Message, error) {
	var (
		offset uint64
		size   uint32
	)
	r := binaryReader{lr.reader, nil}
	r.ReadValue(&offset)
	r.ReadValue(&size)
	if r.err != nil {
		lr.rewind()
		return 0, nil, r.err
	}
	l := &Message{}
	if err := l.ReadFrom(lr.reader); err != nil {
		return 0, nil, err
	}
	if l.ComputeCRC() != l.CRC {
		return 0, nil, BadCRC
	}
	return offset, l, nil
}

func (lr *Reader) rewind() {
	lr.reader.Seek(lr.position, 0)
}
