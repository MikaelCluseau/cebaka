package log

import (
	"errors"
	"hash/crc32"
	"io"
)

var (
	BadCRC        = errors.New("bad CRC in message")
	UnexpectedEOF = errors.New("unexpected EOF")
)

type Reader struct {
	reader   io.ReadSeeker
	position int64
}

func NewReader(backend io.ReadSeeker, position int64) *Reader {
	return &Reader{backend, position}
}

func (lr *Reader) Position() int64 {
	return lr.position
}

// Read and check the next message but don't parse it.
func (lr *Reader) FastRead() (uint64, error) {
    offset, size, r := lr.readPreMessage()
	if r.err != nil {
        return 0, r.err
    }
    crc := r.ReadUint32()
    if err := r.err; err != nil {
		lr.rewind()
		if err == io.EOF {
			err = UnexpectedEOF
		}
		return 0, err
	}
	h := crc32.NewIEEE()
    // we hash only "size-4" because we just read the CRC (4 bytes)
	if _, err := io.CopyN(h, lr.reader, int64(size)-4); err != nil {
		if err == io.EOF {
			err = UnexpectedEOF
		}
		return 0, err
	}
	if crc != h.Sum32() {
		return 0, BadCRC
	}
	lr.updatePosition(size)
	return offset, nil
}

func (lr *Reader) readPreMessage() (uint64, uint32, *BinaryReader) {
	r := &BinaryReader{lr.reader, nil}
    offset := r.ReadUint64()
    size := r.ReadUint32()
    if r.err == nil && size == 0 {
        r.err = BadCRC
    }
    if r.err != nil {
        lr.rewind()
    }
    return offset, size, r
}

// Read and check the next message.
func (lr *Reader) Read() (uint64, *Message, error) {
	var offset uint64

    offset, size, r := lr.readPreMessage()
	if r.err != nil {
		return 0, nil, r.err
	}
	msg, err := lr.readMessage(r)
    if err != nil {
		lr.rewind()
    }
	if err == io.EOF {
		err = UnexpectedEOF
	}
    lr.updatePosition(size)
	return offset, msg, err
}

func (lr *Reader) readMessage(r *BinaryReader) (*Message, error) {
	var size uint32

	l := &Message{}
	if err := l.ReadFrom(lr.reader); err != nil {
		return nil, err
	}

	if l.ComputeCRC() != l.CRC {
		return nil, BadCRC
	}

	lr.updatePosition(size)

	return l, nil
}

func (lr *Reader) updatePosition(messageSize uint32) {
    // 8 bytes for the offset
    // 4 bytes for the size
	lr.position += 8 + 4 + int64(messageSize)
}

func (lr *Reader) rewind() {
	lr.reader.Seek(lr.position, 0)
}
