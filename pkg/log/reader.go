package log

import (
	"bufio"
	"errors"
	"hash/crc32"
	"io"
)

var (
	BadCRC        = errors.New("bad CRC in message")
	UnexpectedEOF = errors.New("unexpected EOF")
)

type Reader struct {
	ReaderBackend
	position int64

	bufferSize int
	buf        *bufio.Reader
}

type ReaderBackend interface {
	io.Reader
	io.Seeker
	io.Closer
}

func NewReader(backend ReaderBackend, position int64, bufferSize int) *Reader {
	if bufferSize == 0 {
		bufferSize = 4096
	}
	r := &Reader{backend, position, bufferSize, nil}
	r.resetBufio()
	return r
}

func (lr *Reader) Position() int64 {
	return lr.position
}

func (lr *Reader) SeekToEnd() (uint64, error) {
	var lastValidOffset uint64 = 0
	for {
		offset, err := lr.FastRead()
		switch err {
		case nil:
			lastValidOffset = offset
			continue
		case io.EOF:
			return lastValidOffset, nil
		default:
			return 0, err
		}
	}
}

func (lr *Reader) SeekToOffset(offset uint64) error {
	// back to the start
	if _, err := lr.Seek(0, 0); err != nil {
		return err
	}

	for {
		positionBeforeRead := lr.position
		offset, err := lr.FastRead()
		if err != nil {
			return err
		}
		if offset >= offset {
			lr.position = positionBeforeRead
			return lr.rewind()
		}
	}
}

// Read and check the next message but don't parse it.
func (lr *Reader) FastRead() (uint64, error) {
	offset, size, r := lr.readPreMessage()
	if r.err != nil {
		return 0, r.err
	}
	failure := func(err error) (uint64, error) {
		lr.rewind()
		if err == io.EOF {
			return 0, UnexpectedEOF
		}
		return 0, err
	}
	crc := r.ReadUint32()
	if err := r.err; err != nil {
		return failure(err)
	}
	h := crc32.NewIEEE()
	// we hash only "size-4" because we just read the CRC (4 bytes)
	if _, err := io.CopyN(h, lr, int64(size)-4); err != nil {
		return failure(err)
	}
	if crc != h.Sum32() {
		return failure(BadCRC)
	}
	lr.updatePosition(size)
	return offset, nil
}

func (lr *Reader) readPreMessage() (uint64, uint32, *BinaryReader) {
	r := &BinaryReader{lr, nil}
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
func (lr *Reader) Next() (uint64, *Message, error) {
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
	if err := l.ReadFrom(lr); err != nil {
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

func (lr *Reader) rewind() error {
	_, err := lr.Seek(lr.position, 0)
	lr.resetBufio()
	return err
}

func (lr *Reader) resetBufio() {
	lr.buf = bufio.NewReaderSize(lr, lr.bufferSize)
}
