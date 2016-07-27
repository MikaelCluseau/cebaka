package log

import (
	"bufio"
	"io"
)

type Writer struct {
	WriterBackend

	// next message position (aka segment size)
	position int64

	bufferSize int
	buf        *bufio.Writer
}

func NewWriter(backend WriterBackend, position int64, bufferSize int) *Writer {
	if bufferSize == 0 {
		bufferSize = 4096
	}
	w := &Writer{backend, position, bufferSize, nil}
	w.resetBufio()
	return w
}

// Append a log message and return the position after append, or any error occured when writing.
func (lw *Writer) Append(offset uint64, message *Message) (int64, error) {
	length := message.Len()

	failure := func(err error) (int64, error) {
		lw.rewind()
		return 0, err
	}

	bw := NewBinaryWriter(lw.buf)
	bw.WriteUint64(offset)
	bw.WriteUint32(length)
	if bw.err != nil {
		return failure(bw.err)
	}

	message.WriteTo(bw)
	if bw.err != nil {
		return failure(bw.err)
	}

	if err := lw.buf.Flush(); err != nil {
		return failure(err)
	}

	lw.position += 8 + 4 + int64(length)
	return lw.position, nil
}

func (lw *Writer) Sync() error {
	return lw.WriterBackend.Sync()
}

func (lw *Writer) rewind() {
	lw.Seek(int64(lw.position), 0)
	// buffer is invalid after seek
	lw.buf = bufio.NewWriter(lw)
}

func (lw *Writer) resetBufio() {
	lw.buf = bufio.NewWriterSize(lw, lw.bufferSize)
}

type WriterBackend interface {
	io.Writer
	io.Seeker
	io.Closer

	Sync() error
}
