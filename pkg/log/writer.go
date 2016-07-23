package log

import (
	"io"
    "bufio"
)

type Writer struct {
    WriterBackend

	// next message offset
	position int64

    buf *bufio.Writer
}

func NewWriter(backend WriterBackend, position int64) *Writer {
    return &Writer{backend, position, bufio.NewWriter(backend)}
}

func (lw *Writer) Position() int64 {
    return lw.position
}

// Append a log message and return the position after append, or any error occured when writing.
func (lw *Writer) Append(offset uint64, message *Message) error {
	length := message.Len()

	bw := NewBinaryWriter(lw.buf)
	bw.WriteUint64(offset)
	bw.WriteUint32(length)
	if bw.err != nil {
		lw.rewind()
		return bw.err
	}
	message.WriteTo(bw)
    if bw.err != nil {
		lw.rewind()
		return bw.err
	}

	lw.position += 8 + 4 + int64(length)
	return nil
}

func (lw *Writer) Sync() error {
    if err := lw.buf.Flush(); err != nil {
        return err
    }
    return lw.WriterBackend.Sync()
}

func (lw *Writer) rewind() {
	lw.Seek(int64(lw.position), 0)
    // buffer is invalid after seek
    lw.buf = bufio.NewWriter(lw)
}

type WriterBackend interface {
    io.Writer
    io.Seeker

    Sync() error
}
