package log

import (
	"io"
)

type LogWriter struct {
    LogWriterBackend

	// next message offset
	position uint64
}

// Append a log message and return the position after append, or any error occured when writing.
func (lw *LogWriter) Append(offset uint64, message *Message) error {
	length := message.Len()

	bw := binaryWriter{lw, nil}
	bw.WriteValue(offset)
	bw.WriteValue(length)
	if bw.err != nil {
		lw.rewind()
		return bw.err
	}
	if err := message.WriteTo(lw); err != nil {
		lw.rewind()
		return err
	}

	shift := 8 + uint64(length)
	lw.position += shift
	return nil
}

func (lw *LogWriter) rewind() {
	lw.Seek(int64(lw.position), 0)
}

type LogWriterBackend interface {
    io.Writer
    io.Seeker
    io.Closer

    Sync() error
}
