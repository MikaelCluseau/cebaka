package kafka

import (
	"os"

	"github.com/MikaelCluseau/webaka/pkg/log"
)

type Segment struct {
	logFileName string
	startOffset uint64
	bufferSize  int
}

var _ = log.Segment(&Segment{})

func (s *Segment) StartOffset() uint64 {
	return s.startOffset
}

func (s *Segment) Appender() (log.SegmentAppender, error) {
	logFile, err := os.OpenFile(s.logFileName, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// Move after the last (valid) message
	r := log.NewReader(logFile, 0, s.bufferSize)

	if _, err := r.SeekToEnd(); err != nil {
		if err == log.UnexpectedEOF || err == log.BadCRC {
			s.lostTail(logFile, r.Position())
		} else {
			return nil, err
		}
	}

	return log.NewWriter(logFile, r.Position(), s.bufferSize), nil
}

func (s *Segment) Reader() (log.SegmentReader, error) {
	// XXX will not need 1 fd per reader if we use ReadAt in log.Reader instead / buuuut no buffering
	f, err := os.Open(s.logFileName)
	if err != nil {
		return nil, err
	}
	return log.NewReader(f, 0, s.bufferSize), nil
}

func (s *Segment) lostTail(logFile *os.File, position int64) error {
	// TODO archive tail
	// TODO truncate
	logFile.Seek(position, 0)
	return nil
}
