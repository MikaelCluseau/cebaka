package log

type Store interface {
    SegmentForOffset(offset uint64) (Segment, error)
	LastSegment() (Segment, error)
	AddSegment(startOffset uint64) (Segment, error)
}

type Segment interface {
	LastOffset() uint64
	Size() int64
	Sync() error
	Close()
	Append(offset uint64, message *Message) error
	Reader() (*Reader, error)
}
