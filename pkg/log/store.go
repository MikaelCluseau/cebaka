package log

// A complete log.
type Store interface {
	// The current list of segments in the store.
	Segments() ([]Segment, error)
	// Add a new segment to the store.
	AddSegment(startOffset uint64) (Segment, error)
}

// A slice of a log.
type Segment interface {
	// The first offset of this segment (given by Store.AddSegment).
	StartOffset() uint64
	// An appender to this segment.
	Appender() (SegmentAppender, error)
	// A reader of this segment.
	Reader() (SegmentReader, error)
}

// Minimum interface to reliably append messages to a segment
type SegmentAppender interface {
	// Append a message to the log. Returns the position after the write (aka segment size).
	Append(offset uint64, message *Message) (int64, error)
	// Flush caches to ensure data is written.
	Sync() error
	// Close the appender.
	Close() error
}

// Minimum interface to read messages from a segment
type SegmentReader interface {
	// The current position in the segment.
	Position() int64
	// Read the next message from the segment.
	// Returns the offset, the message, and any error that occured while reading.
	Next() (uint64, *Message, error)
	// Seek to a given offset
	SeekToOffset(offset uint64) error
	// Seek to the end of the segment, returning the last valid offset read.
	SeekToEnd() (uint64, error)
	// Close the reader
	Close() error
}

type ByStartOffset []Segment

func (s ByStartOffset) Len() int {
	return len(s)
}

func (s ByStartOffset) Less(i, j int) bool {
	return s[i].StartOffset() < s[j].StartOffset()
}

func (s ByStartOffset) Swap(i, j int) {
	s[j], s[i] = s[i], s[j]
}
