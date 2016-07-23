package log

import (
	"sync"
)

type Config struct {
	MaxSegmentSize int64
    MaxSyncLag int
}

type Log struct {
    config Config

	nextOffset    uint64
    syncOffset    uint64
	store         Store
	activeSegment Segment

	writeMutex sync.Mutex
	indexCond  *sync.Cond
}

// Open a log from a store
func Open(config Config, store Store) (*Log, error) {
	l := &Log{
		config: config,
		store:          store,
		indexCond:      sync.NewCond(&sync.Mutex{}),
	}

	segment, err := store.LastSegment()
	if err != nil {
		return nil, err
	}
	var nextOffset uint64 = 1
	if segment != nil {
		lastOffset, err := segment.LastOffset()
		if err != nil {
			return nil, err
		}
		nextOffset = lastOffset + 1
        l.activeSegment = segment
	}
	l.nextOffset = nextOffset
    l.syncOffset = nextOffset-1

	return l, nil
}

// Wait for this log to reach an offset of at least minOffset.
func (l *Log) WaitOffset(minOffset uint64) {
	l.indexCond.L.Lock()
	defer l.indexCond.L.Unlock()
	// (1) invariant: nextOffset == lastOffset+1 <=> nextOffset-1 == lastOffset
	// (2) condition: lastOffset >= minOffset
	// so
	// (2) <=> nextOffset-1 >= minOffset
	//     <=> nextOffset >= minOffset+1
	for l.nextOffset < minOffset+1 {
		l.indexCond.Wait()
	}
}

// Append a message to this log
func (l *Log) Append(message *Message) (uint64, error) {
	l.writeMutex.Lock()
	defer l.writeMutex.Unlock()

	if l.activeSegment == nil || l.activeSegment.Size() > l.config.MaxSegmentSize {
        if l.activeSegment != nil {
            l.activeSegment.Sync()
            l.activeSegment = nil
        }
		segment, err := l.store.LastSegment()
		if err != nil {
			return 0, err
		}
		if segment == nil {
			segment, err = l.store.AddSegment(l.nextOffset)
			if err != nil {
				return 0, err
			}
		}
		l.activeSegment = segment
	}

    offset := l.nextOffset
	if err := l.activeSegment.Append(offset, message); err != nil {
		return 0, err
	}

    // TODO more async "sync" support?
    if l.config.MaxSyncLag >= 0 && (offset - l.syncOffset) > uint64(l.config.MaxSyncLag) {
        l.activeSegment.Sync()
        l.syncOffset = offset
    }

	l.indexCond.L.Lock()
	l.nextOffset++
	l.indexCond.L.Unlock()

	return offset, nil
}

// Change the configuration
func (l *Log) SetConfig(config Config) {
    // TODO not sure sync is needed...
	l.writeMutex.Lock()
    l.config = config
	l.writeMutex.Unlock()
}

type Store interface {
	Segments() ([]Segment, error)
	LastSegment() (Segment, error)
	AddSegment(startOffset uint64) (Segment, error)
}

type Segment interface {
	FirstOffset() (uint64, error)
	LastOffset() (uint64, error)
	Size() int64
    Sync() error
	Append(offset uint64, message *Message) error
	Reader() (*Reader, error)
}
