package log

import (
	"sort"
	"sync"
)

type Config struct {
	MaxSegmentSize int64
	MaxSyncLag     int
}

type Log struct {
	config Config

	store    Store
	segments []Segment
	appender SegmentAppender

	nextOffset uint64
	syncOffset uint64

	writeMutex         sync.Mutex
	segmentSwitchMutex sync.Mutex

	offsetCond     *sync.Cond
	syncOffsetCond *sync.Cond
}

// Open a log from a store
func Open(config Config, store Store) (*Log, error) {
	segments, err := store.Segments()
	if err != nil {
		return nil, err
	}
	var nextOffset uint64 = 1
	if len(segments) == 0 {
		// new store
		segment, err := store.AddSegment(1)
		if err != nil {
			return nil, err
		}
		segments = append(segments, segment)
	}

	sort.Sort(ByStartOffset(segments))

	segment := segments[len(segments)-1]
	reader, err := segment.Reader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	lastOffset, err := reader.SeekToEnd()
	if err != nil {
		return nil, err
	}
	nextOffset = lastOffset + 1
	appender, err := segment.Appender()
	if err != nil {
		return nil, err
	}

	l := &Log{
		config: config,

		nextOffset: nextOffset,
		syncOffset: nextOffset - 1,

		store:    store,
		segments: segments,
		appender: appender,

		offsetCond:     sync.NewCond(&sync.Mutex{}),
		syncOffsetCond: sync.NewCond(&sync.Mutex{}),
	}

	return l, nil
}

func (l *Log) NextOffset() uint64 {
	return l.nextOffset
}

// Wait for this log to reach an offset of at least minOffset.
func (l *Log) WaitOffset(minOffset uint64) {
	// (1) invariant: nextOffset == lastOffset+1 <=> nextOffset-1 == lastOffset
	// (2) condition: lastOffset >= minOffset
	// so
	// (2) <=> nextOffset-1 >= minOffset
	//     <=> nextOffset >= minOffset+1
	// so
	// not (2) <=> nextOffset < minOffset+1
	//
	// also, since offsets are integers
	// (2) <=> nextOffset > minOffset

	if l.nextOffset > minOffset {
		return
	}

	l.offsetCond.L.Lock()
	for l.nextOffset < minOffset+1 {
		l.offsetCond.Wait()
	}
	l.offsetCond.L.Unlock()
}

// Wait for this log to sync an offset of at least minOffset.
func (l *Log) WaitSyncOffset(minOffset uint64) {
	if l.syncOffset >= minOffset {
		return
	}

	l.syncOffsetCond.L.Lock()
	defer l.syncOffsetCond.L.Unlock()
	for l.syncOffset < minOffset {
		l.syncOffsetCond.Wait()
	}
}

// Append a message to this log
func (l *Log) Append(message *Message) (uint64, error) {
	l.writeMutex.Lock()
	defer l.writeMutex.Unlock()

	offset := l.nextOffset
	sizeAfterAppend, err := l.appender.Append(offset, message)
	if err != nil {
		return 0, err
	}

	if sizeAfterAppend > l.config.MaxSegmentSize {
		if err := l.switchSegment(); err != nil {
			return 0, err
		}
	}

	l.offsetCond.L.Lock()
	l.nextOffset++
	l.offsetCond.Broadcast()
	l.offsetCond.L.Unlock()
	//log.Printf("l.nextOffset is now %d", l.nextOffset)

	// TODO more async "sync" support?
	if l.config.MaxSyncLag >= 0 && (offset-l.syncOffset) > uint64(l.config.MaxSyncLag) {
		l.Sync()
	}

	return offset, nil
}

func (l *Log) switchSegment() error {
	l.segmentSwitchMutex.Lock()
	defer l.segmentSwitchMutex.Unlock()

	if l.appender != nil {
		l.appender.Sync()
		l.appender.Close()
		l.appender = nil
	}

	segment, err := l.store.AddSegment(l.nextOffset)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, segment)

	appender, err := segment.Appender()
	if err != nil {
		return err
	}
	l.appender = appender
	return nil
}

func (l *Log) Sync() {
	if l.appender == nil {
		return
	}

	l.syncOffsetCond.L.Lock()
	defer l.syncOffsetCond.L.Unlock()

	offset := l.nextOffset - 1
	l.appender.Sync()
	l.syncOffset = offset
	l.syncOffsetCond.Broadcast()
}

// Change the configuration
func (l *Log) SetConfig(config Config) {
	// not sure sync is needed... but config changes are not frequent
	l.writeMutex.Lock()
	l.config = config
	l.writeMutex.Unlock()
}

func (l *Log) Close() {
	if l.appender != nil {
		l.appender.Sync()
		l.appender.Close()
	}
}

// Creates a new consumer starting at startOffset.
// If startOffset == 0, starts at the end of the log.
func (l *Log) Consumer(startOffset uint64) (*Consumer, error) {
	l.segmentSwitchMutex.Lock()
	defer l.segmentSwitchMutex.Unlock()

	if startOffset == 0 {
		startOffset = l.nextOffset
	}

	c := &Consumer{
		log:    l,
		offset: startOffset,
	}
	if err := c.setReader(); err != nil {
		return nil, err
	}
	return c, nil
}

func (l *Log) segmentForOffset(offset uint64) Segment {
	segment := l.segments[0]
	for _, s := range l.segments[1:] {
		if s.StartOffset() > offset {
			break
		}
		segment = s
	}
	return segment
}
