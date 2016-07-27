package log

import (
	"fmt"
	"io"
)

type Consumer struct {
	log    *Log
	offset uint64
	reader SegmentReader
}

func (c *Consumer) Next() (uint64, *Message, error) {
	c.log.WaitOffset(c.offset)
	// From here, we know we have this offset in this reader or one of the next
	for {
		offset, msg, err := c.reader.Next()
		if err == io.EOF {
			// it's in the next segment
			if err := c.setReader(); err != nil {
				return 0, nil, err
			}
			continue
		} else if err != nil {
			return 0, nil, err
		}
		if offset >= c.offset {
			c.offset = offset
			return offset, msg, nil
		}
	}
}

func (c *Consumer) setReader() error {
	if c.reader != nil {
		c.reader.Close()
		c.reader = nil
	}

	// switch to next segment
	s := c.log.segmentForOffset(c.offset)
	if s == nil {
		// setReader called on an invalid offset is a hard failure
		panic(fmt.Errorf("No segment for offset %d", c.offset))
	}
	reader, err := s.Reader()
	if err != nil {
		return err
	}
	c.reader = reader
	return nil
}

func (c *Consumer) Close() {
	c.reader.Close()
}
