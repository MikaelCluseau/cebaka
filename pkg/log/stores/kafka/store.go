package kafka

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/MikaelCluseau/cebaka/pkg/log"
)

var (
	reLogFile = regexp.MustCompile("^[0-9]{20}.log$")
)

type Store struct {
	dir             string
	writeBufferSize int
}

var _ = log.Store(&Store{})

func Open(dir string, writeBufferSize int) *Store {
	return &Store{
		dir:             dir + "/",
		writeBufferSize: writeBufferSize,
	}
}

func (s *Store) Segments() ([]log.Segment, error) {
	f, err := os.Open(s.dir)
	if err != nil {
		return nil, err
	}

	allNames, err := f.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	segments := make([]log.Segment, 0)
	for _, name := range allNames {
		if !reLogFile.MatchString(name) {
			continue
		}
		n, err := strconv.ParseUint(name[0:20], 10, 64)
		if err != nil {
			panic(err) // may not happen because of the regex
		}
		segments = append(segments, &Segment{
			logFileName: filepath.Join(s.dir, name),
			startOffset: n,
			bufferSize:  s.writeBufferSize,
		})
	}
	return segments, nil
}

func (s *Store) AddSegment(startOffset uint64) (log.Segment, error) {
	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return nil, err
	}
	name := fmt.Sprintf("%s/%020d.log", s.dir, startOffset)
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	f.Close()
	return &Segment{
		logFileName: name,
		startOffset: startOffset,
		bufferSize:  s.writeBufferSize,
	}, nil
}

func (s *Store) mkdirs() error {
	return os.MkdirAll(s.dir, 0755)
}
