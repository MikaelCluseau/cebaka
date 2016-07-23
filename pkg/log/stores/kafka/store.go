package kafka

import (
    "fmt"
    "os"
    "sort"
    "strconv"
    "regexp"
    golog "log"

    "github.com/MikaelCluseau/cebaka/pkg/log"
)

var (
    reLogFile = regexp.MustCompile("^[0-9]{20}.log$")
)

type Store struct {
    dir string
    segments []uint64
}

var _ = log.Store(&Store{})

func Open(dir string) (*Store, error) {
    f, err := os.Open(dir)
    if err != nil {
        return nil, err
    }

    allNames, err := f.Readdirnames(-1)
    if err != nil {
        return nil, err
    }
    sort.Strings(allNames)
    segments := make([]uint64, 0)
    for _, name := range allNames {
        if !reLogFile.MatchString(name) {
            continue
        }
        n, err := strconv.ParseUint(name[0:20], 10, 64)
        if err != nil {
            panic(err)
        }
        segments = append(segments, n)
    }

    s := &Store{
        dir: dir,
        segments: segments,
    }
    return s, nil
}

func (s *Store) SegmentForOffset(offset uint64) (log.Segment, error) {
    if len(s.segments) == 0 {
        return nil, nil
    }
    for i := 0; i < len(s.segments)-1; i++ {
        if offset >= s.segments[i] && offset < s.segments[i+1] {
            return s.openSegment(s.segments[i])
        }
    }
    return s.LastSegment()
}

func (s *Store) LastSegment() (log.Segment, error) {
    if len(s.segments) == 0 {
        return nil, nil
    }
    golog.Print("debug 0003 ...")
    return s.openSegment(s.segments[len(s.segments)-1])
}

func (s *Store) AddSegment(startOffset uint64) (log.Segment, error) {
    if segment, err := CreateSegment(startOffset, s.dir+"/"); err != nil {
        return nil, err
    } else {
        s.segments = append(s.segments, startOffset)
        return segment, nil
    }
}

func (s *Store) openSegment(startOffset uint64) (log.Segment, error) {
    logFile := fmt.Sprintf("%s/%020d.log", s.dir, startOffset)
    golog.Print("debug 0004 ... ", logFile)
    return OpenSegment(logFile)
}
