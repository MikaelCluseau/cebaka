package kafka

import (
    "fmt"
    "io"
    "os"
    golog "log"

    "github.com/golang/glog"
    "github.com/MikaelCluseau/cebaka/pkg/log"
)

type Segment struct {
    log *os.File
    //index *os.File

    lastOffset uint64

    writer *log.Writer
}

var _ = log.Segment(&Segment{})

func CreateSegment(startOffset uint64, fileNamePrefix string) (log.Segment, error) {
    fullFileNamePrefix := fmt.Sprintf("%s%020d", fileNamePrefix, startOffset)
    logFile, err := os.Create(fullFileNamePrefix+".log")
    if err != nil {
        return nil, err
    }
    //index, err := os.Create(fullFileNamePrefix+".index")
    //if err != nil {
    //    glog.Error("Failed to create ", fullFileNamePrefix, ".index: ", err)
    //    return err
    //}
    s := &Segment{
        log: logFile,
        //index: index,
        lastOffset: startOffset,
        writer: log.NewWriter(logFile, 0),
    }
    return s, nil
}

func OpenSegment(logFileName string) (log.Segment, error) {
    logFile, err := os.OpenFile(logFileName, os.O_RDWR, 0666)
    if err != nil {
        return nil, err
    }
    s := &Segment{
        log: logFile,
    }
    // Move after the last (valid) message
    r := log.NewReader(logFile, 0)
    readLoop: for {
        offset, err := r.FastRead()
        golog.Print("offset: ", offset, " / err: ", err, " / pos: ", r.Position())
        switch err {
        case nil:
            // ok
        case io.EOF:
            break readLoop
        case log.BadCRC, log.UnexpectedEOF:
            if err := s.lostTail(r.Position()); err != nil {
                return nil, err
            }
            break readLoop
        default:
            return nil, err
        }
        s.lastOffset = offset
    }
    s.writer = log.NewWriter(logFile, r.Position())
    return s, nil
}

func (s *Segment) LastOffset() uint64 {
    return s.lastOffset
}

func (s *Segment) Append(offset uint64, message *log.Message) error {
    if err := s.writer.Append(offset, message); err != nil {
        return err
    }
    s.lastOffset = offset
    return nil
}

func (s *Segment) Close() {
    if s.writer != nil {
        s.writer = nil
    }
    if s.log != nil {
        if err := s.log.Close(); err != nil {
            glog.Warning("Failed to close ", s.log.Name(), ": ", err)
        }
        s.log = nil
    }
    //if s.index != nil {
    //    if err := s.index.Close(); err != nil {
    //        glog.Warning("Failed to close ", s.indexFile, ": ", err)
    //    }
    //    s.index = nil
    //}
}

func (s *Segment) Reader() (*log.Reader, error) {
    // TODO will not need 1 fd per reader if we use ReadAt instead
    f, err := os.Open(s.log.Name())
    if err != nil {
        return nil, err
    }
    return log.NewReader(f, 0), nil
}

func (s *Segment) Size() int64 {
    return s.writer.Position()
}

func (s *Segment) Sync() error {
    return s.writer.Sync()
}

func (s *Segment) lostTail(position int64) error {
    // TODO archive tail
    if err := s.log.Truncate(position); err != nil {
        return err
    }
    s.log.Seek(position, 0)
    return nil
}
