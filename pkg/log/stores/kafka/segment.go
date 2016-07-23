package kafka

import (
    "github.com/MikaelCluseau/cebaka/pkg/log"
)

type Segment struct {
    logFile,
    indexFile string
}

var _ = log.Segment(&Segment{})


