package main

import (
    golog "log"
    "time"
    "os"
    "crypto/rand"
    "runtime/pprof"

	"github.com/MikaelCluseau/cebaka/pkg/log"
	"github.com/MikaelCluseau/cebaka/pkg/log/stores/kafka"
)

func main() {
    golog.Print("opening store...")
    store, err := kafka.Open("/tmp/test-log")
    if err != nil {
        golog.Fatal(err)
    }
    golog.Print("opening log...")
	l, err := log.Open(log.Config{
		MaxSegmentSize: 100 << 20,
		MaxSyncLag:     -1,
	}, store)
    if err != nil {
        golog.Fatal(err)
    }

    golog.Print("log opened, next offset: ", l.NextOffset())

    data := make([]byte, 4096)
    rand.Read(data)
    msg := log.NewMessage(log.Timestamp(time.Now()), nil, data)

    if false {
        f, err := os.Create("/tmp/cpu.2.prof")
        if err != nil {
            golog.Fatal(err)
        }
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }

    golog.Print("append message...")
    t0 := time.Now()
    count := int(1e6)
    for i := 0; i < count; i++ {
        if _, err := l.Append(msg); err != nil {
            golog.Fatal(err)
        }
    }
    golog.Print("close")
    golog.Print("time taken (pre-close): ", time.Since(t0))
    l.Close()
    golog.Print("time taken (closed):    ", time.Since(t0))
    golog.Print("done")
}
