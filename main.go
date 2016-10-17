package main

import (
	"crypto/rand"
	golog "log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/MikaelCluseau/webaka/pkg/log"
	"github.com/MikaelCluseau/webaka/pkg/log/stores/kafka"
)

var count int = 5e4

func main() {
	golog.Print("opening store...")
	store := kafka.Open("/tmp/test-log", 8<<10)

	golog.Print("opening log...")
	l, err := log.Open(log.Config{
		MaxSegmentSize: 100 << 20,
		MaxSyncLag:     -1,
	}, store)
	if err != nil {
		golog.Fatal(err)
	}

	golog.Print("log opened, next offset: ", l.NextOffset())
	if l.NextOffset() > uint64(count) {
		golog.Print("Topic has many messages, just consumming...")
		c, err := l.Consumer(1)
		if err != nil {
			golog.Fatal(err)
		}
		golog.Print("consuming...")
		lastOffset := l.NextOffset() - 1
		cnt := 0
		t0 := time.Now()
		for {
			o, _, err := c.Next()
			if err != nil {
				golog.Fatal(err)
			}
			cnt++
			if o == lastOffset {
				// finished
				break
			}
		}
		dur := time.Since(t0)
		golog.Print("reading ", cnt, " messages took ", dur)
		golog.Printf("-> %.2f msg/s", float64(cnt)/float64(dur.Seconds()))
		golog.Print("consume finished")
		return
	}

	consumeOk := make(chan bool, 1)
	go func() {
		consume(l)
		close(consumeOk)
	}()

	data := make([]byte, 4096)
	rand.Read(data)
	msg := log.NewMessage(log.Timestamp(time.Now()), nil, data)

	if true {
		f, err := os.Create("/tmp/cpu.2.prof")
		if err != nil {
			golog.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	golog.Printf("appending %d messages...", count)
	t0 := time.Now()
	for i := 0; i < count; i++ {
		if _, err := l.Append(msg); err != nil {
			golog.Fatal("failed to append message: ", err)
		}
	}
	golog.Print("time taken (pre-sync):  ", time.Since(t0))
	l.Sync()
	golog.Print("time taken (post-sync): ", time.Since(t0))

	<-consumeOk

	golog.Print("close")
	l.Close()

	golog.Print("done")
}

func consume(l *log.Log) {
	golog.Print("Consuming...")
	c, err := l.Consumer(1)
	if err != nil {
		golog.Fatal("failed to open consumer: ", err)
	}
	t0 := time.Now()
	for i := 1; i <= count; i++ {
		offset, msg, err := c.Next()
		if err != nil {
			golog.Fatal("consume ", i, " failed: ", err)
		}
		_ = offset
		_ = msg
		//golog.Printf("%d/%d -> offset: %d; len: %d", i, count, offset, msg.Len())
	}
	golog.Print("time taken (read):    ", time.Since(t0))
	c.Close()
}
