package main

import (
    "log"
    "flag"

    "github.com/ceph/go-ceph/rados"
)

var (
    cluster, user string
    pool string
    slot int32
)

func init() {
    flag.StringVar(&cluster, "cluster", "ceph", "The ceph cluster to use.")
    flag.StringVar(&user, "user", "cebaka", "The ceph user to use.")
    flag.StringVar(&pool, "pool", "cebaka", "The ceph pool to use.")
}

func main() {
    flag.Parse()

    log.Print("Preparing Ceph connection to cluster ", cluster, " as ", user)
    conn, err := rados.NewConnWithClusterAndUser(cluster, "client." + user)
    if err != nil {
        log.Fatal(err)
    }
    defer func() {
        log.Print("Closing Ceph connection")
        conn.Shutdown()
        log.Print("Shutdown complete.")
    }()

    log.Print("Loading Ceph configuration")
    if err = conn.ReadDefaultConfigFile(); err != nil {
        log.Fatal(err)
    }
    log.Print("Connecting to Ceph")
    if err = conn.Connect(); err != nil {
        log.Fatal(err)
    }

    ctx, err := conn.OpenIOContext(pool)
    if err != nil {
        log.Fatal(err)
    }
    defer ctx.Destroy()
    log.Print("Connected to Ceph.")

    if err := ctx.SetOmap("test", map[string][]byte{
        "test": []byte("poeutpzou"),
    }); err != nil {
        log.Fatal(err)
    }

    pw := NewPartitionWriter("test-topic", 0, 4<<20, ctx)
    pw.Write([]byte("hello!"))

    log.Print("Finished")
}
