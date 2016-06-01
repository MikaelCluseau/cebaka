package main

// #cgo LDFLAGS: -lrados
// #include <errno.h>
// #include <stdlib.h>
// #include <time.h>
// #include <rados/librados.h>
import "C"

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"unsafe"

	. "github.com/ceph/go-ceph/rados"
	//"github.com/Shopify/sarama"
)

type PartitionWriter struct {
	baseOid    string
    chunkSize  uint64
	ioctx      *IOContext
	raw_ioctx  C.rados_ioctx_t
	lastId     uint64
	tailOffset uint64
	lock       sync.Mutex
}

func NewPartitionWriter(topic string, partition uint32, chunkSize uint64, ioctx *IOContext) *PartitionWriter {
	return &PartitionWriter{
		baseOid:    fmt.Sprintf("%s_%d", topic, partition),
        chunkSize:  chunkSize,
		ioctx:      ioctx,
		raw_ioctx:  C.rados_ioctx_t(ioctx.Pointer()),
		lastId:     0,
		tailOffset: 0,
		lock:       sync.Mutex{},
	}
}

func (pw *PartitionWriter) Write(message []byte) (offset, id uint64) {
    msgLen := uint64(len(message))
	offset, id = pw.registerNewMessage(msgLen)

    chunckId, offsetInChunk := offset / pw.chunkSize, offset % pw.chunkSize

    oid := fmt.Sprintf("%s.%016x", pw.baseOid, chunckId)
    pw.ioctx.Write(oid, uint64Bytes(msgLen), offsetInChunk)
    pw.ioctx.Write(oid, message, offsetInChunk + 8)
	return
}

func (pw *PartitionWriter) registerNewMessage(messageLength uint64) (offset, id uint64) {
    log.Print("registerNewMessage: baseOid=", pw.baseOid)
	pw.lock.Lock()
    defer pw.lock.Unlock()
	for {
		// Atomic compare-and-swap+write
		newLastId := pw.lastId + 1
        newTailOffset := pw.tailOffset+8+messageLength

		op := C.rados_create_write_op()
		if pw.lastId == 0 {
			// Assume we have to create the partition object
			C.rados_write_op_create(op, C.LIBRADOS_CREATE_EXCLUSIVE, nil)
		} else {
			// Otherwise, assume we're last who changed the object
			writeOpCmpXAttr(op, "lastId", uint64Bytes(pw.lastId))
			//writeOpCmpXAttr(op, "tailOffset", uint64Bytes(pw.tailOffset))
		}
		C.rados_write_op_append(op, bytesCcharp(uint64Bytes(pw.tailOffset)), 8)
		C.rados_write_op_setxattr(op, C.CString("chunkSize"), bytesCcharp(uint64Bytes(pw.chunkSize)), 8)
		C.rados_write_op_setxattr(op, C.CString("lastId"), bytesCcharp(uint64Bytes(newLastId)), 8)
		C.rados_write_op_setxattr(op, C.CString("tailOffset"), bytesCcharp(uint64Bytes(newTailOffset)), 8)
		r := C.rados_write_op_operate(op, pw.raw_ioctx, C.CString(pw.baseOid), nil, C.LIBRADOS_OPERATION_NOFLAG)
		C.rados_release_write_op(op)

		if r >= 0 {
			// OK
            messageOffset := pw.tailOffset

			pw.lastId = newLastId
			pw.tailOffset = newTailOffset

            return newLastId, messageOffset
		}
		// TODO more precise handling
		log.Print(r, ": ", GetRadosError(int(r)))
		// Assumption wrong, update state from storage
		pw.updateFromStorage()
		log.Print("changed externaly: lastId=", pw.lastId, " tailOffset=", pw.tailOffset)
	}
}

func (pw *PartitionWriter) updateFromStorage() {
    // TODO: read op (or maybe a single xattr)
	lastId, err := readUint64XAttr(pw.ioctx, pw.baseOid, "lastId")
	if err != nil {
		log.Fatal("failed to read xattr lastId: ", err)
	}
	tailOffset, err := readUint64XAttr(pw.ioctx, pw.baseOid, "tailOffset")
	if err != nil {
		log.Fatal("failed to read xattr tailOffset: ", err)
	}
	chunkSize, err := readUint64XAttr(pw.ioctx, pw.baseOid, "chunkSize")
	if err != nil {
		log.Fatal("failed to read xattr chunkSize: ", err)
	}
	pw.lastId = lastId
	pw.tailOffset = tailOffset
    pw.chunkSize = chunkSize
}

func writeOpCmpXAttr(op C.rados_write_op_t, name string, value []byte) {
	C.rados_write_op_cmpxattr(op, C.CString(name), C.LIBRADOS_CMPXATTR_OP_EQ,
		(*C.char)(unsafe.Pointer(&value[0])), C.size_t(len(value)))
}

func uint64Bytes(x uint64) []byte {
	b := make([]byte, 8)
	binary.PutUvarint(b, x)
	return b
}

func readUint64XAttr(ioctx *IOContext, oid, name string) (uint64, error) {
	data := make([]byte, 8)
	_, err := ioctx.GetXattr(oid, name, data)
	if err != nil {
		log.Print("failed to read ", oid, " xattr ", name, ": ", err)
		return 0, err
	}
	r, _ := binary.Uvarint(data)
	return r, nil
}

func bytesCcharp(b []byte) *C.char {
	return (*C.char)(unsafe.Pointer(&b[0]))
}
