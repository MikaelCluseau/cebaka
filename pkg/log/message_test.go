package log

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestByteOrder(t *testing.T) {
	b32 := [4]byte{}
	byteOrder.PutUint32(b32[:], 0x01020304)
	if !bytes.Equal([]byte{1, 2, 3, 4}, b32[:]) {
		t.Error("bad uint32 byte order: ", b32)
	}
    b64 := [8]byte{}
	byteOrder.PutUint64(b64[:], 0x0102030405060708)
	if !bytes.Equal([]byte{1, 2, 3, 4, 5, 6, 7, 8}, b64[:]) {
		t.Error("bad uint64 byte order: ", b64)
	}
}

func TestEmptyBytes(t *testing.T) {
	buf := &bytes.Buffer{}
	NewBinaryWriter(buf).WriteBytes(nil)
	assertDump(t, buf, `00000000  ff ff ff ff                                       |....|`+"\n")
}

func TestMessageWrite(t *testing.T) {
	key := []byte("key")
	data := []byte("data")

	m := NewMessage(1469067554, key, data)

	buf := &bytes.Buffer{}
	m.WriteTo(NewBinaryWriter(buf))

	if buf.Len() != int(m.Len()) {
		t.Error("bad length: ", m.Len(), " != ", buf.Len())
	}

	// Expected on-disk format:
	// - crc:         0xe0a1b5201        (computed)
	// - format:      0x01
	// - attributes:  0x00
	// - timestamp:   0x0000000057903122 = 1469067554 (padded to 64 bits)
	// - key length:  0x00000003         = len("key")
	// - key:         0x6b6579           = []byte("key")
	// - payload len: 0x00000004         = len("data")
	// - payload:     0x6b6579           = []byte("data")
	assertDump(t, buf, ""+
		`00000000  e0 a1 b5 22 01 00 00 00  00 00 57 90 31 22 00 00  |..."......W.1"..|`+"\n"+
		`00000010  00 03 6b 65 79 00 00 00  04 64 61 74 61           |..key....data|`+"\n")
}

func assertDump(t *testing.T, buf *bytes.Buffer, expectedDump string) {
	dump := hex.Dump(buf.Bytes())
	if dump != expectedDump {
		t.Error("unexpected dump")
		t.Log(dump)
		t.Log(expectedDump)
	}
}

func TestWriteRead(t *testing.T) {
	key := []byte("key")
	data := []byte("data")

	m := NewMessage(1469067554, key, data)

	buf := &bytes.Buffer{}
	m.WriteTo(NewBinaryWriter(buf))

	m2 := &Message{}
	m2.ReadFrom(buf)

	if buf.Len() > 0 {
		t.Error("Remaining bytes")
	}

	if !bytes.Equal(m.Key, m2.Key) {
		t.Errorf("key differs: %q != %q", string(m.Key), string(m2.Key))
	}
	if !bytes.Equal(m.Payload, m2.Payload) {
		t.Errorf("payload differs: %q != %q", string(m.Payload), string(m2.Payload))
	}
}
