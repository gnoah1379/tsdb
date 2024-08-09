package bytebuffer

import (
	"bytes"
	"encoding/binary"
	"io"
)

type Buffer struct {
	*bytes.Buffer
}

func NewBuffer(buf []byte) *Buffer {
	return &Buffer{
		Buffer: bytes.NewBuffer(buf),
	}
}

func (b *Buffer) WriteVarUint(i uint64) {
	b.Write(binary.AppendUvarint(b.AvailableBuffer(), i))
}

func (b *Buffer) ReadVarUint() (uint64, error) {
	return binary.ReadUvarint(b)
}

func (b *Buffer) WriteVarInt(i int64) {
	b.Write(binary.AppendVarint(b.AvailableBuffer(), i))
}

func (b *Buffer) ReadVarInt() (int64, error) {
	return binary.ReadVarint(b)
}

func (b *Buffer) WriteInt64(x int64) {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	b.Write(binary.BigEndian.AppendUint64(b.AvailableBuffer(), ux))
}

func (b *Buffer) WriteInt32(x int32) {
	ux := uint32(x) << 1
	if x < 0 {
		ux = ^ux
	}
	b.Write(binary.BigEndian.AppendUint32(b.AvailableBuffer(), ux))

}

func (b *Buffer) ReadInt64() (int64, error) {
	if b.Len() < 8 {
		return 0, io.EOF
	}
	ux := binary.BigEndian.Uint64(b.Next(8))
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, nil
}

func (b *Buffer) ReadInt32() (int32, error) {
	if b.Len() < 4 {
		return 0, io.EOF
	}
	ux := binary.BigEndian.Uint32(b.Next(4))
	x := int32(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, nil
}
