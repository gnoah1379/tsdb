package tsdb

import (
	"crypto/sha1"
	"errors"
	"github.com/vmihailenco/msgpack/v5"
	"hash"
	"time"
	"tsdb/internal/bytebuffer"
	"tsdb/internal/isync"
)

var ErrInvalidKey = errors.New("invalid key")
var (
	encPool = isync.Pool[*fieldEncoder]{
		New: func() *fieldEncoder {
			buf := bytebuffer.Get()
			return &fieldEncoder{
				Encoder: msgpack.NewEncoder(buf),
				buf:     buf,
			}
		},
		Reset: func(enc *fieldEncoder) {
			enc.buf.Reset()
			enc.Reset(enc.buf)
		},
	}
	decPool = isync.Pool[*fieldDecoder]{
		New: func() *fieldDecoder {
			buf := bytebuffer.Get()
			return &fieldDecoder{
				Decoder: msgpack.NewDecoder(buf),
				buf:     buf,
			}
		},
		Reset: func(dec *fieldDecoder) {
			dec.buf.Reset()
			dec.Reset(dec.buf)
		},
	}
	sha1Pool = isync.Pool[hash.Hash]{
		New: func() hash.Hash {
			return sha1.New()
		},
		Reset: func(h hash.Hash) {
			h.Reset()
		},
	}
)

type fieldEncoder struct {
	*msgpack.Encoder
	buf *bytebuffer.Buffer
}

type fieldDecoder struct {
	*msgpack.Decoder
	buf *bytebuffer.Buffer
}

type labelSetMetadata struct {
	Hash    []byte
	Counter uint64
}

const (
	storageKeySep             = '.'
	storageDataPointKeyPrefix = "points"
	storageLabelKeyPrefix     = "labels"
)

func packFields(fields map[string]any) ([]byte, error) {
	enc := encPool.Get()
	defer encPool.Put(enc)
	err := enc.EncodeMap(fields)
	if err != nil {
		return nil, err
	}
	return enc.buf.ReadAll()
}

func unpackFields(fieldsRaw []byte) (fields map[string]any, err error) {
	dec := decPool.Get()
	defer decPool.Put(dec)
	dec.buf.Write(fieldsRaw)
	fields, err = dec.DecodeMap()
	if err != nil {
		return nil, err
	}
	return fields, nil
}

func hashLabels(labels []byte) ([]byte, error) {
	h := sha1Pool.Get()
	defer sha1Pool.Put(h)
	h.Write(labels)
	return h.Sum(nil), nil
}

func packLabelSetMetadata(meta labelSetMetadata) ([]byte, error) {
	enc := encPool.Get()
	defer encPool.Put(enc)
	err := enc.Encode(meta)
	if err != nil {
		return nil, err
	}
	return enc.buf.ReadAll()
}

func unpackLabelSetMetadata(data []byte) (meta labelSetMetadata, err error) {
	dec := decPool.Get()
	defer decPool.Put(dec)
	dec.buf.Write(data)
	err = dec.Decode(&meta)
	return meta, err
}

func packLabelSetKey(measurement string, labelsPacked []byte) ([]byte, error) {
	buf := bytebuffer.Get()
	defer bytebuffer.Put(buf)
	buf.WriteString(measurement)
	buf.WriteByte(storageKeySep)
	buf.WriteString(storageLabelKeyPrefix)
	buf.WriteByte(storageKeySep)
	buf.Write(labelsPacked)
	return buf.ReadAll()
}

func unpackLabelKey(storageKey []byte) (measurement string, labels map[string]string, err error) {
	buf := bytebuffer.Get()
	defer bytebuffer.Put(buf)
	buf.Write(storageKey)
	measurementBytes, err := buf.ReadBytes(storageKeySep)
	if err != nil {
		return
	}
	measurement = string(measurementBytes[:len(measurementBytes)-1])
	buf.Next(len(storageLabelKeyPrefix))
	_, err = buf.ReadByte()
	if err != nil {
		return
	}
	labels, err = unpackLabels(buf.Bytes())
	if err != nil {
		return
	}
	return measurement, labels, nil
}

func packLabelSet(labels map[string]string) ([]byte, error) {
	enc := encPool.Get()
	defer encPool.Put(enc)
	enc.SetSortMapKeys(true)
	err := enc.Encode(labels)
	if err != nil {
		return nil, err
	}
	return enc.buf.ReadAll()
}

func unpackLabels(labelsRaw []byte) (labels map[string]string, err error) {
	dec := decPool.Get()
	defer decPool.Put(dec)
	dec.buf.Write(labelsRaw)
	err = dec.Decode(&labels)
	if err != nil {
		return nil, err
	}
	return labels, nil
}

func packFieldsKey(measurement string, labelHash []byte, t time.Time) ([]byte, error) {
	buf := bytebuffer.Get()
	defer bytebuffer.Put(buf)
	buf.WriteString(measurement)
	buf.WriteByte(storageKeySep)
	buf.WriteString(storageDataPointKeyPrefix)
	buf.WriteByte(storageKeySep)
	buf.Write(labelHash)
	buf.WriteByte(storageKeySep)
	buf.WriteInt64(t.Unix())
	buf.WriteInt32(int32(t.Nanosecond()))
	return buf.ReadAll()
}

func unpackFieldsKey(storageKey []byte) (measurement string, labelHash []byte, t time.Time, err error) {
	buf := bytebuffer.Get()
	defer bytebuffer.Put(buf)
	buf.Write(storageKey)
	measurementBytes, err := buf.ReadBytes(storageKeySep)
	if err != nil {
		return
	}
	measurement = string(measurementBytes[:len(measurementBytes)-1])
	buf.Next(len(storageDataPointKeyPrefix))
	_, err = buf.ReadByte()
	if err != nil {
		return
	}
	labelHash, err = buf.ReadBytes(storageKeySep)
	if err != nil {
		return
	}
	sec, err := buf.ReadInt64()
	if err != nil {
		return
	}
	nsec, err := buf.ReadInt32()
	if err != nil {
		return
	}
	t = time.Unix(sec, int64(nsec))
	_, err = buf.ReadByte()
	if err != nil {
		return
	}
	return measurement, labelHash, t, nil
}
