package tsdb

import (
	"bytes"
	"errors"
	"github.com/vmihailenco/msgpack/v5"
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
)

type fieldEncoder struct {
	*msgpack.Encoder
	buf *bytebuffer.Buffer
}

type fieldDecoder struct {
	*msgpack.Decoder
	buf *bytebuffer.Buffer
}

const (
	KVSep  = '='
	KeySep = ','
)

func packPoint(point Point) (key []byte, value []byte) {
	key = packKey(point.Measurement, point.Time, point.Labels)
	value = packFields(point.Fields)
	return key, value
}

func packFields(fields map[string]any) []byte {
	enc := encPool.Get()
	defer encPool.Put(enc)
	err := enc.EncodeMap(fields)
	if err != nil {
		return nil
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

func packKey(measurement string, t time.Time, labels map[string]string) []byte {
	buf := bytebuffer.Get()
	defer bytebuffer.Put(buf)
	packKeyPrefix(measurement, t)
	for key, val := range labels {
		buf.WriteString(key)
		buf.WriteByte(KVSep)
		buf.WriteString(val)
		buf.WriteByte(KeySep)
	}
	return buf.ReadAll()
}

func packLabels(labels map[string]string) []byte {
	buf := bytebuffer.Get()
	defer bytebuffer.Put(buf)
	for key, val := range labels {
		buf.WriteString(key)
		buf.WriteByte(KVSep)
		buf.WriteString(val)
		buf.WriteByte(KeySep)
	}
	return buf.ReadAll()

}

func packKeyPrefix(measurement string, t time.Time) []byte {
	buf := bytebuffer.Get()
	defer bytebuffer.Put(buf)
	buf.WriteString(measurement)
	buf.WriteByte(KeySep)
	buf.WriteInt64(t.Unix())
	buf.WriteInt32(int32(t.Nanosecond()))
	buf.WriteByte(KeySep)
	return buf.ReadAll()
}

func unpackKey(storageKey []byte) (measurement string, t time.Time, labels map[string]string, err error) {
	buf := bytebuffer.Get()
	defer bytebuffer.Put(buf)
	buf.Write(storageKey)
	measurementBytes, err := buf.ReadBytes(KeySep)
	if err != nil {
		return "", time.Time{}, nil, err
	}
	measurement = string(measurementBytes[:len(measurementBytes)-1])
	sec, err := buf.ReadInt64()
	if err != nil {
		return "", time.Time{}, nil, err
	}
	nsec, err := buf.ReadInt32()
	if err != nil {
		return "", time.Time{}, nil, err
	}
	t = time.Unix(sec, int64(nsec))
	buf.Next(1)
	if buf.Len() == 0 {
		return measurement, t, labels, nil
	}
	kvs := bytes.Split(bytes.TrimSuffix(buf.Bytes(), []byte{KeySep}), []byte{KeySep})
	for _, kv := range kvs {
		parts := bytes.Split(kv, []byte{KVSep})
		if len(parts) != 2 {
			return "", time.Time{}, nil, ErrInvalidKey
		}
		if labels == nil {
			labels = make(map[string]string)
		}
		labels[string(parts[0])] = string(parts[1])
	}
	return measurement, t, labels, nil
}
