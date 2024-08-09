package tsdb

import (
	"bytes"
	"errors"
	"io"
	"math"
	"time"
	"tsdb/internal/bytebuffer"
	"tsdb/internal/isync"
)

var ErrInvalidKey = errors.New("invalid key")
var packerPool = isync.Pool[*Packer]{
	New: func() *Packer {
		return &Packer{buf: bytebuffer.NewBuffer(nil)}
	},
	Reset: func(p *Packer) {
		p.buf.Reset()
	},
}

const (
	KVSep  = '='
	KeySep = ','
)

type Packer struct {
	buf *bytebuffer.Buffer
}

func (p *Packer) packPoint(point Point) (key []byte, value []byte) {
	key = p.packKey(point.Measurement, point.Time, point.Labels)
	value = p.packFields(point.Fields)
	return key, value
}

func (p *Packer) packFields(fields map[string]float64) []byte {
	p.buf.Reset()
	for key, val := range fields {
		p.buf.WriteString(key)
		p.buf.WriteByte(KVSep)
		bits := math.Float64bits(val)
		p.buf.WriteVarUint(bits)
	}
	return bytes.Clone(p.buf.Bytes())
}

func (p *Packer) unpackFields(fieldsRaw []byte) (fields map[string]float64, err error) {
	defer func() {
		if err == io.EOF {
			err = nil
		}
	}()
	fields = make(map[string]float64)
	p.buf.Reset()
	p.buf.Write(fieldsRaw)
	for {
		keyBytes, err := p.buf.ReadBytes(KVSep)
		if err != nil {
			return fields, err
		}
		key := string(keyBytes[:len(keyBytes)-1])
		bits, err := p.buf.ReadVarUint()
		if err != nil {
			return fields, err
		}
		fields[key] = math.Float64frombits(bits)
	}
}

func (p *Packer) packKey(measurement string, t time.Time, labels map[string]string) []byte {
	p.packKeyPrefix(measurement, t)
	for key, val := range labels {
		p.buf.WriteString(key)
		p.buf.WriteByte(KVSep)
		p.buf.WriteString(val)
		p.buf.WriteByte(KeySep)
	}
	return bytes.Clone(p.buf.Bytes())
}

func (p *Packer) packKeyPrefix(measurement string, t time.Time) []byte {
	p.buf.Reset()
	p.buf.WriteString(measurement)
	p.buf.WriteByte(KeySep)
	p.buf.WriteInt64(t.Unix())
	p.buf.WriteInt32(int32(t.Nanosecond()))
	p.buf.WriteByte(KeySep)
	return bytes.Clone(p.buf.Bytes())
}

func (p *Packer) unpackKey(storageKey []byte) (measurement string, t time.Time, labels map[string]string, err error) {
	p.buf.Reset()
	p.buf.Write(storageKey)
	measurementBytes, err := p.buf.ReadBytes(KeySep)
	if err != nil {
		return "", time.Time{}, nil, err
	}
	measurement = string(measurementBytes[:len(measurementBytes)-1])
	sec, err := p.buf.ReadInt64()
	if err != nil {
		return "", time.Time{}, nil, err
	}
	nsec, err := p.buf.ReadInt32()
	if err != nil {
		return "", time.Time{}, nil, err
	}
	t = time.Unix(sec, int64(nsec))
	_, err = p.buf.ReadByte()
	if err != nil {
		return "", time.Time{}, nil, ErrInvalidKey
	}
	labels = make(map[string]string)
	if p.buf.Len() == 0 {
		return measurement, t, labels, nil
	}

	kvs := bytes.Split(bytes.TrimSuffix(p.buf.Bytes(), []byte{KeySep}), []byte{KeySep})
	for _, kv := range kvs {
		parts := bytes.Split(kv, []byte{KVSep})
		if len(parts) != 2 {
			return "", time.Time{}, nil, ErrInvalidKey
		}
		labels[string(parts[0])] = string(parts[1])
	}
	return measurement, t, labels, nil
}
