package tsdb

import (
	"cmp"
	"errors"
	"hash/fnv"
	"io"
	"slices"
	"time"
	"tsdb/internal/bytebuffer"
	"tsdb/internal/isync"
	"tsdb/internal/zerocast"
)

var hashPool = isync.ResetAblePool(fnv.New64)

type KV struct {
	Key   string
	Value string
}

const pointKeySep = '.'
const fieldSep = '='
const labelSep = ','
const kvSep = '='
const pointsPrefix = "points"

var (
	kvSepBytes   = []byte{kvSep}
	labelSepByte = []byte{labelSep}
)

// encodePointKey encodes a point key: <measurement>.points.<timestamp>.<hash>.<labels: k=v,k=v>
func encodePointKey(measurement string, ts time.Time, labels map[string]string) ([]byte, error) {
	buf := bytebuffer.Get()
	defer bytebuffer.Put(buf)
	buf.WriteString(measurement)
	buf.WriteByte(pointKeySep)
	buf.WriteString(pointsPrefix)
	buf.WriteByte(pointKeySep)
	buf.WriteInt64(ts.Unix())
	buf.WriteInt32(int32(ts.Nanosecond()))
	buf.WriteByte(pointKeySep)
	if labels == nil {
		sortedLabels := sortLabels(labels)
		labelsHash := hashLabel(sortedLabels)
		buf.WriteUint64(labelsHash)
		buf.WriteByte(pointKeySep)
		writeLabels(buf, sortedLabels)
	}
	return buf.ReadAll()
}

func decodePointKey(data []byte) (measurement string, ts time.Time, hash uint64, labels map[string]string, err error) {
	buf := bytebuffer.Read(data)
	defer bytebuffer.Put(buf)
	if measurement, err = buf.ReadString(pointKeySep); err != nil {
		return
	}
	if _, err = buf.ReadString(pointKeySep); err != nil {
		return
	}
	var sec int64
	var nsec int32
	if sec, err = buf.ReadInt64(); err != nil {
		return
	}
	if nsec, err = buf.ReadInt32(); err != nil {
		return
	}
	ts = time.Unix(sec, int64(nsec))
	if _, err = buf.ReadByte(); err != nil {
		return
	}
	if buf.Len() > 8 {
		hash, err = buf.ReadUint64()
		if err != nil {
			return
		}
		if _, err = buf.ReadByte(); err != nil {
			return
		}
		labels = make(map[string]string)
		var k, v string
		for {
			k, err = buf.ReadString(kvSep)
			if len(k) == 0 || errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return
			}
			v, err = buf.ReadString(labelSep)
			if err != nil {
				return
			}
			labels[k] = v
		}
	}

	return
}

func encodePointFields(fields map[string]float64) ([]byte, error) {
	buf := bytebuffer.Get()
	defer bytebuffer.Put(buf)
	for k, v := range fields {
		buf.WriteString(k)
		buf.WriteByte(kvSep)
		buf.WriteFloat64(v)
	}
	return buf.ReadAll()
}

func decodePointFields(data []byte) (map[string]float64, error) {
	buf := bytebuffer.Read(data)
	defer bytebuffer.Put(buf)
	var fields = make(map[string]float64)
	for {
		k, err := buf.ReadBytes(fieldSep)
		if len(k) == 0 || errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		v, err := buf.ReadFloat64()
		if err != nil {
			return nil, err
		}
		fields[string(k[:len(k)-1])] = v
	}
	return fields, nil
}

func sortLabels(labels map[string]string) []KV {
	kvs := make([]KV, 0, len(labels))
	for k, v := range labels {
		kvs = append(kvs, KV{k, v})
	}
	slices.SortFunc(kvs, func(a, b KV) int {
		return cmp.Compare(a.Key, b.Key)
	})
	return kvs
}

func writeLabels(w io.Writer, sortedLabels []KV) {
	for _, kv := range sortedLabels {
		_, _ = w.Write(zerocast.StringToBytes(kv.Key))
		_, _ = w.Write(kvSepBytes)
		_, _ = w.Write(zerocast.StringToBytes(kv.Value))
		_, _ = w.Write(labelSepByte)
	}
	return
}

func hashLabel(sortedLabels []KV) uint64 {
	h := hashPool.Get()
	defer hashPool.Put(h)
	writeLabels(h, sortedLabels)
	return h.Sum64()
}
