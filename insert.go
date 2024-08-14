package tsdb

import (
	"context"
	"errors"
	"github.com/dgraph-io/badger/v4"
)

func (db *DB) Insert(ctx context.Context, points []Point) error {
	locked := make(map[string]struct{})
	defer func() {
		for k := range locked {
			db.metaLocker.Unlock(k)
		}
	}()
	lMetadata := make(map[string]*labelSetMetadata)
	fields := make([][2][]byte, len(points))
	for _, p := range points {
		labelSet, err := packLabelSet(p.Labels)
		if err != nil {
			return err
		}
		labelHash, err := hashLabels(labelSet)
		if err != nil {
			return err
		}
		labelKey, err := packLabelSetKey(p.Measurement, labelSet)
		if err != nil {
			return err
		}
		labelKeyStr := string(labelKey)

		if meta, ok := lMetadata[labelKeyStr]; !ok {
			meta = &labelSetMetadata{
				Hash:    labelHash,
				Counter: 1,
			}
			lMetadata[labelKeyStr] = meta
		} else {
			meta.Counter++
		}
		fieldsKey, err := packFieldsKey(p.Measurement, labelHash, p.Time)
		if err != nil {
			return err
		}
		fieldsValue, err := packFields(p.Fields)
		if err != nil {
			return err
		}
		fields = append(fields, [2][]byte{fieldsKey, fieldsValue})
	}
	writeLabels := make([][2][]byte, 0, len(lMetadata))
	for labelKey, meta := range lMetadata {
		if !db.metaLocker.Lock(ctx, labelKey) {
			return errors.New("deadline exceeded")
		}
		locked[labelKey] = struct{}{}
		metaRaw, err := packLabelSetMetadata(*meta)
		if err != nil {
			return err
		}
		writeLabels = append(writeLabels, [2][]byte{[]byte(labelKey), metaRaw})
	}
	return db.storage.Update(func(txn *badger.Txn) error {
		for _, m := range writeLabels {
			err := txn.Set(m[0], m[1])
			if err != nil {
				return err
			}
		}
		for _, f := range fields {
			err := txn.Set(f[0], f[1])
			if err != nil {
				return err
			}
		}
		return nil
	})
}
