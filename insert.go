package tsdb

import "github.com/dgraph-io/badger/v4"

type storageWriter interface {
	SetEntry(e *badger.Entry) error
	Set(k, v []byte) error
	Delete(k []byte) error
}

func insertPoint(w storageWriter, point Point) error {
	err := point.Validate()
	if err != nil {
		return err
	}
	key, err := encodePointKey(point.Measurement, point.Time, point.Labels)
	if err != nil {
		return err
	}
	value, err := encodePointFields(point.Fields)
	if err != nil {
		return err
	}
	return w.Set(key, value)
}

func (db *DB) Insert(points []Point) error {
	wb := db.storage.NewWriteBatch()
	defer wb.Cancel()
	for _, point := range points {
		err := insertPoint(wb, point)
		if err != nil {
			return err
		}
	}
	return wb.Flush()
}
