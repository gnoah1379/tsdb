package tsdb

import (
	"errors"
	"github.com/dgraph-io/badger/v4"
)

type entrySetter interface {
	SetEntry(e *badger.Entry) error
}

func (db *DB) insert(txn entrySetter, p Point) error {
	key, val := packPoint(p)
	entry := badger.NewEntry(key, val).WithTTL(db.retention)
	return txn.SetEntry(entry)
}

func (db *DB) InsertOne(p Point) error {
	return db.storage.Update(func(txn *badger.Txn) error {
		return db.insert(txn, p)
	})
}

func (db *DB) Insert(points []Point) error {
	var errs []error
	wb := db.storage.NewWriteBatch()
	for _, point := range points {
		err := db.insert(wb, point)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		wb.Cancel()
	} else {
		err := wb.Flush()
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
