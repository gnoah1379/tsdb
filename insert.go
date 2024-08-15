package tsdb

import "github.com/dgraph-io/badger/v4"

func insertTxn(tx *badger.Txn, point Point) error {
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
	return tx.Set(key, value)
}

func (db *DB) BatchInsert(points []Point) error {
	return db.storage.Update(func(txn *badger.Txn) error {
		for _, point := range points {
			err := insertTxn(txn, point)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *DB) Insert(point Point) error {
	return db.storage.Update(func(txn *badger.Txn) error {
		return insertTxn(txn, point)
	})

}
