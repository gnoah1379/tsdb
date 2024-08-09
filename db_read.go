package tsdb

import (
	"github.com/dgraph-io/badger/v4"
	"time"
	"tsdb/exprs"
)

func (db *DB) SelectMeasurement(measurement string, start, end time.Time, reverse bool, filters ...exprs.LabelExpr) ([]Point, error) {
	var points []Point
	var packer = packerPool.Get()
	defer packerPool.Put(packer)
	err := db.storage.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			Reverse: reverse,
			Prefix:  []byte(measurement),
		})
		defer it.Close()
		startPrefix := packer.packKeyPrefix(measurement, start)
		endPrefix := packer.packKeyPrefix(measurement, end)
		if reverse {
			startPrefix, endPrefix = endPrefix, startPrefix
		}

		for it.Seek(startPrefix); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			_, timePoint, labels, err := packer.unpackKey(key)
			if err != nil {
				return err
			}
			if (reverse && timePoint.Before(start)) || (!reverse && timePoint.After(end)) {
				break
			}

			if !exprs.MatchLabels(filters, labels) {
				continue
			}

			point := Point{
				Measurement: measurement,
				Time:        timePoint,
				Labels:      labels,
			}
			err = item.Value(func(val []byte) error {
				var err error
				point.Fields, err = packer.unpackFields(val)
				return err
			})
			if err != nil {
				return err
			}
			points = append(points, point)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return points, nil

}
