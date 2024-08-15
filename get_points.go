package tsdb

import (
	"errors"
	"github.com/dgraph-io/badger/v4"
	"time"
)

var (
	ErrMeasurementRequired = errors.New("measurement is required")
	ErrStartRequired       = errors.New("start time is required")
	ErrEndRequired         = errors.New("end time is required")
)

type LabelFilter func(labels map[string]string) bool
type FieldFilter func(fields map[string]float64) bool
type getPointOptions struct {
	measurement string
	startTime   time.Time
	endTime     time.Time
	labelFilter LabelFilter
	fieldFilter FieldFilter
	reverse     bool
	limit       int
}

func (q getPointOptions) Validate() error {
	if q.measurement == "" {
		return ErrMeasurementRequired
	}
	if q.startTime.IsZero() {
		return ErrStartRequired
	}
	if q.endTime.IsZero() {
		return ErrEndRequired
	}
	return nil
}

func (db *DB) getPoints(query getPointOptions) ([]Point, error) {
	if err := query.Validate(); err != nil {
		return nil, err
	}
	var dataPoints []Point
	startKey, err := encodePointKey(query.measurement, query.startTime, nil)
	if err != nil {
		return nil, err
	}
	endKey, err := encodePointKey(query.measurement, query.endTime, nil)
	if err != nil {
		return nil, err
	}
	if query.reverse {
		startKey, endKey = endKey, startKey
	}
	err = db.storage.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			Reverse:        query.reverse,
			Prefix:         []byte(query.measurement),
		})
		defer it.Close()
		matchHashes := make(map[uint64]bool)
		for it.Seek(startKey); it.Valid(); it.Next() {
			if query.limit > 0 && len(dataPoints) < query.limit {
				break
			}
			item := it.Item()
			key := item.Key()
			if query.reverse && string(key) < string(endKey) {
				break
			}
			if !query.reverse && string(key) > string(endKey) {
				break
			}

			measurement, ts, hash, labels, err := decodePointKey(key)
			if err != nil {
				return err
			}
			if matchHashes[hash] && query.labelFilter != nil && !query.labelFilter(labels) {
				continue
			}
			if _, ok := matchHashes[hash]; !ok {
				matchHashes[hash] = true
			}
			var fields map[string]float64
			err = item.Value(func(val []byte) error {
				fields, err = decodePointFields(val)
				return err
			})
			if query.fieldFilter != nil && !query.fieldFilter(fields) {
				continue
			}

			dataPoints = append(dataPoints, Point{
				Measurement: measurement,
				Labels:      labels,
				Fields:      fields,
				Time:        ts,
			})

		}
		return nil
	})
	return dataPoints, err
}

func (db *DB) GetPoints(measurement string) *GetPointsOptionBuilder {
	return &GetPointsOptionBuilder{
		query: getPointOptions{
			measurement: measurement,
			endTime:     time.Now(),
		},
		db: db,
	}
}

type GetPointsOptionBuilder struct {
	query getPointOptions
	db    *DB
}

func (q *GetPointsOptionBuilder) Points() ([]Point, error) {
	return q.db.getPoints(q.query)
}

func (q *GetPointsOptionBuilder) From(startTime time.Time) *GetPointsOptionBuilder {
	q.query.startTime = startTime
	return q
}

func (q *GetPointsOptionBuilder) FromBefore(dur time.Duration) *GetPointsOptionBuilder {
	q.query.startTime = time.Now().Add(-dur)
	return q
}

func (q *GetPointsOptionBuilder) To(endTime time.Time) *GetPointsOptionBuilder {
	q.query.endTime = endTime
	return q
}

func (q *GetPointsOptionBuilder) Range(startTime, endTime time.Time) *GetPointsOptionBuilder {
	q.query.startTime = startTime
	q.query.endTime = endTime
	return q
}

func (q *GetPointsOptionBuilder) LabelFilter(filter LabelFilter) *GetPointsOptionBuilder {
	q.query.labelFilter = filter
	return q
}

func (q *GetPointsOptionBuilder) FieldFilter(filter FieldFilter) *GetPointsOptionBuilder {
	q.query.fieldFilter = filter
	return q
}

func (q *GetPointsOptionBuilder) Reverse() *GetPointsOptionBuilder {
	q.query.reverse = true
	return q
}

func (q *GetPointsOptionBuilder) Limit(limit int) *GetPointsOptionBuilder {
	if limit > 0 {
		q.query.limit = limit
	}
	return q
}
