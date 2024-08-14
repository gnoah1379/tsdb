package tsdb

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"time"
)

var (
	ErrMeasurementRequired = errors.New("measurement is required")
	ErrStartRequired       = errors.New("start time is required")
	ErrEndRequired         = errors.New("end time is required")
	DataError              = errors.New("data error")
)

type QueryDataPoints struct {
	Measurement string
	StartTime   time.Time
	EndTime     time.Time
	Timeframe   time.Duration
	LabelFilter LabelFilter
	FieldFilter FieldFilter
	Reverse     bool
	Limit       int
}

func (q QueryDataPoints) Validate() error {
	if q.Measurement == "" {
		return ErrMeasurementRequired
	}
	if q.StartTime.IsZero() {
		return ErrStartRequired
	}
	if q.EndTime.IsZero() {
		return ErrEndRequired
	}
	return nil
}

type LabelFilter func(labels map[string]string) bool
type FieldFilter func(fields map[string]any) bool

func (db *DB) FromMeasurement(measurement string) *QueryDataPointsBuilder {
	return &QueryDataPointsBuilder{
		query: QueryDataPoints{
			Measurement: measurement,
		},
		db: db,
	}

}
func (db *DB) QueryDataPoints(query QueryDataPoints) ([]DataPoint, error) {
	var points []DataPoint
	pointLen := 0
	if err := query.Validate(); err != nil {
		return nil, err
	}
	err := db.storage.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			Reverse: query.Reverse,
			Prefix:  []byte(query.Measurement),
		})
		defer it.Close()
		startPrefix := packKeyPrefix(query.Measurement, query.StartTime)
		endPrefix := packKeyPrefix(query.Measurement, query.EndTime)
		if query.Reverse {
			startPrefix, endPrefix = endPrefix, startPrefix
		}

		for it.Seek(startPrefix); it.Valid(); it.Next() {
			if query.Limit > 0 && pointLen >= query.Limit {
				break
			}
			item := it.Item()
			key := item.Key()
			_, timestamp, labels, err := unpackKey(key)
			if err != nil {
				return errors.Join(DataError, fmt.Errorf("error unpacking key: %w", err))
			}
			if (query.Reverse && timestamp.Before(query.EndTime)) || (!query.Reverse && timestamp.After(query.EndTime)) {
				break
			}

			if query.LabelFilter != nil && !query.LabelFilter(labels) {
				continue
			}

			dataPoint := DataPoint{
				Time: timestamp,
			}
			err = item.Value(func(val []byte) error {
				var err error
				dataPoint.Fields, err = unpackFields(val)
				return err
			})
			if err != nil {
				return errors.Join(DataError, fmt.Errorf("error unpacking fields: %w", err))
			}
			if query.FieldFilter != nil && !query.FieldFilter(dataPoint.Fields) {
				continue
			}
			points = append(points, dataPoint)
			pointLen++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	points = Resample(points, query.Timeframe, query.StartTime, query.EndTime)
	return points, nil
}

func Resample(dataPoints []DataPoint, timeframe time.Duration, startTime, endTime time.Time) []DataPoint {
	if timeframe == 0 {
		return dataPoints
	}
	var resampled []DataPoint
	if len(dataPoints) == 0 {
		return resampled
	}
	start := startTime
	end := endTime
	newPoint := DataPoint{
		Time: start,
	}
	var nextTime = start.Add(timeframe)
	for _, point := range dataPoints {
		if point.Time.After(end) {
			break
		}
		if point.Time.Before(start) {
			continue
		}
		if point.Time.Before(nextTime) {
			newPoint.MergeFields(point)
		} else {
			start = nextTime
			nextTime = start.Add(timeframe)
			resampled = append(resampled, newPoint)
			newPoint = DataPoint{
				Time: start,
			}
		}

	}
	return resampled
}
