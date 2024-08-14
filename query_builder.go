package tsdb

import "time"

type QueryDataPointsBuilder struct {
	query QueryDataPoints
	db    *DB
}

func (q *QueryDataPointsBuilder) DataPoints() ([]DataPoint, error) {
	return q.db.QueryDataPoints(q.query)
}

func (q *QueryDataPointsBuilder) From(startTime time.Time) *QueryDataPointsBuilder {
	q.query.StartTime = startTime
	return q
}

func (q *QueryDataPointsBuilder) To(endTime time.Time) *QueryDataPointsBuilder {
	q.query.EndTime = endTime
	return q
}

func (q *QueryDataPointsBuilder) Range(startTime, endTime time.Time) *QueryDataPointsBuilder {
	q.query.StartTime = startTime
	q.query.EndTime = endTime
	return q
}

func (q *QueryDataPointsBuilder) LabelFilter(filter LabelFilter) *QueryDataPointsBuilder {
	q.query.LabelFilter = filter
	return q
}

func (q *QueryDataPointsBuilder) FieldFilter(filter FieldFilter) *QueryDataPointsBuilder {
	q.query.FieldFilter = filter
	return q
}

func (q *QueryDataPointsBuilder) Reverse() *QueryDataPointsBuilder {
	q.query.Reverse = true
	return q
}

func (q *QueryDataPointsBuilder) Limit(limit int) *QueryDataPointsBuilder {
	q.query.Limit = limit
	return q
}
