package tsdb

import (
	"errors"
	"fmt"
	"time"
)

type Point struct {
	Measurement string
	Labels      map[string]string
	Fields      map[string]float64
	Time        time.Time
}

func (p Point) String() string {
	labels := ""
	for k, v := range p.Labels {
		labels += fmt.Sprintf("%s=%s,", k, v)
	}
	labels = labels[:len(labels)-1]
	fields := ""
	for k, v := range p.Fields {
		fields += fmt.Sprintf("%s=%f,", k, v)
	}
	fields = fields[:len(fields)-1]
	return fmt.Sprintf("%s,%s %s %s", p.Measurement, labels, fields, p.Time.Format(time.RFC3339))

}

func (p Point) Validate() error {
	if p.Measurement == "" {
		return errors.New("measurement is required")
	}
	if len(p.Fields) == 0 {
		return errors.New("values are required")
	}
	if p.Time.IsZero() {
		return errors.New("time is required")
	}
	return nil
}
