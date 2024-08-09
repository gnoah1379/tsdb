package main

import (
	"fmt"
	"time"
	"tsdb"
	"tsdb/exprs"
)

func main() {
	db, err := tsdb.Open(nil)
	if err != nil {
		panic(err)
	}
	err = db.Insert([]tsdb.Point{
		{
			Measurement: "cpu",
			Labels: map[string]string{
				"host": "localhost",
			},
			Fields: map[string]float64{
				"usage": 0.5,
			},
			Time: time.Now().Add(-time.Second),
		},
		{
			Measurement: "cpu",
			Labels: map[string]string{
				"host": "host1",
			},
			Fields: map[string]float64{
				"usage": 0.7,
			},
			Time: time.Now().Add(-time.Hour),
		},
	})
	if err != nil {
		panic(err)
	}
	points, err := db.SelectMeasurement("cpu", time.Unix(0, 0), time.Now(), true, exprs.Eq("host", "host1"))
	if err != nil {
		panic(err)
	}
	for _, point := range points {
		fmt.Println(point)
	}
}
