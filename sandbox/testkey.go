package main

import (
	"fmt"
)

func main() {
	var data = []float64{1, 1, 1, 1, 1}
	var sum float64
	for i := 0; i < len(data); i++ {
		sum += data[i]
	}
	fmt.Println(sum)
	avg := sum / float64(len(data))
	fmt.Println(avg)

}
