package main

import (
	"bytes"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"time"
)

func main() {
	buf := bytes.NewBuffer(nil)
	enc := msgpack.NewEncoder(buf)
	val := map[string]any{
		"str":       "value",
		"int":       123,
		"float":     1.23,
		"bytes":     []byte{1, 2, 3},
		"byte":      byte(1),
		"bool":      true,
		"slice":     []int{1, 2, 3},
		"map":       map[string]int{"a": 1, "b": 2},
		"timestamp": time.Now(),
	}
	err := enc.Encode(val)
	if err != nil {
		panic(err)
	}

	data := buf.Next(buf.Len())
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	var val2 map[string]any
	err = dec.Decode(&val2)
	if err != nil {
		panic(err)
	}
	fmt.Println(val2)
}
