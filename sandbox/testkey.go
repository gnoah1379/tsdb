package main

import (
	"bytes"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"io"
)

func main() {
	buf := bytes.NewBuffer(nil)
	enc := msgpack.NewEncoder(buf)
	val := map[string]any{
		"str":   "value",
		"int":   123,
		"float": 1.23,
		"bytes": []byte{1, 2, 3},
		"byte":  byte(1),
		"bool":  true,
		"slice": []int{1, 2, 3},
		"map":   map[string]int{"a": 1, "b": 2},
	}
	enc.SetSortMapKeys(true)
	if err := enc.Encode(val); err != nil {
		fmt.Println(err)
		return
	}
	firstData, _ := io.ReadAll(buf)
	buf.Reset()
	enc.Reset(buf)
	val2 := map[string]any{
		"map":   map[string]int{"a": 1, "b": 2},
		"str":   "value",
		"int":   123,
		"float": 1.23,
		"bytes": []byte{1, 2, 3},
		"byte":  byte(1),
		"bool":  true,
		"slice": []int{1, 2, 3},
	}
	enc.SetSortMapKeys(true)
	if err := enc.Encode(val2); err != nil {
		fmt.Println(err)
		return
	}
	secData, _ := io.ReadAll(buf)
	//enc.EncodeMapSorted()
	fmt.Println(bytes.Equal(firstData, secData))
}
