package main

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
)

func main() {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	wb := db.NewWriteBatch()
	err = wb.Set([]byte("key1"), []byte("value"))
	if err != nil {
		panic(err)
	}
	wb.Cancel()
	_ = db.View(func(txn *badger.Txn) error {
		it, err := txn.Get([]byte("key1"))
		if !errors.Is(err, badger.ErrKeyNotFound) {
			panic("key1 should not exist")
		}
		if err != nil {
			fmt.Println(err)
			return err
		}
		fmt.Println(it.String())
		return nil
	})
}
