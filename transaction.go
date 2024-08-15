package tsdb

import "github.com/dgraph-io/badger/v4"

type Tx struct {
	txn *badger.Txn
}

func (db *DB) Begin() *Tx {
	txn := db.storage.NewTransaction(true)
	return &Tx{txn: txn}
}

func (db *DB) BeginReadOnly() *Tx {
	txn := db.storage.NewTransaction(false)
	return &Tx{txn: txn}
}

func (t *Tx) Insert(point Point) error {
	return insertTxn(t.txn, point)
}

func (t *Tx) Commit() error {
	return t.txn.Commit()
}

func (t *Tx) Rollback() {
	t.txn.Discard()
}
