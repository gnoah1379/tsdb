package tsdb

import (
	"github.com/dgraph-io/badger/v4"
	"time"
)

type DB struct {
	storage   *badger.DB
	retention time.Duration
}

type Options struct {
	Retention  time.Duration
	BadgerOpts badger.Options
}

func getOptions(opts *Options) *Options {
	if opts == nil {
		opts = &Options{
			BadgerOpts: badger.DefaultOptions("").WithInMemory(true),
		}
	}
	if opts.Retention == 0 {
		opts.Retention = time.Hour * 24 * 7
	}
	return opts

}

func Open(opts *Options) (*DB, error) {
	opts = getOptions(opts)
	storage, err := badger.Open(opts.BadgerOpts)
	if err != nil {
		return nil, err
	}

	return &DB{
		storage:   storage,
		retention: opts.Retention,
	}, nil
}

func (db *DB) Close() error {
	return db.storage.Close()

}
