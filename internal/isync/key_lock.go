package isync

import (
	"context"
	"github.com/viney-shih/go-lock"
	"sync"
)

type KeyLock struct {
	locks   map[string]lock.RWMutex
	mapLock sync.Mutex
}

func (k *KeyLock) Locker(key string) lock.RWMutex {
	k.mapLock.Lock()
	if k.locks == nil {
		k.locks = make(map[string]lock.RWMutex)
	}
	if _, ok := k.locks[key]; !ok {
		k.locks[key] = lock.NewCASMutex()
	}
	locker := k.locks[key]
	k.mapLock.Unlock()
	return locker

}

func (k *KeyLock) Lock(ctx context.Context, key string) bool {
	locker := k.Locker(key)
	return locker.TryLockWithContext(ctx)
}

func (k *KeyLock) Unlock(key string) {
	locker := k.Locker(key)
	locker.Unlock()
}

func (k *KeyLock) RLock(ctx context.Context, key string) bool {
	locker := k.Locker(key)
	return locker.RTryLockWithContext(ctx)
}

func (k *KeyLock) RUnlock(key string) {
	locker := k.Locker(key)
	locker.RUnlock()
}
