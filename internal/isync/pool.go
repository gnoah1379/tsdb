package isync

import (
	"sync"
)

type Pool[T any] struct {
	New   func() T
	Reset func(T)
	p     sync.Pool
}

type ResetAble interface {
	Reset()
}

func ResetAblePool[T ResetAble](fn func() T) Pool[T] {
	return Pool[T]{
		New:   fn,
		Reset: func(x T) { x.Reset() },
	}
}

func (p *Pool[T]) Get() (val T) {
	if p.New == nil {
		return
	}
	v := p.p.Get()
	if v == nil {
		return p.New()
	}
	return v.(T)
}

func (p *Pool[T]) Put(x T) {
	if p.Reset != nil {
		p.Reset(x)
	}
	p.p.Put(x)
}
