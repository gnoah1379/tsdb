package isync

import "sync"

type Pool[T any] struct {
	New   func() T
	Reset func(T)
	p     sync.Pool
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
