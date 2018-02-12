package bufpool

import (
	"errors"
	"sync"
)

var (
	ErrExistPool     = errors.New("exist the pool")
	ErrNotExistPool  = errors.New("do not exist the pool")
	GlobalBufferPool = &BufferPool{unitPools: make(map[int]*UnitPool)}
)

const (
	UnitPoolCapMax  = 10240
	UnitPoolMaxSize = 1 * 1024 * 1024 //B
)

type UnitPool struct {
	bufSize int
	capx    int
	mu      sync.Mutex
	bufs    [][]byte
	new     func() []byte
}

type BufferPool struct {
	rwMu      sync.RWMutex
	unitPools map[int]*UnitPool
}

func (bp *BufferPool) NewUnitPool(bufSize int, capx int) (*UnitPool, error) {
	p, ok := bp.getUnitPool(bufSize)
	if ok {
		return p, nil
	}

	p = &UnitPool{bufSize: bufSize, capx: capx, bufs: make([][]byte, 0, capx),
		new: func() []byte { return make([]byte, bufSize) }}
	bp.rwMu.Lock()
	bp.unitPools[bufSize] = p
	bp.rwMu.Unlock()

	return p, nil
}

func (bp *BufferPool) Get(size int) ([]byte, error) {
	p, ok := bp.getUnitPool(size)
	if !ok {
		return nil, ErrNotExistPool
	}

	p.mu.Lock()
	l := len(p.bufs)
	defer p.mu.Unlock()
	if l <= 0 {
		return p.new(), nil
	}
	buf := p.bufs[l-1]
	p.bufs = p.bufs[0 : l-1]

	return buf, nil
}

func (bp *BufferPool) Free(buf []byte) {
	p, ok := bp.getUnitPool(len(buf))
	if !ok {
		return
	}

	p.mu.Lock()
	if len(p.bufs) < p.capx {
		p.bufs = append(p.bufs, buf)
	}
	p.mu.Unlock()
}

func (bp *BufferPool) getUnitPool(size int) (p *UnitPool, ok bool) {
	bp.rwMu.RLock()
	p, ok = bp.unitPools[size]
	bp.rwMu.RUnlock()

	return
}

func (p *UnitPool) Get(size int) ([]byte, error) {
	if size != p.bufSize {
		return nil, ErrNotExistPool
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	l := len(p.bufs)
	if l <= 0 {
		return p.new(), nil
	}
	buf := p.bufs[l-1]
	p.bufs = p.bufs[0 : l-1]

	return buf, nil
}

func (p *UnitPool) Free(buf []byte) {
	if len(buf) != p.bufSize {
		return
	}

	p.mu.Lock()
	if len(p.bufs) < p.capx {
		p.bufs = append(p.bufs, buf)
	}
	p.mu.Unlock()
}
