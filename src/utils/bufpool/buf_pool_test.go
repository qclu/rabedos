package bufpool

import (
	"runtime"
	"sync"
	"testing"
)

func TestBasicBufPool(t *testing.T) {
	p := &BufferPool{unitPools: make(map[int]*UnitPool)}
	size := 1024

	//hasn't  the unitpool
	if _, err := p.Get(size); err == nil {
		t.Log("get  the size hasn't in pool.")
		t.Fail()
	}
	b := make([]byte, size)
	p.Free(b)

	//has the unitpool
	up, err := p.NewUnitPool(size, 10, func() []byte { return make([]byte, size) })
	buf, err := p.Get(size)
	if err != nil {
		t.Log("get  the size err:", err)
		t.Fail()
	}
	p.Free(buf)

	buf, err = up.Get(size)
	if err != nil {
		t.Log("get  the size err:", err)
		t.Fail()
	}
	up.Free(buf)
}

func TestExceptionBufPool(t *testing.T) {
	p := &BufferPool{unitPools: make(map[int]*UnitPool)}
	capx := 10
	size := 1024
	up, err := p.NewUnitPool(size, 10, func() []byte { return make([]byte, size) })
	if _, err := p.NewUnitPool(size, 10, func() []byte { return make([]byte, size) }); err != nil {
		t.Log("NewUnitPool err:", err)
		t.Fail()
	}

	for i := 0; i < capx+2; i++ {
		if i%2 == 0 {
			_, err = p.Get(size)
		} else {
			_, err = up.Get(size)
		}
		if err != nil {
			t.Log("get  the size err:", err)
			t.Fail()
		}
	}
	for i := 0; i < capx+1; i++ {
		b := make([]byte, size)
		if i%2 == 0 {
			p.Free(b)
		} else {
			up.Free(b)
		}
	}
	if len(up.bufs) != up.capx || up.capx != capx {
		t.Log("the cap is wrong,  correct capx:", capx, " bufs len:", len(up.bufs), " capx:", up.capx)
		t.Fail()
	}
}

func BenchmarkBufPool(b *testing.B) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	p := &BufferPool{unitPools: make(map[int]*UnitPool)}
	capx := 10
	size_a := 10
	size_b := 20
	p.NewUnitPool(size_a, capx, func() []byte { return make([]byte, size_a) })
	up, _ := p.NewUnitPool(size_b, capx, func() []byte { return make([]byte, size_b) })

	wg := sync.WaitGroup{}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			if _, err := p.Get(size_a); err != nil {
				b.Log("err:", err)
				b.Fail()
			}
			wg.Done()
		}()
	}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			b := make([]byte, size_a)
			p.Free(b)
			wg.Done()
		}()
	}

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			if _, err := up.Get(size_b); err != nil {
				b.Log("err:", err)
				b.Fail()
			}
			wg.Done()
		}()
	}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			b := make([]byte, size_b)
			up.Free(b)
			wg.Done()
		}()
	}

	wg.Wait()
}

func BenchmarkBufGetFree(b *testing.B) {
	p := &BufferPool{unitPools: make(map[int]*UnitPool)}
	capx := 1000000
	size := 1024
	up, _ := p.NewUnitPool(size, capx, func() []byte { return make([]byte, size) })

	for i := 0; i < b.N; i++ {
		for i := 0; i < 5<<10; i++ {
			b, _ := up.Get(size)
			up.Free(b)
		}
	}
}
