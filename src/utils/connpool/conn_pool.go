package connpool

import (
	"errors"
	"net"
	"sync"
	"time"
)

const (
	DefaultSize          = 50
	DefaultTrys          = 1
	DefaultBufferSize    = 32768
	DefaultRetryInterval = 1
	ConnType             = "tcp"
)

var (
	ErrSameAddr         = errors.New("SameAddrError")
	ErrPoolFull         = errors.New("PoolFullError")
	ErrAddrEmpty        = errors.New("AddrEmptyError")
	ErrNotExistUnitPool = errors.New("NotExistUnitPoolErr")
)

type ConnPool struct {
	rwMu      sync.RWMutex
	unitPools map[string]*UnitConnPool
}

type UnitConnPool struct {
	size       int
	bufferSize int
	timeout    int
	trys       int
	addr       string
	pool       chan *net.TCPConn
}

func NewConnPool() *ConnPool {
	return &ConnPool{unitPools: make(map[string]*UnitConnPool)}
}

func (connp *ConnPool) NewUnitPool(size, bufferSize int, addr string, timeout, trys int) (p *UnitConnPool, err error) {
	if size <= 0 {
		size = DefaultSize
	}
	if trys <= 0 {
		trys = DefaultTrys
	}
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}
	if addr == "" {
		return nil, ErrAddrEmpty
	}

	p = &UnitConnPool{addr: addr, size: size, bufferSize: bufferSize, timeout: timeout, trys: trys, pool: make(chan *net.TCPConn, size)}
	for i := 0; i < size; i++ {
		p.pool <- nil
	}
	connp.SetUintPool(addr, p)

	return
}

func (p *UnitConnPool) GetNewConn() (c *net.TCPConn, err error) {
	var conn net.Conn
	for i := 0; i < p.trys; i++ {
		if conn, err = net.DialTimeout(ConnType, p.addr, time.Duration(p.timeout)*time.Second); err == nil {
			//print("get nil c :", p.addr, " trys:", i, "\n")
			break
		}
	}
	if err != nil {
		return
	}

	c, _ = conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)
	c.SetReadBuffer(p.bufferSize)
	c.SetWriteBuffer(p.bufferSize)

	return
}

func (p *UnitConnPool) Get() (c *net.TCPConn, err error) {
	select {
	case c = <-p.pool:
		if c != nil {
			//print("get no nil c :", p.addr, "\n")
			return
		}
	default:
	}

	return p.GetNewConn()
}

func (p *UnitConnPool) Put(conn *net.TCPConn) (err error) {
	select {
	case p.pool <- conn:
		// if conn == nil {
		// 	print("put nil c :", p.addr, "\n")
		// } else {
		// 	print("put no nil c :", p.addr, "\n")
		// }
	default:
		if conn != nil {
			conn.Close()
		}
	}

	return
}

func (connp *ConnPool) GetNewConn(addr string) (c *net.TCPConn, err error) {
	p, ok := connp.GetUintPool(addr)
	if !ok {
		return nil, ErrNotExistUnitPool
	}

	return p.GetNewConn()
}

func (connp *ConnPool) GetConn(addr string) (c *net.TCPConn, err error) {
	p, ok := connp.GetUintPool(addr)
	if !ok {
		return nil, ErrNotExistUnitPool
	}

	return p.Get()
}

func (connp *ConnPool) PutConn(addr string, conn *net.TCPConn) (err error) {
	p, ok := connp.GetUintPool(addr)
	if !ok {
		if conn != nil {
			conn.Close()
		}
		return ErrNotExistUnitPool
	}

	return p.Put(conn)
}

func (connp *ConnPool) SetUintPool(addr string, p *UnitConnPool) {
	connp.rwMu.Lock()
	connp.unitPools[addr] = p
	connp.rwMu.Unlock()
}

func (connp *ConnPool) GetUintPool(addr string) (p *UnitConnPool, ok bool) {
	connp.rwMu.RLock()
	p, ok = connp.unitPools[addr]
	connp.rwMu.RUnlock()

	return
}
