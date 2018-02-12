package datanodeIdc

import (
	"container/list"
	"os"
	"sync"
)

type ExtentInCore struct {
	fp        *os.File
	name      string
	key       uint32
	lock      sync.RWMutex
	extentCrc []byte
	element   *list.Element
}

func NewExtentInCore(name string, keyId uint32) (ec *ExtentInCore) {
	ec = new(ExtentInCore)
	ec.key = keyId
	ec.name = name
	ec.extentCrc = make([]byte, ExtentHeaderSize)

	return
}

func (ec *ExtentInCore) readlock() {
	ec.lock.RLock()
}

func (ec *ExtentInCore) readUnlock() {
	ec.lock.RUnlock()
}

func (ec *ExtentInCore) writelock() {
	ec.lock.Lock()
}

func (ec *ExtentInCore) writeUnlock() {
	ec.lock.Unlock()
}

func (ec *ExtentInCore) closeExtent() (err error) {
	ec.writelock()
	_, err = ec.fp.WriteAt(ec.extentCrc, 0)
	err = ec.fp.Close()
	ec.writeUnlock()

	return
}

func (ec *ExtentInCore) deleteExtent() (err error) {
	ec.writelock()
	err = os.Remove(ec.name)
	ec.writeUnlock()

	return
}
