package datanodeIdc

import (
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"runtime"
	"testing"
)

var (
	s      *ExtentStore
	extent ExtentId
)

const (
	BlockSize = 32 * 1024
)

func setUp(t *testing.T) (err error) {
	if s, err = NewExtentStore("/tmp/test", 100000, StartMode); err != nil {
		t.Error("failed to create NewStore, err:", err.Error())
	}

	extent.Ino = 100
	extent.Blocksize = 32 * 1024
	extent.Blockno = 0
	return
}

func TestCreateExtent(t *testing.T) {
	var err error

	if err = setUp(t); err != nil {
		t.Error(err)
	}
	s.Delete(extent)

	if err = s.Create(extent); err != nil {
		t.Error(err)
	}

	s.Delete(extent)
}

func randWriteAndRead(t *testing.T) {
	var (
		rcrc uint32
		err  error
	)

	for i := 0; i < 1000; i++ {
		offset := (int64)(BlockSize * (BlockCount - 1))
		wbuf := make([]byte, BlockSize)
		rbuf := make([]byte, BlockSize)
		rand.Seed(255)
		for i := 0; i < len(wbuf); i++ {
			wbuf[i] = byte(rand.Uint32())
		}

		bcrc := crc32.ChecksumIEEE(wbuf)

		if err = s.Write(extent, offset, wbuf, bcrc); err != nil {
			fmt.Println(err)
			continue
		}

		if rcrc, err = s.Read(extent, offset, int64(BlockSize), rbuf); err != nil {
			fmt.Println(err)
			continue
		}

		if rcrc != bcrc {
			t.Fatal("  rcrc[%d]  bcrc[%d]", rcrc, bcrc)
		}

	}
}

func TestAppendAndReadBlocks(t *testing.T) {
	runtime.GOMAXPROCS(4)
	var err error

	if err = setUp(t); err != nil {
		t.Error(err)
	}

	s.Delete(extent)
	defer s.Delete(extent)

	if err = s.Create(extent); err != nil {
		t.Error(err)
	}
	randWriteAndRead(t)

}

func TestNewExtentStore(t *testing.T) {
	var err error
	path := "/tmp/testn"
	if err := os.MkdirAll(path, 0775); err != nil {
		t.Error("failed to mkdir, err:", err.Error())
		t.FailNow()
	}
	if s, err = NewExtentStore(path, 100000, StartMode); err != nil {
		t.Error("failed to create NewStore, err:", err.Error())
	}
	status := s.GetStoreStatus()
	if status != OkDisk {
		t.Error("store status:", status)
	}

	os.Remove(path)
	if s, err = NewExtentStore(path, 100000, RestartMode); err != nil {
		t.Error("failed to create NewStore, err:", err.Error())
	}
	status = s.GetStoreStatus()
	print("status:", status)
}

func TestSnapshoot(t *testing.T) {
	if err := setUp(t); err != nil {
		t.Error(err)
		t.FailNow()
	}
	s.Delete(extent)

	//no file
	sn, err := s.Snapshoot()
	if err != nil {
		t.Error("Snapshoot, err:", err)
		t.Fail()
	} else if string(sn) != "" {
		t.Error("sn:", string(sn))
	}

	if err = s.Create(extent); err != nil {
		t.Error("Create, err:", err)
		t.FailNow()
	}
	sn, err = s.Snapshoot()
	if err != nil {
		t.Error("Snapshoot, err:", err)
		t.Fail()
	}
	print("sn: ", string(sn))
}
