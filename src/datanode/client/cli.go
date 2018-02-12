package main

import (
	"flag"
	"log"
	"runtime"
	"sync"

	"volstore/datanodeIdc"
)

var (
	sAddr          = flag.String("sAddr", "127.0.0.1:20140", "server addr")
	nAddrs         = flag.String("nAddrs", "127.0.0.1:20141/", "other nodes' addrs,  split with /")
	nodes          = flag.Int("nodes", 1, "the num of nodes")
	opt            = flag.String("opt", "create", "opt, [create], [delete], [write], [readE] or [readC], defaut create")
	storeType      = flag.Int("store", 1, "the store type, [chunkstore], [extentstore], defaut extentstore")
	threads        = flag.Int("threads", 1, "the num of threads")
	reqs           = flag.Int("reqs", 10, "the requests num for every threads")
	blockNo        = flag.Int("bno", 0, "the start block number, 0-1023")
	base           = flag.Int("base", 1000, "the base num between ervery thread")
	perThreadFiles = flag.Int("files", 10, "the max files num of ervery thread")
	pkgDataMaxSize = flag.Int("pkgSize", 65536, "pkg data max size")
	vols           = flag.Int("vols", 2, "vols num")
)

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	cli := datanodeIdc.NewDriver(*sAddr, 100, *pkgDataMaxSize, *vols)
	// read test, two server
	// cli2 := datanode.NewDriver("192.168.204.101:20140", 100)
	datanodeIdc.InitUnitBufPools([]int{datanodeIdc.HeaderSize, datanodeIdc.PkgArgMaxSize, *pkgDataMaxSize},
		[]int{datanodeIdc.PkgHeaderPoolMaxCap, datanodeIdc.PkgArgPoolMaxCap, datanodeIdc.PkgDataPoolMaxCap})

	switch {
	case *opt == "create":
		f := func(base, n, files, nodes int, addrs string, storeT uint8) {
			cli.CreateExtent(base, n, files, nodes, addrs, storeT)
		}
		Handle(*threads, f)
	case *opt == "write":
		f := func(base, n, files, nodes int, addrs string, storeT uint8) {
			cli.Write(base, n, files, nodes, addrs, storeT)
		}
		Handle(*threads, f)
	case *opt == "readE":
		f := func(base, n, files, nodes int, addrs string, storeT uint8) {
			cli.ReadExtent(base, n, files, nodes, addrs, storeT)
		}
		Handle(*threads, f)
	case *opt == "readC":
		f := func(base, n, files, nodes int, addrs string, storeT uint8) {
			cli.ReadChunkPer(base, n, files, nodes, addrs, storeT)
		}
		// f2 := func(base, n, files, nodes int, addrs string, storeT string) {
		// 	cli2.ReadChunkPer(base, n, files, nodes, addrs, storeT)
		// }
		go Handle(*threads, f)
		// go Handle(*threads, f2)
		select {}
	case *opt == "delete":
		f := func(base, n, files, nodes int, addrs string, storeT uint8) {
			cli.DeleteExtent(base, n, files, nodes, addrs, storeT)
		}
		Handle(*threads, f)
	default:
		log.Println("opt isn't existing")
	}
}

func Handle(threads int, f func(int, int, int, int, string, uint8)) {
	wg := sync.WaitGroup{}
	wg.Add(threads)

	for i := 0; i < threads; i++ {
		go func(n int) {
			f(*base*n, *reqs, *perThreadFiles, *nodes, *nAddrs, (uint8)(*storeType))
			wg.Done()
		}(i)
	}

	wg.Wait()
}
