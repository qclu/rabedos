package datanodeIdc

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"utils/config"
)

var configFile = flag.String("c", "/home/zimuxia/svn/src/volstore/datanode/cfg/cfg.json", "config file path")

const (
	dir            = "/tmp"
	cfg            = "cfg.json"
	hPortExtent    = "20140"
	tPortExtent    = "20141"
	hAddrExtent    = "127.0.0.1:20140"
	tAddrsExtent   = "127.0.0.1:20141"
	wAddrs         = "127.0.0.1:20160"
	hPortChunk     = "20150"
	tPortChunk     = "20151"
	hAddrChunk     = "127.0.0.1:20150"
	tAddrChunk     = "127.0.0.1:20151"
	defaultVolId   = 1
	defaultVolSize = 10 * 1024 * 1024 * 1024
)

type Config struct {
	Id                     string
	Version                string
	Ip                     string
	Port                   string
	MasterAddr             string
	MonitorAddr            string
	ClusterId              string
	LogDir                 string
	StoreType              string
	ChunkInspectorInterval string
	DisksInfo              []string
}

var sid = 1
var driverExtent *Driver
var driverChunk *Driver

func startServer(cfgFile string) (server *DataNode) {
	cfg := config.LoadConfigFile(cfgFile)
	server = NewServer()
	interceptSignal(server)

	go func() {
		log.Println("start server the cfg:", cfgFile)
		if err := server.Start(cfg); err != nil {
			log.Fatal("Fatal: failed to start the jfs daemon - ", err)
			os.Exit(0)
		}
	}()

	time.Sleep(5 * time.Second)
	return
}

func initStartGroup(headPrefix, tailPrefix, hPort, tPort, storeType string) (hS, tS *DataNode) {
	os.RemoveAll(dir + "/" + headPrefix)
	os.RemoveAll(dir + "/" + tailPrefix)
	os.Mkdir(dir+"/"+headPrefix, 0777)
	os.Mkdir(dir+"/"+tailPrefix, 0777)
	hCfg := initDatanode(dir, headPrefix, hPort, storeType)
	tCfg := initDatanode(dir, tailPrefix, tPort, storeType)
	hS = startServer(hCfg)
	tS = startServer(tCfg)
	time.Sleep(20 * time.Second)

	cmd := &Cmd{Id: "create 1", VolId: defaultVolId, Code: OpCreateVol, VolSize: defaultVolSize}
	hS.createVol(cmd)
	cmd = &Cmd{Id: "create 2", VolId: defaultVolId, Code: OpCreateVol, VolSize: defaultVolSize}
	tS.createVol(cmd)

	return
}

func CleanVols(id string, s *DataNode) {
	cmd := &Cmd{Id: id, VolId: defaultVolId, Code: OpDeleteVol, VolSize: defaultVolSize}
	s.deleteVol(cmd)
}

func init() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.Println("init end.")
}

//no master to test
// func TestOperationsChunk(t *testing.T) {
// 	hS, tS := initStartGroup("chunkHead", "chunkTail", hPortChunk, tPortChunk, ChunkStoreType)
// 	driverChunk = NewDriver(hAddrChunk, 10, 1048576, defaultVolId)
// 	time.Sleep(60 * time.Second)
// 	log.Println("chunk init end. vol1 len:", len(hS.vols), " vol2 len:", len(tS.vols))

// 	crcsW, pkgs, err := driverChunk.Write(0, 1, 1, 0, "", ChunkStoreType)
// 	if err != nil || pkgs[0].Opcode == OpError {
// 		t.Log("Write, err:", err)
// 		t.FailNow()
// 	}
// 	crcsR, err := driverChunk.ReadChunk(0, 1, 0, "", pkgs)
// 	if err != nil || pkgs[0].Opcode == OpError {
// 		t.Log("ReadChunk, err:", err)
// 		t.FailNow()
// 	}
// 	if crcsR[0] != crcsW[0] {
// 		t.Log("Wrong crc, read crc:", crcsR[0], " write crc:", crcsW[0])
// 	}

// 	// TestMutilNodesOperationsChunk
// 	crcsW, pkgs, err = driverChunk.Write(0, 1, 1, 1, tAddrChunk+AddrSplit, ChunkStoreType)
// 	if err != nil || pkgs[0].Opcode == OpError {
// 		t.Log("Write, err:", err)
// 		t.FailNow()
// 	}
// 	crcsR, err = driverChunk.ReadChunk(0, 1, 1, "", pkgs)
// 	if err != nil || pkgs[0].Opcode == OpError {
// 		t.Log("ReadChunk, err:", err)
// 		t.FailNow()
// 	}
// 	if crcsR[0] != crcsW[0] {
// 		t.Log("Wrong crc, read crc:", crcsR[0], " write crc:", crcsW[0])
// 	}

// 	CleanVols("chunk 1", hS)
// 	CleanVols("chunk 2", tS)
// }

func TestOperationsExtent(t *testing.T) {
	hS, tS := initStartGroup("extentHead", "extentTail", hPortExtent, tPortExtent, ExtentStoreType)
	driverExtent = NewDriver(hAddrExtent, 10, 65536, defaultVolId)
	log.Println("extent init end.")

	if err := driverExtent.CreateExtent(0, 1, 1, 0, "", ExtentStoreType); err != nil {
		t.Log("CreateExtent, err:", err)
	}
	crcsW, _, err := driverExtent.Write(0, 1, 1, 0, "", ExtentStoreType)
	if err != nil {
		t.Log("Write, err:", err)
		t.FailNow()
	}
	crcsR, err := driverExtent.ReadExtent(0, 1, 1, 0, "", ExtentStoreType)
	if err != nil {
		t.Log("ReadExtent, err:", err)
		t.FailNow()
	}
	if crcsR[0] != crcsW[0] {
		t.Log("Wrong crc, read crc:", crcsR[0], " write crc:", crcsW[0])
	}

	if err := driverExtent.DeleteExtent(0, 1, 1, 0, "", ExtentStoreType); err != nil {
		t.Log("DeleteExtent, err:", err)
	}

	// TestMutilNodesOperationsExtent
	if err := driverExtent.CreateExtent(1, 1, 2, 1, tAddrsExtent+AddrSplit, ExtentStoreType); err != nil {
		t.Log("CreateExtent, err:", err)
	}
	crcsW, _, err = driverExtent.Write(1, 1, 2, 1, tAddrsExtent+AddrSplit, ExtentStoreType)
	if err != nil {
		t.Log("Write, err:", err)
		t.FailNow()
	}
	crcsR, err = driverExtent.ReadExtent(1, 1, 2, 1, tAddrsExtent+AddrSplit, ExtentStoreType)
	if err != nil {
		t.Log("ReadExtent, err:", err)
		t.FailNow()
	}
	if crcsR[0] != crcsW[0] {
		t.Log("Wrong crc, read crc:", crcsR[0], " write crc:", crcsW[0])
		t.Fail()
	}

	if err := driverExtent.MarkDeleteExtent(1, 1, 2, 1, tAddrsExtent+AddrSplit, ExtentStoreType); err != nil {
		t.Log("MarkDeleteExtent, err:", err)
	}

	// TestExceptionExtentNodes
	pkgs := make([]*Packet, 1)

	//write local err, file don't exist
	c, err := driverExtent.pool.Get()
	if err != nil {
		t.Log("get err:", err)
		t.FailNow()
	}
	pkgs[0] = PackExtentRequest(OpAppend, 101, 101, PkgExtentDataMaxSize, 1, 0, 1, "")
	if _, err := handleExtentReq(pkgs, c); err == nil {
		t.Log("handleExtentReq, err:", err)
		t.Fail()
	}
	driverExtent.pool.Put(c)

	//wrong next addr
	_, _, err = driverExtent.Write(1, 1, 2, 1, wAddrs+AddrSplit, ExtentStoreType)
	if err == nil {
		t.FailNow()
	}

	// TestExceptionSingleNode
	pkgs = make([]*Packet, 1)

	//wrong pkg: wrong opcode
	c, _ = driverExtent.pool.Get()
	pkgs[0] = PackExtentRequest(OkCreateFile, 1, 1, PkgExtentDataMaxSize, 1, 0, 1, "")
	if _, err := handleExtentReq(pkgs, c); err == nil {
		t.Log("wrong pkg: wrong opcode, err:", err)
		t.FailNow()
	}
	driverExtent.pool.Put(c)
	// //wrong pkg: wrong ino, bno
	// c, _ = driverExtent.pool.Get()
	// pkgs[0] = PackExtentRequest(OpCreateFile, -1, -1, PkgExtentDataMaxSize, 1, 0, 1, "")
	// if _, err := handleExtentReq(pkgs, c); err == nil {
	// 	t.Log("wrong pkg: wrong ino, bno, err:", err)
	// 	t.FailNow()
	// }
	// driverExtent.pool.Put(c)
	//wrong pkg: wrong volId
	c, _ = driverExtent.pool.Get()
	pkgs[0] = PackExtentRequest(OkCreateFile, 1, 1, PkgExtentDataMaxSize, 2, 0, 10, "")
	if _, err := handleExtentReq(pkgs, c); err == nil {
		t.Log("wrong pkg, err:", err)
		t.FailNow()
	}
	driverExtent.pool.Put(c)
	//wrong pkg:  nodes and addrs unmatch
	c, _ = driverExtent.pool.Get()
	pkgs[0] = PackExtentRequest(OpCreateFile, 1, 1, PkgExtentDataMaxSize, 12, 1, 1, "")
	if _, err := handleExtentReq(pkgs, c); err == nil {
		t.Log("wrong pkg: nodes unmatch, err:", err)
		t.FailNow()
	}
	c.Close()
	driverExtent.pool.Put(nil)
	c, _ = driverExtent.pool.Get()
	pkgs[0] = PackExtentRequest(OpCreateFile, 10, 10, PkgExtentDataMaxSize, 13, 1, 1, tAddrsExtent+"/")
	if _, err := handleExtentReq(pkgs, c); err != nil {
		t.Log("wrong pkg:  addrs unmatch, err:", err)
		t.FailNow()
	}
	driverExtent.pool.Put(c)
	//wrong pkg:  unmatch store type
	c, _ = driverExtent.pool.Get()
	pkgs[0] = PackExtentRequest(OpCreateFile, 1, 1, 0, 21, 1, 1, "")
	if _, err := handleExtentReq(pkgs, c); err == nil {
		t.Log("wrong pkg: unmatch store type, err:", err)
		t.FailNow()
	}
	c.Close()
	driverExtent.pool.Put(nil)
	//right pkg: diff bsize
	c, err = driverExtent.pool.Get()
	if err != nil {
		t.Log("right pkg, get err:", err)
		t.FailNow()
	}
	pkgs[0] = PackExtentRequest(OpCreateFile, 1, 1, 64, 31, 0, 1, "")
	if _, err := handleExtentReq(pkgs, c); err != nil {
		t.Log("right pkg: diff bsize, err:", err)
		t.FailNow()
	}
	driverExtent.pool.Put(c)

	//wrong pkg:  append err crc
	c, _ = driverExtent.pool.Get()
	pkgs[0] = PackExtentRequest(OpAppend, 1, 1, PkgExtentDataMaxSize, 41, 1, 1, "")
	pkgs[0].Data = []byte("lalalaalalalalala")
	pkgs[0].Crc = uint32(11)
	if _, err := handleExtentReq(pkgs, c); err == nil {
		t.Log("wrong pkg: append err crc, err:", err)
		t.FailNow()
	}
	c.Close()
	driverExtent.pool.Put(nil)

	CleanVols("extent 1", hS)
	CleanVols("extent 2", tS)
}

func PackExtentRequest(opcode uint8, ino, bno, bsiz, rid, nodes, volId int, addrs string) (p *Packet) {
	p = NewPacketReady(opcode)
	p.ExtentId = ExtentId{Ino: int64(ino), Blockno: int64(bno), Blocksize: int64(bsiz)}
	p.ReqId = int64(rid)
	p.Nodes = uint8(nodes)
	p.VolId = uint32(volId)
	p.Arg = []byte(addrs)
	p.Arglen = uint32(len(p.Arg))

	return
}

func GetCfg(cfg string) (config Config) {
	jsonbuf, err := ioutil.ReadFile(cfg)
	if err != nil {
		log.Fatal("read configure err:", err)
		return
	}
	if err := json.Unmarshal(jsonbuf, &config); err != nil {
		log.Fatal("unmarshal err:", err)
		return
	}

	return
}

func GenerateFixedCfg(metaDir, dataDir, curFile, port, storeType string) {
	cfgJson := GetCfg(*configFile)
	cfgJson.Port = port
	cfgJson.DisksInfo[0] = "path:" + dataDir + ";minsize:8;maxerrs:10"
	cfgJson.LogDir = metaDir
	cfgJson.Id = strconv.Itoa(sid)
	cfgJson.StoreType = storeType
	sid++

	log.Println("dir:", cfgJson.DisksInfo[0], " sid:", sid)

	fp, err := os.OpenFile(curFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal("openfile err:", err)
		return
	}
	data, err := json.Marshal(cfgJson)
	if err != nil {
		log.Fatal("marshal err:", err)
		return
	}
	_, err = fp.WriteAt(data, 0)
	if err != nil {
		log.Fatal("write err:", err)
		return
	}
	fp.Sync()
	fp.Close()

	return
}

func initDatanode(dir, nodeIdentify, port, storeType string) (cfgFile string) {
	metaDir := dir + "/" + nodeIdentify + "/log"
	dataDir := dir + "/" + nodeIdentify + "/data"
	cfgFile = dir + "/" + nodeIdentify + "/" + cfg

	os.Mkdir(metaDir, 0777)
	os.Mkdir(dataDir, 0777)
	GenerateFixedCfg(metaDir, dataDir, cfgFile, port, storeType)
	return
}

func interceptSignal(s *DataNode) {
	sign := make(chan os.Signal, 1)
	signal.Notify(sign, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case <-sign:
			s.Shutdown()
			os.Exit(0)
		}
	}()
}
