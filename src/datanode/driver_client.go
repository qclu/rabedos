package datanodeIdc

import (
	"bufio"
	"errors"
	"hash/crc32"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"datanode/store"
	"datanode/util"
	"proto"
)

type Driver struct {
	dataMaxSize int
	pool        *UnitConnPool
}

const ErrReplyOpErr = "ReplyOpErr"

var volNum int
var data []byte

func NewDriver(addr string, conns, dataMaxSize, vols int) *Driver {
	pool := NewConnPool()
	p, err := pool.NewUnitPool(conns, ConnBufferSize, addr, ConnDialTimeout, ConnTryTimes)
	if err != nil {
		log.Fatal("new conn pool err:", err)
		return nil
	}
	volNum = vols

	return &Driver{pool: p, dataMaxSize: dataMaxSize}
}

func (d *Driver) CreateExtent(base, n, files, nodes int, addrs string, storeT uint8) (err error) {
	var pkgs []*Packet
	for i := 0; i < n; i++ {
		pkg := NewPacketReady(OpCreateFile)
		PackRequest(base+i, i, nodes, addrs, pkg)
		//		pkg.FileId = uint32((base + i) % files)
		pkg.StoreType = storeT
		pkgs = append(pkgs, pkg)
		log.Println("CreateExtent, FileId:", pkg.FileId, " arg:", string(pkg.Arg))
	}

	c, err := d.pool.Get()
	if err != nil {
		log.Println("GetConnErr: ", err)
		return
	}
	if storeT == ExtentStoreType {
		_, err = handleExtentReq(pkgs, c)
	}

	return
}

func (d *Driver) MarkDeleteExtent(base, n, files, nodes int, addrs string, storeT uint8) (err error) {
	var pkgs []*Packet
	for i := 0; i < n; i++ {
		pkg := NewPacketReady(OpMarkDelete)
		PackRequest(base+i, i, nodes, addrs, pkg)
		pkg.StoreType = storeT
		pkg.FileId = uint32((base + i) % files)
		pkgs = append(pkgs, pkg)
	}

	c, err := d.pool.Get()
	if err != nil {
		log.Println("GetConnErr: ", err)
		return
	}
	if storeT == ExtentStoreType {
		_, err = handleExtentReq(pkgs, c)
	}

	return
}

func (d *Driver) DeleteExtent(base, n, files, nodes int, addrs string, storeT uint8) (err error) {
	var pkgs []*Packet
	for i := 0; i < n; i++ {
		pkg := NewPacketReady(OpDeleteFile)
		pkg.StoreType = storeT
		PackRequest(base+i, i, nodes, addrs, pkg)
		pkg.FileId = uint32((base + i) % files)
		pkgs = append(pkgs, pkg)
	}

	c, err := d.pool.Get()
	if err != nil {
		log.Println("GetConnErr: ", err)
		return
	}
	if storeT == ExtentStoreType {
		_, err = handleExtentReq(pkgs, c)
	}

	return
}

func GetPacket(base, files, nodes, num, dataMaxSize int, addrs string, storeT uint8) *Packet {
	pkg := NewPacketReady(OpWrite)
	pkg.StoreType = storeT
	PackRequest(base+num, num, nodes, addrs, pkg)
	if storeT == ExtentStoreType {
		pkg.FileId = (uint32)(files)
	}
	pkg.Offset = 0
	pkg.Size = uint32(dataMaxSize)
	pkg.Data, _ = GlobalBufferPool.Get(dataMaxSize)
	copy(pkg.Data, data)
	// for i := 0; i < dataMaxSize; i++ {
	// 	pkg.Data[i] = byte(rand.Uint32())
	// }
	pkg.Crc = crc32.ChecksumIEEE(pkg.Data)

	return pkg
}

func (d *Driver) Write(base, n, files, nodes int, addrs string, storeT uint8) (crcs []uint32, pkgs []*Packet, err error) {
	start := time.Now()
	if data, err = GlobalBufferPool.Get(d.dataMaxSize); err != nil {
		log.Println("err:", err)
	}
	for i := 0; i < d.dataMaxSize; i++ {
		data[i] = byte(rand.Uint32())
	}
	if GetIntLessThenTen(base)%2 == 0 {
		log.Println("Write start, order base:", GetIntLessThenTen(base), " n:", n)
		for i := 0; i < n; i++ {
			pkg := GetPacket(base, files, nodes, i, d.dataMaxSize, addrs, storeT)
			pkgs = append(pkgs, pkg)
			crcs = append(crcs, pkg.Crc)
		}
	} else {
		log.Println("Write start, reversed order base:", GetIntLessThenTen(base))
		for i := n - 1; i >= 0; i-- {
			pkg := GetPacket(base, files, nodes, i, d.dataMaxSize, addrs, storeT)
			pkgs = append(pkgs, pkg)
			crcs = append(crcs, pkg.Crc)
		}
	}

	log.Println("Write, generate data take time:", time.Now().Sub(start), " store type:", storeT)

	c, err := d.pool.Get()
	if err != nil {
		log.Println("GetConnErr: ", err)
		return
	}

	if storeT == ExtentStoreType {
		_, err = handleExtentReq(pkgs, c)
	} else if storeT == ChunkStoreType {
		_, err = handleChunkReq(pkgs, c, d.dataMaxSize)
	}

	return
}

func readKeysPack(base, num int) (pkgs []*Packet, err error) {
	fp, err := os.OpenFile("keys", os.O_RDWR, 0666)
	if err != nil {
		log.Fatal("openfile err:", err)
		return
	}
	defer fp.Close()

	reader := bufio.NewReader(fp)
	for i := 0; i < num; i++ {
		p := NewPacket()
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal("read line, err:", err)
		}

		datas := strings.SplitN(string(line), "/", -1)
		if len(datas) != 10 {
			log.Println("datas len:", len(datas))
			break
		}
		cId, err := strconv.Atoi(datas[2])
		if err != nil {
			log.Println("cid atoi, err:", err)
			break
		}
		p.FileId = uint32(cId)

		vId, err := strconv.Atoi(datas[3])
		if err != nil {
			log.Println("vid atoi, err:", err)
			break
		}
		p.VolId = uint32(vId)

		off, err := strconv.Atoi(datas[4])
		if err != nil {
			log.Println("off atoi, err:", err)
			break
		}
		p.Offset = int64(off)

		size, err := strconv.Atoi(datas[5])
		if err != nil {
			log.Println("size atoi, err:", err)
			break
		}
		p.Size = uint32(size)
		//p.Data = make([]byte, int(p.Size))
		p.ReqId = int64(i + base)

		pkgs = append(pkgs, p)
	}

	return
}

func (d *Driver) ReadChunkPer(base, n, files, nodes int, addrs string, storeT uint8) {
	log.Println("ReadChunkPer, num:", n, " d.dataMaxSize: ", d.dataMaxSize)
	pkgs, err := readKeysPack(base, n)
	if err != nil {
		return
	}

	d.ReadChunk(base, n, nodes, addrs, pkgs)
}

func (d *Driver) ReadChunk(base, n, nodes int, addrs string, wReplys []*Packet) (crcs []uint32, err error) {
	var pkgs []*Packet
	start := time.Now()
	data, _ := GlobalBufferPool.Get(d.dataMaxSize)
	for i := 0; i < d.dataMaxSize; i++ {
		data[i] = byte(rand.Uint32())
	}

	for i := 0; i < n; i++ {
		pkg := NewPacketReady(OpRead)
		PackRequest(base+i, i, nodes, addrs, pkg)
		pkg.FileId = wReplys[i].FileId
		pkg.Offset = wReplys[i].Offset
		pkg.Crc = wReplys[i].Crc
		pkg.Size = wReplys[i].Size
		//pkg.Data = data[:int(wReplys[i].Size)]

		pkgs = append(pkgs, pkg)
	}
	log.Println("ReadChunk, generate data take time:", time.Now().Sub(start))

	c, err := d.pool.Get()
	if err != nil {
		log.Println("GetConnErr: ", err)
		return
	}
	crcs, err = handleChunkReq(pkgs, c, d.dataMaxSize)

	return
}

func (d *Driver) ReadExtent(base, n, files, nodes int, addrs string, storeT uint8) (crcs []uint32, err error) {
	var pkgs []*Packet
	start := time.Now()
	for i := 0; i < n; i++ {
		pkg := NewPacketReady(OpStreamRead)
		pkg.StoreType = storeT
		PackRequest(base+i, i, nodes, addrs, pkg)
		if storeT == ExtentStoreType {
			pkg.FileId = uint32((base + i) % files)
		}
		pkg.Offset = 0
		pkg.Size = PkgExtentDataMaxSize
		//pkg.Data, _ = GlobalBufferPool.Get(PkgExtentDataMaxSize)

		pkgs = append(pkgs, pkg)
	}
	log.Println("ReadExtent, generate data take time:", time.Now().Sub(start))

	c, err := d.pool.Get()
	if err != nil {
		log.Println("GetConnErr: ", err)
		return
	}

	crcs, err = handleExtentReq(pkgs, c)

	return
}

func GetIntLessThenTen(base int) int {
	for base >= 10 {
		base = base / 10
	}

	return base
}

func PackRequest(req, num, nodes int, addrs string, p *Packet) {
	p.ReqId = int64(req)
	p.Nodes = uint8(nodes)
	p.VolId = uint32(volNum)
	p.goals = 3
	p.Arg = []byte(addrs)
	p.Arglen = uint32(len(p.Arg))
}

func handleChunkReq(pkgs []*Packet, c net.Conn, dataMaxSize int) (crcs []uint32, err error) {
	crcs = make([]uint32, len(pkgs))
	for i, pkg := range pkgs {
		log.Println("handleChunkReq, no.", pkg.ReqId, " magic:", pkg.Magic, " size:", pkg.Size)
		if err = pkg.WriteToConn(c, WriteDeadlineTime, NotFreeBodySpace); err != nil {
			log.Println("WriteToConnErr, no.", pkg.ReqId, " err:", err)
			break
		}
		if err = pkg.ReadFromConn(c, ReadDeadlineTime, int64(dataMaxSize)); err != nil {
			log.Println("ReadFromConnErr, reqId:", pkg.ReqId, " err:", err)
			break
		}
		if pkg.Opcode == OpErr {
			log.Println("reply Opcode == OpErr,  reqId:", pkg.ReqId, " err:", string(pkg.Data[:pkg.Size]))
			continue
		}
		if (pkg.Opcode == OpOk) && crcs[i] != pkg.Crc {
			log.Println("err, self crc:", crcs[i], " reply crc:", pkg.Crc, " no.", i, " req:", pkg.ReqId)
			continue
		}
		log.Println("handleChunkReq, no.", pkg.ReqId, " size:", pkg.Size, " len:", len(pkg.Data))
		//TODO: crcs[i] = pkg.Crc, chunk read doesn't generate crc, now
		crcs[i] = crc32.ChecksumIEEE(pkg.Data[:pkg.Size])
	}
	log.Println("handleChunkReq end")

	return
}

func handleExtentReq(pkgs []*Packet, c net.Conn) (crcs []uint32, err error) {
	exitCh := make(chan bool, 1)
	var (
		sendSeq, recvSeq int
	)
	go func(ps []*Packet, c net.Conn, exit chan bool) {
		for i := 0; i < len(ps); i++ {
			pkg := ps[i]
			if sendSeq-recvSeq > 200 {
				i--
				continue
			}
			if len(exit) > 0 {
				break
			}
			sendSeq++
			log.Println("reqId:", pkg.ReqId, " addr:", string(pkg.Arg), " opcode:", pkg.Opcode, " sendSeq:",
				sendSeq, " recvSeq:", recvSeq)
			if err := pkg.WriteToConn(c, WriteDeadlineTime, NotFreeBodySpace); err != nil {
				log.Println("WriteToConnErr no.", i, " reqId:", pkg.ReqId, " err:", err)
			}
		}
	}(pkgs, c, exitCh)

	crcs = make([]uint32, len(pkgs))
	for i, pkg := range pkgs {
		recvSeq++
		if err = pkg.ReadFromConn(c, ReadDeadlineTime, PkgExtentDataMaxSize); err != nil {
			log.Println("ReadFromConnErr, reqId:", pkg.ReqId, " err:", err)
			exitCh <- true
			break
		}
		if pkg.Opcode == OpErr {
			err = errors.New(ErrReplyOpErr)
			log.Println("reply opcode=OpErr, reqId:", pkg.ReqId, " err:", string(pkg.Data[:pkg.Size]))
			exitCh <- true
			break
		}
		crcs[i] = pkg.Crc
	}
	log.Println("handleExtentReq end")

	return
}
