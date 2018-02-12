package datanodeIdc

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"time"
	"utils/rbdlogger"
)

var (
	ErrRepaireFormat = errors.New("RepaireFormatErr")
	ErrRepaireFail   = errors.New("RepaireFailErr")
)

func DialWithRetry(try int, dstAddr string) (conn net.Conn, err error) {
	if try < 1 {
		try = ConnTryTimes
	}

	for i := 0; i < try; i++ {
		if conn, err = net.Dial(NetType, dstAddr); err == nil {
			break
		}
		time.Sleep(time.Second * ConnTimeSleep)
	}

	return
}

func Interact(pkg *Packet, dstAddr string) (err error) {
	pkg.Nodes = 0
	pkg.ReqId = GetReqId()

	conn, err := DialWithRetry(-1, dstAddr)
	if err != nil {
		return
	}

	log.GetLog().LogInfo("Interact:", pkg.GetInfo(GetPackInfoOpDetail)+"dstAddr:"+dstAddr)
	if err = pkg.WriteToConn(conn, WriteDeadlineTime, FreeBodySpace); err == nil {
		err = pkg.ReadFromConn(conn, ReadDeadlineTime, PkgRepairDataMaxSize)
	}
	if err == nil && pkg.IsErrPack() {
		err = ErrRepaireFail
	}
	conn.Close()

	return
}

func GetRepairData(buf []byte) (addr string, size int, err error) {
	arr := strings.SplitN(string(buf), RepairDataSplit, -1)
	if len(arr) != 2 {
		return "", -1, ErrRepaireFormat
	}
	addr = arr[0]
	size, err = strconv.Atoi(arr[1])

	return
}

func DoRepair(begin, end int64, bufSize int64, pkg *Packet, vol *VolInfo, addr string) (errFlag int, err error) {
	start := time.Now()
	isFreeBody := true
	conn, err := DialWithRetry(-1, addr)
	if err != nil {
		return
	}

	for begin < end {
		if begin+bufSize > end {
			bufSize = end - begin
			pkg.Size = uint32(bufSize)
		}
		pkg.Offset = begin
		if err = pkg.WriteToConn(conn, WriteDeadlineTime, NotFreeBodySpace); err != nil {
			break
		}
		if err = pkg.ReadFromConn(conn, ReadDeadlineTime, PkgRepairDataMaxSize); err != nil {
			isFreeBody = false
			break
		}
		if pkg.IsErrPack() {
			err = errors.New(string(pkg.Data[:pkg.Size]))
			break
		}
		if err = CheckCrc(pkg); err != nil {
			break
		}

		if err = vol.Store.Write(pkg.FileId, int64(pkg.Offset), int64(pkg.Size), pkg.Data, pkg.Crc, RepairOpFlag); err != nil {
			errFlag = WriteFlag
			break
		}

		log.GetLog().LogInfo(LogRepair, pkg.GetInfo(GetPackInfoOpDetail), "begin:", begin, "size:", bufSize, "no.", pkg.ReqId,
			" crc:", pkg.Crc, "t:", time.Now().Sub(start).Nanoseconds()/1e6, "ms")
		pkg.Opcode = OpRepairRead
		begin += bufSize
		pkg.ReqId++
	}
	if err == nil || (begin != 0 && isFreeBody) {
		pkg.FreeBody()
	}

	return
}
