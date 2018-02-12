package datanodeIdc

import (
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"strings"
	"sync/atomic"
	"time"
	"utils/rbdlogger"
)

var (
	ErrNoAvailAddr = errors.New("NoAvailAddrErr")
)

var (
	ReqIdGlobal = int64(1)
)

func GetReqId() int64 {
	return atomic.AddInt64(&ReqIdGlobal, 1)
}

func TakeTime(start *time.Time) int32 {
	return int32(time.Now().Sub(*start).Nanoseconds() / 1e6)
}

func TakeTimeUs(start *time.Time) int32 {
	return int32(time.Now().Sub(*start).Nanoseconds() / 1e3)
}

func HandleTimeoutLog(start int64, logTab string) {
	t := (time.Now().UnixNano() - start) / 1e6
	log.GetLog().LogInfo(logTab+" take time:", t, " ms")
	if t >= WarningTime {
		log.GetLog().LogWarn(logTab+" take time:", t, " ms")
	}

	return
}

func GetLocalIp() (ipv4Str string, err error) {
	interFs, err := net.Interfaces()
	if err != nil {
		return
	}

	for _, interF := range interFs {
		if strings.Contains(interF.Name, "lo") {
			continue
		}
		addrs, err := interF.Addrs()
		if err != nil {
			return "", err
		}

		for _, addr := range addrs {
			inet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}

			ipv4 := inet.IP.To4()
			if ipv4 == nil {
				continue
			}
			return ipv4.String(), nil
		}
	}

	return "", ErrNoAvailAddr
}

func GetSign(ch chan bool) (ok bool) {
	select {
	case <-ch:
		ok = true
	default:
	}

	return
}

func PutSign(ch chan struct{}, exitCh chan bool, sign struct{}) (ok bool) {
	select {
	case ch <- sign:
		ok = true
	case <-exitCh:
	}

	return
}

func PutPkg(pkgCh chan *Packet, signCh chan bool, pkg *Packet) (ok bool) {
	select {
	case pkgCh <- pkg:
		ok = true
	case <-signCh:
	}

	return
}

func CheckCrc(p *Packet) (err error) {
	if p.Opcode != OpWrite {
		return
	}

	crc := crc32.ChecksumIEEE(p.Data[:p.Size])
	if crc == p.Crc {
		return
	}
	log.GetLog().LogInfo("check crc, reqId:", p.ReqId, "size:", p.Size, "remote crc:", p.Crc, "local crc:", crc)

	return ErrPkgCrcUnmatch
}

func AddDiskErrs(d *DiskInfo, errMsg string, errFlag int) {
	if !IsDiskErr(errMsg) {
		return
	}
	addDiskErrs(d, errFlag)
	log.GetLog().LogError("DiskErr: path:", d.Path, " err:", errMsg)
}

func addDiskErrs(d *DiskInfo, errFlag int) {
	if errFlag == WriteFlag {
		d.AddWriteErrs()
	} else if errFlag == ReadFlag {
		d.AddReadErrs()
	}
	if d.GetDiskErrs() >= d.MaxErrs && d.GetStatus() != BadDisk {
		d.SetStatus(BadDisk)
		ump.Alarm(UmpKeyDisk, fmt.Sprintf("node %s disk %s %s", LocalAddr, d.Path, UmpDetailDiskErr))
	}
}

func (s *DataNode) CleanConn(p *Packet) {
	if (p.IsGroupNetErr() || p.isSent) && p.nextConn != nil {
		p.nextConn.Close()
		p.nextConn = nil
	}
	if p.nextConn != nil {
		log.GetLog().LogDebug("CleanConn rId:", p.ReqId, "op:", p.Opcode, "vId:", p.VolId, "size:", p.Size, "addr:", p.nextConn.LocalAddr().String())
	}
	s.connPool.PutConn(p.nextAddr, p.nextConn)
}

func (s *DataNode) GetMasterAddr() (addr string) {
	s.masterLock.RLock()
	addr = s.masterAddr
	log.GetLog().LogDebug("server, GetMasterAddr:", addr)
	s.masterLock.RUnlock()

	return
}

func (s *DataNode) SetMasterAddr(addr string) {
	s.masterLock.Lock()
	s.masterAddr = addr
	log.GetLog().LogDebug("server, SetMasterAddr:", addr)
	s.masterLock.Unlock()
}
