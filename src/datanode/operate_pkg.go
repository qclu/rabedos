package datanodeIdc

import (
	"encoding/json"
	"hash/crc32"
	"net"
	"strconv"
	"time"
	"utils/rbdlogger"
)

func (s *DataNode) operatePacket(pkg *Packet, c *net.TCPConn) {
	switch pkg.Opcode {
	case OpCreateFile:
		s.createFile(pkg)
	case OpWrite:
		s.append(pkg)
	case OpRead:
		s.read(pkg)
	case OpRepairRead:
		s.repairRead(pkg)
	case OpStreamRead:
		s.streamRead(pkg, c)
	case OpMarkDelete:
		s.markDel(pkg)
	case OpRepair:
		s.repairChunk(pkg)
	case OpGetWatermark:
		s.getWatermark(pkg)
	case OpGetAllWatermark:
		s.getAllWatermark(pkg)
	case OpFlowInfo:
		s.GetFlowInfo(pkg)
	default:
		pkg.PackErrorReply(NoFlag, ErrorUnknowOp.Error(), ErrorUnknowOp.Error()+strconv.Itoa(int(pkg.Opcode)), nil)
	}

	return
}

func (s *DataNode) createFile(pkg *Packet) {
	start := time.Now()

	if err := pkg.vol.Store.Create(pkg.FileId); err != nil {
		pkg.PackErrorReply(WriteFlag, LogCreateFile, err.Error(), s.smMgr.disks[pkg.vol.Path])
		return
	}
	pkg.PackOkReply()
	log.GetLog().LogWrite(LogCreateFile, pkg.GetInfo(GetPackInfoOnlyId), "t:", TakeTime(&start), " ms")

	return
}

func (s *DataNode) markDel(pkg *Packet) {
	start := time.Now()

	if err := pkg.vol.Store.MarkDelete(pkg.FileId, pkg.Offset, int64(pkg.Size)); err != nil {
		pkg.PackErrorReply(WriteFlag, LogMarkDel, err.Error(), s.smMgr.disks[pkg.vol.Path])
		return
	}
	pkg.PackOkReply()
	log.GetLog().LogWrite(LogMarkDel, pkg.GetInfo(GetPackInfoOnlyId), "crc:", pkg.Crc, "t:", TakeTime(&start), " ms")

	return
}

func (s *DataNode) append(pkg *Packet) {
	start := time.Now()

	//TODO:
	// if err = s.UpdateCrc(pkg); err != nil {
	// 	pkg.PackErrorReply(AppendUpdateCrcError,  err.Error())
	// }
	size := pkg.Size
	err := pkg.vol.Store.Write(pkg.FileId, int64(pkg.Offset), int64(pkg.Size), pkg.Data, pkg.Crc, !RepairOpFlag)
	if err != nil {
		pkg.PackErrorReply(WriteFlag, LogWrite, err.Error(), s.smMgr.disks[pkg.vol.Path])
		return
	}

	t := TakeTime(&start)
	pkg.vol.AddUsedSpace(int64(size))
	pkg.vol.RecordWriteInfo(t, int64(size))
	pkg.PackOkReply()
	log.GetLog().LogWrite(LogWrite, pkg.GetInfo(GetPackInfoOnlyId), "crc:", pkg.Crc, "off:",
		pkg.Offset, "size:", size, "t:", t, "ms")

	return
}

func (s *DataNode) rd(pkg *Packet, sucPrefix, errPrefix string, repair bool) (err error) {
	start := time.Now()

	pkg.Crc, err = pkg.vol.Store.Read(pkg.FileId, int64(pkg.Offset), int64(pkg.Size), pkg.Data, repair)
	if err != nil {
		log.GetLog().LogInfo("read-store-error", err.Error())
		pkg.PackErrorReply(ReadFlag, errPrefix, err.Error(), s.smMgr.disks[pkg.vol.Path])
		return
	}
	t := TakeTime(&start)
	pkg.vol.RecordReadInfo(t, int64(pkg.Size))
	log.GetLog().LogRead(sucPrefix, pkg.GetInfo(GetPackInfoOnlyId), "crc:", pkg.Crc, "t:", t, "ms", "size:", pkg.Size)

	return
}

func (s *DataNode) read(pkg *Packet) {
	if err := s.rd(pkg, LogRead, LogRead, !RepairOpFlag); err == nil {
		pkg.PackOkReadReply()
	}

	return
}

func (s *DataNode) repairRead(pkg *Packet) {
	if err := s.rd(pkg, LogRepairRead, LogRepairRead, RepairOpFlag); err == nil {
		pkg.Crc = crc32.ChecksumIEEE(pkg.Data[:pkg.Size])
		pkg.PackOkReadReply()
	}

	return
}

func (s *DataNode) streamRead(pkg *Packet, c *net.TCPConn) {
	start := time.Now()
	sucPrefix := LogStreamRead + strconv.Itoa(int(pkg.FileId))

	if pkg.Size <= PkgExtentDataMaxSize {
		if err := s.rd(pkg, sucPrefix, LogStreamRead, !RepairOpFlag); err == nil {
			pkg.PackOkReadReply()
		}
		return
	}

	//write body
	totalSize, last, rNo := uint32(pkg.Size), false, 1
	pkg.Offset, pkg.Size = int64(pkg.Offset), uint32(PkgExtentDataMaxSize)
	for totalSize > 0 {
		err := s.rd(pkg, sucPrefix+"no."+strconv.Itoa(rNo), LogStreamRead, !RepairOpFlag)
		if err != nil {
			pkg.PackErrorReply(ReadFlag, LogStreamRead, err.Error(), s.smMgr.disks[pkg.vol.Path])
			return
		}
		pkg.PackOkReadReply()
		if last {
			break
		}
		if err := pkg.WriteToConn(c, WriteDeadlineTime, NotFreeBodySpace); err != nil {
			pkg.PackErrorReply(NoFlag, LogStreamRead, err.Error(), nil)
			return
		}

		rNo++
		pkg.Offset += PkgExtentDataMaxSize
		totalSize -= pkg.Size
		if totalSize <= PkgExtentDataMaxSize && totalSize > 0 {
			pkg.Size = totalSize
			last = true
		}
	}
	log.GetLog().LogRead(sucPrefix, pkg.GetInfo(GetPackInfoOnlyId), "crc:", pkg.Crc, "last one, t:", TakeTime(&start), "ms")

	return
}

func (s *DataNode) getWatermark(pkg *Packet) {
	start := time.Now()

	size, err := pkg.vol.Store.GetWatermark(pkg.FileId)
	if err != nil {
		pkg.PackErrorReply(ReadFlag, LogGetWm, err.Error(), s.smMgr.disks[pkg.vol.Path])
		return
	}
	pkg.PackOkGetWatermarkReply(size)
	log.GetLog().LogRead(LogGetWm, pkg.GetInfo(GetPackInfoOnlyId), "wm:", size, "t:", TakeTime(&start), "ms")

	return
}

func (s *DataNode) getAllWatermark(pkg *Packet) {
	var buf []byte
	start := time.Now()
	errFlag := NoFlag

	chunks, err := pkg.vol.Store.GetAllWatermark()
	if err != nil {
		errFlag = ReadFlag
	} else {
		buf, err = json.Marshal(chunks)
	}
	if err != nil {
		pkg.PackErrorReply(errFlag, LogGetAllWm, err.Error(), s.smMgr.disks[pkg.vol.Path])
		return
	}
	pkg.PackOkGetInfoReply(buf)
	log.GetLog().LogRead(LogGetAllWm, pkg.GetInfo(GetPackInfoOnlyId), "len:", len(chunks), "t:", TakeTime(&start), "ms")

	return
}

func (s *DataNode) repairChunk(pkg *Packet) {
	start := time.Now()
	req := NewRepairePacket(pkg.FileId, pkg.ReqId, pkg.vol.Id, pkg.FileId)

	addr, size, err := GetRepairData(pkg.Data[:pkg.Size])
	if err != nil {
		pkg.PackErrorReply(NoFlag, LogRepair, err.Error(), s.smMgr.disks[pkg.vol.Path])
	}

	errFlag, err := DoRepair(pkg.Offset, pkg.Offset+int64(size), PkgRepairDataMaxSize, req, pkg.vol, addr)
	if err != nil {
		pkg.PackErrorReply(errFlag, LogRepair, err.Error()+" size:"+strconv.Itoa(size)+
			" t:"+strconv.Itoa(int(TakeTime(&start)))+"ms", s.smMgr.disks[pkg.vol.Path])
	} else {
		pkg.PackOkReply()
		log.GetLog().LogWrite(LogRepair, pkg.GetInfo(GetPackInfoOnlyId), "last one, t:", TakeTime(&start), "ms")
	}

	return
}

func (s *DataNode) GetFlowInfo(pkg *Packet) {
	in, out := s.stats.GetFlow()
	flow := strconv.Itoa(int(in/MB)) + InOutFlowSplit + strconv.Itoa(int(out/MB))
	pkg.PackOkGetInfoReply([]byte(flow))

	return
}
