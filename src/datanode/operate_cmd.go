package datanodeIdc

import (
	"errors"
	"strconv"
	"time"
	"utils/rbdlogger"
)

var (
	ErrorUnknowOp = errors.New("UnknowOpcodeErr")
)

func (s *DataNode) processCmd(cmd *Cmd) (resp *CmdResp) {
	log.GetLog().LogDebug("ReportHeartbeat, code:", cmd.Code, cmd.GetIdInfo())
	switch cmd.Code {
	case OpCreateVol:
		s.createVol(cmd)
	case OpDeleteVol:
		s.deleteVol(cmd)
	case OpDeleteFile:
		s.deleteFile(cmd)
	case OpReplicationFile:
		if s.storeType == ChunkStoreType {
			cmd.PackOkReply(NoFlag, LogRepair, 0)
			break
		}
		s.repaireExtent(cmd)
	case OpVolSnapshot:
		s.volSnapshot(cmd)
	default:
		cmd.PackErrorReply(NoFlag, ErrorUnknowOp.Error(), ErrorUnknowOp.Error()+strconv.Itoa(int(cmd.Code)), nil)
	}

	return cmd.Resp
}

func (s *DataNode) chooseSuitDisk(size int) (has bool, diskPath string) {
	var maxAvail int64

	for _, d := range s.smMgr.disks {
		avail := d.GetCreateVolsAvailSpace()
		if avail < int64(size) {
			d.SetStatus(ReadOnlyDisk)
			continue
		}
		if maxAvail < avail && d.GetRealAvailSpace() >= int64(size) && d.GetStatus() == ReadWriteDisk {
			maxAvail = avail
			diskPath = d.Path
			log.GetLog().LogInfo("chooseSuitDisk, tmpAvail:", maxAvail/GB, "G path:", diskPath)
		}
	}
	if maxAvail >= int64(size) {
		has = true
		log.GetLog().LogInfo("chooseSuitDisk, maxAvail:", maxAvail/GB, "G path:", diskPath)
	} else {
		log.GetLog().LogWarn("chooseSuitDisk, maxAvail:", maxAvail/GB, "G path:", diskPath)
	}

	return
}

func (s *DataNode) createVol(cmd *Cmd) {
	var createOk bool
	start := time.Now()

	if _, ok := s.smMgr.IsExistVol(cmd.VolId); ok {
		cmd.PackOkReply(WriteFlag, LogCreateVol+ErrVolExist.Error(), TakeTime(&start))
		return
	}
	for i := 0; i < ChooseDiskTimes; i++ {
		has, path := s.chooseSuitDisk(cmd.VolSize)
		if !has {
			continue
		}
		vol, err := NewVolInfo(uint32(s.id), cmd.VolId, cmd.VolSize, path, JoinVolName(cmd.VolId, cmd.VolSize), s.storeType)
		if err != nil {
			AddDiskErrs(s.smMgr.disks[path], err.Error(), WriteFlag)
			log.GetLog().LogError(LogCreateVol+":"+cmd.GetIdInfo()+" err:", err)
			continue
		}
		s.smMgr.AddVol(path, vol)
		createOk = true
		break
	}

	if !createOk {
		cmd.PackErrorReply(NoFlag, LogCreateVol, CreateVolErr, nil)
		return
	}
	cmd.PackOkReply(WriteFlag, LogCreateVol, TakeTime(&start))
}

func (s *DataNode) deleteVol(cmd *Cmd) {
	start := time.Now()

	vol, ok := s.smMgr.IsExistVol(cmd.VolId)
	if ok {
		s.smMgr.DelVol(vol)
	}

	cmd.PackOkReply(WriteFlag, LogDelVol, TakeTime(&start))
}

func (s *DataNode) deleteFile(cmd *Cmd) {
	start := time.Now()
	vol, ok := s.smMgr.IsExistVol(cmd.VolId)
	if !ok {
		cmd.PackErrorReply(NoFlag, LogDelFile, ErrVolIsMissing.Error(), nil)
		return
	}

	eId, err := strconv.Atoi(cmd.VolFileId)
	if err != nil {
		cmd.PackErrorReply(NoFlag, LogDelFile, err.Error(), s.smMgr.disks[vol.Path])
		return
	}
	fsize, _ := vol.Store.GetWatermark(uint32(eId))
	fsize = fsize + ExtentHeaderSize
	if err := vol.Store.Delete(uint32(eId)); err != nil {
		cmd.PackErrorReply(WriteFlag, LogDelFile, err.Error(), s.smMgr.disks[vol.Path])
		return
	}
	vol.AddUsedSpace(-fsize)
	cmd.PackOkReply(WriteFlag, LogDelFile, TakeTime(&start))

	return
}

func (s *DataNode) volSnapshot(cmd *Cmd) {
	start := time.Now()
	vol, ok := s.smMgr.IsExistVol(cmd.VolId)
	if !ok {
		cmd.PackErrorReply(NoFlag, LogVolSnapshot, ErrVolIsMissing.Error(), nil)
		return
	}

	filesInfo, err := vol.Store.Snapshot()
	if err != nil {
		if disk := s.smMgr.disks[vol.Path]; disk != nil {
			AddDiskErrs(disk, err.Error(), ReadFlag)
		}
		cmd.PackErrorReply(ReadFlag, LogVolSnapshot, err.Error(), s.smMgr.disks[vol.Path])
		return
	}

	cmd.PackOkReply(ReadFlag, LogVolSnapshot, TakeTime(&start))
	cmd.Resp.VolSnapshot = filesInfo
}

func (s *DataNode) repaireExtent(cmd *Cmd) {
	start := time.Now()
	vol, ok := s.smMgr.IsExistVol(cmd.VolId)
	if !ok {
		cmd.PackErrorReply(NoFlag, LogRepair, ErrVolIsMissing.Error(), nil)
		return
	}

	fId, err := strconv.Atoi(cmd.VolFileId)
	if err != nil {
		cmd.PackErrorReply(NoFlag, LogRepair, err.Error(), s.smMgr.disks[vol.Path])
		return
	}
	if vol.Store.FileExist(uint32(fId)) {
		cmd.PackOkReply(WriteFlag, LogRepair, TakeTime(&start))
	}

	if err := vol.Store.Create(uint32(fId)); err != nil {
		cmd.PackErrorReply(WriteFlag, LogRepair, err.Error(), s.smMgr.disks[vol.Path])
		return
	}

	wmPkg := NewGetWatermarkPacket(uint32(fId), GetReqId(), vol.Id)
	wmPkg.StoreType = ExtentStoreType
	err = Interact(wmPkg, cmd.TargetAddr)
	if err != nil {
		cmd.PackErrorReply(NoFlag, LogRepair, err.Error(), s.smMgr.disks[vol.Path])
		return
	}

	pkg := NewRepairePacket(uint32(fId), GetReqId(), vol.Id, 0)
	pkg.StoreType = ExtentStoreType
	errFlag, err := DoRepair(0, wmPkg.Offset, PkgRepairDataMaxSize, pkg, vol, cmd.TargetAddr)
	if err != nil {
		cmd.PackErrorReply(errFlag, LogRepair, err.Error(), s.smMgr.disks[vol.Path])
	} else {
		cmd.PackOkReply(WriteFlag, LogRepair, TakeTime(&start))
	}

	return
}
