package datanodeIdc

import (
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	. "datanode/util"
	"proto"
	"utils/rbdlogger"
)

var (
	ErrLackOfGoal      = errors.New("LackOfGoalErr")
	ErrVolOnBadDisk    = errors.New("VolOnBadDiskErr")
	ErrGoalInfoUnmatch = errors.New("GoalInfoUnmatchErr")
)

const (
	LeastGoalNum               = 1
	GetUnavailChunkMaxTryTimes = 3
)

type ChunkInspector struct {
	intervalTime int
	smMgr        *SpaceMapManager
	clusterId    string
	localAddr    string
	masterAddr   string
	masterLock   sync.RWMutex
}

type ChunkInfo struct {
	CId  int
	Size int64
	addr string
}

type VolDuplication struct {
	volId      uint32
	chunksInfo []*ChunkInfo
	addr       string
}

type VolLeaderFollower struct {
	VolStatus int32
	VolId     int
	VolGoal   int
	VolHosts  []string
}

func NewChunkInspector(intervalTime int, cId, localAddr, masterAddr string, smMgr *SpaceMapManager) *ChunkInspector {
	return &ChunkInspector{
		clusterId:    cId,
		localAddr:    localAddr,
		masterAddr:   masterAddr,
		intervalTime: intervalTime,
		smMgr:        smMgr,
	}
}

func (insptor *ChunkInspector) InspectInGlobal() {
	log.GetLog().LogInfo("Start: InspectInGlobal")

	for {
		for _, d := range insptor.smMgr.disks {
			log.GetLog().LogInfo(LogInspector+"InspectInGlobal d:", d.Path, " vols:", d.VolIds)
			volIds := d.GetVolIds()

			for _, vId := range volIds {
				vol, ok := insptor.smMgr.IsExistVol(uint32(vId))
				if !ok {
					continue
				}

				fHosts, err := insptor.InspectAvailChunksOnVol(vol)
				if err != nil || fHosts == nil {
					continue
				}
				insptor.InspectUnavailChunkOnVol(vol, fHosts)
			}
		}
		time.Sleep(time.Duration(insptor.intervalTime) * time.Second)
	}

	ump.Alarm(UmpKeyInspector, UmpDetailInspector)
}

func (insptor *ChunkInspector) InspectAvailChunksOnVol(vol *VolInfo) ([]string, error) {
	if vol.GetStatus() == BadDisk {
		log.GetLog().LogWarn(LogInspector+"the vol couldn't repair vId:"+vol.GetIdStr()+" status:", vol.GetStatus())
		return nil, nil
	}

	leader, fHosts, err := insptor.GetVolGoalsInfo(vol)
	if err != nil {
		log.GetLog().LogError(LogInspector+"GetVolGoalsInfo vId:"+vol.GetIdStr()+" err:", err)
		return nil, err
	}
	if !leader {
		return nil, nil
	}

	if !CompareFollowHosts(vol.followHosts, fHosts) {
		if vol.followHosts == nil {
			log.GetLog().LogInfo(LogInspector + "InspectAvailChunksOnVol vId:" + vol.GetIdStr() + " local host is nil.")
		} else {
			insptor.ChangeChunksStatus(vol)
		}
		vol.followHosts = fHosts
	}

	return fHosts, nil
}

func (insptor *ChunkInspector) GetVolGoalsInfo(vol *VolInfo) (leader bool, fHosts []string, err error) {
	addr := insptor.GetMasterAddr()
	_, volHostsBuf := PostToMaster(data, "http://"+addr+"/node/getvol?vol="+strconv.Itoa(int(vol.Id))+
		"&cluster="+insptor.clusterId, "getvol")
	volHosts := new(VolLeaderFollower)
	if err = json.Unmarshal(volHostsBuf, &volHosts); err != nil {
		return
	}

	log.GetLog().LogInfo(LogInspector+"GetVolGoalsInfo vId:"+vol.GetIdStr()+" goals:", volHosts.VolGoal, " volHosts:",
		volHosts.VolHosts, " unavali chunks:", vol.Store.GetUnAvaliChanLen())
	if len(volHosts.VolHosts) >= LeastGoalNum && volHosts.VolHosts[0] != insptor.localAddr {
		return
	}
	if volHosts.VolGoal < LeastGoalNum || len(volHosts.VolHosts) < volHosts.VolGoal {
		return false, nil, ErrLackOfGoal
	}
	if volHosts.VolStatus == BadDisk || vol.GetStatus() == BadDisk {
		return false, nil, ErrVolOnBadDisk
	}

	return true, volHosts.VolHosts[1:], nil
}

func (insptor *ChunkInspector) ChangeChunksStatus(vol *VolInfo) {
	for i := 0; i < ChunkCount; i++ {
		cId, err := vol.Store.GetAvaliChunk()
		if err != nil {
			break
		}
		vol.Store.PutUnAvaliChunk(cId)
		log.GetLog().LogWarn(LogInspector+"ChangeChunksStatus, put unavailchunk vId:"+vol.GetIdStr()+" CId:", cId)
	}
}

func (insptor *ChunkInspector) InspectUnavailChunkOnVol(vol *VolInfo, fHosts []string) {
	start := time.Now()

	if vol.GetStatus() == BadDisk || vol.Store.GetUnAvaliChanLen() == 0 {
		log.GetLog().LogDebug(LogInspector+"the vol needn't repair vId:"+vol.GetIdStr()+" status:", vol.GetStatus())
		return
	}

	fs := make([]*VolDuplication, len(fHosts))
	for i, h := range fHosts {
		fs[i] = &VolDuplication{volId: vol.Id, addr: h}
	}

	ok, err := insptor.RepairChunks(vol, fs)
	if err != nil {
		log.GetLog().LogError(LogInspector+"RepairChunks vId:"+vol.GetIdStr()+" err:", err)
	}
	log.GetLog().LogInfo(LogInspector+"vId:"+vol.GetIdStr()+" repair ok is", ok, " take time:", TakeTime(&start), "ms end.")
}

func (insptor *ChunkInspector) GetLocalUnavailChunksInfo(vol *VolInfo) (volduplication *VolDuplication, cIds []int, err error) {
	log.GetLog().LogInfo(LogInspector + "GetLocalUnavailChunksInfo, vId:" + vol.GetIdStr())
	failedTimes, cId, offset := 0, 0, int64(0)
	cIds = make([]int, 0)
	volduplication = &VolDuplication{volId: vol.Id, chunksInfo: make([]*ChunkInfo, 0), addr: insptor.localAddr}

	for failedTimes < GetUnavailChunkMaxTryTimes {
		cId, err = vol.Store.GetUnAvaliChunk()
		if err != nil {
			err = nil
			break
		}
		cIds = append(cIds, cId)
		offset, err = vol.Store.GetWatermark(uint32(cId))
		if err != nil {
			AddDiskErrs(insptor.smMgr.disks[vol.Path], err.Error(), ReadFlag)
			failedTimes++
			vol.Store.PutUnAvaliChunk(cId)
			continue
		}
		err = nil
		cInfo := &ChunkInfo{CId: cId, Size: offset, addr: insptor.localAddr}
		volduplication.chunksInfo = append(volduplication.chunksInfo, cInfo)
		log.GetLog().LogInfo(LogInspector+"GetLocalUnavailChunksInfo, vId:"+strconv.Itoa(int(volduplication.volId))+" cId:",
			cInfo.CId, " offset:", cInfo.Size)
	}

	return
}

func GetSuitChunk(cId int, chunks []*ChunkInfo) *ChunkInfo {
	for _, cInfo := range chunks {
		if cInfo != nil && cInfo.CId == cId {
			return cInfo
		}
	}

	return &ChunkInfo{CId: cId, Size: 0}
}

func GetRemoteChunksInfo(volduplication *VolDuplication, unavailChunkInfos []*ChunkInfo) (err error) {
	pkg := NewPacket()
	pkg.Opcode = OpGetAllWatermark
	pkg.VolId = uint32(volduplication.volId)
	if err = Interact(pkg, volduplication.addr); err != nil {
		log.GetLog().LogError(LogInspector+"GetRemoteChunksInfo, failed Interact, vId:"+strconv.Itoa(int(volduplication.volId))+
			" addr:", volduplication.addr, " err:", err)
		return
	}

	remoteChunks := make([]*ChunkInfo, 0)
	volduplication.chunksInfo = make([]*ChunkInfo, len(unavailChunkInfos))
	if err = json.Unmarshal(pkg.Data[:pkg.Size], &remoteChunks); err != nil {
		log.GetLog().LogError(LogInspector+"GetRemoteChunksInfo, failed Unmarshal, vId:"+strconv.Itoa(int(volduplication.volId))+
			" data:", pkg.Data[:pkg.Size], " addr:", volduplication.addr, " err:", err)
		return
	}

	for i, unavailC := range unavailChunkInfos {
		cInfo := GetSuitChunk(unavailC.CId, remoteChunks)
		volduplication.chunksInfo[i] = &ChunkInfo{CId: cInfo.CId, Size: cInfo.Size, addr: volduplication.addr}
		log.GetLog().LogInfo(LogInspector+"GetRemoteChunksInfo, vId:"+strconv.Itoa(int(volduplication.volId))+" addr:",
			volduplication.addr, " cId:", cInfo.CId, " offset:", cInfo.Size)
	}

	return
}

func (insptor *ChunkInspector) RepairChunks(vol *VolInfo, followers []*VolDuplication) (ok bool, err error) {
	var okCIds []int

	if len(followers) == LeastGoalNum {
		vol.Store.SetAllChunkAvali()
		log.GetLog().LogWarn(LogInspector+"RepairChunks, put all chunks available vId:", vol.Id)
		return
	}

	Leader, failedCIds, err := insptor.GetLocalUnavailChunksInfo(vol)
	if err != nil || len(failedCIds) == 0 {
		insptor.ClearChunks(vol, nil, failedCIds)
		return
	}

	for _, f := range followers {
		if err = GetRemoteChunksInfo(f, Leader.chunksInfo); err != nil {
			break
		}
	}
	if err == nil {
		okCIds, failedCIds = insptor.CompareRepairChunks(Leader, followers)
	}

	insptor.ClearChunks(vol, okCIds, failedCIds)
	if err == nil && len(failedCIds) == 0 {
		ok = true
	}

	return
}

func MaxSizeChunk(leader *ChunkInfo, cs []*ChunkInfo) (maxSizeChunk *ChunkInfo, csQ []*ChunkInfo) {
	maxSizeChunk = leader

	csQ = make([]*ChunkInfo, len(cs))
	for i, c := range cs {
		if c.Size <= maxSizeChunk.Size {
			csQ[i] = c
			continue
		}
		csQ[i], maxSizeChunk = maxSizeChunk, c
	}

	// str := ""
	// var cc *ChunkInfo
	// for _, c := range csQ {
	// 	str += "addr:" + c.addr + " size:" + strconv.Itoa(int(c.Size)) + "\t"
	// 	cc = c
	// }
	// if maxSizeChunk.Size == cc.Size {
	// 	return
	// }
	// str += "addr:" + maxSizeChunk.addr + " size:" + strconv.Itoa(int(maxSizeChunk.Size)) + "\t"
	// log.GetLog().LogInfo("MaxSizeChunk, cId:", maxSizeChunk.CId, str)

	return
}

func NotifyRepair(vId uint32, dst, src *ChunkInfo) (err error) {
	if dst.Size == src.Size {
		return
	}
	if dst == nil || src == nil || src.addr == "" {
		return ErrGoalInfoUnmatch
	}

	bufSize := src.Size - dst.Size
	pkg := NewPacket()
	pkg.StoreType = ChunkStoreType
	pkg.Opcode = OpRepair
	pkg.VolId = vId
	pkg.FileId = uint32(src.CId)
	pkg.Offset = int64(dst.Size)
	pkg.Data = []byte(src.addr + RepairDataSplit + strconv.Itoa(int(bufSize)))
	pkg.Size = uint32(len(pkg.Data))
	if err = Interact(pkg, dst.addr); err != nil {
		return
	}

	return
}

func (insptor *ChunkInspector) CompareRepairChunks(Leader *VolDuplication, followers []*VolDuplication) (okCIds, failedCIds []int) {
	log.GetLog().LogInfo(LogInspector + "CompareRepairChunks, vId:" + strconv.Itoa(int(Leader.volId)))
	var err error

	for _, cInfo := range Leader.chunksInfo {
		cs := make([]*ChunkInfo, len(followers))
		for i, f := range followers {
			cs[i] = GetSuitChunk(cInfo.CId, f.chunksInfo)
		}
		maxSizeChunk, csQ := MaxSizeChunk(cInfo, cs)
		for _, c := range csQ {
			if err = NotifyRepair(Leader.volId, c, maxSizeChunk); err != nil {
				break
			}
		}
		if err != nil {
			failedCIds = append(failedCIds, cInfo.CId)
			log.GetLog().LogError(LogInspector+"CompareRepairChunks, vId:"+strconv.Itoa(int(Leader.volId))+" err:", err)
			continue
		}
		okCIds = append(okCIds, cInfo.CId)
	}

	return
}

func (insptor *ChunkInspector) ClearChunks(vol *VolInfo, okCIds, failedCIds []int) {
	for _, cId := range failedCIds {
		vol.Store.PutUnAvaliChunk(cId)
		log.GetLog().LogWarn(LogInspector+"RepairChunks, put unavailchunk vId:"+vol.GetIdStr()+" CId:", cId)
	}
	for _, cId := range okCIds {
		vol.Store.PutAvaliChunk(cId)
		log.GetLog().LogWarn(LogInspector+"RepairChunks, put availchunk vId:"+vol.GetIdStr()+" CId:", cId)
	}
}

func CompareFollowHosts(localHosts, remoteHosts []string) bool {
	if localHosts == nil {
		return false
	}

	if len(localHosts) != len(remoteHosts) {
		return false
	}

	for i, h := range localHosts {
		if remoteHosts[i] != h {
			return false
		}
	}

	return true
}

func (insptor *ChunkInspector) GetMasterAddr() (addr string) {
	insptor.masterLock.RLock()
	addr = insptor.masterAddr
	log.GetLog().LogDebug("insptor, GetMasterAddr:", addr)
	insptor.masterLock.RUnlock()

	return
}

func (insptor *ChunkInspector) SetMasterAddr(addr string) {
	insptor.masterLock.Lock()
	insptor.masterAddr = addr
	log.GetLog().LogDebug("insptor, SetMasterAddr:", addr)
	insptor.masterLock.Unlock()
}
