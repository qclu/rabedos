package datanodeIdc

import (
	"errors"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"utils/rbdlogger"
)

var (
	ErrVolCfgUnmatch = errors.New("VolCfgUnmatchErr")
	ErrVolIsMissing  = errors.New("VolIsMissingErr")
	ErrVolExist      = errors.New("VolExistErr")
)

type VolInfo struct {
	totalReadLatency  int32
	readCount         int32
	totalWriteLatency int32
	writeCount        int32
	readBytes         int64
	writeBytes        int64
	used              int64
	followHosts       []string
	lock              sync.RWMutex

	Status int32
	Id     uint32
	Total  int
	Path   string
	Name   string
	Store  Store
}

func NewVolInfo(srvId, vId uint32, size int, path, name string, storeType uint8) (vol *VolInfo, err error) {
	vol = &VolInfo{Status: ReadWriteDisk, Total: size, Id: vId, Path: path, Name: name}
	vol.Store, err = vol.NewStore(srvId, storeType, NewStoreMode)

	return
}

func (vol *VolInfo) GetIdStr() string {
	return strconv.Itoa(int(vol.Id))
}

func (vol *VolInfo) AddUsedSpace(delta int64) {
	atomic.AddInt64(&vol.used, delta)
}

func (vol *VolInfo) GetUsedSpace() int64 {
	return atomic.LoadInt64(&vol.used)
}

func (vol *VolInfo) RecordReadInfo(latency int32, size int64) {
	atomic.AddInt32(&vol.readCount, 1)
	atomic.AddInt32(&vol.totalReadLatency, latency)
	atomic.AddInt64(&vol.readBytes, size)
}

func (vol *VolInfo) CleanReadRecord() {
	atomic.StoreInt32(&vol.readCount, 0)
	atomic.StoreInt32(&vol.totalReadLatency, 0)
	atomic.StoreInt64(&vol.readBytes, 0)
}

func (vol *VolInfo) GetReadRecord() (latency int32, bytes int64) {
	count := atomic.LoadInt32(&vol.readCount)
	totalLatency := atomic.LoadInt32(&vol.totalReadLatency)
	bytes = atomic.LoadInt64(&vol.readBytes)
	if count != 0 {
		latency = totalLatency / count
	}

	return
}

func (vol *VolInfo) RecordWriteInfo(latency int32, size int64) {
	atomic.AddInt32(&vol.writeCount, 1)
	atomic.AddInt32(&vol.totalWriteLatency, latency)
	atomic.AddInt64(&vol.writeBytes, size)
}

func (vol *VolInfo) CleanWriteRecord() {
	atomic.StoreInt32(&vol.writeCount, 0)
	atomic.StoreInt32(&vol.totalWriteLatency, 0)
	atomic.StoreInt64(&vol.writeBytes, 0)
}

func (vol *VolInfo) GetWriteRecord() (latency int32, bytes int64) {
	count := atomic.LoadInt32(&vol.writeCount)
	totalLatency := atomic.LoadInt32(&vol.totalWriteLatency)
	bytes = atomic.LoadInt64(&vol.writeBytes)
	if count != 0 {
		latency = totalLatency / count
	}

	return
}

func (vol *VolInfo) DelInfo() {
	vol.Store.DeleteStore()
}

func (vol *VolInfo) GetFds() (count int) {
	return vol.Store.GetStoreActiveFiles()
}

func (vol *VolInfo) HalveFds() {
	vol.Store.CloseStoreActiveFiles()
}

func (vol *VolInfo) SetStatus(diskStatus int32) {
	vol.lock.Lock()

	if diskStatus == ReadWriteDisk && (vol.Total-int(vol.used)) <= 0 {
		vol.Status = ReadOnlyDisk
		log.GetLog().LogInfo("SetStatus, path:", vol.Path, " id:", vol.Id, " status:", ReadOnlyDisk, " used:", vol.used, " total:", vol.Total)
	} else if diskStatus != ReadOnlyDisk {
		vol.Status = diskStatus
	}

	vol.lock.Unlock()
}

func (vol *VolInfo) GetStatus() (status int32) {
	vol.lock.RLock()
	status = vol.Status
	vol.lock.RUnlock()

	return
}

func (vol *VolInfo) GetTotalSpace() (int, error) {
	arr := strings.Split(vol.Name, VolNameSplit)
	if len(arr) < 2 {
		return 0, ErrVolCfgUnmatch
	}

	return strconv.Atoi(arr[1])
}

func (vol *VolInfo) GetId() (uint32, error) {
	arr := strings.Split(vol.Name, VolNameSplit)
	if len(arr) != 2 {
		return 0, ErrVolCfgUnmatch
	}
	id, err := strconv.Atoi(arr[0])

	return uint32(id), err
}

func JoinVolName(vId uint32, vSize int) (vName string) {
	return strconv.Itoa(int(vId)) + VolNameJoin + strconv.Itoa(vSize)
}

func (vol *VolInfo) NewStore(srvId uint32, storeType uint8, startStoreMode bool) (store Store, err error) {
	name := path.Join(vol.Path, vol.Name)

	if storeType == ChunkStoreType {
		store, err = NewChunkStore(name, vol.Total, startStoreMode)
	} else if storeType == ExtentStoreType {
		store, err = NewExtentStore(srvId, name, startStoreMode)
	}
	if err != nil {
		vol.SetStatus(BadDisk)
		log.GetLog().LogError("failed to new store, path:", name, " size:", vol.Total, " err:", err)
		return
	}

	return
}
