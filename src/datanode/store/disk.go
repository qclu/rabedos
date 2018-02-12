package datanodeIdc

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

const (
	BadDisk       = -1
	ReadOnlyDisk  = 1
	ReadWriteDisk = 2
)

var (
	ErrorDiskFreeSpaceLess = errors.New("DiskFreeSpaceLessErr")
)

type DiskInfo struct {
	status       int32
	readErrs     int32
	writeErrs    int32
	realAvail    int64
	volsAvail    int64
	volAllocLock sync.RWMutex

	MinRestSize int32
	MaxErrs     int32
	Total       int64
	Path        string
	VolIds      []uint32
}

func NewDiskInfo(path string, minRestSize, maxErrs int32) (disk *DiskInfo, err error) {
	disk = &DiskInfo{status: ReadWriteDisk, MinRestSize: minRestSize, MaxErrs: maxErrs, Path: path}

	var statsInfo syscall.Statfs_t
	if err = syscall.Statfs(path, &statsInfo); err != nil {
		disk.SetStatus(BadDisk)
		return
	}

	disk.Total = int64(statsInfo.Blocks) * statsInfo.Bsize
	if disk.Total < int64(disk.MinRestSize)*GB {
		return nil, ErrorDiskFreeSpaceLess
	}
	disk.realAvail, disk.volsAvail = disk.Total-int64(disk.MinRestSize)*GB, disk.Total-int64(disk.MinRestSize)*GB

	return
}

func (d *DiskInfo) AddReadErrs() {
	atomic.AddInt32(&d.readErrs, 1)
}

func (d *DiskInfo) GetReadErrs() int32 {
	return atomic.LoadInt32(&d.readErrs)
}

func (d *DiskInfo) AddWriteErrs() {
	atomic.AddInt32(&d.writeErrs, 1)
}

func (d *DiskInfo) GetWriteErrs() int32 {
	return atomic.LoadInt32(&d.writeErrs)
}

func (d *DiskInfo) GetDiskErrs() int32 {
	return atomic.LoadInt32(&d.readErrs) + atomic.LoadInt32(&d.writeErrs)
}

func (d *DiskInfo) SetStatus(status int32) {
	atomic.StoreInt32(&d.status, status)
}

func (d *DiskInfo) GetStatus() (status int32) {
	return atomic.LoadInt32(&d.status)
}

func (d *DiskInfo) SetRealAvailSpace(avail int64) {
	atomic.StoreInt64(&d.realAvail, avail)
}

func (d *DiskInfo) GetRealAvailSpace() int64 {
	return atomic.LoadInt64(&d.realAvail)
}

func (d *DiskInfo) AddCreateVolsAvailSpace(avail int64) {
	atomic.AddInt64(&d.volsAvail, avail)
}

func (d *DiskInfo) GetCreateVolsAvailSpace() int64 {
	return atomic.LoadInt64(&d.volsAvail)
}

func (d *DiskInfo) GetVolIds() (volIds []uint32) {
	d.volAllocLock.RLock()
	volIds = d.VolIds
	d.volAllocLock.RUnlock()

	return
}

func (d *DiskInfo) AddVol(vId uint32) {
	d.volAllocLock.Lock()
	d.VolIds = append(d.VolIds, vId)
	d.volAllocLock.Unlock()
}

func (d *DiskInfo) DelVol(vId uint32) {
	d.volAllocLock.Lock()
	for i, id := range d.VolIds {
		if vId != id {
			continue
		}
		vIds := d.VolIds[:i]
		if len(d.VolIds) == i+1 {
			d.VolIds = vIds
			break
		}
		d.VolIds = append(vIds, d.VolIds[i+1:]...)
		break
	}
	d.volAllocLock.Unlock()
}

func (d *DiskInfo) UpdateSpaceInfo() (err error) {
	var statsInfo syscall.Statfs_t
	if err = syscall.Statfs(d.Path, &statsInfo); err != nil {
		d.AddReadErrs()
	}

	d.SetRealAvailSpace(int64(statsInfo.Bavail)*statsInfo.Bsize - int64(d.MinRestSize)*GB)
	currErrs := d.GetReadErrs() + d.GetWriteErrs()
	if currErrs >= d.MaxErrs {
		d.SetStatus(BadDisk)
		warnMesg := fmt.Sprintf(" node %s DiskError -path %s ", LocalAddr, d.Path)
		ump.Alarm(UmpKeyDisk, UmpDetailDiskErr+warnMesg)
	} else if d.realAvail < 0 {
		d.SetStatus(ReadOnlyDisk)
	} else {
		d.SetStatus(ReadWriteDisk)
	}

	return
}

func (d *DiskInfo) LoadVol(srvId uint32, storeType uint8, num int, fInfo os.FileInfo) (vol *VolInfo, err error) {
	vol = &VolInfo{Path: d.Path, Name: fInfo.Name()}
	if vol.Id, err = vol.GetId(); err == nil {
		vol.Store, err = vol.NewStore(srvId, storeType, ReBootStoreMode)
	}
	if err != nil {
		vol.SetStatus(BadDisk)
		return
	}
	if vol.Total, err = vol.GetTotalSpace(); err != nil {
		d.AddReadErrs()
		err = nil
	}
	vol.SetStatus(d.GetStatus())
	vol.AddUsedSpace(vol.Store.GetStoreUsedSize())

	d.VolIds[num] = vol.Id
	d.AddCreateVolsAvailSpace(-int64(vol.Total))

	return
}
