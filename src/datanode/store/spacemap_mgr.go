package datanodeIdc

import (
	"sync"
	"utils/rbdlogger"
)

type SpaceMapManager struct {
	disks   map[string]*DiskInfo
	vols    map[uint32]*VolInfo
	volLock sync.RWMutex
}

func (mgr *SpaceMapManager) GetAllDisks() (disks []DiskInfo) {
	disks = make([]DiskInfo, len(mgr.disks))
	i := 0
	for _, d := range mgr.disks {
		disks[i] = *d
		i++
	}

	return
}

func (mgr *SpaceMapManager) GetAllVols() (vols []*VolInfo) {
	mgr.volLock.RLock()
	vols = make([]*VolInfo, len(mgr.vols))
	i := 0
	for _, v := range mgr.vols {
		vols[i] = v
		i++
	}
	mgr.volLock.RUnlock()

	return
}

func (mgr *SpaceMapManager) IsExistVol(vId uint32) (vol *VolInfo, ok bool) {
	mgr.volLock.RLock()
	vol, ok = mgr.vols[vId]
	mgr.volLock.RUnlock()

	return
}

func (mgr *SpaceMapManager) AddVol(path string, vol *VolInfo) {
	mgr.volLock.Lock()
	mgr.vols[vol.Id] = vol
	mgr.volLock.Unlock()

	mgr.disks[path].AddVol(vol.Id)
	mgr.disks[path].AddCreateVolsAvailSpace(-int64(vol.Total))
	log.GetLog().LogDebug("AddCreateVolsAvailSpace, -", vol.Total/GB, " path:", path, " volId:", vol.Id)
}

func (mgr *SpaceMapManager) DelVol(vol *VolInfo) {
	d, ok := mgr.disks[vol.Path]
	if ok {
		d.DelVol(vol.Id)
		d.AddCreateVolsAvailSpace(int64(vol.Total))
		log.GetLog().LogDebug("AddCreateVolsAvailSpace, ok", vol.Total/GB, " path:", d.Path, " volId:", vol.Id)
	} else {
		log.GetLog().LogDebug("AddCreateVolsAvailSpace, ", vol.Total/GB, " path:", d.Path, " volId:", vol.Id)
	}

	vol.DelInfo()
	mgr.volLock.Lock()
	delete(mgr.vols, vol.Id)
	mgr.volLock.Unlock()
}
