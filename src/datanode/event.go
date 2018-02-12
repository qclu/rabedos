package datanodeIdc

import (
	"encoding/json"
	"io"
	"net/http"
	"time"
	"utils/rbdlogger"
)

type order struct {
	isFailed bool
	cmd      string
	args     []int
	result   chan []byte
}

type event struct {
	kind    uint8
	content interface{}
}

func (s *DataNode) HandleEvents() {
	log.GetLog().LogInfo("Start: HandleEvents")
	getMasterTick := time.NewTicker(GetMasterInfoIntervalTime * time.Second)
	checkFdsTick := time.NewTicker(CheckFdsIntervalTime * time.Second)
	modifyVolsStatus := time.NewTicker(ModifyVolsStatusIntervalTime * time.Minute)

Loop:
	for {
		select {
		case <-getMasterTick.C:
			s.HandleMasterInfo()
		case <-checkFdsTick.C:
			s.controlFds()
		case <-modifyVolsStatus.C:
			s.modifyVolsStatus()
		case e := <-evtCh:
			s.handleEvent(e)
		case <-s.shutdownCh:
			break Loop
		}
	}

	getMasterTick.Stop()
	checkFdsTick.Stop()
	ump.Alarm(UmpKeyEvents, UmpDetailEvents)
}

func query(w http.ResponseWriter, r *http.Request, flag string) {
	cmd := &order{cmd: flag, result: make(chan []byte, 1)}
	evtCh <- &event{kind: EventKindShowInnerInfo, content: cmd}
	buf := <-cmd.result
	if cmd.isFailed {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, "show "+flag+" infomation failed. err:"+string(buf))
		return
	}

	w.Write(buf)
}

func (s *DataNode) ShowInnerInfo(order *order) {
	var buf []byte
	var err error

	switch order.cmd {
	case ShowDisks:
		disks := s.smMgr.GetAllDisks()
		buf, err = json.MarshalIndent(disks, "", "\t")
	case ShowVols:
		vols := s.smMgr.GetAllVols()
		buf, err = json.MarshalIndent(vols, "", "\t")
	case ShowStats:
		buf, err = s.stats.GetStat(s.smMgr)
	}

	if err != nil {
		buf = []byte(err.Error())
		order.isFailed = true
	}
	order.result <- buf
}

func (s *DataNode) HandleMasterInfo() {
	m, err := s.zw.GetValue(MasterZkDir)
	if err != nil {
		log.GetLog().LogError("GetValue from zk failed, err:", err)
		return
	}
	if s.GetMasterAddr() != m && m != "" {
		s.SetMasterAddr(m)
		if s.storeType == ChunkStoreType {
			s.chunkInspector.SetMasterAddr(m)
		}
		log.GetLog().LogInfo("HandleMasterInfo, preM:", s.GetMasterAddr(), " nowM:", m)
	}
}

func (s *DataNode) handleEvent(e *event) {
	switch e.kind {
	case EventKindShowInnerInfo:
		s.ShowInnerInfo(e.content.(*order))
	default:
		log.GetLog().LogDebug("handleEvent, kind:", e.kind, " unmatch.")
	}
}

func (s *DataNode) controlFds() {
	fds := 0

	vols := s.smMgr.GetAllVols()
	for _, v := range vols {
		fds += v.GetFds()
	}
	log.GetLog().LogInfo("controlFds, fds:", fds, " maxFds:", MaxFds)
	if fds < MaxFds {
		return
	}

	for _, v := range vols {
		v.HalveFds()
	}
}

func (s *DataNode) modifyVolsStatus() {
	for _, d := range s.smMgr.disks {
		volsId := d.GetVolIds()
		diskStatus := d.GetStatus()
		log.GetLog().LogInfo("modifyVolsStatus, path", d.Path, " status:", diskStatus)

		for _, vId := range volsId {
			if v, ok := s.smMgr.IsExistVol(vId); ok {
				v.SetStatus(diskStatus)
			}
		}
	}
}
