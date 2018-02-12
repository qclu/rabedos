package datanodeIdc

import (
	"encoding/json"
	"io/ioutil"
	"utils/rbdlogger"
)

func (s *DataNode) initStats() {
	var total uint64
	for _, d := range s.smMgr.disks {
		total += uint64(d.Total) - uint64(d.MinRestSize*GB)
	}
	s.stats = NewStats(ReportToMasterRole, s.ver, s.cId, s.ip+":"+s.port, s.id, s.idc, total)
	s.monitStats = NewStats(ReportToMonitorRole, s.ver, s.cId, s.ip+":"+s.port, s.id, s.idc, total)
}

func (s *DataNode) ReportStatsToMonitor() {
	data, err := s.monitStats.GetStat(s.smMgr)
	log.GetLog().LogDebug("ReportStatsToMonitor, data:", string(data))
	if err != nil {
		log.GetLog().LogError("ReportStatsToMonitor, stats to json err:", err)
	}

	resp, err := post(data, "http://"+s.monitAddr+"/node/report")
	if err != nil {
		log.GetLog().LogError("ReportStatsToMonitor, post to monitor, err:", err)
		return
	}

	scode := resp.StatusCode
	msg, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if scode == 200 {
		log.GetLog().LogDebug("ReportStatsToMonitor, success, scode:", scode, " cmd:", string(msg))
		return
	}

	log.GetLog().LogError("ReportStatsToMonitor, data send failed, scode:", scode, " cmd:", string(msg))
}

func (s *DataNode) ReportStatsToMaster() {
	data, err := s.stats.GetStat(s.smMgr)
	log.GetLog().LogInfo("ReportStatsToMaster, data:", string(data))
	if err != nil {
		log.GetLog().LogError("ReportStatsToMaster, stats to json err:", err)
		return
	}

	post := true
	for post {
		addr := s.GetMasterAddr()
		post, _ = PostToMaster(data, "http://"+addr+"/node/report", LogStats)
		log.GetLog().LogDebug("ReportStatsToMaster, GetMasterAddr:", addr)
	}

	return
}

func (s *DataNode) ReportHeartbeat(hb *Hb) []byte {
	hb.Addr = s.ip + ":" + s.port
	data, err := json.Marshal(hb)
	if hb.TaskResps != nil {
		log.GetLog().LogDebug("ReportHeartbeat, data:", string(data))
	}
	if err != nil {
		log.GetLog().LogError(LogHeartbeat+"info to json err:", err)
		return nil
	}

	addr := s.GetMasterAddr()
	postStat, msg := PostToMaster(data, "http://"+addr+"/node/heartbeat?cluster="+s.cId, LogHeartbeat)
	if postStat {
		s.ReportStatsToMaster()
	}
	log.GetLog().LogDebug("ReportHeartbeat, GetMasterAddr:", addr)

	return msg
}
