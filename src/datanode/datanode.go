package datanodeIdc

import (
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"utils/config"
	"utils/rbdlogger"
	zk "utils/zkwrapper"
)

var (
	ErrDiskCfgUnmatch      = errors.New("DiskCfgUnmatchErr")
	ErrPkgCrcUnmatch       = errors.New("PkgCrcUnmatchErr")
	ErrStoreTypeUnmatch    = errors.New("StoreTypeUnmatchErr")
	ErrLackOfAvailDisk     = errors.New("LackOfAvailDiskErr")
	ErrNotExistMasterZkDir = errors.New("NotExistMasterZkDirErr")
)

var (
	LocalAddr string
)

type DataNode struct {
	storeType              uint8
	id                     int
	idc                    int
	chunkInspectorInterval int
	ip                     string
	ver                    string
	cId                    string
	port                   string
	logDir                 string
	zkAddr                 string
	monitAddr              string
	masterAddr             string
	masterLock             sync.RWMutex

	pkgDataMaxSize int
	connPool       *ConnPool
	stats          *Stats
	monitStats     *Stats
	chunkInspector *ChunkInspector
	zw             *zk.ZkWrapper
	shutdownCh     chan bool
	smMgr          *SpaceMapManager
}

var (
	evtCh = make(chan *event, 1024)
)

var (
	ErrBadConfFile         = errors.New("BadConfFileErr")
	ErrBadVolId            = errors.New("BadVolIdErr")
	ErrConnIsNull          = errors.New("ConnIsNullErr")
	ErrMidNodeUnmatchReqId = errors.New("MidNodeUnmatchReqIdErr")
	ErrNextNodeAddrUnmatch = errors.New("NextNodeAddrUnmatchErr")
)

// server main operations - New, Start, and Shutdown
func NewServer() *DataNode {
	smMgr := &SpaceMapManager{
		disks: make(map[string]*DiskInfo, DefaultDisksSize),
		vols:  make(map[uint32]*VolInfo, DefaultVolsSize)}

	return &DataNode{
		connPool:   NewConnPool(),
		shutdownCh: make(chan bool, ShutdownChanSize),
		smMgr:      smMgr,
	}
}

func (s *DataNode) Start(configFile *config.Config) (err error) {
	ump.InitUmp("dsDataNode")
	if s.smMgr.disks, err = s.checkConfig(configFile); err != nil {
		print("failed to checkConfig, err:", err.Error(), "\n")
		return
	}
	//initialize unit buf pools
	InitUnitBufPools([]int{HeaderSize, PkgArgMaxSize, s.pkgDataMaxSize},
		[]int{PkgHeaderPoolMaxCap, PkgArgPoolMaxCap, PkgDataPoolMaxCap})

	if _, err = log.NewLog(s.logDir, LogModule, 0); err != nil {
		print("failed to initLog, err:", err.Error(), "\n")
		return
	}

	if err = s.initZw(); err != nil {
		log.GetLog().LogError("failed to initZw, err:", err, "\n")
		return
	}

	s.initStats()
	s.LoadVols()
	s.ReportStats()

	go s.ReportHb()
	go s.HandleEvents()

	//init checkers
	if s.storeType == ChunkStoreType {
		s.chunkInspector = NewChunkInspector(s.chunkInspectorInterval, s.cId, s.ip+":"+s.port,
			s.GetMasterAddr(), s.smMgr)
		go s.chunkInspector.InspectInGlobal()
	}

	s.PrintServerInfo()
	return s.listenAndServe()
}

func (s *DataNode) PrintServerInfo() {
	log.GetLog().LogInfo("[ServerInfo] addr:", s.ip, ":", s.port, "\tcId:", s.cId, "\tver:", s.ver, "\tstore type:",
		s.storeType, "\tdisk count:", len(s.smMgr.disks), "\nmaster:", s.GetMasterAddr(), "\tmonitor:",
		s.monitAddr, "\tchunk inspector interval:", s.chunkInspectorInterval, "\nThis server pid is ", os.Getpid())
}

func (s *DataNode) checkConfig(cfg *config.Config) (disks map[string]*DiskInfo, err error) {
	s.ip = cfg.GetString("Ip")
	s.port = cfg.GetString("Port")
	s.cId = cfg.GetString("ClusterId")
	s.logDir = cfg.GetString("LogDir")
	s.ver = cfg.GetString("Version")
	s.zkAddr = cfg.GetString("ZkAddr")
	s.monitAddr = cfg.GetString("MonitAddr")
	storeType := cfg.GetString("StoreType")
	if s.idc, err = strconv.Atoi(cfg.GetString("IDC")); err == nil {
		s.chunkInspectorInterval, err = strconv.Atoi(cfg.GetString("ChunkInspectorInterval"))
	}

	if err != nil || s.ip == "" || s.port == "" || s.cId == "" || s.logDir == "" || s.ver == "" ||
		s.monitAddr == "" || storeType == "" || s.zkAddr == "" || s.chunkInspectorInterval < 0 {
		return nil, ErrBadConfFile
	}
	if s.id, err = strconv.Atoi(cfg.GetString("Id")); err != nil {
		return nil, ErrBadConfFile
	}
	if storeType == ExtentStoreTypeStr {
		s.storeType = ExtentStoreType
		// for extent data storage, max package size is 64K
		s.pkgDataMaxSize = PkgExtentDataMaxSize
	} else if storeType == ChunkStoreTypeStr {
		s.storeType = ChunkStoreType
		// file smaller than 1M is considered as tiny file, and will be transfer in one package
		s.pkgDataMaxSize = PkgChunkDataMaxSize
	}

	LocalAddr = s.ip + ":" + s.port

	return GetDisks(cfg.GetArray("DisksInfo"))
}

func (s *DataNode) ReportStats() {
	report := func(funcName string, interval time.Duration, stats func()) {
		log.GetLog().LogInfo("Start: " + funcName)
		for {
			stats()
			time.Sleep(interval * time.Minute)
		}
		ump.Alarm(UmpKeyReport, UmpDetailReportStats)
	}

	go report(ReportToMasterStats, ReportToMasterIntervalTime, func() { s.ReportStatsToMaster() })
	go report(ReportToMonitorStats, ReportToMonitIntervalTime, func() { s.ReportStatsToMonitor() })
}

func (s *DataNode) ReportHb() {
	log.GetLog().LogInfo("Start: ReportHb")
	hb := new(Hb)
	taskCmdCh := make(chan *Cmd, 1024)
	loadVolCmdCh := make(chan *Cmd, 1024)
	replicationFileCmdCh := make(chan *Cmd, 100)
	respCh := make(chan *CmdResp, 1024)

	go s.handleCmds(taskCmdCh, loadVolCmdCh, replicationFileCmdCh, respCh)

	for {
		time.Sleep(HeartbeatIntervalTime * time.Second)
		getBatchResps(hb, respCh)

		cmdBytes := s.ReportHeartbeat(hb)
		hb = new(Hb)
		if cmdBytes == nil || len(cmdBytes) == 0 {
			continue
		}
		cmds, err := UnmarshalCmd(cmdBytes)
		if err != nil {
			log.GetLog().LogError("UnmarshalCmd err:", err.Error())
			continue
		}
		hb.TaskResps = preHandleCmds(taskCmdCh, loadVolCmdCh, replicationFileCmdCh, cmds)
	}

	ump.Alarm(UmpKeyReport, UmpDetailReport)
}

func (s *DataNode) handleFunctions() {
	http.HandleFunc("/disks", QueryDisks)
	http.HandleFunc("/vols", QueryVols)
	http.HandleFunc("/stats", QueryStats)
}

func (s *DataNode) listenAndServe() (err error) {
	log.GetLog().LogInfo("Start: listenAndServe")
	l, err := net.Listen(NetType, ":"+s.port)
	if err != nil {
		log.GetLog().LogError("failed to listen, err:", err)
		return
	}

	s.handleFunctions()

	for {
		if s.IsShutdown("listenAndServe") {
			break
		}

		conn, err := l.Accept()
		if err != nil {
			log.GetLog().LogError("failed to accept, err:", err)
			break
		}
		go s.serveConn(conn)
	}

	log.GetLog().LogError(LogShutdown + " return listenAndServe, listen is closing")
	ump.Alarm(UmpKeyListen, UmpDetailListen)
	return l.Close()
}

func (s *DataNode) serveConn(conn net.Conn) {
	log.GetLog().LogInfo("Start: serveConn, ", conn.RemoteAddr().String())
	s.stats.AddConnection()
	c, _ := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)

	c.SetReadBuffer(ConnBufferSize)
	c.SetWriteBuffer(ConnBufferSize)

	msgH := NewMsgHandler(ExitChanSize, RequstChanSize, RequstChanSize, c)
	go s.handleReqs(msgH)
	go s.writeToCli(msgH)

	for {
		if s.IsShutdown("serveConn") {
			break
		}
		if err := s.readFromCli(msgH); err != nil {
			break
		}
	}

	log.GetLog().LogInfo("Exit: return serveConn:", conn.RemoteAddr().String())
	s.stats.RemoveConnection()
	c.Close()
	return
}

func (s *DataNode) IsShutdown(funcName string) (cmd bool) {
	select {
	case <-s.shutdownCh:
		cmd = true
		log.GetLog().LogWarn(LogShutdown+"server start exit: ", funcName)
	default:
	}

	return
}

func (s *DataNode) Shutdown() {
	connCount := int(s.stats.GetConnectionNum())
	log.GetLog().LogWarn(LogShutdown+"conn count:", connCount)
	close(s.shutdownCh)
	time.Sleep(1 * time.Second)

	select {
	case <-time.After(NotifyShutdownSleepTime * time.Millisecond):
	case <-time.After(MaxShutdownSleepTime * time.Millisecond):
		log.GetLog().LogWarn(LogShutdown+"chan len:", len(s.shutdownCh))
	}

	vols := s.smMgr.GetAllVols()
	for vId, vol := range vols {
		vol.Store.SyncAll()
		log.GetLog().LogWarn(LogShutdown+"sync all vol id:", vId)
	}

	<-time.After(FinalShutdownSleepTime * time.Second)
	log.GetLog().LogWarn(LogShutdown + " is completed.")
}

func GetDisks(disksIntf []interface{}) (disks map[string](*DiskInfo), err error) {
	if len(disksIntf) == 0 {
		return nil, ErrBadConfFile
	}

	disks = make(map[string]*DiskInfo)
	for _, d := range disksIntf {
		infos := strings.SplitN(d.(string), CfgDisksInfoSplit, -1)
		if len(infos) != 3 {
			return nil, ErrDiskCfgUnmatch
		}

		args := make([]string, 3)
		for i, info := range infos {
			arr := strings.SplitN(info, CfgDisksContentSplit, -1)
			if len(arr) != 2 {
				return nil, ErrDiskCfgUnmatch
			}
			args[i] = arr[1]
		}

		minRestSize, err := strconv.Atoi(args[1])
		if err != nil || minRestSize < 0 {
			return nil, err
		}
		maxErrs, err := strconv.Atoi(args[2])
		if err == nil && maxErrs >= 0 {
			disks[args[0]], err = NewDiskInfo(args[0], int32(minRestSize), int32(maxErrs))
		}
		if err != nil || maxErrs < 0 {
			print("NewDiskInfo, disk path:", args[0], " maxErrs:", maxErrs, " err:", err.Error(), "\n")
			ump.Alarm(UmpKeyDisk, UmpDetailDiskErr)
			continue
		}
		print("NewDiskInfo, disk path:", disks[args[0]].Path, " total available space:", disks[args[0]].Total,
			" real space:", disks[args[0]].realAvail, " vol space:", disks[args[0]].volsAvail, " maxErrs:", maxErrs, "\n")
	}

	if len(disks) < LeastAvailDiskNum {
		err = ErrLackOfAvailDisk
	}

	return
}

func (s *DataNode) LoadVols() (err error) {
	for _, d := range s.smMgr.disks {
		fList, err := ioutil.ReadDir(d.Path)
		if err != nil {
			d.SetStatus(BadDisk)
			log.GetLog().LogError(LogLoad+"failed to load vol store, path:", d.Path, " err:", err)
			ump.Alarm(UmpKeyDisk, UmpDetailDiskErr)
			continue
		}

		num := 0
		d.VolIds = make([]uint32, len(fList))
		log.GetLog().LogInfo(LogLoad+"path:", d.Path, " fList:", fList, " len:", len(fList))
		for _, fInfo := range fList {
			vol, err := d.LoadVol(uint32(s.id), s.storeType, num, fInfo)
			if err != nil {
				log.GetLog().LogError(LogLoad+"failed to load vol store, path:", d.Path, " name:", fInfo.Name(), " err:", err)
				continue
			}

			num++
			s.smMgr.vols[vol.Id] = vol
			log.GetLog().LogInfo(LogLoad+"AddCreateVolsAvailSpace, -", vol.Total/GB, " path:", d.Path, " volId:", vol.Id)
		}
		log.GetLog().LogInfo(LogLoad+"path:", d.Path, " volIds", d.VolIds)
		if err = d.UpdateSpaceInfo(); err != nil {
			log.GetLog().LogError(LogLoad+"failed to UpdateSpaceInfo, disk path:", d.Path, " status:", d.GetStatus(), " err:", err)
		}
	}

	return
}

func (s *DataNode) initZw() (err error) {
	zw, err := zk.NewZkWrapper(s.zkAddr)
	if err != nil {
		return
	}
	if !zw.CheckExistingNode(MasterZkDir) {
		return ErrNotExistMasterZkDir
	}

	for i := 0; i < InitTryGetMasterInfoTimes; i++ {
		m, err := zw.GetValue(MasterZkDir)
		if err == nil {
			s.SetMasterAddr(m)
			break
		}
		if i == InitTryGetMasterInfoTimes-1 {
			return err
		}
	}
	s.zw = zw

	return
}

func QueryDisks(w http.ResponseWriter, r *http.Request) {
	query(w, r, ShowDisks)
}

func QueryVols(w http.ResponseWriter, r *http.Request) {
	query(w, r, ShowVols)
}

func QueryStats(w http.ResponseWriter, r *http.Request) {
	query(w, r, ShowStats)
}
