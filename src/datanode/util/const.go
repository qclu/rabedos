package datanodeutil

const (
	CfgDisksInfoSplit    = ";"
	CfgDisksContentSplit = ":"
	VolNameJoin          = "_"
	RepairDataSplit      = "/"
	InOutFlowSplit       = "/"
	LogModule            = "datanode"
)

const ChooseDiskTimes = 3

//event kind
const (
	EventKindShowInnerInfo = 1
	ShowDisks              = "disks"
	ShowVols               = "vols"
	ShowStats              = "stats"
)

//store type
const (
	ChunkStoreType     = 0 // for tiny files
	ExtentStoreType    = 1 // for large files
	ChunkStoreTypeStr  = "chunkstore"
	ExtentStoreTypeStr = "extentstore"
)

//get package info
const (
	GetPackInfoOnlyId   = 0
	GetPackInfoOpDetail = 1
)

//shutdown
const (
	NotifyShutdownSleepTime = 100 //ms
	MaxShutdownSleepTime    = 100 //s
	FinalShutdownSleepTime  = 1   //s
)

//the response of master cmd
const (
	CmdFail    = -1
	CmdRunning = 1
	CmdSuccess = 2
)

//stats
const (
	ReportToMonitorRole        = 1
	ReportToMasterRole         = 2
	ReportToSelfRole           = 3
	ReportToMonitorStats       = "ReportStatsToMonitor"
	ReportToMasterStats        = "ReportStatsToMaster"
	ReportToMonitIntervalTime  = 5   //min
	ReportToMasterIntervalTime = 1   //min
	HeartbeatIntervalTime      = 1   //s
	WarningTime                = 600 //ms
	MB                         = 1 << 20
	GB                         = 1 << 30
	InFlow                     = true
	OutFlow                    = false
)

//event
const (
	//get master info
	MasterZkDir               = "/ds-root/master"
	InitTryGetMasterInfoTimes = 3
	GetMasterInfoIntervalTime = 1 //s

	//the cache of fds
	MaxFds               = (1 << 13)
	CheckFdsIntervalTime = 10

	//modify vols status
	ModifyVolsStatusIntervalTime = 1 //min
)

//write to conn if free body space
const (
	FreeBodySpace    = true
	NotFreeBodySpace = false
)

const (
	NetType = "tcp"
)

//pack cmd response
const (
	NoFlag    = 0
	ReadFlag  = 1
	WriteFlag = 2
)

//ump
const (
	UmpKeyReport         = "report"
	UmpKeyEvents         = "events"
	UmpKeyInspector      = "inspector"
	UmpKeyCmds           = "cmds"
	UmpKeyListen         = "listen"
	UmpKeyDisk           = "disk"
	UmpDetailReport      = "report to master goroutine exit."
	UmpDetailReportStats = "report stats to master goroutine exit."
	UmpDetailEvents      = "handling events goroutine exit."
	UmpDetailInspector   = "inspecting chunks goroutine exit."
	UmpDetailCmds        = "handling cmds goroutine exit."
	UmpDetailListen      = "listening goroutine exit."
	UmpDetailDiskErr     = "disk errs count more than default"
)

const (
	HandleChunk  = "HandleChunk"
	HandleReqs   = "HandleReqs"
	ReadFromCli  = "ReadFromCli"
	WriteToCli   = "WriteToCli"
	ReadFromNext = "ReadFromNext"
	WriteToNext  = "WriteToNext"

	ConnIsNullErr = "ConnIsNullErr"
	GroupNetErr   = "GroupNetErr"
	CreateVolErr  = "CreateVolErr"
)

const (
	LogHeartbeat   = "HB:"
	LogStats       = "Stats:"
	LogLoad        = "Load:"
	LogExit        = "Exit:"
	LogShutdown    = "Shutdown:"
	LogCreateVol   = "CRV:"
	LogCreateFile  = "CRF:"
	LogDelVol      = "DELV:"
	LogDelFile     = "DELF:"
	LogMarkDel     = "MDEL:"
	LogVolSnapshot = "Snapshot:"
	LogGetWm       = "WM:"
	LogGetAllWm    = "AllWM:"
	LogWrite       = "WR:"
	LogRead        = "RD:"
	LogRepairRead  = "RRD:"
	LogStreamRead  = "SRD:"
	LogRepair      = "Repair:"
	LogInspector   = "Inspector:"
)
