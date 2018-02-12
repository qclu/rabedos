package rbdproto

import (
	"errors"
)

const ProtoMagic uint8 = 0xFF

//opcode
const (
	ReqCreateFile      uint8 = 0x01
	RplCreateFile      uint8 = 0x01
	ReqDelete                = 0x02
	RplDelete                = 0x02
	ReqWrite                 = 0x03
	RplWrite                 = 0x03
	ReqRead                  = 0x04
	ReqStreamRead            = 0x05
	ReqGetWatermark          = 0x06
	ReqGetAllWatermark       = 0x07
	ReqRepair                = 0x08
	ReqRepairRead            = 0x09
	ReqFlowInfo              = 0x0A

	OpIntraGroupNetErr uint8 = 0xF3
	OpArgUnmatchErr    uint8 = 0xF4
	OpFileIsExistErr   uint8 = 0xF5
	OpDiskNoSpaceErr   uint8 = 0xF6
	OpDiskErr          uint8 = 0xF7
	OpErr              uint8 = 0xF8

	OpOk uint8 = 0xFA
)

var (
	ErrBadNodes           = errors.New("BadNodesErr")
	ErrArgLenUnmatch      = errors.New("ArgLenUnmatchErr")
	ErrAddrsNodesUnmatch  = errors.New("AddrsNodesUnmatchErr")
	ErrBufPoolInfoUnmatch = errors.New("BufPoolDataUnmatchErr")
	ErrBufPoolTypeUnmatch = errors.New("BufPoolTypeUnmatchErr")
	ErrExtentIdFormat     = errors.New("ExtentIdFormatErr")
)

//buf unit pool
const (
	PkgDelAndCrcSize     = 5
	PkgArgPoolMaxCap     = 10240
	PkgExtentDataMaxSize = 1 << 16 // 64K
	PkgChunkDataMaxSize  = 1<<20 + PkgDelAndCrcSize
	PkgRepairDataMaxSize = 1 << 16
	PkgDataPoolMaxCap    = 10240
	PkgHeaderPoolMaxCap  = 20480
)

//write to conn if free body space
const (
	ExtentNameSplit       = "_"
	VolNameSplit          = "_"
	AddrSplit             = "/"
	MaxArgCount     uint8 = 2
	HeaderSize            = 34
	PkgArgMaxSize         = 100
)

//about conn
const (
	ReadDeadlineTime   = 5
	ReadOpDeadlineTime = 2
	NoReadDeadlineTime = -1
	WriteDeadlineTime  = 5
	ConnTryTimes       = 2
	ConnTimeSleep      = 1
	ConnDialTimeout    = 2
	ConnBufferSize     = 128 * 1024
)

const (
	ShutdownChanSize  = 1024
	RequstChanSize    = 1024
	ExitChanSize      = 2
	UnitConnPoolSize  = 30
	DefaultDisksSize  = 10
	DefaultVolsSize   = 1024
	LeastAvailDiskNum = 1
)
