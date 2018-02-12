package datanodeIdc

import (
	"errors"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"syscall"
)

const (
	ExtentDir       = "extent"
	ChunkDir        = "chunk"
	ReBootStoreMode = false
	NewStoreMode    = true
)

const (
	ExtentHeaderSize = 4097
	BlockCount       = 1024
	ExtentLock       = 1
	ExtentAvali      = 2
	DefaultFileOpen  = 2000
	MarkDelete       = 'D'
	UnMarkDelete     = 'U'
	MarkDeleteIndex  = 4096
	RecoverNewStore  = -1
	RepairOpFlag     = true
	BlockSize        = 65536
	HighBit          = 16
	MinServId        = 1
	MaxServId        = 65535
)

var (
	ErrorNotFound        = errors.New("no such file or directory")
	ErrorHasDelete       = errors.New("has delete")
	ErrorUnmatchPara     = errors.New("unmatch offset")
	ErrorCreateExsitFile = errors.New("file exsits")
	ErrorNoAvaliFile     = errors.New("no avail file")
	ErrorNoUnAvaliFile   = errors.New("no Unavali file")
	ErrorNewStoreMode    = errors.New("error new store mode ")
	ErrExtentNameFormat  = errors.New("extent name format error")
	ErrSyscallNoSpace    = errors.New("no space left on device")
	UnavaliServId        = errors.New("unavali servid")
)

type Store interface {
	Create(fileId uint32) (err error)
	Delete(fileId uint32) (err error)
	MarkDelete(fileId uint32, offset, size int64) (err error)
	DeleteStore()
	Write(fileId uint32, offset, size int64, data []byte, crc uint32, repairFlag bool) (err error)
	Read(fileId uint32, offset, size int64, nbuf []byte, repairFlag bool) (crc uint32, err error)
	GetExtentHeader(fileId uint32, headerBuff []byte) (err error)
	Sync(fileId uint32) (err error)
	SyncAll()
	Snapshot() (files []*VolFile, err error)
	FileExist(fileId uint32) (exist bool)
	GetAvaliChunk() (chunkId int, err error)
	PutAvaliChunk(chunkId int)
	GetUnAvaliChunk() (chunkId int, err error)
	GetUnAvaliChanLen() (chanLen int)
	PutUnAvaliChunk(chunkId int)
	GetAllWatermark() (chunks []*ChunkInfo, err error)
	SetAllChunkAvali()
	GetStoreActiveFiles() (activeFiles int)
	CloseStoreActiveFiles()
	GetSoreFileCount() (files int, err error)
	GetWatermark(fileId uint32) (size int64, err error)
	GetStoreUsedSize() (size int64)
	GetAvaliFileId() (fileId uint32)
}

func CheckAndCreateSubdir(name string, newMode bool) (err error) {
	var fd int
	if newMode == ReBootStoreMode {
		err = os.MkdirAll(name, 0755)
	} else if newMode == NewStoreMode {
		err = os.Mkdir(name, 0755)
	} else {
		err = ErrorNewStoreMode
	}

	if err != nil {
		return
	}

	if fd, err = syscall.Open(name, os.O_RDONLY, 0666); err != nil {
		return
	}

	if err = syscall.Fsync(fd); err != nil {
		return
	}
	syscall.Close(fd)

	return
}

func getBlockNo(name string) (no int, err error) {
	arr := strings.Split(name, ExtentNameSplit)
	if len(arr) != 3 {
		return -1, ErrExtentNameFormat
	}

	return strconv.Atoi(arr[1])
}

//snapshot the file list of the given subdirectory as lines of fileId/crc/mtim/del
func DirSnapshot(storeType, path string) (files []*VolFile, err error) {
	fList, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}

	var crc uint32
	b := make([]byte, ExtentHeaderSize)
	files = make([]*VolFile, len(fList))

	for i, info := range fList {
		var del bool
		if storeType == ExtentDir {
			fp, err := os.OpenFile(path+"/"+info.Name(), os.O_RDONLY, 0666)
			if err == nil {
				_, err = fp.ReadAt(b, 0)
				fp.Close()
			}
			if err != nil && err != io.EOF {
				return nil, err
			}
			if err == io.EOF {
				b[MarkDeleteIndex] = UnMarkDelete
			}
			crc = crc32.ChecksumIEEE(b[:MarkDeleteIndex])
		}

		if b[MarkDeleteIndex] == MarkDelete {
			del = true
		}
		f := &VolFile{Name: info.Name(), Crc: crc, Modifiyed: info.ModTime().Unix(), MarkDel: del}
		files[i] = f
	}

	return
}

func IsDiskErr(errMsg string) bool {
	if strings.Contains(errMsg, ErrorUnmatchPara.Error()) || strings.Contains(errMsg, ErrorCreateExsitFile.Error()) ||
		strings.Contains(errMsg, ErrorNoAvaliFile.Error()) || strings.Contains(errMsg, ErrorNotFound.Error()) ||
		strings.Contains(errMsg, io.EOF.Error()) || strings.Contains(errMsg, ErrSyscallNoSpace.Error()) ||
		strings.Contains(errMsg, ErrorHasDelete.Error()) {
		return false
	}
	return true
}
