package datanodeIdc

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"utils/rbdlogger"
)

var (
	ExtentOpenOpt = os.O_CREATE | os.O_RDWR | os.O_EXCL
)

type ExtentStore struct {
	dataDir       string
	lock          sync.Mutex
	extentCoreMap map[uint32]*ExtentInCore
	fdlist        *list.List
	baseFileId    uint32
	servId        uint32
}

func NewExtentStore(servId uint32, dataDir string, newMode bool) (s *ExtentStore, err error) {
	s = new(ExtentStore)
	s.dataDir = dataDir
	if err = CheckAndCreateSubdir(dataDir, newMode); err != nil {
		goto errDeal
	}

	s.extentCoreMap = make(map[uint32]*ExtentInCore)
	s.fdlist = list.New()
	s.servId = servId
	if servId < MinServId || servId > MaxServId {
		err = UnavaliServId
		goto errDeal
	}

	if err = s.initBaseFileId(); err != nil {
		goto errDeal
	}

	return
errDeal:
	err = fmt.Errorf("NewExtentStore [%v] err[%v]", dataDir, err)
	log.GetLog().LogError(err)

	return nil, err
}

func (s *ExtentStore) DeleteStore() {
	s.ClearAllCache()
	os.RemoveAll(s.dataDir)

	return
}

func (s *ExtentStore) Create(fileId uint32) (err error) {
	var ec *ExtentInCore
	emptyCrc := crc32.ChecksumIEEE(make([]byte, BlockSize))
	if ec, err = s.createExtentIncore(fileId); err != nil {
		return
	}

	for blockNo := 0; blockNo < BlockCount; blockNo++ {
		binary.BigEndian.PutUint32(ec.extentCrc[blockNo*4:(blockNo+1)*4], emptyCrc)
	}

	if _, err = ec.fp.WriteAt(ec.extentCrc, 0); err != nil {
		return
	}
	if err = ec.fp.Sync(); err != nil {
		return
	}
	s.addExtentToCache(ec)

	return
}

func (s *ExtentStore) createExtentIncore(fileId uint32) (ec *ExtentInCore, err error) {
	name := s.dataDir + "/" + strconv.Itoa((int)(fileId))

	ec = NewExtentInCore(name, fileId)
	if ec.fp, err = os.OpenFile(ec.name, ExtentOpenOpt, 0666); err != nil {
		return nil, ErrorCreateExsitFile
	}
	if err = os.Truncate(name, ExtentHeaderSize); err != nil {
		return nil, err
	}

	return
}

func (s *ExtentStore) getExtentIncoreById(fileId uint32) (ec *ExtentInCore, err error) {
	var ok bool
	if ec, ok = s.getExtentFromCache(fileId); !ok {
		ec, err = s.loadExtentIncoreFromDisk(fileId)
	}

	return ec, err
}

func (s *ExtentStore) loadExtentIncoreFromDisk(fileId uint32) (ec *ExtentInCore, err error) {
	name := s.dataDir + "/" + strconv.Itoa((int)(fileId))
	ec = NewExtentInCore(name, fileId)
	if err = s.openExtentFromDisk(ec); err == nil {
		s.addExtentToCache(ec)
	}

	return
}

func (s *ExtentStore) initBaseFileId() (err error) {
	var maxFileId int
	if files, err := ioutil.ReadDir(s.dataDir); err == nil {
		for _, f := range files {
			fileId, err := strconv.Atoi(f.Name())
			if err != nil {
				continue
			}
			if fileId < (int)(s.servId<<HighBit) || fileId > (int)(s.servId<<(HighBit+1)) {
				continue
			}
			if fileId >= maxFileId {
				maxFileId = fileId
			}
		}
		s.baseFileId = (uint32)(maxFileId)
		if s.baseFileId == 0 {
			s.emptyDirInitBaseFileId()
		}
	}

	return
}

func (s *ExtentStore) emptyDirInitBaseFileId() {
	s.baseFileId = s.servId << HighBit
}

func (s *ExtentStore) GetAvaliFileId() (fileId uint32) {
	fileId = atomic.AddUint32(&s.baseFileId, (uint32)(1))

	return
}

func (s *ExtentStore) openExtentFromDisk(ec *ExtentInCore) (err error) {
	ec.writelock()
	defer ec.writeUnlock()

	if ec.fp, err = os.OpenFile(ec.name, os.O_RDWR|os.O_EXCL, 0666); err != nil {
		if strings.Contains(err.Error(), syscall.ENOENT.Error()) {
			err = ErrorNotFound
		}
		return err
	}

	if _, err = ec.fp.ReadAt(ec.extentCrc, 0); err != nil {
		return
	}

	return
}

func (s *ExtentStore) Write(fileId uint32, offset, size int64, data []byte, crc uint32, repairFlag bool) (err error) {
	var ec *ExtentInCore
	if ec, err = s.getExtentIncoreById(fileId); err != nil {
		return
	}
	if err = s.checkOffsetAndSize(offset, size); err != nil {
		return
	}
	if repairFlag != RepairOpFlag && ec.extentCrc[MarkDeleteIndex] == MarkDelete {
		err = ErrorHasDelete
		return
	}

	ec.readlock()
	defer ec.readUnlock()
	if _, err = ec.fp.WriteAt(data[:size], offset+ExtentHeaderSize); err != nil {
		return
	}

	blockNo := offset / BlockSize
	binary.BigEndian.PutUint32(ec.extentCrc[blockNo*4:(blockNo+1)*4], crc)
	if _, err = ec.fp.WriteAt(ec.extentCrc[blockNo*4:(blockNo+1)*4], blockNo*4); err != nil {
		return
	}

	return
}

func (s *ExtentStore) checkOffsetAndSize(offset, size int64) error {
	if offset+size > BlockSize*BlockCount {
		return ErrorUnmatchPara
	}
	if offset >= BlockCount*BlockSize || size == 0 {
		return ErrorUnmatchPara
	}

	offsetInBlock := offset % BlockSize
	if offsetInBlock+size > BlockSize || offsetInBlock != 0 {
		return ErrorUnmatchPara
	}

	return nil
}

func (s *ExtentStore) Read(fileId uint32, offset, size int64, nbuf []byte, repairFlag bool) (crc uint32, err error) {
	var ec *ExtentInCore
	if ec, err = s.getExtentIncoreById(fileId); err != nil {
		return
	}
	if err = s.checkOffsetAndSize(offset, size); err != nil {
		return
	}
	if repairFlag != RepairOpFlag && ec.extentCrc[MarkDeleteIndex] == MarkDelete {
		err = ErrorHasDelete
		return
	}

	ec.readlock()
	defer ec.readUnlock()
	if _, err = ec.fp.ReadAt(nbuf[:size], offset+ExtentHeaderSize); err != nil {
		return
	}

	blockNo := offset / BlockSize
	crc = binary.BigEndian.Uint32(ec.extentCrc[blockNo*4 : (blockNo+1)*4])

	return
}

func (s *ExtentStore) MarkDelete(fileId uint32, offset, size int64) (err error) {
	var ec *ExtentInCore
	if ec, err = s.getExtentIncoreById(fileId); err != nil {
		return
	}

	ec.readlock()
	defer ec.readUnlock()
	ec.extentCrc[MarkDeleteIndex] = MarkDelete
	if _, err = ec.fp.WriteAt(ec.extentCrc, 0); err != nil {
		return
	}

	return
}

func (s *ExtentStore) Delete(fileId uint32) (err error) {
	var ec *ExtentInCore
	if ec, err = s.getExtentIncoreById(fileId); err != nil {
		return nil
	}

	s.delExtentFromCache(ec)
	if err = ec.deleteExtent(); err != nil {
		return nil
	}

	return
}

func (s *ExtentStore) IsMarkDelete(fileId uint32) (isMarkDelete bool, err error) {
	var ec *ExtentInCore
	if ec, err = s.getExtentIncoreById(fileId); err != nil {
		return
	}
	isMarkDelete = ec.extentCrc[MarkDeleteIndex] == MarkDelete

	return
}

func (s *ExtentStore) Sync(fileId uint32) (err error) {
	var ec *ExtentInCore
	if ec, err = s.getExtentIncoreById(fileId); err != nil {
		return
	}
	ec.readlock()
	defer ec.readUnlock()

	return ec.fp.Sync()
}

func (s *ExtentStore) SyncAll() { /*notici this function must called on program exit or kill */
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, v := range s.extentCoreMap {
		v.readlock()
		v.fp.Sync()
		v.readUnlock()
	}
}

func (s *ExtentStore) GetExtentHeader(fileId uint32, headerBuff []byte) (err error) {
	var ec *ExtentInCore
	if ec, err = s.getExtentIncoreById(fileId); err != nil {
		return
	}

	if len(headerBuff) != ExtentHeaderSize {
		return errors.New("header buff is not ExtentHeaderSize")
	}

	ec.readlock()
	_, err = ec.fp.ReadAt(headerBuff, 0)
	ec.readUnlock()

	return
}

func (s *ExtentStore) GetWatermark(fileId uint32) (size int64, err error) {
	var (
		ec    *ExtentInCore
		finfo os.FileInfo
	)
	if ec, err = s.getExtentIncoreById(fileId); err != nil {
		return
	}
	ec.readlock()
	defer ec.readUnlock()

	finfo, err = ec.fp.Stat()
	if err != nil {
		return
	}
	size = finfo.Size() - ExtentHeaderSize

	return
}

func (s *ExtentStore) Snapshot() (files []*VolFile, err error) {
	files, err = DirSnapshot(ExtentDir, s.dataDir)

	return files, err
}

func (s *ExtentStore) FileExist(fileId uint32) (exist bool) {
	name := s.dataDir + "/" + strconv.Itoa((int)(fileId))
	if _, err := os.Stat(name); err == nil {
		exist = true
		warterMark, err := s.GetWatermark(fileId)
		if err == io.EOF || warterMark < ExtentHeaderSize {
			err = s.FillEmptyCrc(name, BlockSize)
		}
	}

	return
}

func (s *ExtentStore) FillEmptyCrc(name string, blockSize int64) (err error) {
	if fp, err := os.OpenFile(name, os.O_RDWR|os.O_EXCL, 0666); err == nil {
		emptyCrc := crc32.ChecksumIEEE(make([]byte, blockSize))
		extentCrc := make([]byte, ExtentHeaderSize)
		for blockNo := 0; blockNo < BlockCount; blockNo++ {
			binary.BigEndian.PutUint32(extentCrc[blockNo*4:(blockNo+1)*4], emptyCrc)
		}
		if _, err = fp.WriteAt(extentCrc, 0); err != nil {
			log.GetLog().LogError("write header errror", err.Error())
		}
		fp.Close()
	}

	return
}

func (s *ExtentStore) GetSoreFileCount() (files int, err error) {
	var finfos []os.FileInfo

	if finfos, err = ioutil.ReadDir(s.dataDir); err == nil {
		files = len(finfos)
	}

	return
}

func (s *ExtentStore) GetStoreUsedSize() (size int64) {
	if finfoArray, err := ioutil.ReadDir(s.dataDir); err == nil {
		for _, finfo := range finfoArray {
			size += finfo.Size()
		}
	}

	return
}

func (s *ExtentStore) GetAvaliChunk() (chunkId int, err error) {
	return
}

func (s *ExtentStore) PutAvaliChunk(chunkId int) {
}

func (s *ExtentStore) GetUnAvaliChunk() (chunkId int, err error) {
	return
}

func (s *ExtentStore) PutUnAvaliChunk(chunkId int) {
}

func (s *ExtentStore) GetUnAvaliChanLen() (chanLen int) {
	return 0
}

func (s *ExtentStore) GetAllWatermark() (chunks []*ChunkInfo, err error) {

	return
}

func (s *ExtentStore) SetAllChunkAvali() {
	return
}
