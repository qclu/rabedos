package datanodeIdc

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"utils/rbdlogger"
)

const (
	ChunkCount   = 30
	ChunkOpenOpt = os.O_CREATE | os.O_RDWR
)

type ChunkIndexEnt struct {
}

type ChunkCore struct {
	cfp *os.File
	ifp *os.File
}

func (cc *ChunkCore) release() {
	cc.cfp.Close()
	cc.ifp.Close()
}

type ChunkStore struct {
	dataDir        string
	chunkCoreMap   map[int]*ChunkCore
	availChunkCh   chan int
	unavaliChunkCh chan int
	storeSize      int
}

func NewChunkStore(dataDir string, storeSize int, newMode bool) (s *ChunkStore, err error) {
	s = new(ChunkStore)
	s.dataDir = dataDir
	if err = CheckAndCreateSubdir(dataDir, newMode); err != nil {
		goto errDeal
	}
	s.chunkCoreMap = make(map[int]*ChunkCore)
	if err = s.InitChunkFile(); err != nil {
		goto errDeal
	}

	s.availChunkCh = make(chan int, ChunkCount+1)
	s.unavaliChunkCh = make(chan int, ChunkCount+1)
	for i := 1; i <= ChunkCount; i++ {
		s.unavaliChunkCh <- i
	}
	s.storeSize = storeSize

	return
errDeal:
	err = fmt.Errorf("NewChunkStore [%v] err[%v]", dataDir, err)
	log.GetLog().LogError(err)
	return nil, err
}

func (s *ChunkStore) DeleteStore() {
	s.availChunkCh = make(chan int, ChunkCount+1)
	s.unavaliChunkCh = make(chan int, ChunkCount+1)

	for index, cc := range s.chunkCoreMap {
		cc.cfp.Close()
		delete(s.chunkCoreMap, index)
	}
	os.RemoveAll(s.dataDir)
}

func (s *ChunkStore) InitChunkFile() (err error) {
	for i := 1; i <= ChunkCount; i++ {
		var cc *ChunkCore
		if cc, err = s.createChunkCore(i); err != nil {
			return fmt.Errorf("initChunkFile Error %s", err.Error())
		}
		s.chunkCoreMap[i] = cc
	}

	return
}

func (s *ChunkStore) createChunkCore(chunkId int) (cc *ChunkCore, err error) {
	cc = new(ChunkCore)
	name := s.dataDir + "/" + strconv.Itoa(chunkId)
	if cc.cfp, err = os.OpenFile(name, ChunkOpenOpt, 0666); err != nil {
		return nil, err
	}

	return
}

func (s *ChunkStore) FileExist(fileId uint32) (exist bool) {
	chunkId := (int)(fileId)
	name := s.dataDir + "/" + strconv.Itoa(chunkId)
	if _, err := os.Stat(name); err == nil {
		exist = true
	}
	return
}

func (s *ChunkStore) Write(fileId uint32, offset, size int64, data []byte, crc uint32, repairFlag bool) (err error) {
	var (
		fi os.FileInfo
	)
	chunkId := (int)(fileId)
	cc, ok := s.chunkCoreMap[chunkId]
	if !ok {
		return ErrorNotFound
	}
	if fi, err = cc.cfp.Stat(); err != nil {
		return
	}
	if offset < fi.Size() {
		return ErrorUnmatchPara
	}
	realSize := size
	if repairFlag != RepairOpFlag {
		realSize += 5
		data[size] = UnMarkDelete
		binary.BigEndian.PutUint32(data[size+1:realSize], crc)
	}
	if _, err = cc.cfp.WriteAt(data[:realSize], offset); err != nil {
		return
	}

	return
}

func (s *ChunkStore) Read(fileId uint32, offset, size int64, nbuf []byte, repairFlag bool) (crc uint32, err error) {
	chunkId := (int)(fileId)
	cc, ok := s.chunkCoreMap[chunkId]
	if !ok {
		return 0, ErrorNotFound
	}

	var fi os.FileInfo
	if fi, err = cc.cfp.Stat(); err != nil {
		return
	}

	realSize := size
	if repairFlag != RepairOpFlag {
		realSize += 5
	}

	if offset+realSize > fi.Size() {
		return 0, ErrorUnmatchPara
	}

	if _, err = cc.cfp.ReadAt(nbuf[:realSize], offset); err != nil {
		return
	}
	if repairFlag != RepairOpFlag {
		if nbuf[size] == MarkDelete {
			return 0, ErrorHasDelete
		}
		crc = binary.BigEndian.Uint32(nbuf[size+1 : realSize])
	}

	return
}

func (s *ChunkStore) Sync(fileId uint32) (err error) {
	chunkId := (int)(fileId)
	cc, ok := s.chunkCoreMap[chunkId]
	if !ok {
		return ErrorNotFound
	}

	return cc.cfp.Sync()
}

func (s *ChunkStore) GetAllWatermark() (chunks []*ChunkInfo, err error) {
	chunks = make([]*ChunkInfo, 0)
	for chunkId, cc := range s.chunkCoreMap {
		var finfo os.FileInfo
		finfo, err = cc.cfp.Stat()
		if err != nil {
			return
		}

		ci := &ChunkInfo{CId: chunkId, Size: finfo.Size()}
		chunks = append(chunks, ci)
	}

	return
}

func (s *ChunkStore) GetWatermark(fileId uint32) (offset int64, err error) {
	chunkId := (int)(fileId)
	cc, ok := s.chunkCoreMap[chunkId]
	if !ok {
		return -1, ErrorNotFound
	}
	var fi os.FileInfo
	if fi, err = cc.cfp.Stat(); err == nil {
		offset = fi.Size()
	}

	return
}

func (s *ChunkStore) GetStoreUsedSize() (size int64) {
	var err error
	for _, cc := range s.chunkCoreMap {
		var finfo os.FileInfo
		if finfo, err = cc.cfp.Stat(); err == nil {
			size += finfo.Size()
		}
	}

	return
}

func (s *ChunkStore) GetAvaliChunk() (chunkId int, err error) {
	select {
	case chunkId = <-s.availChunkCh:
	default:
		err = ErrorNoAvaliFile
	}

	return
}

func (s *ChunkStore) SyncAll() {
	for _, chunkFp := range s.chunkCoreMap {
		chunkFp.cfp.Sync()
	}
}

func (s *ChunkStore) Snapshot() (files []*VolFile, err error) {
	files, err = DirSnapshot(ChunkStoreTypeStr, s.dataDir)

	return files, err
}

func (s *ChunkStore) PutAvaliChunk(chunkId int) {
	s.availChunkCh <- chunkId
}

func (s *ChunkStore) SetAllChunkAvali() {
	for {
		chunkid, err := s.GetUnAvaliChunk()
		if err != nil {
			break
		}
		s.PutAvaliChunk(chunkid)
	}
}

func (s *ChunkStore) GetUnAvaliChunk() (chunkId int, err error) {
	select {
	case chunkId = <-s.unavaliChunkCh:
	default:
		err = ErrorNoUnAvaliFile
	}

	return
}

func (s *ChunkStore) PutUnAvaliChunk(chunkId int) {
	s.unavaliChunkCh <- chunkId
}

func (s *ChunkStore) GetSoreFileCount() (files int, err error) {
	var finfos []os.FileInfo

	if finfos, err = ioutil.ReadDir(s.dataDir); err == nil {
		files = len(finfos)
	}

	return
}

func (s *ChunkStore) MarkDelete(fileId uint32, offset, size int64) (err error) {
	chunkId := (int)(fileId)
	cc, ok := s.chunkCoreMap[chunkId]
	if !ok {
		return ErrorNotFound
	}

	var fi os.FileInfo
	if fi, err = cc.cfp.Stat(); err != nil {
		return
	}
	realSize := size + 5
	if offset+realSize > fi.Size() {
		return ErrorUnmatchPara
	}
	delBuf := make([]byte, 1)
	delBuf[0] = MarkDelete
	_, err = cc.cfp.WriteAt(delBuf, offset+size)

	return
}

func (s *ChunkStore) GetAvaliFileId() (fileId uint32) {

	return
}

func (s *ChunkStore) GetExtentHeader(fileId uint32, headerBuff []byte) (err error) {
	return
}

func (s *ChunkStore) Create(fileId uint32) (err error) {
	return
}

func (s *ChunkStore) Delete(fileId uint32) (err error) {
	return
}

func (s *ChunkStore) GetStoreActiveFiles() (activeFiles int) {
	return
}

func (s *ChunkStore) CloseStoreActiveFiles() {
}

func (s *ChunkStore) GetUnAvaliChanLen() (chanLen int) {
	return len(s.unavaliChunkCh)
}
