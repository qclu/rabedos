package datanodeIdc

import (
	"errors"
	"utils/rbdlogger"
)

var (
	ErrChunkOffsetUnmatch = errors.New("ChunkOffsetUnmatchErr")
)

func (s *DataNode) HandleChunkInfo(pkg *Packet) (err error) {
	if s.storeType != ChunkStoreType || !pkg.IsWriteOperation() {
		return
	}

	if !pkg.IsHeadNode() {
		err = s.CheckChunkInfo(pkg)
	} else {
		err = s.HeadNodeSetChunkInfo(pkg)
	}

	if err != nil {
		pkg.PackErrorReply(NoFlag, HandleChunk, err.Error(), nil)
	}

	return
}

func (s *DataNode) CheckAndAddInfos(pkg *Packet) (err error) {
	switch s.storeType {
	case ChunkStoreType:
		return s.HandleChunkInfo(pkg)
	case ExtentStoreType:
		if pkg.IsHeadNode() && pkg.Opcode == OpCreateFile {
			pkg.FileId = pkg.vol.Store.GetAvaliFileId()
		}
		return nil
	}
	return nil
}

func (s *DataNode) CheckChunkInfo(pkg *Packet) (err error) {
	offset, err := pkg.vol.Store.GetWatermark(pkg.FileId)
	if pkg.Offset != int64(offset) {
		err = ErrChunkOffsetUnmatch
		log.GetLog().LogWarn("CheckChunkInfo,", pkg.GetInfo(GetPackInfoOnlyId), "offset:", pkg.Offset, "self offset:", offset)
	}

	return
}

func (s *DataNode) HeadNodeSetChunkInfo(pkg *Packet) (err error) {
	cId, err := pkg.vol.Store.GetAvaliChunk()
	if err != nil {
		log.GetLog().LogWarn("HeadNodeSetChunkInfo, get avalichunk,", pkg.GetInfo(GetPackInfoOnlyId), "err:", err)
		return
	}

	pkg.FileId = uint32(cId)
	offset, err := pkg.vol.Store.GetWatermark(pkg.FileId)
	pkg.Offset = int64(offset)
	if err == nil {
		log.GetLog().LogDebug("HeadNodeSetChunkInfo,", pkg.GetInfo(GetPackInfoOnlyId), "offset:", pkg.Offset)
		return
	}
	log.GetLog().LogWarn("HeadNodeSetChunkInfo, put unavailchunk,", pkg.GetInfo(GetPackInfoOnlyId))
	pkg.vol.Store.PutUnAvaliChunk(int(pkg.FileId))

	return
}

func (s *DataNode) HeadNodePutChunk(pkg *Packet) {
	if s.storeType != ChunkStoreType || !pkg.IsHeadNode() || !pkg.IsWriteOperation() {
		return
	}

	if pkg.IsErrPack() {
		log.GetLog().LogWarn("put unavailchunk,", pkg.GetInfo(GetPackInfoOnlyId))
		pkg.vol.Store.PutUnAvaliChunk(int(pkg.FileId))
	} else {
		log.GetLog().LogDebug("put availchunk,", pkg.GetInfo(GetPackInfoOnlyId))
		pkg.vol.Store.PutAvaliChunk(int(pkg.FileId))
	}
}

func (s *DataNode) HeadNodePutChunks(msgH *MsgHandler) {
	if msgH.sentList.Len() <= 0 {
		return
	}

	pkg := msgH.sentList.Front().Value.(*Packet)
	if s.storeType != ChunkStoreType || !pkg.IsHeadNode() || !pkg.IsWriteOperation() {
		return
	}

	for e := msgH.sentList.Front(); e != nil; e = e.Next() {
		s.HeadNodePutChunk(e.Value.(*Packet))
	}
}
