package datanodeIdc

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
	"utils/rbdlogger"
)

func (s *DataNode) readFromCli(msgH *MsgHandler) (err error) {
	pkg := NewPacket()
	start := time.Now().UnixNano()
	err = pkg.ReadFromConn(msgH.inConn, NoReadDeadlineTime, int64(s.pkgDataMaxSize))
	if err == nil {
		log.GetLog().LogDebug(ReadFromCli, pkg.GetInfo(GetPackInfoOpDetail), "size:", pkg.Size, "offset:", pkg.Offset, "nodes:",
			pkg.Nodes, "inC:", msgH.inConn.RemoteAddr().String(), "t:", (time.Now().UnixNano()-start)/1e6, " chuli ", time.Now().Unix())
		err = s.CheckPacket(pkg)
		s.statsFlow(pkg, InFlow)
	}
	if err != nil {
		inConn := msgH.inConn.RemoteAddr().String()
		mesg := ""
		if err == io.EOF {
			mesg = fmt.Sprintf("the conn was closed by peer")
		}
		mesg = fmt.Sprintf(ReadFromCli+mesg+" occous error %v on connect:%v pkg:%v", err.Error(),
			inConn, pkg.GetInfo(GetPackInfoOpDetail))
		log.GetLog().LogError(mesg)
		msgH.Exit()
		return
	}

	if err := s.CheckAndAddInfos(pkg); err == nil {
		PutPkg(msgH.reqCh, msgH.exitCh, pkg)
		return nil
	}
	msgH.replyCh <- pkg
	return nil

	// if err := s.HandleChunkInfo(pkg); err == nil {
	// 	PutPkg(msgH.reqCh, msgH.exitCh, pkg)
	// 	return err
	// }
	// msgH.replyCh <- pkg

	// return nil
}

func (s *DataNode) handleReqs(msgH *MsgHandler) {
	for {
		select {
		case <-msgH.handleCh:
			pkg, exit := s.receiveFromNext(msgH)
			if exit {
				s.HeadNodePutChunks(msgH)
				log.GetLog().LogInfo(LogExit+HandleReqs, pkg.GetInfo(GetPackInfoOpDetail))
				return
			}
		case <-msgH.exitCh:
			s.HeadNodePutChunks(msgH)
			msgH.ClearReqs(s)
			log.GetLog().LogDebug(LogExit + HandleReqs + msgH.inConn.RemoteAddr().String())
			return
		case <-s.shutdownCh:
			log.GetLog().LogWarn(LogShutdown+"server start exit:", HandleReqs)
			return
		}
	}
}

func (s *DataNode) writeToCli(msgH *MsgHandler) {
	for {
		select {
		case req := <-msgH.reqCh:
			log.GetLog().LogDebug("writeToCli, sendToNext", req.GetInfo(GetPackInfoOpDetail), "len:", len(msgH.reqCh))
			received, err := s.sendToNext(req, msgH)
			if err == nil && !received {
				s.operatePacket(req, msgH.inConn)
			}
			if !PutSign(msgH.handleCh, msgH.exitCh, single) {
				log.GetLog().LogInfo(LogExit+WriteToCli, req.GetInfo(GetPackInfoOnlyId))
				return
			}
		case reply := <-msgH.replyCh:
			//HandleTimeoutLog(reply.startT, "all end, "+reply.GetInfo(GetPackInfoOpDetail)+" inC:"+msgH.inConn.RemoteAddr().String())
			logmesg := "all end, " + reply.GetInfo(GetPackInfoOpDetail) + " inC:" + msgH.inConn.RemoteAddr().String() +
				" replyCh len "
			if reply.IsErrPack() {
				logmesg += string(reply.Data[:reply.Size])
			}
			if err := reply.WriteToConn(msgH.inConn, WriteDeadlineTime, FreeBodySpace); err != nil {
				mesg := fmt.Sprintf(WriteToCli+" failed to reply to %v occous err %v,pkg:%v so close MsgH Exitch",
					msgH.inConn.RemoteAddr().String(), err.Error(), reply.GetInfo(GetPackInfoOnlyId))
				log.GetLog().LogError(mesg)
				msgH.Exit()
			}
			HandleTimeoutLog(reply.startT, logmesg+strconv.Itoa(len(msgH.replyCh)))
			s.statsFlow(reply, OutFlow)
		case <-msgH.exitCh:
			msgH.ClearReplys()
			log.GetLog().LogDebug(LogExit+WriteToCli, "conn:", msgH.inConn.RemoteAddr().String())
			return
		case <-s.shutdownCh:
			log.GetLog().LogWarn(LogShutdown+"server start exit: ", WriteToCli)
			return
		}
	}
}

func (s *DataNode) receiveFromNext(msgH *MsgHandler) (req *Packet, exit bool) {
	var unhandled, isSave bool
	start := time.Now().UnixNano()
	e := msgH.GetListElement()
	if e == nil {
		log.GetLog().LogInfo(ReadFromNext, "no element in list.")
		return
	}
	req = e.Value.(*Packet)
	defer log.GetLog().LogDebug(ReadFromNext, "end", req.GetInfo(GetPackInfoOnlyId), "size:", req.Size, "t:",
		(time.Now().UnixNano()-start)/1e6)

	if req.IsRelayed() && req.nextConn != nil {
		reply := NewPacket()
		err := reply.ReadFromConn(req.nextConn, ReadOpDeadlineTime, int64(s.pkgDataMaxSize))
		defer func() {
			if reply.Data != nil && !isSave {
				reply.FreeBody()
			}
			s.statsFlow(req, OutFlow)
		}()
		if err == nil {
			s.statsFlow(reply, InFlow)
		}
		if (err == nil && reply.ReqId == req.ReqId) || (req.IsErrPack()) {
			log.GetLog().LogDebug(ReadFromNext, "reply rId:", reply.ReqId, "op:", reply.Opcode, "rId:", req.ReqId, "op:", req.Opcode)
			if !req.IsErrPack() && reply.IsErrPack() {
				req.CopyFrom(reply)
				isSave = true
				log.GetLog().LogWarn(ReadFromNext, "rId:", req.ReqId, "err:", string(req.Data[:req.Size]))
			}
			exit = msgH.DelListElement(req.ReqId, e, s)
			return
		}

		readErr := ""
		if err == nil && !req.IsErrPack() && !req.IsHeadNode() {
			err = ErrMidNodeUnmatchReqId
		}
		if err != nil {
			readErr = err.Error()
			log.GetLog().LogError(ReadFromNext, req.nextConn.RemoteAddr().String(), "reply rId:", reply.ReqId, "rId:", req.ReqId, "err:"+readErr)
			msgH.Renew(req.IsHeadNode())
			req.nextConn, err = s.connPool.GetNewConn(req.nextAddr)
		}

		log.GetLog().LogDebug(ReadFromNext, "reply rId:", reply.ReqId, "rId:", req.ReqId, "readErr:", readErr)
		if err == nil && msgH.PutSentReqsToCh(reply.ReqId, req.IsHeadNode(), &unhandled, &readErr) {
			return req, true
		}
		if err != nil || (readErr != "" && !unhandled) {
			if err != nil {
				readErr += " new err:" + err.Error()
			}
			req.PackErrorReply(NoFlag, ReadFromNext, readErr, nil)
		}
	} else if req.IsRelayed() && !req.IsErrPack() {
		req.PackErrorReply(NoFlag, ReadFromNext, ConnIsNullErr, nil)
	}
	if !unhandled {
		exit = msgH.DelListElement(req.ReqId, e, s)
	}

	return
}

func (s *DataNode) sendToNext(pkg *Packet, msgH *MsgHandler) (received bool, err error) {
	if !pkg.isLocalHandled {
		msgH.PushListElement(pkg)
		log.GetLog().LogDebug("sendToNext,", pkg.GetInfo(GetPackInfoOpDetail), "relay:", pkg.IsRelayed())
	} else {
		received = true
	}
	if !pkg.IsRelayed() {
		return
	}

	pkg.nextConn, err = s.GetNextConn(pkg.nextAddr)
	pkg.Nodes--
	if err == nil {
		log.GetLog().LogDebug("sendToNext, WriteToNext", pkg.GetInfo(GetPackInfoOpDetail))
		err = pkg.WriteToConn(pkg.nextConn, WriteDeadlineTime, NotFreeBodySpace)
		pkg.isSent = true
	}
	if err != nil {
		pkg.PackErrorReply(NoFlag, WriteToNext, err.Error()+" addr:"+pkg.nextAddr, nil)
	}
	pkg.Nodes++

	return
}

func (s *DataNode) CheckStoreType(p *Packet) (err error) {
	if (s.storeType == ChunkStoreType && !p.IsChunkStoreType()) || (s.storeType == ExtentStoreType && p.IsChunkStoreType()) {
		log.GetLog().LogWarn("server store type:", p.GetInfo(GetPackInfoOnlyId), "req fileId:", p.FileId)
		err = ErrStoreTypeUnmatch
	}

	return
}

func (s *DataNode) CheckPacket(pkg *Packet) (err error) {
	pkg.startT = time.Now().UnixNano()
	if err = s.CheckStoreType(pkg); err != nil {
		return
	}

	if err = CheckCrc(pkg); err != nil {
		return
	}
	var addrs []string
	if addrs, err = pkg.UnmarshalAddrs(); err == nil {
		err = pkg.GetNextAddr(addrs)
	}
	if err != nil {
		return
	}

	pkg.preOpcode = pkg.Opcode
	var ok bool
	if pkg.vol, ok = s.smMgr.IsExistVol(pkg.VolId); !ok {
		err = ErrBadVolId
	}

	return
}

func (s *DataNode) statsFlow(pkg *Packet, flag bool) {
	if flag == OutFlow {
		s.stats.AddInDataSize(uint64(pkg.Size + pkg.Arglen + HeaderSize))
		return
	}

	if pkg.IsReadReq() {
		s.stats.AddInDataSize(uint64(pkg.Arglen + HeaderSize))
	} else {
		s.stats.AddInDataSize(uint64(pkg.Size + pkg.Arglen + HeaderSize))
	}

}

func (s *DataNode) GetNextConn(nextAddr string) (conn *net.TCPConn, err error) {
	p, ok := s.connPool.GetUintPool(nextAddr)
	if !ok {
		p, err = s.connPool.NewUnitPool(UnitConnPoolSize, ConnBufferSize, nextAddr, ConnDialTimeout, ConnTryTimes)
	}
	if err != nil {
		return
	}

	return p.Get()
}
