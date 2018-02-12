package datanodeIdc

import (
	"container/list"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"utils/rbdlogger"
)

var single = struct{}{}

var ErrReplyIdLessThanReqId = errors.New("ReplyIdLessThanReqIdErr")

type MsgHandler struct {
	listMux  sync.RWMutex
	sentList *list.List
	handleCh chan struct{}
	exitCh   chan bool
	exitMux  sync.Mutex
	reqCh    chan *Packet
	replyCh  chan *Packet
	inConn   *net.TCPConn
}

func NewMsgHandler(exitChLen, reqChLen, replyChLen int, inConn *net.TCPConn) *MsgHandler {
	m := new(MsgHandler)
	m.sentList = list.New()
	m.exitCh = make(chan bool, exitChLen)
	m.handleCh = make(chan struct{}, replyChLen)
	m.reqCh = make(chan *Packet, reqChLen)
	m.replyCh = make(chan *Packet, replyChLen)
	m.inConn = inConn

	return m
}

func (msgH *MsgHandler) Renew(isHeadNode bool) {
	if !isHeadNode {
		msgH.sentList = list.New()
	}
}

func (msgH *MsgHandler) Exit() {
	msgH.exitMux.Lock()
	if !GetSign(msgH.exitCh) {
		close(msgH.exitCh)
	}
	msgH.exitMux.Unlock()
}

func (msgH *MsgHandler) PutSentReqsToCh(replyId int64, isHeadNode bool, unhandled *bool, errMsg *string) (exit bool) {
	if !isHeadNode {
		return
	}
	msgH.listMux.Lock()
	defer msgH.listMux.Unlock()

	for e := msgH.sentList.Front(); e != nil; e = e.Next() {
		eof := strings.Contains(*errMsg, io.EOF.Error())
		log.GetLog().LogWarn("PutSentReqsToCh, reply rId:", replyId, "list front rId:", e.Value.(*Packet).ReqId, "errMsg:", *errMsg, "eof:", eof)
		if (replyId < e.Value.(*Packet).ReqId) || eof {
			if !eof {
				//*unhandled = true
				*errMsg = *errMsg + ":" + ErrReplyIdLessThanReqId.Error()
			}
			log.GetLog().LogWarn("wrong reply rId:", replyId, "list front rId:", e.Value.(*Packet).ReqId)
			break
		}
		if replyId == e.Value.(*Packet).ReqId {
			e.Value.(*Packet).Received()
			break
		}

		*unhandled = true
		newReq := PackPreReq(e.Value.(*Packet))
		addr := ""
		if newReq.nextConn != nil {
			addr = newReq.nextConn.LocalAddr().String()
		}
		log.GetLog().LogWarn("wrong reply rId:", replyId, "put rId:", newReq.ReqId, "to chan again", "unhandled:", *unhandled, "size:", newReq.Size,
			" next addr:", addr)
		// if !PutPkg(msgH.reqCh, msgH.exitCh, newReq) {
		// 	return true
		// }
	}

	return
}

func (msgH *MsgHandler) GetListElement() (e *list.Element) {
	msgH.listMux.RLock()
	e = msgH.sentList.Front()
	msgH.listMux.RUnlock()

	return
}

func (msgH *MsgHandler) PushListElement(e *Packet) {
	msgH.listMux.Lock()
	log.GetLog().LogDebug("PushListElement, rId:", e.ReqId)
	msgH.sentList.PushBack(e)
	msgH.listMux.Unlock()
}

func (msgH *MsgHandler) ClearReqs(s *DataNode) {
	msgH.listMux.Lock()
	log.GetLog().LogDebug("Clear, element len:", msgH.sentList.Len(), " reqs:", len(msgH.reqCh), " reps:", len(msgH.replyCh))
	for e := msgH.sentList.Front(); e != nil; e = e.Next() {
		if e.Value.(*Packet).nextAddr != "" && !e.Value.(*Packet).IsTailNode() {
			s.HeadNodePutChunk(e.Value.(*Packet))
			s.CleanConn(e.Value.(*Packet))
			log.GetLog().LogDebug("ClearReqs, remove rId:", e.Value.(*Packet).ReqId)
		}
		e.Value.(*Packet).FreeBody()
	}
	msgH.listMux.Unlock()

	reqs := len(msgH.reqCh)
	for i := 0; i < reqs; i++ {
		rep := <-msgH.reqCh
		s.HeadNodePutChunk(rep)
		log.GetLog().LogDebug("ClearReqs, remove rId:", rep.ReqId)
		rep.FreeBody()
	}
}

func (msgH *MsgHandler) ClearReplys() {
	replys := len(msgH.replyCh)
	for i := 0; i < replys; i++ {
		reply := <-msgH.replyCh
		reply.FreeBody()
	}
}

func (msgH *MsgHandler) DelListElement(reqId int64, e *list.Element, s *DataNode) (exit bool) {
	msgH.listMux.Lock()
	defer msgH.listMux.Unlock()

	if !e.Value.(*Packet).IsHeadNode() {
		msgH.sentList.Remove(e)
		if !e.Value.(*Packet).IsTailNode() {
			s.CleanConn(e.Value.(*Packet))
		}
		log.GetLog().LogDebug("DelListElement, remove rId:", e.Value.(*Packet).ReqId)
		if !PutPkg(msgH.replyCh, msgH.exitCh, e.Value.(*Packet)) {
			exit = true
		}
		return
	}

	IsReceivedFront := false
	for e := msgH.sentList.Front(); e != nil; e = e.Next() {
		if reqId == e.Value.(*Packet).ReqId {
			IsReceivedFront = true
		}
		if IsReceivedFront || (IsReceivedFront && e.Value.(*Packet).isLocalHandled) {
			msgH.sentList.Remove(e)
			s.HeadNodePutChunk(e.Value.(*Packet))
			s.CleanConn(e.Value.(*Packet))
			log.GetLog().LogDebug("DelListElement, remove rId:", e.Value.(*Packet).ReqId)
			if !PutPkg(msgH.replyCh, msgH.exitCh, e.Value.(*Packet)) {
				return true
			}
		} else {
			break
		}
	}

	return
}
