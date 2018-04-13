package netservice

import (
	"fmt"
	"net"
	"strconv"
	"sync"
)

type phandler func(*Packet)

type TcpServer struct {
	ip         string
	port       uint32
	conns      map[int]net.Conn
	conMapLock sync.Mutex

	recvChan chan *Packet
	sendChan chan *Packet

	handlers map[int]phandler
}

type PacketHeader struct {
	ptype   uint32
	id      uint64
	datalen uint16
	crc     uint32
}

type Packet struct {
	connid int
	header *PacketHeader
	data   []byte
}

func (this *TcpServer) RegisterHandler(packettype int, handler phandler) (err error) {
	if exist, _ = this.handlers[packettype]; exist {
		return ErrHandlerDupReigster
	}
	this.handlers[packettype] = handler
	return
}

func NewTcpServer(ip string, port uint32) (s *TcpServer) {
	s := &TcpServer{
		ip:       ip,
		port:     port,
		recvChan: make(chan *Packet, MAX_RECV_CHAN_LEN),
		sendChan: make(chan *Packet, MAX_SEND_CHAN_LEN),
		conns:    make(map[int]net.Conn),
		handlers: make(map[int]phandler),
	}
	return s
}

func (this *TcpServer) handleConnections(conn net.Conn) {

}

func (this *TcpServer) Start() (err error) {
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(this.port))
	if err != nil {
		return errors.New("Failed to bind port " + strconv.Itoa(this.port) + err.Error())
	}
}
