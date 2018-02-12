package rbdproto

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"time"

	. "utils/bufpool"
)

var (
	headBufPool *UnitPool
	dataBufPool *UnitPool
)

func InitUnitBufPools(size, poolCap []int) error {
	if len(size) != len(poolCap) {
		return ErrBufPoolInfoUnmatch
	}
	headBufPool, _ = GlobalBufferPool.NewUnitPool(size[0], poolCap[0])
	dataBufPool, _ = GlobalBufferPool.NewUnitPool(size[2], poolCap[2])
	print("init buf pool, size:", poolCap[2], "\n")
	return nil
}

type PacketHeader struct {
	Magic  uint8
	Opcode uint8
	Crc    uint32
	Size   uint32
	VolId  uint32
	FileId uint32
	Offset int64
	ReqId  int64
}

type Packet struct {
	Header PacketHeader
	Data   []byte
}

func NewPacket() *Packet {
	p := new(Packet)
	p.Header.Magic = ProtoMagic

	return p
}

func NewPacketOpc(opc uint8) *Packet {
	p := new(Packet)
	p.Header.Magic = ProtoMagic
	p.Header.Opcode = opc

	return p
}

func (p *Packet) marshalHeader(out []byte) {
	out[0] = p.Header.Magic
	out[1] = p.Header.Opcode
	binary.BigEndian.PutUint32(out[2:6], p.Header.Crc)
	binary.BigEndian.PutUint32(out[6:10], p.Header.Size)
	binary.BigEndian.PutUint32(out[10:14], p.Header.VolId)
	binary.BigEndian.PutUint32(out[14:18], p.Header.FileId)
	binary.BigEndian.PutUint64(out[18:26], uint64(p.Header.Offset))
	binary.BigEndian.PutUint64(out[26:34], uint64(p.Header.ReqId))

	return
}

func (p *Packet) unmarshalHeader(in []byte) error {
	p.Header.Magic = in[0]
	if p.Header.Magic != ProtoMagic {
		return errors.New("Bad Magic " + strconv.Itoa(int(p.Header.Magic)))
	}

	p.Header.Opcode = in[1]
	p.Header.Crc = binary.BigEndian.Uint32(in[2:6])
	p.Header.Size = binary.BigEndian.Uint32(in[6:10])
	p.Header.VolId = binary.BigEndian.Uint32(in[10:14])
	p.Header.FileId = binary.BigEndian.Uint32(in[14:18])
	p.Header.Offset = int64(binary.BigEndian.Uint64(in[18:26]))
	p.Header.ReqId = int64(binary.BigEndian.Uint64(in[26:34]))

	return nil
}

func (p *Packet) ReleaseBuf() {
	dataBufPool.Free(p.Data)
}

func (p *Packet) GetRealDataSize() (realSize int) {
	realSize = int(p.Header.Size)
	if p.Header.Opcode == OpRead || p.Header.Opcode == OpMarkDelete {
		realSize = 0
	}

	return
}

func (p *Packet) LimitDataSize(defaultDataSize int) (size int) {
	if p.Header.Opcode == OpStreamRead && int(p.Header.Size) > defaultDataSize {
		size = defaultDataSize
		return
	}

	return int(p.Header.Size)
}

func (p *Packet) WriteToConn(c net.Conn, deadlineTime time.Duration, freeBody bool) (err error) {
	c.SetWriteDeadline(time.Now().Add(deadlineTime * time.Second))
	header, _ := headBufPool.Get(HeaderSize)

	p.marshalHeader(header)
	if _, err = c.Write(header); err == nil {
		if p.Data != nil {
			_, err = c.Write(p.Data[:p.GetRealDataSize()])
		}
	}

	headBufPool.Free(header)
	if freeBody {
		p.ReleaseBuf()
	}

	return
}

func (p *Packet) WriteHeaderToConn(c net.Conn) (err error) {
	header, _ := headBufPool.Get(HeaderSize)

	p.marshalHeader(header)
	_, err = c.Write(header)

	headBufPool.Free(header)

	return
}

func (p *Packet) ReadFromConn(c net.Conn, deadlineTime time.Duration) (err error) {
	if deadlineTime != NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(deadlineTime * time.Second))
	}
	header, _ := headBufPool.Get(HeaderSize)
	defer headBufPool.Free(header)

	if _, err = io.ReadFull(c, header); err != nil {
		return
	}
	if err = p.unmarshalHeader(header); err != nil {
		return
	}

	if p.Header.Size <= 0 {
		return
	}

	p.Data, err = dataBufPool.Get(int(p.Header.Size))
	if err != nil {
		return
	}
	if _, err = io.ReadFull(c, p.Data); err != nil {
		p.ReleaseBuf()
	}

	return
}
