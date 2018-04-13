package netservice

import (
	"io"
	"net"
	"sync"
	"time"
)

type TCPClient struct {
	conn           *net.TCPConn
	recvChan       chan *Message
	sendChan       chan *Message
	exitChan       chan bool
	recvRtStopChan chan bool
	sendRtStopChan chan bool
	reconnectChan  chan bool
	taskMap        map[int64]*Task
	tmLock         sync.Mutex
	srvAddr        string
	keepAlive      bool
	wg             sync.WaitGroup
	lastActive     time.Time
}

func needReconnect(err error) (reconnect bool) {
	if err == nil {
		return false
	}
	if err == io.EOF {
		return true
	}
	return false
}

func (this *TCPClient) fetchTask(taskID int64) (*Task, bool) {
	this.tmLock.Lock()
	defer this.tmLock.Unlock()
	task, ok := this.taskMap[taskID]
	if ok {
		delete(this.taskMap, task.req.ReqID)
	}
	return task, ok
}

func (this *TCPClient) addTask(task *Task) (err error) {
	this.tmLock.Lock()
	defer this.tmLock.Unlock()
	if _, ok := this.taskMap[task.req.ReqID]; ok {
		err = ErrInternalReqIDDup
		return
	}
	this.taskMap[task.req.ReqID] = task
	this.sendChan <- task.req
	return
}

func (this *TCPClient) delTask(task *Task) (err error) {
	this.tmLock.Lock()
	defer this.tmLock.Unlock()
	if _, ok := this.taskMap[task.req.ReqID]; !ok {
		err = ErrInternalTaskNotExist
		return
	}
	delete(this.taskMap, task.req.ReqID)
	return
}

func NewTcpClient(srvaddr string, isLongConn bool) (*TCPClient, error) {
	var (
		err  error
		conn net.Conn
	)
	clt := &TCPClient{
		srvAddr:        srvaddr,
		sendChan:       make(chan *Message, TCP_SEND_CHAN_LEN),
		recvChan:       make(chan *Message, TCP_RECV_CHAN_LEN),
		exitChan:       make(chan bool, 1),
		recvRtStopChan: make(chan bool, 1),
		sendRtStopChan: make(chan bool, 1),
		reconnectChan:  make(chan bool, 1),
		keepAlive:      isLongConn,
		taskMap:        make(map[int64]*Task, TCP_SEND_CHAN_LEN),
	}
	for i := 0; i < CONN_MAX_RETRY_TIMES; i++ {
		if conn, err = net.DialTimeout(CONN_TYPE, srvaddr, time.Duration(CONN_TIMEOUT_DURATION)*time.Second); err == nil {
			clt.conn = conn.(*net.TCPConn)
			clt.conn.SetKeepAlive(true)
			clt.conn.SetNoDelay(true)
			clt.conn.SetReadBuffer(CONN_BUFFER_SIZE)
			break
		}
	}
	if err != nil {
		return nil, err
	}
	go clt.workingprocess()
	go clt.receiveData()
	go clt.sendDataAndHB()
	return clt, err
}

func (this *TCPClient) reconnect() {
	var (
		err  error
		conn net.Conn
	)
	this.close()

	Info.Println("Start reconnect...")
	for i := 0; i < CONN_MAX_RETRY_TIMES; i++ {
		if conn, err = net.DialTimeout(CONN_TYPE, this.srvAddr, time.Duration(CONN_TIMEOUT_DURATION)*time.Second); err == nil {
			this.conn = conn.(*net.TCPConn)
			this.conn.SetKeepAlive(true)
			this.conn.SetNoDelay(true)
			this.conn.SetReadBuffer(CONN_BUFFER_SIZE)
			break
		}
	}
	if err != nil {
		Error.Fatal("reconnect to ", this.srvAddr, " failed, err: ", err.Error())
		this.exitChan <- true
		return
	}

	this.tmLock.Lock()
	for _, task := range this.taskMap {
		if task.rpl == nil {
			this.sendChan <- task.req
		}
	}
	this.tmLock.Unlock()

	go this.receiveData()
	go this.sendDataAndHB()
	Info.Println("reconnect finish...")
	return
}

func (this *TCPClient) workingprocess() {
	for true {
		select {
		case reply := <-this.recvChan:
			task, ok := this.fetchTask(reply.ReqID)
			if !ok {
				this.exitChan <- true
				Error.Fatal("unexpected reply, no corresponding task, reqid: ", reply.ReqID)
			}
			task.rpl = reply
			task.wg.Done()
		case <-this.reconnectChan:
			Info.Println("reconnect ...")
			this.close()
			this.reconnect()
		case <-this.exitChan:
			if len(this.recvChan) > 0 {
				continue
			}
			this.close()
			this.taskMap = make(map[int64]*Task, 0)
			return
		}
	}
	return
}

func (this *TCPClient) release() {
	this.exitChan <- true
	this.close()
}

func (this *TCPClient) close() {
	Info.Println("start close current connection")
	this.wg.Wait()
	this.conn.Close()
	Info.Println("finish close current connection")
}

func (this *TCPClient) sendHeartPacket() error {
	_, err := this.conn.Write([]byte{})
	return err
}

func (this *TCPClient) sendDataAndHB() {
	this.wg.Add(1)
	defer this.wg.Done()
	var (
		err     error
		sendBuf []byte
	)

	Info.Println("send routine start...")
	Info.Println("send chan len: ", len(this.sendChan))
	for {
		select {
		//数据发送统一接口
		case msg := <-this.sendChan:
			sendBuf, err := msg.Marshal()
			if _, err = this.conn.Write(sendBuf); err != nil {
				if needReconnect(err) {
					Error.Println(err)
					this.sendChan <- pkg
					goto Stop
				}
				if err != nil {
					Error.Println(err)
					goto Exit
				}
			}
			this.lastActive = time.Now()
			//定时发送心跳数据
		case <-time.Tick(10 * time.Second):
			err = this.sendHeartPacket()
			if needReconnect(err) {
				goto Stop
			}
			if err != nil {
				goto Exit
			}
			//防止心跳发送chan
		case <-this.sendRtStopChan:
			return
		}
	}
Exit:
	Error.Fatal(err)
	this.exitChan <- true
	return
Stop:
	Error.Println("try reconnect to ", this.srvAddr, ", as error occured when send data, err: ", err.Error())
	this.recvRtStopChan <- true
	this.reconnectChan <- true
	return
}

func (this *TCPClient) receiveData() {
	header := make([]byte, datanode.HeaderSize)
	this.wg.Add(1)
	defer this.wg.Done()
	Info.Println("receive routine start...")
	Info.Println("receive chan len: ", len(this.recvChan))
	var (
		err error
	)

	for {
		reply := datanode.NewPacket()
		if _, err = io.ReadFull(this.conn, header); err != nil {
			if needReconnect(err) {
				goto Stop
			}
			Error.Println(err.Error())
			goto Exit
		}
		err = reply.UnmarshalHeader(header)
		if err != nil {
			Error.Println(err.Error())
			goto Exit
		}

		if reply.Arglen > 0 {
			reply.Arg = make([]byte, reply.Arglen)
			if _, err = io.ReadFull(this.conn, reply.Arg); err != nil {
				if needReconnect(err) {
					goto Stop
				}
				Error.Println(err.Error())
				goto Exit
			}
		}

		if reply.Size > 0 {
			reply.Data = make([]byte, reply.Size)
			if _, err = io.ReadFull(this.conn, reply.Data); err != nil {
				if needReconnect(err) {
					goto Stop
				}
				Error.Println(err.Error())
				goto Exit
			}
		}
		this.lastActive = time.Now()
		this.recvChan <- reply

		select {
		case <-this.recvRtStopChan:
			return
		default:
		}
	}
Exit:
	Error.Fatal(ErrInternalUnmatchedPkg)
	this.exitChan <- true
	return
Stop:
	Error.Println("try reconnect to ", this.srvAddr, ", as error occured when receive data, err: ", err.Error())
	this.sendRtStopChan <- true
	this.reconnectChan <- true
	return
}
