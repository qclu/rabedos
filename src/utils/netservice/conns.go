package netservice

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

type TCPConns struct {
	clts      map[string]*TCPClient
	connsLock sync.Mutex
}

func (this *TCPConns) connChecker() {
	tick := time.NewTicker(time.Second * CONN_TIMEOUT_DURATION)
	for {
		select {
		case <-tick.C:
			this.connsLock.Lock()
			for _, conn := range this.clts {
				now := time.Now()
				if now.Sub(conn.lastActive)/time.Second < CONN_TIMEOUT_DURATION {
					continue
				}
				conn.release()
				delete(this.clts, conn.srvAddr)
			}
			this.connsLock.Unlock()
		}
	}
}

func newTcpConns() *TCPConns {
	conns := &TCPConns{
		clts: make(map[string]*TCPClient),
	}
	return conns
}

func (this *TCPConns) release() {
	this.connsLock.Lock()
	defer this.connsLock.Unlock()
	for _, conn := range this.clts {
		conn.release()
	}
	this.clts = make(map[string]*TCPClient)
}

func (this *TCPConns) getConn(addr string) (conn *TCPClient, err error) {
	var (
		exist bool
	)
	this.connsLock.Lock()
	conn, exist = this.clts[addr]
	defer this.connsLock.Unlock()
	if exist {
		return
	}
	for i := 0; i < CONN_MAX_RETRY_TIMES; i++ {
		if conn, err = NewTcpClient(addr, true); err == nil {
			break
		}
	}
	if err != nil {
		return
	}
	this.clts[addr] = conn

	return
}

func (this *JFSClient) httpPostMaster(url string, data []byte) (res []byte, err error) {
	retry := 0
Retry:
	resp, err := post(data, "http://"+this.masterAddr+url)
	if err != nil {
		Error.Println(err)
		err = fmt.Errorf("postTo %v error %v", url, err.Error())
		return
	}
	scode := resp.StatusCode
	res, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if scode == http.StatusForbidden && retry < CONN_MAX_RETRY_TIMES {
		this.masterAddr = strings.Replace(string(res), "\n", "", -1)
		Info.Println(this.masterAddr)
		retry++
		goto Retry
	}
	if retry >= CONN_MAX_RETRY_TIMES {
		err = fmt.Errorf("postTo %v scode %v msg %v", url, scode, string(res))
		Error.Println(err)
		return
	}
	if scode != http.StatusOK {
		err = fmt.Errorf("postTo %v scode %v msg %v", url, scode, string(res))
		Error.Println(err)
	}
	return
}

func post(data []byte, url string) (*http.Response, error) {
	client := &http.Client{}
	buff := bytes.NewBuffer(data)
	req, err := http.NewRequest("POST", url, buff)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")

	return client.Do(req)
}
