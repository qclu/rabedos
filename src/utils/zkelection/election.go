package zkelection

import (
	"errors"
	"github.com/samuel/go-zookeeper/zk"
	"strconv"
	"strings"
	"sync"
	"time"
)

var defaultAcl = zk.WorldACL(zk.PermAll)

var ErrInterrupted = errors.New("zkelection:interrupted")
var ErrClosed = errors.New("zkelection:closed")
var ErrMemberNull = errors.New("zkelection:members is null")
var ErrInvavlidNode = errors.New("zkelection:inavali node")

type ZkEventChan <-chan zk.Event

type Candidate struct {
	servers      []string
	electionPath string
	masterPath   string
	zkConn       *zk.Conn
	sessionId    int64
	closed       bool
	mu           sync.Mutex
	selfPath     string
}

func (can *Candidate) UpdateMasterValue(value string, acl []zk.ACL) error {
	exists, _, err := can.zkConn.Exists(can.masterPath)
	if err != nil {
		return err
	}

	if exists == false {
		_, err = can.zkConn.Create(can.masterPath, []byte(value), 0, acl)
	} else {
		_, err = can.zkConn.Set(can.masterPath, []byte(value), -1)
	}
	return err
}

func (can *Candidate) GetSelfPath() string {
	can.mu.Lock()
	defer can.mu.Unlock()

	return can.selfPath
}

func (can *Candidate) SelfPathExsit() (exsits bool, err error) {
	can.mu.Lock()
	defer can.mu.Unlock()

	if can.closed == true {
		err = ErrClosed
		return
	}
	exsits, _, err = can.zkConn.Exists(can.selfPath)

	return
}

func (can *Candidate) Close() {
	can.mu.Lock()
	defer can.mu.Unlock()
	can.zkConn.Close()
	can.closed = true
}

func (can *Candidate) ReConn() (err error) {
	can.zkConn, _, err = zk.Connect(can.servers, time.Second)
	if err == nil {
		can.closed = false
	}
	return
}

func (can *Candidate) SortChildren(allPaths []string) (minPath string, err error) {
	err = ErrInvavlidNode
	minSeq := 99999999999
	for i := 0; i < len(allPaths); i++ {
		path := allPaths[i]
		allSeps := strings.Split(path, "_")
		if len(allSeps) < 1 {
			continue
		}
		currSeqStr := allSeps[len(allSeps)-1]
		if currSeq, err := strconv.Atoi(currSeqStr); err == nil {
			if currSeq < minSeq {
				minPath = path
				minSeq = currSeq
			}
		}
	}

	if minPath != "" {
		err = nil
	}

	return
}

func (can *Candidate) ElectionStatus(acl []zk.ACL) (isLeader bool, leaderPath, value string, err error) {
	if acl == nil {
		acl = defaultAcl
	}

	can.mu.Lock()
	defer can.mu.Unlock()
	if can.closed {
		err = ErrClosed
		return
	}
	var minPath string
	for i := 0; i < 3; i++ {
		var allPaths []string
		allPaths, _, err = can.zkConn.Children(can.electionPath)
		if err != nil {
			continue
		}
		if allPaths == nil || len(allPaths) == 0 || allPaths[0] == "" {
			err = ErrMemberNull
			continue
		}
		if minPath, err = can.SortChildren(allPaths); err != nil {
			continue
		}
		leaderPath = can.electionPath + "/" + minPath
		var stat *zk.Stat
		var valueBytes []byte
		valueBytes, stat, err = can.zkConn.Get(leaderPath)
		if err != nil {
			continue
		}
		isLeader = stat.EphemeralOwner == can.sessionId
		value = string(valueBytes)

		if isLeader {
			if err = can.UpdateMasterValue(value, acl); err != nil {
				continue
			}
		}
		return
	}
	return
}

func (can *Candidate) WatchChildren() (watch ZkEventChan, e error) {
	var w <-chan zk.Event
	_, _, w, e = can.zkConn.ChildrenW(can.electionPath)
	<-w
	return
}

func NewCandidate(servers []string, timeout time.Duration, path, value string, acl []zk.ACL) (can *Candidate, err error) {
	if acl == nil {
		acl = defaultAcl
	}

	zkConn, _, err := zk.Connect(servers, timeout)
	if err != nil {
		return
	}

	electionPath := path + "/members"
	err = createParents(zkConn, electionPath, acl)
	if err != nil {
		return
	}

	selfPath, sessionId, err := createEphemeralSequence(zkConn, electionPath, value, acl)
	if err != nil {
		return
	}

	can = new(Candidate)
	can.selfPath = selfPath
	can.servers = servers
	can.electionPath = electionPath
	can.masterPath = path + "/master"
	can.zkConn = zkConn
	can.sessionId = sessionId
	return
}

func createEphemeralSequence(zkConn *zk.Conn, electionPath, value string, acl []zk.ACL) (path string, owner int64, err error) {
	for i := 0; i < 3; i++ {
		path, err = zkConn.CreateProtectedEphemeralSequential(electionPath+"/n_", []byte(value), acl)
		if err == nil {
			var stat *zk.Stat
			_, stat, err = zkConn.Get(path)
			if err == nil {
				owner = stat.EphemeralOwner
				return
			}
		}
	}
	return
}

func createParents(zkConn *zk.Conn, electionPath string, acl []zk.ACL) (err error) {
	var parents []string
	for i := 1; i < len(electionPath); i++ {
		if electionPath[i] == '/' {
			parents = append(parents, electionPath[:i])
		}
	}
	parents = append(parents, electionPath)
	for _, parent := range parents {
		var exists bool
		exists, _, err = zkConn.Exists(electionPath)
		if err != nil {
			return
		}
		if !exists {
			_, err = zkConn.Create(parent, []byte("election"), 0, acl)
			if err == zk.ErrNodeExists {
				err = nil
			}
			if err != nil {
				return
			}
		}
	}
	return
}
