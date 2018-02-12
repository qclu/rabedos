package zkwrapper

import (
	"errors"
	"github.com/samuel/go-zookeeper/zk"
	"path"
	"strconv"
	"strings"
	"time"
)

type ZkEventChan <-chan zk.Event

type ZkWrapper struct {
	zkAddr  string
	zconn   *zk.Conn
	session <-chan zk.Event
}

func NewZkWrapper(addr string) (zw *ZkWrapper, e error) {
	zw = new(ZkWrapper)

	addrArray := strings.Split(addr, ",")
	zw.zconn, zw.session, e = zk.Connect(addrArray, 5*time.Second)

	if e != nil {
		return nil, e
	}
	// Wait for connection - avoid to be blocked forever!
	select {
	case <-time.After(10 * time.Second):
		return nil, errors.New("DialTimeout")
	case event := <-zw.session:
		if event.State != zk.StateConnecting {

			return nil, errors.New("Failed to connect ZK")
		}
	}

	zw.zkAddr = addr
	return zw, nil
}

func (zw *ZkWrapper) Close() error {
	if zw.zconn != nil {
		zw.zconn.Close()
	} else {
		return errors.New("null point")
	}
	return nil
}

func (zw *ZkWrapper) CreateDir(path string) error {
	_, err := zw.zconn.Create(path, []byte(""), 0, zk.WorldACL(zk.PermAll))
	return err
}

func (zw *ZkWrapper) AddEphemeralServer(dir string, id int, value string) error {
	dir = dir + "/" + strconv.Itoa(id)
	_, err := zw.zconn.Create(dir, []byte(value), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	return err
}

//remove itself from ZK
func (zw *ZkWrapper) RemoveServer(dir string, id int) (err error) {
	path := dir + "/" + strconv.Itoa(id)
	return zw.zconn.Delete(path, -1)
}

func (zw *ZkWrapper) CheckExistingNode(path string) bool {
	exist, _, e := zw.zconn.Exists(path)
	return e == nil && exist
}

func (zw *ZkWrapper) SetServerValue(dir string, id int, value string) error {
	dir = dir + "/" + strconv.Itoa(id)
	_, err := zw.zconn.Set(dir, []byte(value), -1)
	return err
}

func (zw *ZkWrapper) GetServerValue(dir string, id int) (value string, e error) {
	dir = dir + "/" + strconv.Itoa(id)
	var valueByte []byte
	valueByte, _, e = zw.zconn.Get(dir)
	value = string(valueByte)
	return
}

func (zw *ZkWrapper) WatchChildren(dir string) (watch ZkEventChan, e error) {
	var w <-chan zk.Event
	_, _, w, e = zw.zconn.ChildrenW(dir)
	watch = ZkEventChan(w)
	return
}

func (zw *ZkWrapper) SetValue(path string, value string) (e error) {
	_, e = zw.zconn.Set(path, []byte(value), -1)
	return e
}

func (zw *ZkWrapper) GetValue(dir string) (value string, e error) {
	var valueByte []byte
	valueByte, _, e = zw.zconn.Get(dir)
	value = string(valueByte)
	return
}

func (zw *ZkWrapper) GetChildren(dir string) (values []string, e error) {
	values, _, err := zw.zconn.Children(dir)
	return values, err
}

func (zw *ZkWrapper) CreateRecursive(zkPath, value string, flags int32, aclv []zk.ACL) (pathCreated string, err error) {
	if zkPath == "/" {
		//println("root node reached")
		return "", zk.ErrNodeExists
	}

	pathCreated, err = zw.zconn.Create(zkPath, []byte(value), flags, aclv)
	if err == zk.ErrNoNode {
		_, err = zw.CreateRecursive(path.Dir(zkPath), "", flags, aclv)
		if (err != nil) && (err != zk.ErrNodeExists) {
			return "", err
		}

		pathCreated, err = zw.zconn.Create(zkPath, []byte(value), flags, aclv)
	}

	if err == zk.ErrNodeExists {
		err = nil
	}

	return
}

func (zw *ZkWrapper) CreateEphemeral(zkPath, value string) error {
	_, err := zw.zconn.Create(zkPath, []byte(value), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	return err
}

func (zw *ZkWrapper) GetValueW(path string) (watch ZkEventChan, value string, e error) {
	var (
		w         <-chan zk.Event
		valueByte []byte
	)
	valueByte, _, w, e = zw.zconn.GetW(path)
	value = string(valueByte)
	watch = ZkEventChan(w)
	return
}

func (zw *ZkWrapper) DeletePath(path string) error {
	return zw.zconn.Delete(path, -1)
}

// auth about

const (
	PermRead   = zk.PermRead
	PermWrite  = zk.PermWrite
	PermCreate = zk.PermCreate
	PermDelete = zk.PermDelete
	PermAdmin  = zk.PermAdmin
	PermAll    = zk.PermAll
)

func NewZkWrapperWithDigest(addr, user, password string) (zw *ZkWrapper, e error) {
	zw, e = NewZkWrapper(addr)
	if e == nil && (user != "" || password != "") {
		e = zw.AddAuth(user, password)
	}
	return
}
func (zw *ZkWrapper) AddAuth(user, password string) error {
	return zw.zconn.AddAuth("digest", []byte(user+":"+password))
}
func (zw *ZkWrapper) RemovePath(path string) (err error) {
	return zw.zconn.Delete(path, -1)
}
func (zw *ZkWrapper) SetACL(path string, acl []zk.ACL) (err error) {
	_, err = zw.zconn.SetACL(path, acl, -1)
	return
}
func (zw *ZkWrapper) GetACL(path string) (acl []zk.ACL, err error) {
	acl, _, err = zw.zconn.GetACL(path)
	return
}

func WorldACL(perms int32) []zk.ACL {
	return zk.WorldACL(perms)
}
func DigestACL(perms int32, user, password string) []zk.ACL {
	return zk.DigestACL(perms, user, password)
}
func IpACL(perms int32, ipList []string) (acl []zk.ACL) {
	for _, ip := range ipList {
		acl = append(acl, zk.ACL{perms, "ip", ip})
	}
	return
}
