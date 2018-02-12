package controller

import (
	"github.com/spf13/viper"

	"rrstorage"
)

type cnconfig struct {
	RaftDir  string
	RaftBind string //"xxx.xxx.xxx.xxx:xxx"
	DataDir  string
}

type Controller struct {
	Id         int32
	Ip         string
	Port       int32
	DataCenter string
	DataDir    string

	hibernator *RRStore
}
