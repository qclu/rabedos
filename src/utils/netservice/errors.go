package netservice

import (
	"errors"
)

var (
	ErrInvalidReplicatioGroup = errors.New("invalid replication group")
	ErrEmptyDataToSend        = errors.New("data to send is empty")
	ErrNoWritableVolume       = errors.New("no writable volume available")
	ErrConnectFailed          = errors.New("failed to connect ")
	ErrInvalidKey             = errors.New("invalid key of object")
	ErrObjNotExist            = errors.New("object not exist")
	ErrDataLenUnmatch         = errors.New("data length is unmatched")

	ErrInternalReqIDDup     = errors.New("internal error, duplicate request id")
	ErrInternalTaskNotExist = errors.New("task not exist")
	ErrInternalUnmatchedPkg = errors.New("unmatched package receive")
)
