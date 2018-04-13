package netservice

import (
	"errors"
)

const (
	MAX_RECV_CHAN_LEN = 2048
	MAX_SEND_CHAN_LEN = 2048
	MAX_CONN_CNT      = 1024
)

const (
	ErrHandlerDupReigster = errors.New("Packet type is already registered")
)
const (
	ColonSeparator     = ":"
	SemicolonSeparator = ";"
)

const (
	VolMapUpdateIntevalTime = 20 //second
)

const (
	CONN_TYPE             = "tcp4"
	CONN_MAX_RETRY_TIMES  = 3
	CONN_TIMEOUT_DURATION = 5 //secondo
	CONN_BUFFER_SIZE      = 128 << 10
	CONN_RECONNECT_WAIT   = 300 // ms
)

const (
	URL_MASTER_GETCLIENTVIEW = "/client/getview?cluster="
)

const (
	IO_DEPTH          = 400 << 10
	TCP_RECV_CHAN_LEN = 4 << 10
	TCP_SEND_CHAN_LEN = 4 << 10
)
