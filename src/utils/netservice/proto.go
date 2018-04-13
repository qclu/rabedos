package netservice

type Message interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	HeaderSize() uint32
	DataSize() uint32
}
