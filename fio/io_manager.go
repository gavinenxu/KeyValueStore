package fio

const FileDataPermission = 0644

type IOType = byte

const (
	StandardFileIOType IOType = iota + 1
	MMapIOType
)

// IOManager interface for File IO
type IOManager interface {
	// Read byte from given offset
	Read([]byte, int64) (int, error)

	// Write byte to file
	Write([]byte) (int, error)

	// Sync Flush to disk
	Sync() error

	// Close To close file
	Close() error

	// Size of a file
	Size() (int64, error)
}

// NewIOManager Initialize IOManager
func NewIOManager(fileName string, ioType IOType) (IOManager, error) {
	switch ioType {
	case StandardFileIOType:
		return NewFileIOManager(fileName)
	case MMapIOType:
		return NewMMapIOManager(fileName)
	default:
		panic("unknown io type")
	}
}
