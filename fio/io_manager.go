package fio

const FileDataPermission = 0644

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
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
