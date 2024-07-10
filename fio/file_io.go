package fio

import "os"

type FileIO struct {
	// OS file descriptor
	fd *os.File
}

func NewFileIOManager(filename string) (*FileIO, error) {
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, FileDataPermission)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (fio *FileIO) Read(bt []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(bt, offset)
}

// Write byte to file
func (fio *FileIO) Write(bt []byte) (int, error) {
	return fio.fd.Write(bt)
}

// Sync Flush to disk
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// Close To close file
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

// Size of a file
func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
