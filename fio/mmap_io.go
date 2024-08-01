package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

type MMapIO struct {
	// reader in mmap
	readerAt *mmap.ReaderAt
}

func NewMMapIOManager(fileName string) (*MMapIO, error) {
	_, err := os.OpenFile(fileName, os.O_RDONLY|os.O_CREATE, FileDataPermission)
	if err != nil {
		return nil, err
	}

	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMapIO{readerAt: readerAt}, nil
}

// Read byte from given offset
func (mm *MMapIO) Read(key []byte, offset int64) (int, error) {
	return mm.readerAt.ReadAt(key, offset)
}

// Write byte to file
func (mm *MMapIO) Write([]byte) (int, error) {
	panic("not implemented")
}

// Sync Flush to disk
func (mm *MMapIO) Sync() error {
	panic("not implemented")
}

// Close To close file
func (mm *MMapIO) Close() error {
	return mm.readerAt.Close()
}

// Size of a file
func (mm *MMapIO) Size() (int64, error) {
	return int64(mm.readerAt.Len()), nil
}
