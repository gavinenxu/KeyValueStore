package fio

import (
	"github.com/stretchr/testify/assert"
	"io"
	"path/filepath"
	"testing"
)

func TestNewMMapIOManager(t *testing.T) {
	filename := filepath.Join("/tmp", "mmap.storage")
	defer destroyFile(filename)

	mmapIO, err := NewMMapIOManager(filename)
	assert.Nil(t, err)
	assert.NotNil(t, mmapIO)
}

func TestMMapIOManager_Read(t *testing.T) {
	filename := filepath.Join("/tmp", "mmap.storage")
	defer destroyFile(filename)

	mmapIO, err := NewMMapIOManager(filename)
	assert.Nil(t, err)
	assert.NotNil(t, mmapIO)

	buf := make([]byte, 1024)
	size1, err := mmapIO.Read(buf, 0)
	assert.NotNil(t, err)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, size1, 0)

	// write data
	fio, err := NewFileIOManager(filename)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	val := []byte("hello world 123456")
	n, err := fio.Write(val)
	assert.Nil(t, err)
	assert.Equal(t, len(val), n)

	// reopen mmap
	err = mmapIO.Close()
	assert.Nil(t, err)

	mmapIO, err = NewMMapIOManager(filename)

	size, err := mmapIO.Size()
	assert.Nil(t, err)
	assert.Equal(t, int(size), len(val))

	buf = make([]byte, len(val))
	size2, err := mmapIO.Read(buf, 0)
	assert.Nil(t, err)
	assert.Equal(t, n, size2)
	assert.Equal(t, val, buf)

	buf = make([]byte, 1)
	_, err = mmapIO.Read(buf, size)
	assert.NotNil(t, err)
	assert.Equal(t, io.EOF, err)
}

func TestMMapIOManager_Close(t *testing.T) {
	filename := filepath.Join("/tmp", "mmap.storage")
	defer destroyFile(filename)

	mmapIO, err := NewMMapIOManager(filename)
	assert.Nil(t, err)
	assert.NotNil(t, mmapIO)

	err = mmapIO.Close()
	assert.Nil(t, err)
}
