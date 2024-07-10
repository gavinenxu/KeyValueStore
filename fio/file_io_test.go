package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(name string) {
	if err := os.Remove(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join("/tmp", "test.storage")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIOManager_Read(t *testing.T) {
	path := filepath.Join("/tmp", "test.storage")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key1"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key2"))
	assert.Nil(t, err)

	b := make([]byte, 4)
	n, err := fio.Read(b, 0)
	assert.Nil(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("key1"), b)

	b = make([]byte, 4)
	n, err = fio.Read(b, 4)
	assert.Nil(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("key2"), b)
}

func TestFileIOManager_Write(t *testing.T) {
	path := filepath.Join("/tmp", "test.storage")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)

	n, err = fio.Write([]byte("hello"))
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
}

func TestFileIOManager_Sync(t *testing.T) {
	path := filepath.Join("/tmp", "test.storage")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIOManager_Close(t *testing.T) {
	path := filepath.Join("/tmp", "test.storage")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
