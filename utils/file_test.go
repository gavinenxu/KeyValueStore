package utils

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func createTmpDir() (string, error) {
	return os.MkdirTemp("", "file-test")
}

func destroyTmpDir(dirPath string) {
	if err := os.RemoveAll(dirPath); err != nil {
		panic(err)
	}
}

func TestDirSize(t *testing.T) {
	dir, err := createTmpDir()
	defer destroyTmpDir(dir)
	assert.Nil(t, err)
	t.Log(dir)

	data := []byte("Hello, Golang!")

	err = os.WriteFile(filepath.Join(dir, "file1"), data, os.ModePerm)
	assert.Nil(t, err)

	size, err := DirSize(dir)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(data)), size)
}

func TestAvailableSizeOnDisk(t *testing.T) {
	size, err := AvailableSizeOnDiskInBytes()
	assert.Nil(t, err)
	assert.Greater(t, size, uint64(0))
	t.Log(size / 1000 / 1000 / 1000)
}
