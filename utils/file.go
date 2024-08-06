package utils

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func AvailableSizeOnDiskInBytes() (uint64, error) {
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}

	var stat syscall.Statfs_t
	err = syscall.Statfs(wd, &stat)
	if err != nil {
		return 0, err
	}

	return stat.Bavail * uint64(stat.Bsize), nil
}

func CopyDirWithFiles(src, dst string, excludes []string) error {
	if _, err := os.Stat(src); os.IsNotExist(err) {
		return errors.New("source directory does not exist")
	}

	if _, err := os.Stat(dst); os.IsNotExist(err) {
		if err = os.MkdirAll(dst, os.ModePerm); err != nil {
			return err
		}
	}

	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		filename := strings.Replace(path, src, "", 1)
		if filename == "" {
			return nil
		}

		for _, exclude := range excludes {
			matched, err := filepath.Match(exclude, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}

		// copy dir
		if info.IsDir() {
			return os.Mkdir(filepath.Join(dst, filename), os.ModePerm)
		}

		// copy file
		file, err := os.ReadFile(filepath.Join(src, filename))
		if err != nil {
			return err
		}

		return os.WriteFile(filepath.Join(dst, filename), file, info.Mode())
	})
}
