package benchmark

import (
	bitcask "bitcask-go"
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

var database *bitcask.DB

func init() {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_benchmark")
	configs.DirPath = dir

	var err error
	database, err = bitcask.OpenDatabase(configs)
	if err != nil {
		panic(err)
	}
}

func Benchmark_DB_PUT(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := database.Put(utils.GenerateTestKey(i), utils.GenerateRandomValue(1<<10))
		assert.NoError(b, err)
	}
}

func Benchmark_DB_GET(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := database.Put(utils.GenerateTestKey(i), utils.GenerateRandomValue(1<<10))
		assert.NoError(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := database.Get(utils.GenerateTestKey(rand.Int()))
		if err != nil {
			assert.Equal(b, bitcask.ErrKeyNotFound, err)
		}
	}
}

func Benchmark_DB_DELETE(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := database.Delete(utils.GenerateTestKey(rand.Int()))
		if err != nil {
			assert.Equal(b, bitcask.ErrKeyNotFound, err)
		}
	}
}
