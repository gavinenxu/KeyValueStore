package utils

import (
	"fmt"
	"math/rand"
)

func GenerateTestKey(i int) []byte {
	return []byte(fmt.Sprintf("bitcask-key-%09d", i))
}

func GenerateRandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rand.Intn(256))
	}

	return []byte("bitcask-value-" + string(b))
}
