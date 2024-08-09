package utils

import (
	"strconv"
)

func Float64ToBytes(f float64) []byte {
	return []byte(strconv.FormatFloat(f, 'f', -1, 64))
}

func BytesToFloat64(b []byte) float64 {
	f, _ := strconv.ParseFloat(string(b), 64)
	return f
}
