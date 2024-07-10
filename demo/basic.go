package main

import (
	bitcask "bitcask-go"
	"fmt"
)

func main() {
	configuration := bitcask.DefaultConfig
	configuration.DirPath = "/tmp/data"

	db, err := bitcask.OpenDatabase(configuration)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("key"), []byte("value"))
	if err != nil {
		panic(err)
	}

	data, err := db.Get([]byte("key"))
	if err != nil {
		panic(err)
	}
	fmt.Println("value:", string(data))

	err = db.Delete([]byte("key"))
	if err != nil {
		panic(err)
	}
}
