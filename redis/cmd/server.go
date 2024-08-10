package main

import (
	bitcask "bitcask-go"
	"bitcask-go/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "localhost:6380"

type Server struct {
	databaseMap  map[int]*redis.RedisDataStruct
	redconServer *redcon.Server
	mu           *sync.RWMutex
}

func main() {
	server := &Server{
		databaseMap: make(map[int]*redis.RedisDataStruct),
		mu:          new(sync.RWMutex),
	}

	// initiate a redis instance
	dataStruct, err := redis.NewRedisDataStruct(bitcask.DefaultConfig)
	if err != nil {
		panic(err)
	}
	server.databaseMap[0] = dataStruct

	server.redconServer = redcon.NewServer(addr, execClientCommand, server.accept, server.close)

	server.listen()
}

func (server *Server) listen() {
	log.Println("Listening on: ", addr)
	err := server.redconServer.ListenAndServe()
	if err != nil {
		panic(err)
	}
}

func (server *Server) accept(conn redcon.Conn) bool {
	server.mu.Lock()
	defer server.mu.Unlock()
	cli := &Client{
		database: server.databaseMap[0],
	}
	conn.SetContext(cli)

	return true
}

func (server *Server) close(conn redcon.Conn, err error) {
	for _, db := range server.databaseMap {
		_ = db.Close()
	}
	_ = server.redconServer.Close()
}
