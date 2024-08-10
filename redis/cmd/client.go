package main

import (
	"bitcask-go"
	"bitcask-go/redis"
	"bitcask-go/utils"
	"errors"
	"github.com/tidwall/redcon"
	"strings"
)

type Client struct {
	database *redis.RedisDataStruct
}

type commandHandler func(cli *Client, args [][]byte) (interface{}, error)

var supportedCommandMap = map[string]commandHandler{
	"set":       set,
	"get":       get,
	"hset":      hset,
	"hget":      hget,
	"hdel":      hdel,
	"sadd":      sadd,
	"sismember": sismember,
	"srem":      srem,
	"zadd":      zadd,
	"zscore":    zscore,
	"lpush":     lpush,
	"rpush":     rpush,
	"lpop":      lpop,
	"rpop":      rpop,
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))

	switch command {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		handler := supportedCommandMap[command]
		if handler == nil {
			conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			return
		}

		client := conn.Context().(*Client)
		res, err := handler(client, cmd.Args[1:])
		if err != nil {
			if errors.Is(err, bitcask_go.ErrKeyNotFound) {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}

func set(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errWrongNumberOfArgs("set")
	}

	key, value := args[0], args[1]

	if err := cli.database.Set(key, value, 0); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func get(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errWrongNumberOfArgs("get")
	}

	key := args[0]

	value, err := cli.database.Get(key)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleString(value), nil
}

func hset(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, errWrongNumberOfArgs("hset")
	}

	key, field, value := args[0], args[1], args[2]
	ok, err := cli.database.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if ok {
		return redcon.SimpleInt(1), nil
	}
	return redcon.SimpleInt(0), nil
}

func hget(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errWrongNumberOfArgs("hget")
	}

	key, field := args[0], args[1]
	value, err := cli.database.HGet(key, field)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(value), nil
}

func hdel(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errWrongNumberOfArgs("hdel")
	}

	key, field := args[0], args[1]

	ok, err := cli.database.HDel(key, field)
	if err != nil {
		return nil, err
	}
	if ok {
		return redcon.SimpleInt(1), nil
	}
	return redcon.SimpleInt(0), nil
}

func sadd(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errWrongNumberOfArgs("sadd")
	}

	key, member := args[0], args[1]
	ok, err := cli.database.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if ok {
		return redcon.SimpleInt(1), nil
	}
	return redcon.SimpleInt(0), nil
}

func sismember(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errWrongNumberOfArgs("sismember")
	}

	key, member := args[0], args[1]
	isMember, err := cli.database.SIsMember(key, member)
	if err != nil {
		return nil, err
	}
	if isMember {
		return redcon.SimpleInt(1), nil
	}
	return redcon.SimpleInt(0), nil
}

func srem(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errWrongNumberOfArgs("srem")
	}

	key, member := args[0], args[1]
	ok, err := cli.database.SRem(key, member)
	if err != nil {
		return nil, err
	}
	if ok {
		return redcon.SimpleInt(1), nil
	}
	return redcon.SimpleInt(0), nil
}

func zadd(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, errWrongNumberOfArgs("zadd")
	}

	key, score, member := args[0], args[1], args[2]
	ok, err := cli.database.ZAdd(key, utils.BytesToFloat64([]byte(score)), member)
	if err != nil {
		return nil, err
	}
	if ok {
		return redcon.SimpleInt(1), nil
	}
	return redcon.SimpleInt(0), nil
}

func zscore(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errWrongNumberOfArgs("zscore")
	}

	key, member := args[0], args[1]
	score, err := cli.database.ZScore(key, member)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleString(utils.Float64ToBytes(score)), nil
}

func lpush(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errWrongNumberOfArgs("lpush")
	}

	key, element := args[0], args[1]
	size, err := cli.database.LPush(key, element)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(size), nil
}

func rpush(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errWrongNumberOfArgs("rpush")
	}
	key, element := args[0], args[1]
	size, err := cli.database.RPush(key, element)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(size), nil
}

func lpop(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errWrongNumberOfArgs("lpop")
	}

	key := args[0]
	value, err := cli.database.LPop(key)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(value), nil
}

func rpop(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errWrongNumberOfArgs("rpop")
	}

	key := args[0]
	value, err := cli.database.RPop(key)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(value), nil
}

func errWrongNumberOfArgs(cmd string) error {
	return errors.New("ERR wrong number of arguments for '" + string(cmd) + "'")
}
