package redis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
)

type RedisArgs struct {
	connKey       string
	key           string
	value         string
	originalValue any
	ttl           time.Duration
}

var (
	_conManager func(connKey string) (*redis.Client, error)
)

func RedisSet(jo *[]any, row any, args []any) (any, error) {
	redisArgs, err := readRedisSetArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	conn, err := _conManager(redisArgs.connKey)
	if err != nil {
		return nil, err
	}
	uuid := uuid.New()
	go conn.Set(context.TODO(), uuid.String(), redisArgs.value, redisArgs.ttl)
	return uuid.String(), nil
}

func RedisSetWithKey(jo *[]any, row any, args []any) (any, error) {
	redisArgs, err := readRedisSetWithKeyArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	conn, err := _conManager(redisArgs.connKey)
	if err != nil {
		return nil, err
	}
	go conn.Set(context.TODO(), redisArgs.key, redisArgs.value, redisArgs.ttl)
	return redisArgs.originalValue, nil
}

func RedisGet(jo *[]any, row any, args []any) (any, error) {
	redisArgs, err := readRedisGetArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	conn, err := _conManager(redisArgs.connKey)
	if err != nil {
		return nil, err
	}
	res := conn.Get(context.TODO(), redisArgs.key)
	if res.Err() != nil {
		return nil, res.Err()
	}
	data, err := base64.StdEncoding.DecodeString(res.Val())
	if err != nil {
		return nil, err
	}
	mapper := make(map[string]any)
	err = json.Unmarshal(data, &mapper)
	if err != nil {
		return nil, err
	}
	return mapper, nil
}

func readRedisSetArgs(args []any, row any, jo *[]any) (*RedisArgs, error) {
	redisArgs := RedisArgs{}
	err := functions.CheckSingnature(
		args,
		[]functions.ArgTypes{
			functions.STRING,
			functions.ANY,
			functions.NUMBER,
		},
		[]functions.Reader{
			func(arg any) error {
				redisArgs.connKey = arg.(string)
				return nil
			},
			func(arg any) error {
				json, err := json.Marshal(arg)
				if err != nil {
					return err
				}
				base64 := base64.StdEncoding.EncodeToString(json)
				redisArgs.value = base64
				return nil
			},
			func(arg any) error {
				redisArgs.ttl = time.Second * time.Duration((arg.(float64)))
				return nil
			},
		},
	)
	if err != nil {
		return nil, err
	}
	return &redisArgs, nil
}

func readRedisSetWithKeyArgs(args []any, row any, jo *[]any) (*RedisArgs, error) {
	redisArgs := RedisArgs{}
	err := functions.CheckSingnature(
		args,
		[]functions.ArgTypes{
			functions.STRING, // Connection String
			functions.STRING, // Key
			functions.ANY,    // Value
			functions.NUMBER, // TTL
		},
		[]functions.Reader{
			func(arg any) error {
				redisArgs.connKey = arg.(string)
				return nil
			},
			func(arg any) error {
				redisArgs.key = arg.(string)
				return nil
			},
			func(arg any) error {
				redisArgs.originalValue = arg
				json, err := json.Marshal(arg)
				if err != nil {
					return err
				}
				base64 := base64.StdEncoding.EncodeToString(json)
				redisArgs.value = base64
				return nil
			},
			func(arg any) error {
				redisArgs.ttl = time.Second * time.Duration((arg.(float64)))
				return nil
			},
		},
	)
	if err != nil {
		return nil, err
	}
	return &redisArgs, nil
}

func readRedisGetArgs(args []any, row any, jo *[]any) (*RedisArgs, error) {
	redisArgs := RedisArgs{}
	err := functions.CheckSingnature(
		args,
		[]functions.ArgTypes{
			functions.STRING,
			functions.STRING,
		},
		[]functions.Reader{
			func(arg any) error {
				redisArgs.connKey = arg.(string)
				return nil
			},
			func(arg any) error {
				redisArgs.key = arg.(string)
				return nil
			},
		},
	)
	if err != nil {
		return nil, err
	}
	return &redisArgs, nil
}

func RegisterConManager(fn func(connKey string) (*redis.Client, error)) {
	_conManager = fn
}

func init() {
	cmn.RegisterFunction("redisset", RedisSet)
	cmn.RegisterFunction("redissetkey", RedisSet)
	cmn.RegisterFunction("redisget", RedisGet)
}
