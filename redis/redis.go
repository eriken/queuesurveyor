package redis

import (
	"errors"
	"fmt"

	goredis "github.com/go-redis/redis"
)

var (
	ErrEmptyResponse = errors.New("empty repsponse from redis")
)

type Redis struct {
	client       *goredis.Client
	masterList   string
	progressList string
}

func Init(
	addr string,
	db int,
	masterList,
	progressList string,
) Redis {
	return Redis{
		client: goredis.NewClient(
			&goredis.Options{
				Addr: addr,
				DB:   db,
			},
		),
		masterList:   masterList,
		progressList: progressList,
	}
}

func InitWithSentinel(
	masterName,
	sentinelAddr string,
	db int,
	masterList,
	progressList string,
) Redis {
	return Redis{
		client: goredis.NewFailoverClient(
			&goredis.FailoverOptions{
				MasterName:    masterName,
				SentinelAddrs: []string{sentinelAddr},
				DB:            db,
			},
		),
		masterList:   masterList,
		progressList: progressList,
	}
}

func (redis Redis) Push(payload string) error {
	err := redis.client.LPush(
		redis.masterList,
		[]byte(payload),
	).Err()
	if err != nil {
		return fmt.Errorf(
			"unable to pop user from list, reason: %s",
			err.Error(),
		)
	}
	return nil
}

func (redis Redis) Remove(payload string) error {
	err := redis.client.LRem(
		redis.progressList,
		-1,
		[]byte(payload),
	).Err()
	if err != nil {
		return fmt.Errorf(
			"unable to remove from list, reason: %s",
			err.Error(),
		)
	}
	return nil
}

func (redis Redis) Range() ([]string, error) {
	result, err := redis.client.LRange(
		redis.progressList,
		0,
		-1,
	).Result()
	if err != nil {
		return nil,
			fmt.Errorf(
				"unable to remove from list, reason: %s",
				err.Error(),
			)
	}
	return result, nil
}

func (redis Redis) Close() {
	redis.client.Close()
}
