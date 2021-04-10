package kv

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()
var redisChannel = "kv_broadcast2"
var redisHost = "localhost"
var redisPort = 6379
var redisAddr = redisHost + ":" + strconv.Itoa(redisPort)
var redisPassword = ""
var redisDB = 0

type Store struct {
	db          map[string]string
	redisClient redis.Client
}

func NewStore() *Store {

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})

	store := &Store{
		db:          map[string]string{},
		redisClient: *redisClient,
	}

	// gorutineで変更を監視
	go store.update()
	return store
}

func (k Store) Get(key string) (string, error) {
	value, ok := k.db[key]
	if !ok {
		return "", errors.New("value not exist")
	}
	return value, nil
}

func (k Store) Put(key string, value string) {
	k.db[key] = value
	msg := key + ":" + value
	k.redisClient.Publish(ctx, redisChannel, msg)
}

func (k Store) update() {
	pubsub := k.redisClient.Subscribe(ctx)
	pubsub.Subscribe(ctx, redisChannel)

	for {
		msg, _ := pubsub.ReceiveMessage(ctx)
		parsedMessages := strings.Split(msg.Payload, ":")
		key, value := parsedMessages[0], parsedMessages[1]
		k.db[key] = value
	}
}
