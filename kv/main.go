package kv

import (
	"context"
	"log"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/prologic/bitcask"
)

var ctx = context.Background()
var redisChannel = "kv_broadcast2"
var redisHost = "localhost"
var redisPort = 6379
var redisAddr = redisHost + ":" + strconv.Itoa(redisPort)
var redisPassword = ""
var redisDB = 0

type KVStore struct {
	db          *bitcask.Bitcask
	redisClient redis.Client
}

func NewKVStore(clientId *string) *KVStore {

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
	var dbFile = "/tmp/rtdb/" + *clientId

	db, err := bitcask.Open(dbFile)

	if err != nil {
		log.Fatal("Failed to open db " + err.Error())
	}
	store := &KVStore{db: db,
		redisClient: *redisClient,
	}
	go store.subscribe()
	return store
}

func (k KVStore) Get(key string) string {
	val, err := k.db.Get([]byte(key))
	if err != nil {
		log.Printf("Failed to get message " + err.Error())
	}
	return string(val)
}

func (k KVStore) put(key string, value string) error {
	err := k.db.Put([]byte(key), []byte(value))
	if err != nil {
		log.Printf("Failed to write message " + err.Error())
		return err
	}
	return nil
}

func (k KVStore) Put(key string, value string) {
	k.put(key, value)
	publish_message := key + ":" + value
	k.redisClient.Publish(ctx, redisChannel, publish_message)
}

func (k KVStore) subscribe() {
	pubsub := k.redisClient.Subscribe(ctx)
	pubsub.Subscribe(ctx, redisChannel)

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("Error " + err.Error())
		}
		payload := strings.Split(msg.Payload, ":")
		key, value := payload[0], payload[1]
		k.put(key, value)
	}

}
