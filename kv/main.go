package kv

import (
	"context"
	"flag"
	"log"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/prologic/bitcask"
)

var ctx = context.Background()
var redisChannel = "kv_broadcast"
var redisHost = "localhost"
var redisPort = 6379
var redisAddr = redisHost + ":" + strconv.Itoa(redisPort)
var redisPassword = ""
var redisDB = 0

var clientId = flag.String("client_id", "client1", "Client ID for this client")

type KVStore struct {
	db          *bitcask.Bitcask
	redisClient redis.Client
	buffer      chan string
	initDone    chan bool
}

func NewKVStore() *KVStore {

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
	// clientIdを指定しない場合は適当に指定される
	var dbFile = "/tmp/rtdb/" + *clientId

	db, err := bitcask.Open(dbFile)

	if err != nil {
		log.Fatal("Failed to open db " + err.Error())
	}
	store := &KVStore{db: db,
		redisClient: *redisClient,
		buffer:      make(chan string, 100),
		initDone:    make(chan bool),
	}
	go store.loadData()
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

func (k KVStore) broadCasetAndPersist(key string, value string) {
	publish_message := key + ":" + value
	k.redisClient.Publish(ctx, redisChannel, publish_message)
	err := k.redisClient.Set(ctx, key, value, 0).Err()
	if err != nil {
		log.Println("Failed to persist " + err.Error())
	}
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
	defer k.broadCasetAndPersist(key, value)
}

func (k KVStore) loadData() {
	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = k.redisClient.Scan(ctx, cursor, "*", 1).Result()
		if err != nil {
			log.Println("Failed to retrieve data ", err.Error())
		}
		for _, key := range keys {
			val, _ := k.redisClient.Get(ctx, key).Result()
			k.put(key, val)
		}
		if cursor == 0 {
			break
		}
	}
	k.initDone <- true
}

func (k KVStore) subscribe() {
	pubsub := k.redisClient.Subscribe(ctx)
	pubsub.Subscribe(ctx, redisChannel)

	go func() {
		<-k.initDone
		for {
			msg := <-k.buffer
			payload := strings.Split(msg, ":")
			key, value := payload[0], payload[1]
			k.put(key, value)
		}
	}()

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("Error " + err.Error())
		}

		k.buffer <- msg.Payload
	}

}
