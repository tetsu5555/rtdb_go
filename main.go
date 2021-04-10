package main

import (
	"fmt"
	"kvshakyo/kv"
)

func main() {
	fmt.Println("hello")

	kv := kv.NewStore()
	kv.Put("key", "value")

	val := kv.Get("key")
	fmt.Println(val)
}
