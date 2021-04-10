package main

import (
	"bufio"
	"flag"
	"fmt"
	"kvshakyo/kv"
	"os"
	"strings"
)

func main() {
	flag.Parse()
	var clientId = flag.String("client_id", "client1", "Client ID for this client")

	if *clientId == "" {
		panic("clientId not specified")
	}

	s := *kv.NewKVStore(clientId)

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Real time db")
	flag := true
	for flag {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		input := strings.Split(strings.Trim(text, "\n"), " ")
		switch input[0] {
		case "GET":
			key := input[1]
			value := s.Get(key)
			fmt.Println(value)
		case "PUT":
			key, value := input[1], input[2]
			s.Put(key, value)
			fmt.Println("Done...")
		case "EXIT":
			fmt.Println("EXIT...")
			flag = false
			break
		}

	}
}
