package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

func main() {

	fmt.Println("HELLO PUBLISHER")

	init_con_pub()

}

func init_con_pub() {

	var m Message

	for {

		//inserisce il contenuto del nuovo messaggio
		reader := bufio.NewScanner(os.Stdin)

		fmt.Printf("Insert message data :")

		reader.Scan()

		text := reader.Text()

		m.Data = text

		//inserisce il topic del nuovo messaggio
		reader2 := bufio.NewScanner(os.Stdin)

		fmt.Printf("Insert topic : ")

		reader2.Scan()

		text2 := reader2.Text()

		m.Topic = text2

		//inserisce la data di inserimento del nuovo messaggio
		m.Date = strings.Split(time.Now().String(),".")[0]

		s, err := json.Marshal(&m)
		if err != nil {
			log.Fatal(err)
		}

		client, err := rpc.Dial("tcp", "localhost:1235")
		if err != nil {
			log.Fatal("dialing:", err)
		}

		if err != nil {
			log.Fatal(err)
		}

		var reply string
		divCall := client.Go("Listener.Publish", s, &reply, nil)
		replyCall := <-divCall.Done

		if replyCall == nil {

			log.Fatal(replyCall)
		}

		fmt.Println("Message received from server : " + reply)

		client.Close()

	}

}
