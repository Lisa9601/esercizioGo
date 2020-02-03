package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

var IP string 		//indirizzo IP del subscriber
var port string		//porta su cui verranno ricevuti i messaggi dal server

//funzione che riceve un messaggio dal server
func (l *Listener) Receive(line []byte, reply *string) error {

	var m Message

	err := json.Unmarshal(line, &m)
	if err != nil {
		log.Fatal(err)
	}

	//stampa del messaggio ricevuto
	fmt.Printf("Message received from server: %+v\n", m)

	*reply = "ACK"

	return nil
}

func main() {

	rand.Seed(time.Now().UTC().UnixNano())	//seed per la funzione rand

	fmt.Println("HELLO SUBSCRIBER")

	// genero una porta casuale compresa fra min e max
	max := 60000
	min := 50000

	p := rand.Intn(max-min) + min
	port = strconv.Itoa(p)

	//imposto l'indirizzo IP
	IP = "127.0.0.1"

	go init_con_sub()

	go receive_from_server()

	//ciclo infinito
	for {}

}


//funzione che si occupa di inviare le richieste del subscriber al server
func init_con_sub() {

	var command int
	var text string

	for {

		//lista comandi
		fmt.Printf("1) Show message list\n" + "2) Subscribe to topic\n" + "3) Unsubscribe from topic\n" + "Enter a command : ")

		_, err := fmt.Scanf("%d\n", &command)
		if err != nil {
			log.Fatal(err)
		}

		switch command {

		case 1:

			client, err := rpc.Dial("tcp", "localhost:1234")
			if err != nil {
				log.Fatal("dialing:", err)
			}

			var reply []Message
			divCall := client.Go("Listener.List", "list", &reply, nil)
			replyCall := <-divCall.Done

			if replyCall == nil {

				log.Fatal(replyCall)
			}

			fmt.Printf("Message list: %+v\n", reply[:])

			client.Close()

		case 2:

			fmt.Printf("Insert topic : ")

			reader := bufio.NewScanner(os.Stdin)

			reader.Scan()

			text = reader.Text() + " " + IP + " " + port

			client, err := rpc.Dial("tcp", "localhost:1236")
			if err != nil {
				log.Fatal("dialing:", err)
			}

			var reply string
			divCall := client.Go("Listener.Subscribe", text, &reply, nil)
			replyCall := <-divCall.Done //Will be equal to divCall

			if replyCall == nil {

				log.Fatal(replyCall)
			}

			fmt.Println("Message received : ", reply)

			client.Close()


		case 3:


			fmt.Printf("Insert topic : ")

			reader := bufio.NewScanner(os.Stdin)

			reader.Scan()

			text = reader.Text() + " " + IP + " " + port

			client, err := rpc.Dial("tcp", "localhost:1237")
			if err != nil {
				log.Fatal("dialing:", err)
			}

			var reply string
			divCall := client.Go("Listener.Unsubscribe", text, &reply, nil)
			replyCall := <-divCall.Done //Will be equal to divCall

			if replyCall == nil {

				log.Fatal(replyCall)
			}

			fmt.Println("Message received : ", reply)

			client.Close()
		}

	}

}

//funzione che inizializza il listener per ricevere messaggi dal server
func receive_from_server() {

	addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:"+port)
	if err != nil {
		log.Fatal(err)
	}

	l, e := net.ListenTCP("tcp", addr)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	listener := new(Listener)

	rpc.Register(listener)

	for {

		rpc.Accept(l)

	}

}
