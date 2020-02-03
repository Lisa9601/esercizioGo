package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)


//struttura dei topic, contiene anche la lista dei subscribers sottoscritti ad esso
type Topic struct {
	topic string   //topic
	IP    []string //IP subscribers sottoscritti al topic
	port  []int    //porte dei subscribers

}

var queue *[]Message //coda dei messaggi
var topics []Topic   //lista dei topic
var semantic int     //semantica scelta 		1: at_least_once		2: timeout_based
var timeout_sem int      //timeout di ritrasmissione/ di visibilità

// REMOTE PROCEDURES ---------------------------------------------------------------------------------------------------

//funzione che inserisce un nuovo messaggio ricevuto da un publisher nella coda
func (l *Listener) Publish(line []byte, reply *string) error {

	var m Message
	var topic Topic

	err := json.Unmarshal(line, &m)
	if err != nil {
		log.Fatal(err)
	}

	m.Visible = true //imposto la visibilità di default a true

	fmt.Printf("Command publish : %+v\n", m) //stampa il messaggio che ha ricevuto

	queue = insert(*queue, &m) //inserimento

	topic.topic = m.Topic

	flag := 0

	//Controllo che il topic non sia già presente
	for i := 0; i < len(topics); i++ {
		if topics[i].topic == topic.topic {
			flag = 1
			break
		}
	}

	if flag == 0 {
		topics = append(topics, topic)
	}

	*reply = "ACK"

	//richiama le funzioni che si occupano di inviare i messaggi ai subscriber in base alla semantica
	switch semantic {

	case 1:
		go send_mess_1()
	case 2:
		go send_mess_2()

	}

	return nil
}

// funzione che ritorna l'elenco dei messaggi in coda
func (l *Listener) List(in string , list *[]Message) error {

	fmt.Println("Command list")

	switch semantic {

	case 1:
		*list = *queue

	case 2:

		var newlist []Message

		for i := 0; i < len(*queue); i++ {

			if (*queue)[i].Visible {

				newlist = append(newlist, (*queue)[i])

			}

		}

		*list = newlist

	}

	return nil
}

//funzione che sottoscrive un subscriber ad un topic
func (l *Listener) Subscribe(line string, reply *string) error {

	fmt.Println("Command subscribe: " + line) //stampa il comando che ha ricevuto in input

	splits := strings.Split(line, " ")

	flag := 0
	remote_ip := strings.Split(splits[1], ":")[0]
	port, err := strconv.Atoi(splits[2])
	if err != nil {
		// handle error
		fmt.Println(err)
		os.Exit(2)
	}

	// controllo che esista il topic
	for i := 0; i < len(topics); i++ {

		if topics[i].topic == splits[0] {

			flag = 1

			//controllo che non sia già sottoscritto
			for j := 0; j < len(topics[i].IP); j++ {

				if topics[i].IP[j] == remote_ip && topics[i].port[j] == port {

					flag = 2
					break
				}

			}

			if flag == 1 {
				topics[i].IP = append(topics[i].IP, remote_ip)

				topics[i].port = append(topics[i].port, port)
			}

			break

		}

	}

	if flag == 0 {
		*reply = "Topic not found!"
	} else if flag == 1 {
		*reply = "Subscribed to " + splits[0]
	} else {
		*reply = "You are already subscribed to " + splits[0]
	}

	switch semantic {

	//richiama le funzioni che si occupano di inviare i messaggi ai subscriber in base alla semantica
	case 1:
		go send_mess_1()
	case 2:
		go send_mess_2()

	}

	return nil
}

//funzione che rimuove la sottoscrizione di un subscriber ad un topic
func (l *Listener) Unsubscribe(line string, reply *string) error {

	fmt.Println("Command unsubscribe: " + line) //stampa il comando che ha ricevuto in input

	splits := strings.Split(line, " ")

	flag := 0
	remote_ip := strings.Split(splits[1], ":")[0]
	port, err := strconv.Atoi(splits[2])

	if err != nil {
		// handle error
		fmt.Println(err)
		os.Exit(2)
	}

	var i int
	var j int

	// controllo che esista il topic
	for i = 0; i < len(topics); i++ {

		if topics[i].topic == splits[0] {

			flag = 1

			//controllo che sia sottoscritto
			for j = 0; j < len(topics[i].IP); j++ {

				if topics[i].IP[j] == remote_ip && topics[i].port[j] == port {

					flag = 2
					break
				}

			}

			if flag == 2 {
				copy(topics[i].IP[j:], topics[i].IP[j+1:])
				topics[i].IP = topics[i].IP[:len(topics[i].IP)-1]

				copy(topics[i].port[j:], topics[i].port[j+1:])
				topics[i].port = topics[i].port[:len(topics[i].port)-1]

			}

			break

		}

	}

	if flag == 0 {
		*reply = "Topic not found!"
	} else if flag == 2 {
		*reply = "Unsubscribed from " + splits[0]
	} else {
		*reply = "You are not subscribed to " + splits[0]
	}

	return nil
}

//----------------------------------------------------------------------------------------------------------------------

func main() {

	//richiamo le routine per inizializzare la connessione

	go publish()

	go list()

	go subscribe()

	go unsubscribe()

	//creo la nuova coda
	queue = create_queue()

	//scelta della semantica da usare
	fmt.Printf("1) at_least_once\n" + "2) timeout_based\n" + "Choose the semantic you wish to use : ")

	_, err := fmt.Scanf("%d\n", &semantic)
	if err != nil {
		log.Fatal(err)
	}

	//scelta del timeout
	fmt.Printf("Insert the timeout you wish to use (milliseconds) : ")

	_, err = fmt.Scanf("%d\n", &timeout_sem)
	if err != nil {
		log.Fatal(err)
	}

	//ciclo infinito
	for {}
}

func list() {

	addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:1234")
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

func publish() {

	addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:1235")
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

func subscribe() {

	addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:1236")
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

func unsubscribe() {

	addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:1237")
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

func send_mess_1() {

	var i int
	var k int
	var flag int
	var currmess int //numero del messaggio corrente
	var lenlist int  //lunghezza della lista dei messaggi

	if len(*queue) == 0 {
		return
	}

	if len(topics) == 0 {
		return
	}

	currmess = 0
	lenlist = len(*queue)

	for k = 0; k < lenlist; k++ {

		// prendo il messaggio nella coda
		m := (*queue)[currmess]
		topic := m.Topic
		flag = 0

		for i = 0; i < len(topics); i++ {
			if topic == topics[i].topic {
				break
			}
		}

		for j := 0; j < len(topics[i].IP); j++ {

			// Prova ad inviare il messaggio
			client, err := rpc.Dial("tcp", string(topics[i].IP[j])+":"+strconv.Itoa(topics[i].port[j]))
			if err != nil {
				fmt.Println("dialing : "+ err.Error())
				continue
			}

			s, err := json.Marshal(&m)
			if err != nil {
				log.Fatal(err)
				client.Close()
				continue
			}

			var reply string
			divCall := client.Go("Listener.Receive", s, &reply, nil)
			replyCall := <-divCall.Done //Will be equal to divCall

			if replyCall == nil {
				fmt.Println(replyCall)
				client.Close()
				continue
			}

			fmt.Println("Message received :", reply)

			client.Close()
			flag++

		}


		if len(topics[i].IP) != 0 && flag == len(topics[i].IP) {

			//cancello il messaggio dalla coda
			*queue = remove(*queue, currmess)

		} else {
			currmess++
		}

		timeout := make(chan bool, 1)
		go func() {

			time.After(time.Duration(timeout_sem)*time.Microsecond)
			timeout <- true

		}()

	}

}

func send_mess_2() {

	var i int
	var k int
	var flag int
	var currmess int //numero del messaggio corrente
	var lenlist int  //lunghezza della lista dei messaggi

	if len(*queue) == 0 {
		return
	}

	if len(topics) == 0 {
		return
	}

	currmess = 0
	lenlist = len(*queue)

	for k = 0; k < lenlist; k++ {

		// prendo il messaggio nella coda
		m := (*queue)[currmess]
		topic := m.Topic
		flag = 0

		for i = 0; i < len(topics); i++ {
			if topic == topics[i].topic {
				break
			}
		}

		for j := 0; j < len(topics[i].IP); j++ {

			// Prova ad inviare il messaggio finchè non ci riesce
			for {

				m.Visible = false

				client, err := rpc.Dial("tcp", string(topics[i].IP[j])+":"+strconv.Itoa(topics[i].port[j]))
				if err != nil {
					fmt.Println("dialing:" + err.Error())
					m.Visible = true
					break
				}

				s, err := json.Marshal(&m)
				if err != nil {
					fmt.Println("dialing : "+err.Error())
					client.Close()
					m.Visible = true
					break
				}

				var reply string
				divCall := client.Go("Listener.Receive", s, &reply, nil)
				replyCall := <-divCall.Done //Will be equal to divCall

				if replyCall == nil {
					fmt.Println(replyCall)
					client.Close()
					m.Visible = true
					break
				}

				fmt.Println("Message received :", reply)

				client.Close()
				flag++

				//cancello il messaggio dalla coda
				*queue = remove(*queue, currmess)

				timeout := make(chan bool, 1)
				go func() {

					time.After(time.Duration(timeout_sem)*time.Millisecond)
					timeout <- true

				}()

				break
			}

			if flag > 0 {
				break
			}
		}

		if flag == 0 {
			currmess++
		}

	}

}
