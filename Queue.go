package main

type Listener int

//struttura dei messaggi scambiati
type Message struct {
	Topic   string //topic
	Data    string //contenuto del messaggio
	Date    string //data creazione
	Visible bool   //flag visibilità del messaggio per la semantica timeout_based

}

//funzione che crea una nuova coda di messaggi
func create_queue() *[]Message {

	var list []Message
	return &list

}

//funzione che inserisce i messaggi nella coda
func insert(list []Message, message *Message) *[]Message {

	list = append(list, *message)
	return &list

}

//funzione che rimuove un messaggio dalla coda
func remove(list []Message, i int) []Message {

	if len(list) <= 1 {
		list = *create_queue()
		return list
	}

	copy(list[i:], list[i+1:])
	list = list[:len(list)-1]

	return list

}
