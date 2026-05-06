package broadcast

import (
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

type event int

// iota makes increasing numbers
const (
	Join event = iota
	Leave
	Message
)

type channel struct {
	kind   event
	msg    string
	client *websocket.Conn
}

var connectedClients = make(map[*websocket.Conn]bool)

var broker = make(chan channel)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func Init() {
	go safeBroadcast()
	http.HandleFunc("/listen", handle)
	log.Fatal(http.ListenAndServe(":7070", nil))
}

func safeBroadcast() {
	// this is the owner of map and keeps listening for messages on the channel
	for {
		packet := <-broker

		switch packet.kind {
		case Join:
			connectedClients[packet.client] = true
		case Leave:
			fmt.Println("closing connection for " + packet.client.RemoteAddr().String()[6:])
			delete(connectedClients, packet.client)
			packet.client.Close()
		case Message:
			for client := range connectedClients {
				if packet.client != client {
					err := client.WriteMessage(1, []byte(packet.msg))
					if err != nil {
						delete(connectedClients, client)
						packet.client.Close()
					}
				}
			}
		}

	}
}

func handle(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(conn.RemoteAddr())
	id := conn.RemoteAddr().String()[6:]

	// add the client to the connectedClients map
	broker <- channel{
		kind:   Join,
		client: conn,
	}

	// keeps listening for messages from the connected client
	for {
		_, msg, err := conn.ReadMessage()

		if err != nil {
			break
		}

		if strings.TrimSpace(string(msg)) == "X" {
			broker <- channel{
				kind:   Leave,
				client: conn,
			}
			break
		}

		send := []byte("\n" + id + ":  ")
		send = append(send, msg...)

		broker <- channel{
			kind:   Message,
			msg:    string(send),
			client: conn,
		}
	}
}
