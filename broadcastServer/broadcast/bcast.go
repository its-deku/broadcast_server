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
	client Client
}

type Client struct {
	conn *websocket.Conn
	send chan []byte
}

var connectedClients = make(map[Client]bool)

var broker = make(chan channel)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func Init() {
	go safeHub()
	http.HandleFunc("/listen", handle)
	log.Fatal(http.ListenAndServe(":7070", nil))
}

func sendMessage(client Client) {
	for msg := range client.send {
		err := client.conn.WriteMessage(1, msg)
		if err != nil {
			broker <- channel{
				kind:   Leave,
				client: client,
			}
			return
		}
	}
}

func safeHub() {
	// this is the owner of map and keeps listening for messages on the channel
	for {
		packet := <-broker

		switch packet.kind {
		case Join:
			connectedClients[packet.client] = true
		case Leave:
			fmt.Println("closing connection for " + packet.client.conn.RemoteAddr().String()[6:])
			delete(connectedClients, packet.client)
			packet.client.conn.Close()
		case Message:
			for client := range connectedClients {
				if packet.client != client {
					client.send <- []byte(packet.msg)
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
	client := Client{conn, make(chan []byte)}

	// add the client to the connectedClients map
	broker <- channel{
		kind:   Join,
		client: client,
	}

	go sendMessage(client)

	// keeps listening for messages from the connected client
	for {
		_, msg, err := conn.ReadMessage()

		if err != nil {
			break
		}

		if strings.TrimSpace(string(msg)) == "X" {
			broker <- channel{
				kind:   Leave,
				client: client,
			}
			break
		}

		send := []byte("\n" + id + ":  ")
		send = append(send, msg...)

		broker <- channel{
			kind:   Message,
			msg:    string(send),
			client: client,
		}
	}
}
