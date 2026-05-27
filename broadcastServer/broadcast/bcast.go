package broadcast

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

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
	msg    []byte
	client *Client
}

type Client struct {
	conn       *websocket.Conn
	send       chan []byte
	room       string
	lastActive time.Time
}

var connectedClients = make(map[*Client]bool)
var rooms = make(map[string]map[*Client]bool)
var mod = 1
var opts = []string{"books", "OTT", "games", "party", "project"}

var broker = make(chan channel)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func Init() {
	makeRooms()
	go safeHub()
	http.HandleFunc("/listen", handle)
	log.Fatal(http.ListenAndServe(":7070", nil))
}

func makeRooms() {
	for _, opt := range opts {
		rooms[opt] = make(map[*Client]bool)
	}
	mod = len(opts)
}

func (client *Client) equals(nclient *Client) bool {
	if client.conn == nclient.conn && client.send == nclient.send && client.room == nclient.room {
		return true
	}
	return false
}

func receiveMessageFromClient(conn *websocket.Conn, client *Client) {
	id := conn.RemoteAddr().String()[6:]
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

		client.lastActive = time.Now()

		if bytes.Equal(msg, []byte("pong")) {
			// fmt.Println("pong received from " + id)
			continue
		}

		send := append([]byte("\n"+id+":  "), msg...)

		broker <- channel{
			kind:   Message,
			msg:    send,
			client: client,
		}
	}
}

func sendMessageToClient(client *Client) {
	for msg := range client.send {
		err := client.conn.WriteMessage(1, msg)
		if err != nil {
			broker <- channel{
				kind:   Leave,
				client: client,
			}
			return
		}
		client.lastActive = time.Now()
	}
}

func heartBeat(client *Client) {
	for {
		// sends a ping to connected client every 30 sec
		broker <- channel{
			kind:   Message,
			client: client,
			msg:    []byte("ping"),
		}
		client.lastActive = time.Now()
		time.Sleep(30 * time.Second)
		if time.Since(client.lastActive) > time.Duration(35*time.Second) {
			broker <- channel{
				kind:   Leave,
				client: client,
			}
		}
	}
}

func dropClient(client *Client) {
	fmt.Println("closing connection for " + client.conn.RemoteAddr().String()[6:])
	delete(connectedClients, client)
	close(client.send)
	client.conn.Close()
}

func safeHub() {
	// this is the owner of map and keeps listening for messages on the channel
	for {
		packet := <-broker

		switch packet.kind {
		case Join:
			roomNo := rand.Intn(100) % 2
			rooms[opts[roomNo]][(packet.client)] = true
			packet.client.room = opts[roomNo]
			fmt.Println("client: " + packet.client.conn.RemoteAddr().String()[6:] + " added to room " + strconv.Itoa(roomNo))
			connectedClients[(packet.client)] = true
		case Leave:
			dropClient((packet.client))
		case Message:
			for client := range rooms[packet.client.room] {
				if !packet.client.equals(client) && client.room == packet.client.room {
					select {
					case client.send <- packet.msg:
						continue
					default:
						dropClient(client)
					}
				}
			}
		}

	}
}

func handle(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil) // upgrades the http connection to a websocket

	if err != nil {
		log.Println(err)
	}

	client := &Client{conn, make(chan []byte, 32), "-", time.Now()}

	// add the client to the connectedClients map
	broker <- channel{
		kind:   Join,
		client: client,
	}

	go heartBeat(client)
	go receiveMessageFromClient(conn, client)
	go sendMessageToClient(client)
}
