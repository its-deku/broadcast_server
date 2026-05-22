package main

import (
	"bufio"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/gorilla/websocket"
)

var msg chan []byte = make(chan []byte)

func receive(c *websocket.Conn) {
	for {
		_, reply, err := c.ReadMessage()

		if err != nil {
			log.Println(err)
			os.Exit(0)
		}

		replyc := strings.ToLower(strings.Trim(string(reply), " "))

		if replyc == "ping" {
			msg <- []byte("pong")
		} else {
			fmt.Println(replyc)
		}
	}
}

func hub(c *websocket.Conn) {
	for {
		packet := <-msg
		if err := c.WriteMessage(1, packet); err != nil {
			log.Println(err)
		}
	}
}

func send(c *websocket.Conn) {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("send a message here: ")
		line, err := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		if err != nil {
			c.Close()
			continue
		}

		msg <- []byte(line)

		if line == "X" {
			c.Close()
			break
		}

	}
}

func main() {
	u := url.URL{Scheme: "ws", Host: "localhost:7070", Path: "/listen"}

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	go hub(c)
	// listen for a message from the server
	go receive(c)
	go send(c)
	for {
	}
}
