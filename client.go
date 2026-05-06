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

func receive(c *websocket.Conn) {
	for {
		_, reply, err := c.ReadMessage()

		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(string(reply))
	}
}

func main() {
	u := url.URL{Scheme: "ws", Host: "localhost:7070", Path: "/listen"}

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	reader := bufio.NewReader(os.Stdin)

	// listen for a message from the server
	go receive(c)

	for {
		fmt.Print("send a message here: ")
		line, err := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		if err != nil {
			continue
		}

		if err := c.WriteMessage(1, []byte(line)); err != nil {
			log.Println(err)
		}

		if line == "X" {
			c.Close()
			break
		}

	}
}
