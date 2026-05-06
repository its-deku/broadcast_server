package main

import (
	"fmt"

	bcasthandler "sample.com/v2/broadcast/broadcast"
)

func main() {
	fmt.Println("Broadcast server")
	// bchan := make(chan bool)

	bcasthandler.Init()
	// for range 3 {
	// 	bcasthandler.Connect()
	// }
	// <-bchan
}

/* possibly the thing to use here is r.Context()*/
/*ps - websockets was the best option :)*/
