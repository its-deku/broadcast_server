package main

import (
	"fmt"

	bcasthandler "sample.com/v2/broadcast/broadcast"
)

func main() {
	fmt.Println("Broadcast server")

	bcasthandler.Init()

}

/* possibly the thing to use here is r.Context()*/
/*ps - websockets was the best option :)*/
