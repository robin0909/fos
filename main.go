package main

import (
	"com.github/robin0909/fos/log"
	"com.github/robin0909/fos/mq"
	"com.github/robin0909/fos/obj"
	"fmt"
	"os"
)

func main() {

	var consumer mq.DefaultConsumer
	mq.Receive(&consumer)

	for i := 0; i < 10; i++ {
		mq.Push(fmt.Sprintf("index = %d", i))
	}

	var dataDir string
	args := os.Args
	if len(args) >= 2 {
		dataDir = args[1]
	} else {
		dataDir = "/tmp/data"
	}

	log.Info.Println(`welcome fos for obj or object system`)
	log.Info.Println(`data-dir: ` + dataDir)

	server := obj.New(dataDir)
	server.RunFos()

}
