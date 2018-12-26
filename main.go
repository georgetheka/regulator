package main

import (
	"flag"
	"fmt"
	"log"
	"regulator/client"
	"regulator/common"
	"regulator/producer"
	"regulator/proxy"
	"regulator/queue"
	"regulator/server"
)

func kill(port int) {
	_, _, _ = common.HttpGet(fmt.Sprintf("http://localhost:%d/%s", port, common.KillEndpoint))
}

func killall() {
	kill(common.ProducerPort)
	kill(common.ClientPort)
	kill(common.QueuePort)
	kill(common.ProxyPort)
	kill(common.ServerPort)
}

func main() {
	component := flag.String("c", "server", "System component to run as")
	flag.Parse()

	switch *component {
	case "server":
		server.Run()
	case "proxy":
		proxy.Run()
	case "client":
		client.Run()
	case "producer":
		producer.Run()
	case "queue":
		queue.Run()
	case "killall":
		killall()
	default:
		log.Printf("unknown component %s\n", *component)
	}
}
