package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	. "result"
	"strconv"
)

var host = flag.String("host", "localhost", "The hostname or IP to connect to; defaults to \"localhost\".")
var port = flag.Int("port", 8000, "The port to connect to; defaults to 8000.")

func main() {
	flag.Parse()
	reg := os.Args[1]

	dest := *host + ":" + strconv.Itoa(*port)
	conn, err := rpc.Dial("tcp", dest)

	if err != nil {
		if _, t := err.(*net.OpError); t {
			fmt.Println("Some problem connecting.")
		} else {
			fmt.Println("Unknown error: " + err.Error())
		}
		os.Exit(1)
	}

	var reply []byte
	err = conn.Call("GrepService.Grep", reg, &reply)
	if err != nil {
		log.Fatal(err)
	} else {
		var machineResult MachineResult
		err := json.Unmarshal(reply, &machineResult)
		if err != nil {
			log.Fatal(err)
		} else {
			PrintResult(machineResult)
		}
	}
}
