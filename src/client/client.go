package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	. "result"
	"strconv"
	"bufio"
)

var port = flag.Int("port", 8000, "The port to connect to; defaults to 8000.")
var servers_file = "./scripts/servers"
func main() {
	flag.Parse()
	reg := os.Args[1]
	file, err := os.Open(servers_file)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file) 
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		host := scanner.Text()
		dest := host + ":" + strconv.Itoa(*port)
		request_grep(dest, reg)
	}
}
func request_grep(dest string, reg string) {
	conn, err := rpc.Dial("tcp", dest)

	if err != nil {
		if _, t := err.(*net.OpError); t {
			fmt.Println("Some problem connecting.", err)
		} else {
			fmt.Println("Unknown error: " + err.Error())
		}
		os.Exit(1)
	}

	var machineResult MachineResult
	err = conn.Call("GrepService.Grep", reg, &machineResult)
	if err != nil {
		log.Fatal(err)
	} else {
		PrintResult(machineResult)
	}
}
