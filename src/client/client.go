package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"github.com/fatih/color"
)

var port = flag.Int("port", 8000, "The port to connect to; defaults to 8000.")
var servers_file = "/usr/app/log_querier/servers"
var wg sync.WaitGroup

func main() {
	flag.Parse()
	cmd := os.Args[len(os.Args)-1]
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
		wg.Add(1)
		go request_cmd(host, *port, cmd)
	}
	wg.Wait()

	// For quick test on local host
	// host := "localhost"
	// request_cmd(host, *port, cmd)

}

func request_cmd(host string, port int, cmd string) {
	dest := host + ":" + strconv.Itoa(port)
	conn, err := rpc.Dial("tcp", dest)

	defer wg.Done()
	if err != nil {
		if _, t := err.(*net.OpError); t {
			fmt.Println("Failed to connect " + host, err)
		} else {
			fmt.Println("Unknown error: " + err.Error())
		}
		return
	}

	var output string
	err = conn.Call("CmdService.Exec", cmd, &output)
	if err != nil {
		log.Fatal(err)
	} else {
		lines := strings.Split(strings.TrimRight(output, "\n"), "\n")
		colorCyan := color.New(color.FgCyan)
		for _, l := range lines {
			colorCyan.Print(host + ":")
			fmt.Println(l)
		}
	}
}
