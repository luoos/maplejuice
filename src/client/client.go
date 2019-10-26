package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"node"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/fatih/color"
)

const usage_prompt = `Client commands:

1. exec "<command>" - execute command on all servers
2. dump - dump local host membership list
3. ls <sdfsfilename> - list all machine addresses where this file is currently being stored
4. store - list all files currently being stored at this machine
5. put <localfilename> <sdfsfilename> - Insert or update a local file to the distributed file system
6. get <sdfsfilename> <localfilename> - Get the file from the distributed file system, and store it to <localfilename>
7. delete <sdfsfilename> - Delete a file from the distributed file system`

var port = flag.Int("port", 8000, "The port to connect to; defaults to 8000.")
var dump = flag.Bool("dump", false, "Dump membership list")
var servers_file = "/usr/app/log_querier/servers"
var wg sync.WaitGroup

func main() {
	flag.Parse()
	if len(os.Args) == 1 {
		fmt.Println(usage_prompt)
		os.Exit(1)
	}
	parseCommand()
}

func parseCommand() {
	switch os.Args[1] {
	case "exec":
		cmd := os.Args[2]
		execCommand(cmd)
	case "dump":
		dumpMembershipList()
	case "ls":
		sdfsName := os.Args[2]
		listHostsForFile(sdfsName)
	case "store":
		listLocalFiles()
	case "put":
		source := os.Args[2]
		destination := os.Args[3]
		putFileToSystem(source, destination)
	case "get":
		source := os.Args[2]
		destination := os.Args[3]
		getFileFromSystem(source, destination)
	case "delete":
		sdfsName := os.Args[2]
		deleteFileFromSystem(sdfsName)
	default:
		fmt.Println(usage_prompt)
		os.Exit(1)
	}
}

func listHostsForFile(sdfsName string) {

}

func listLocalFiles() {

}

func putFileToSystem(localName, sdfsName string) {

}

func getFileFromSystem(sdfsName, localName string) {

}

func deleteFileFromSystem(sdfsName string) {

}

func dumpMembershipList() {
	mbList := node.ConstructFromTmpFile()
	mbList.NicePrint()
}

func execCommand(cmd string) {
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
		wg.Add(1) // add wait logic for goroutine
		go request_cmd(host, *port, cmd)
	}
	wg.Wait() // wait until all goroutine finished
}

func request_cmd(host string, port int, cmd string) {
	dest := host + ":" + strconv.Itoa(port)
	conn, err := rpc.Dial("tcp", dest)

	defer wg.Done()
	if err != nil {
		if _, t := err.(*net.OpError); t {
			fmt.Println("Failed to connect "+host, err)
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
			colorCyan.Print(host + ":") // print with color for vm name
			fmt.Println(l)
		}
	}
}
