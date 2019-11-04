package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"node"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
)

const usage_prompt = `Client commands:

1. exec "<command>" - execute command on all servers
2. dump - dump local host membership list
3. ls <sdfsfilename> - list all machine addresses where this file is currently being stored
4. store - list all files currently being stored at this machine
5.1 put <localfilename> <sdfsfilename> - Insert or update a local file to the distributed file system
5.2 put <localdirname> - Insert or update all local files in a directory
6. get <sdfsfilename> <localfilename> - Get the file from the distributed file system, and store it to <localfilename>
7. delete <sdfsfilename> - Delete a file from the distributed file system`

var port = flag.Int("port", 8000, "The port to connect to; defaults to 8000.")
var dump = flag.Bool("dump", false, "Dump membership list")
var servers_file = "/usr/app/log_querier/servers"
var wg sync.WaitGroup

const sdfsDir = "/apps/files"

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
		fstat, err := os.Stat(source)
		if err != nil {
			log.Fatal(err)
		}
		var destination string
		if len(os.Args) == 4 {
			if fstat.IsDir() {
				log.Fatal("this is not a regular file")
			}
			destination = os.Args[3]
		} else {
			if !fstat.IsDir() {
				log.Fatal("this is not a directory")
			}
			destination = source
		}
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

func dialLocalNode() (*rpc.Client, string) {
	hostname, _ := os.Hostname()
	addr_raw, err := net.LookupIP(hostname)
	if err != nil {
		fmt.Println("Unknown host")
	}
	ip := fmt.Sprintf("%s", addr_raw[0])
	address := ip + ":" + node.FILE_SERVICE_DEFAULT_PORT
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	return client, address
}
func listHostsForFile(sdfsName string) {
	client, address := dialLocalNode()
	var addrs []string
	err := client.Call(node.FileServiceName+address+".Ls", sdfsName, &addrs)
	if err != nil {
		log.Fatal(err)
	}
	for _, addr := range addrs {
		fmt.Println(addr)
	}
}

func listLocalFiles() {
	files, err := ioutil.ReadDir(sdfsDir)
	if err != nil {
		log.Fatal(err)
	}
	cnt := 0
	for _, file := range files {
		fmt.Println(file.Name(), file.Size())
		cnt++
	}
	fmt.Printf("\n%d files.\n", cnt)
}

func prompRoutine(c chan string) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Last update was in 1 minute, type \"yes\" to confirm update: ")
	text, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	c <- text
}
func putFileToSystem(localName, sdfsName string) {
	localAbsPath, _ := filepath.Abs(localName)
	reply := CallPutFileRequest(localAbsPath, sdfsName, false)
	if reply == node.RPC_PROMPT {
		c := make(chan string)
		go prompRoutine(c)
		select {
		case text := <-c:
			text = strings.TrimSuffix(text, "\n")
			if text == "yes" {
				CallPutFileRequest(localAbsPath, sdfsName, true)
			} else {
				fmt.Println("Abort")
			}
		case <-time.After(10 * time.Second):
			fmt.Printf("\nAbort\n")
		}
	}
}

func getFileFromSystem(sdfsName, localName string) {
	localAbsPath, _ := filepath.Abs(localName)
	err := CallGetFileRequest(sdfsName, localAbsPath)
	if err != nil {
		fmt.Printf("Failed to get file %s\n", sdfsName)
	}
}

func deleteFileFromSystem(sdfsName string) {
	CallDeleteFileRequest(sdfsName)
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
			colorCyan.Print(host + ": ") // print with color for vm name
			fmt.Println(l)
		}
	}
}

func CallPutFileRequest(src, dest string, forceUpdate bool) node.RPCResultType {
	// src is absolute path.
	// dest is sdfs filename
	if !filepath.IsAbs(src) {
		fmt.Printf("%s is not a absolute path\n", src)
		os.Exit(1)
	}
	if _, err := os.Stat(src); os.IsNotExist(err) {
		fmt.Printf("%s doesn't exist\n", src)
		os.Exit(1)
	}
	client, address := dialLocalNode()
	var reply node.RPCResultType
	err := client.Call(node.FileServiceName+address+".PutFileRequest", node.PutFileArgs{src, dest, forceUpdate}, &reply)
	if err != nil {
		log.Printf("call PutFileRequest return err")
		log.Fatal(err)
	}
	return reply
}

func CallGetFileRequest(sdfsName, localPath string) error {
	// localPath should be absolute path
	client, address := dialLocalNode()
	var result node.RPCResultType
	err := client.Call(node.FileServiceName+address+".GetFileRequest", []string{sdfsName, localPath}, &result)
	return err
}

func CallDeleteFileRequest(sdfsName string) error {
	client, address := dialLocalNode()
	var result node.RPCResultType
	err := client.Call(node.FileServiceName+address+".DeleteFileRequest", sdfsName, &result)
	if result != node.RPC_SUCCESS {
		fmt.Println("Fail to delete file, check SLOG output")
		fmt.Println(err)
	}
	return err
}
