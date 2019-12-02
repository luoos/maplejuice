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

- exec "<command>" - execute command on all servers
- dump - dump local host membership list
- ls <sdfsfilename> - list all machine addresses where this file is currently being stored
- lsdir <sdfsDir> - list all sdfsfiles in sdfs directory
- store - list all files currently being stored at this machine
- put <localfilepath> <sdfsfilepath> - Insert or update a local file to the distributed file system
- put <localdirpath> <sdfsfilepath> - Insert or update all local files in a directory
- append <localfilepath> <sdfsfilepath> append a local file to the distributed file system
- get <sdfsfilename> <localfilename> - Get the file from the distributed file system, and store it to <localfilename>
- delete <sdfsfilename> - Delete a file from the distributed file system
- deleteDir <sdfsdir> - Delete a directory from the distributed file system
- maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory> - Send Maple Task
- juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1} - Send Juice Task
`

var port = flag.Int("port", 8000, "The port to connect to; defaults to 8000.")
var dump = flag.Bool("dump", false, "Dump membership list")
var servers_file = "/usr/app/log_querier/servers"
var wg sync.WaitGroup

const sdfsDir = "/apps/files"
const DcliReceiverPort = node.DcliReceiverPort

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
	case "lsdir":
		sdfsDir := os.Args[2]
		listDirFromSystem(sdfsDir)
	case "store":
		listLocalFiles()
	case "put":
		if len(os.Args) != 4 {
			log.Fatal("Need More Arguments!")
			fmt.Println(usage_prompt)
		}
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
	case "deleteDir":
		sdfsDir := os.Args[2]
		deleteDirFromSystem(sdfsDir)
	case "maple":
		if len(os.Args) != 6 {
			log.Fatal("Need More Arguments!")
			fmt.Println(usage_prompt)
		}
		maple_exe := os.Args[2]
		num_maples, _ := strconv.Atoi(os.Args[3])
		prefix := os.Args[4]
		src_dir := os.Args[5]
		CallMapleTask(maple_exe, num_maples, prefix, src_dir)
	case "juice":
		if len(os.Args) != 7 {
			log.Fatal("Need More Arguments!")
			fmt.Println(usage_prompt)
		}
		juice_exe := os.Args[2]
		num_juices, _ := strconv.Atoi(os.Args[3])
		prefix := os.Args[4]
		destFilename := os.Args[5]
		deleteInput := (os.Args[6] == "1") // TODO: Should check if input is valid
		CallJuiceTask(juice_exe, num_juices, prefix, destFilename, deleteInput)
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
	address := ip + ":" + node.RPC_DEFAULT_PORT
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	return client, address
}

func listHostsForFile(sdfsName string) {
	client, address := dialLocalNode()
	defer client.Close()
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

func deleteDirFromSystem(dirName string) {
	CallDeleteDirRequest(dirName)
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
	defer conn.Close()
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
	defer client.Close()
	var reply node.RPCResultType
	err := client.Call(node.FileServiceName+address+".PutFileRequest", node.PutFileArgs{src, dest, forceUpdate, false}, &reply)
	if err != nil {
		log.Printf("call PutFileRequest return err")
		log.Fatal(err)
	}
	return reply
}

func CallGetFileRequest(sdfsName, localPath string) error {
	// localPath should be absolute path
	client, address := dialLocalNode()
	defer client.Close()
	var result node.RPCResultType
	err := client.Call(node.FileServiceName+address+".GetFileRequest", []string{sdfsName, localPath}, &result)
	return err
}

func CallDeleteFileRequest(sdfsName string) error {
	client, address := dialLocalNode()
	defer client.Close()
	var result node.RPCResultType
	err := client.Call(node.FileServiceName+address+".DeleteFileRequest", sdfsName, &result)
	if result != node.RPC_SUCCESS {
		fmt.Println("Fail to delete file, check SLOG output")
		fmt.Println(err)
	}
	return err
}

func CallDeleteDirRequest(sdfsDir string) error {
	client, address := dialLocalNode()
	defer client.Close()
	var result node.RPCResultType
	err := client.Call(node.FileServiceName+address+".DeleteSDFSDirRequest", sdfsDir, &result)
	if result != node.RPC_SUCCESS {
		fmt.Println("Fail to delete file, check SLOG output")
		fmt.Println(err)
	}
	return err
}

func CallMapleTask(maple_exe string, num_maples int, prefix, src_dir string) {
	client, address := dialLocalNode()
	ip := strings.Split(address, ":")[0]
	defer client.Close()
	args := &node.MapleJuiceTaskArgs{
		TaskType:   node.MapleTask,
		Exe:        maple_exe,
		NumWorkers: num_maples,
		InputPath:  src_dir,
		OutputPath: prefix,
		ClientAddr: ip + ":" + DcliReceiverPort,
	}
	var result node.RPCResultType
	err := client.Call(node.MapleJuiceServiceName+address+".ForwardMapleJuiceRequest", args, &result)
	if result != node.RPC_SUCCESS {
		fmt.Println("Fail, check SLOG output")
		fmt.Println(err)
	}
	// open a tcp listener
	waitResponse()
}

func CallJuiceTask(juice_exe string, num_juices int, prefix string, destFilename string, deleteInput bool) {
	client, address := dialLocalNode()
	ip := strings.Split(address, ":")[0]
	defer client.Close()
	args := &node.MapleJuiceTaskArgs{
		TaskType:    node.JuiceTask,
		Exe:         juice_exe,
		NumWorkers:  num_juices,
		InputPath:   prefix,
		OutputPath:  destFilename,
		ClientAddr:  ip + ":" + DcliReceiverPort,
		DeleteInput: deleteInput,
	}
	var result node.RPCResultType
	err := client.Call(node.MapleJuiceServiceName+address+".ForwardMapleJuiceRequest", args, &result)
	if result != node.RPC_SUCCESS {
		fmt.Println("Fail, check SLOG output")
		fmt.Println(err)
	}
	// open a tcp listener
	waitResponse()
}

func waitResponse() {
	ln, err := net.Listen("tcp", "0.0.0.0:"+DcliReceiverPort)
	if err != nil {
		fmt.Print(err)
	}
	conn, err := ln.Accept()
	if err != nil {
		fmt.Print(err)
	}
	message, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Print(err)
	}
	fmt.Print(message)
}

func listDirFromSystem(sdfsDir string) {
	client, address := dialLocalNode()
	defer client.Close()
	var result []string
	client.Call(node.FileServiceName+address+".ListFileInDirRequest", sdfsDir, &result)
	for _, filePath := range result {
		fmt.Println(filePath)
	}
	fmt.Printf("\n%d files in total\n", len(result))
}
