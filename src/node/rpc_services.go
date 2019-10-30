/*
This file defines rpc services for a node.

Includes:

1. Client puts a file
2. Client deletes a file
3. Client uses command "ls"
*/

package node

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	. "slogger"
)

const FileServiceName = "SimpleFileService"
const FILE_SERVICE_DEFAULT_PORT = "8011"
const READ_QUORUM = 2
const MIN_UPDATE_INTERVAL = 60 * 1000

type RPCResultType int8

type Pair struct {
	Address string
	Ts      int // Timestamp
}

type PutFileArgs struct {
	LocalName   string
	SdfsName    string
	ForceUpdate bool
}

type StoreFileArgs struct {
	masterNodeId int
	SdfsName     string
	Ts           int
	Content      string
}

const (
	RPC_SUCCESS     RPCResultType = 1 << 0
	LOCAL_PATH_ROOT               = "/apps/files"
)

type FileService struct {
	node *Node
}

type FileServiceInterface = interface {
	PutFileRequest(args PutFileArgs, code *RPCResultType) error
	GetTimeStamp(sdfsFileName string, timestamp *int) error
	StoreFileToLocal(args StoreFileArgs, result *RPCResultType) error
}

func DialFileService(address string) *rpc.Client {
	// address: IP + Port, such as "0.0.0.0:8011"
	c, err := rpc.Dial("tcp", address)
	if err != nil {
		SLOG.Fatal(err)
	}
	return c
}

func (node *Node) RegisterFileService(address string, svc FileServiceInterface) error {
	return rpc.RegisterName(FileServiceName+address, svc)
}

func (node *Node) StartRPCFileService(port string) {
	node.RegisterFileService(node.IP+":"+port, &FileService{node: node})
	listener, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		log.Fatal("ListenTCP error:", err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Accept error:", err)
		}

		go rpc.ServeConn(conn)
	}
}

/* Callee begin */
func (fileService *FileService) PutFileRequest(args PutFileArgs, result *RPCResultType) error {
	// args should have three elements: [localFilePath, sdfsFileName, forceUpdate]
	// _, sdfsFileName := args[0], args[1] // TODO: use localFilePath, (args[0])
	// _, err := strconv.ParseBool("true") // TODO: use forceUpdate
	// if err != nil {
	// 	SLOG.Fatal(err)
	// }
	// addressList := fileService.node.GetAddressWithSDFSFileName(sdfsFileName)

	// filePath, sdfsFileName := args[0], args[1]
	// filename := filepath.Base(filePath)
	// hashId := getHashID(filename)
	/*TODO:
	1. find machines responsible for this file
	2. collect timestamp from above machines through RPC
		2.1 if timestamp is not nil and and last update is within 60s, return RPC_CAUTION
		2.2 otherwise transfer the file to responsible machines and wait for 3 ACK, return RPC_SUCCESS
	*/
	// if GetMillisecond-file_ts < MIN_UPDATE_INTERVAL {
	// TODO:	promp to user
	// }

	*result = RPC_SUCCESS
	return nil
}

func (fileService *FileService) GetFileRequest(sdfsfilename string, result *string) error {
	file_addr, _ := fileService.node.GetAddressOfLatestTS(sdfsfilename)
	*result = GetFile(file_addr, sdfsfilename)
	return nil
}

func (fileService *FileService) Ls(sdfsfilename string, addrs *[]string) error {
	*addrs = fileService.node.GetResponsibleAddresses(sdfsfilename, FILE_SERVICE_DEFAULT_PORT)
	return nil
}

func (fileService *FileService) GetTimeStamp(sdfsFileName string, timestamp *int) error {
	*timestamp = fileService.node.FileList.GetTimeStamp(sdfsFileName)
	return nil
}

func (fileService *FileService) StoreFileToLocal(args StoreFileArgs, result *RPCResultType) error {
	filePath := LOCAL_PATH_ROOT + "/" + args.SdfsName
	fileService.node.FileList.PutFileInfo(args.SdfsName, LOCAL_PATH_ROOT, args.Ts, args.masterNodeId)
	content_bytes := []byte(args.Content)
	err := ioutil.WriteFile(filePath, content_bytes, 0777)
	if err != nil {
		SLOG.Print(err)
	}
	*result = RPC_SUCCESS
	return nil
}

func (fileService *FileService) ServeLocalFile(sdfsfilename string, result *string) error {
	fileinfo := fileService.node.FileList.GetFileInfo(sdfsfilename)
	data, err := ioutil.ReadFile(fileinfo.Localpath)
	if err != nil {
		log.Fatal(err)
	}
	*result = string(data)
	return nil
}

/* Callee end */

/* Caller begin */

/** from client **/
func CallPutFileRequest(address, src, dest string, forceUpdate bool) RPCResultType {
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
	client := DialFileService(address)
	var reply RPCResultType
	err := client.Call(FileServiceName+address+".PutFileRequest", PutFileArgs{src, dest, forceUpdate}, &reply)
	if err != nil {
		SLOG.Fatal(err)
	}
	return reply
}

func CallLs(address, sdfsfilename string) []string {
	clien := DialFileService(address)
	var addresses []string
	err := clien.Call(FileServiceName+address+".Ls", sdfsfilename, &addresses)
	if err != nil {
		SLOG.Fatal(err)
	}
	return addresses
}

/** from coordinator **/
func PutFile(masterNodeID int, timestamp int, address, sdfsfilename, content string) {
	client := DialFileService(address)
	var reply RPCResultType
	args := StoreFileArgs{masterNodeID, sdfsfilename, timestamp, content}
	send_err := client.Call(FileServiceName+address+".StoreFileToLocal", args, &reply)
	if send_err != nil {
		log.Fatal("send_err:", send_err)
	}
}

func GetFile(address, sdfsfilename string) string {
	sender := DialFileService(address)
	var file_content string
	send_err := sender.Call(FileServiceName+address+".ServeLocalFile", sdfsfilename, &file_content)
	if send_err != nil {
		log.Fatal("send_err:", send_err)
	}
	return file_content
}

func CallGetTimeStamp(address, sdfsFileName string, c chan Pair) {
	sender := DialFileService(address)
	var timestamp int
	err := sender.Call(FileServiceName+address+".GetTimeStamp", sdfsFileName, &timestamp)
	if err != nil {
		SLOG.Fatal(err)
	}
	c <- Pair{address, timestamp}
}

/* Caller end */
