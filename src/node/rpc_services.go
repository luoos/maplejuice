/*
This file defines rpc services for a node.

Includes:

1. Client puts a file
2. Client deletes a file
3. Client uses command "ls"
*/

package node

import (
	"log"
	"net"
	"net/rpc"
	. "slogger"
	"strconv"
)

const FileServiceName = "SimpleFileService"
const FILE_SERVICE_DEFAULT_PORT = "8011"

type RPCResultType int8

const (
	RPC_SUCCESS RPCResultType = 1 << 0
)

type FileService struct {
	node *Node
}

type FileServiceInterface = interface {
	PutFileRequest(args []string, code *RPCResultType) error
	GetTimeStamp(sdfsFileName string, timestamp *int) error
}

func DialFileService(address string) *rpc.Client {
	// address: IP + Port, such as "0.0.0.0:8011"
	c, err := rpc.Dial("tcp", address)
	if err != nil {
		SLOG.Fatal(err)
	}
	return c
}

func (node *Node) RegisterFileService(svc FileServiceInterface) error {
	return rpc.RegisterName(FileServiceName, svc)
}

func (node *Node) StartRPCFileService(port string) {
	node.RegisterFileService(&FileService{node: node})
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
func (fileService *FileService) PutFileRequest(args []string, result *RPCResultType) error {
	// args should have three elements: [localFilePath, sdfsFileName, forceUpdate]

	// filePath, sdfsFileName := args[0], args[1]
	// filename := filepath.Base(filePath)
	// hashId := getHashID(filename)
	/*TODO:
	1. find machines responsible for this file
	2. collect timestamp from above machines through RPC
		2.1 if timestamp is not nil and and last update is within 60s, return RPC_CAUTION
		2.2 otherwise transfer the file to responsible machines and wait for 3 ACK, return RPC_SUCCESS
	*/
	*result = RPC_SUCCESS
	return nil
}

func (fileService *FileService) GetTimeStamp(sdfsFileName string, timestamp *int) error {
	*timestamp = fileService.node.FileList.GetTimeStamp(sdfsFileName)
	return nil
}

/* Callee end */

/* Caller begin */
func CallPutFileRequest(address, src, dest string, forceUpdate bool) RPCResultType {
	/* If forceUpdate is false,
	 */
	client := DialFileService(address)
	var reply RPCResultType
	err := client.Call(FileServiceName+".PutFileRequest", []string{src, dest, strconv.FormatBool(forceUpdate)}, &reply)
	if err != nil {
		SLOG.Fatal(err)
	}
	return reply
}

func CallGetTimeStamp(address, sdfsFileName string) int {
	client := DialFileService(address)
	var timestamp int
	err := client.Call(FileServiceName+".GetTimeStamp", sdfsFileName, &timestamp)
	if err != nil {
		SLOG.Fatal(err)
	}
	return timestamp
}

/* Caller end */
