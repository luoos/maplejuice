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
)

const FileServiceName = "SimpleFileService"

type RPCResultType int8

const (
	RPC_SUCCESS RPCResultType = 1 << 0
)

type FileService struct{}

type FileServiceInterface = interface {
	PutFile(args []string, code *RPCResultType) error
}

func (node *Node) RegisterFileService(svc FileServiceInterface) error {
	return rpc.RegisterName(FileServiceName, svc)
}

func (node *Node) StartRPCFileService(port string) {
	node.RegisterFileService(new(FileService))
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

func (service *FileService) PutFile(args []string, result *RPCResultType) error {
	*result = RPC_SUCCESS
	return nil
}
