package test

import (
	"log"
	"net/rpc"
	"node"
	"testing"
	"time"
)

func DialFileService(address string) (*rpc.Client, error) {
	c, err := rpc.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func TestRegisterFileService(t *testing.T) {
	node0 := node.CreateNode("0.0.0.0", "9200")
	go node0.StartRPCFileService("9300")
	time.Sleep(50 * time.Millisecond)
	client, err := DialFileService("0.0.0.0:9300")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	var reply node.RPCResultType
	err = client.Call(node.FileServiceName+".PutFile", []string{"src", "dest"}, &reply)
	if err != nil {
		log.Fatal(err)
	}
	if reply != node.RPC_SUCCESS {
		log.Fatal(reply)
	}
}
