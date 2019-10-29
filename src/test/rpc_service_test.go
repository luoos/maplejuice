package test

import (
	"log"
	"net/rpc"
	"node"
	"testing"
	"time"
)

func TestRegisterFileService(t *testing.T) {
	node0 := node.CreateNode("0.0.0.0", "9200")
	go node0.StartRPCFileService("9300")
	time.Sleep(50 * time.Millisecond)
	var reply node.RPCResultType
	reply = node.CallPutFileRequest("0.0.0.0:9300", "src", "dest", false)
	if reply != node.RPC_SUCCESS {
		log.Fatal(reply)
	}
}

func TestGetTimeStampRPC(t *testing.T) {
	node0 := node.CreateNode("0.0.0.0", "9210")
	go node0.StartRPCFileService("9310")
	time.Sleep(50 * time.Millisecond)
	sdfsfilename := "testFilename"
	var addr_and_ts string
	c := make(chan string)
	go node.CallGetTimeStamp("0.0.0.0:9310", sdfsfilename, c)
	addr_and_ts = <-c
	assert(addr_and_ts == "0.0.0.0:9310 -1", "wrong timestamp1")
	localpath := "/app/fs/testFilename"
	timestamp := 100
	masterNodeID := 128
	node0.FileList.PutFileInfo(sdfsfilename, localpath, timestamp, masterNodeID)
	go node.CallGetTimeStamp("0.0.0.0:9310", sdfsfilename, c)
	addr_and_ts = <-c
	assert(addr_and_ts == "0.0.0.0:9310 100", "wrong timestamp2")
	node0.FileList.DeleteFileInfo(sdfsfilename)
	go node.CallGetTimeStamp("0.0.0.0:9310", sdfsfilename, c)
	addr_and_ts = <-c
	assert(addr_and_ts == "0.0.0.0:9310 -1", "wrong timestamp3")
}

func TestPutAndGetFileRPC(t *testing.T) {
	coordinator := node.CreateNode("0.0.0.0", "9200")
	master := node.CreateNode("0.0.0.0", "9201")
	go coordinator.StartRPCFileService("9320")
	go master.StartRPCFileService("9321")
	time.Sleep(50 * time.Millisecond)
	sdfsfilename := "testFilename"
	content := "this is my file content"
	node.PutFile(master.Id, 1, "0.0.0.0:9321", "/apps/files", sdfsfilename, content)
	assert(node.GetFile("0.0.0.0:9321", sdfsfilename) == content, "wrong")
}

func TestLs(t *testing.T) {
	coordinator := node.CreateNode("0.0.0.0", "9400")
	node1 := node.CreateNode("0.0.0.0", "9410")
	node2 := node.CreateNode("0.0.0.0", "9420")
	node3 := node.CreateNode("0.0.0.0", "9430")
	coordinator.InitMemberList()
	go coordinator.MonitorInputPacket()
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	go node3.MonitorInputPacket()
	node1.Join(coordinator.IP + ":" + coordinator.Port)
	node2.Join(coordinator.IP + ":" + coordinator.Port)
	node3.Join(coordinator.IP + ":" + coordinator.Port)
	go coordinator.StartRPCFileService("9401")
	go node1.StartRPCFileService("9411")
	go node2.StartRPCFileService("9421")
	go node3.StartRPCFileService("9431")
	time.Sleep(50 * time.Millisecond)
	address := "0.0.0.0:9401"
	client, err := rpc.Dial("tcp", "0.0.0.0:9401")
	if err != nil {
		log.Fatal(err)
	}
	sdfsfilename := "testFilename"
	hashid := getHashID(sdfsfilename)
	var addrs []string
	err = client.Call(node.FileServiceName+address+".Ls", sdfsfilename, &addrs)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("addrs:")
	assert(len(addrs) == 4, "wrong")
	assert(hashid == 392 &&
		addrs[0] == "0.0.0.0:9400" &&
		addrs[1] == "0.0.0.0:9410" &&
		addrs[2] == "0.0.0.0:9420" &&
		addrs[3] == "0.0.0.0:9430", "wrong order")
}
