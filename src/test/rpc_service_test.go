package test

import (
	"io/ioutil"
	"log"
	"net/rpc"
	"node"
	"os"
	"testing"
	"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func writeDummyFile(filename, content string) {
	data := []byte(content)
	err := ioutil.WriteFile(filename, data, 0777)
	check(err)
}

func deleteDummyFile(filename string) {
	err := os.Remove(filename)
	check(err)
}

func TestRegisterFileService(t *testing.T) {
	node0 := node.CreateNode("0.0.0.0", "9200", "9300")
	go node0.StartRPCFileService()
	time.Sleep(50 * time.Millisecond)
	filename := "/tmp/dummyrpcfile"
	writeDummyFile(filename, "hello")
	defer deleteDummyFile(filename)
	var reply node.RPCResultType
	client, _ := rpc.Dial("tcp", "0.0.0.0:9300")
	err := client.Call(node.FileServiceName+"0.0.0.0:9300"+".PutFileRequest", node.PutFileArgs{filename, "dest", false}, &reply)
	if err != nil {
		log.Fatal(err)
	}
	if reply != node.RPC_SUCCESS {
		log.Fatal(reply)
	}
}

func TestGetTimeStampRPC(t *testing.T) {
	node0 := node.CreateNode("0.0.0.0", "9210", "9310")
	go node0.StartRPCFileService()
	time.Sleep(50 * time.Millisecond)
	sdfsfilename := "testFilename"
	c := make(chan node.Pair, 4)
	go node.CallGetTimeStamp("0.0.0.0:9310", sdfsfilename, c)
	pair := <-c
	assert(pair.Address == "0.0.0.0:9310" && pair.Ts == -1, "wrong timestamp1")
	localpath := "/app/fs/testFilename"
	timestamp := 100
	masterNodeID := 128
	node0.FileList.PutFileInfo(sdfsfilename, localpath, timestamp, masterNodeID)
	go node.CallGetTimeStamp("0.0.0.0:9310", sdfsfilename, c)
	pair = <-c
	assert(pair.Address == "0.0.0.0:9310" && pair.Ts == 100, "wrong timestamp2")
	node0.FileList.DeleteFileInfo(sdfsfilename)
	go node.CallGetTimeStamp("0.0.0.0:9310", sdfsfilename, c)
	pair = <-c
	assert(pair.Address == "0.0.0.0:9310" && pair.Ts == -1, "wrong timestamp3")
}

func TestPutAndGetFileRPC(t *testing.T) {
	coordinator := node.CreateNode("0.0.0.0", "9200", "9320")
	master := node.CreateNode("0.0.0.0", "9201", "9321")
	go coordinator.StartRPCFileService()
	go master.StartRPCFileService()
	time.Sleep(50 * time.Millisecond)
	sdfsfilename := "testFilename"
	content := "this is my file content"
	node.PutFile(master.Id, 1, "0.0.0.0:9321", sdfsfilename, content)
	assert(string(node.GetFile("0.0.0.0:9321", sdfsfilename)) == content, "wrong")
}

func TestLs(t *testing.T) {
	coordinator := node.CreateNode("0.0.0.0", "9400", "9401")
	node1 := node.CreateNode("0.0.0.0", "9410", "9411")
	node2 := node.CreateNode("0.0.0.0", "9420", "9421")
	node3 := node.CreateNode("0.0.0.0", "9430", "9431")
	coordinator.InitMemberList()
	go coordinator.MonitorInputPacket()
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	go node3.MonitorInputPacket()
	node1.Join(coordinator.IP + ":" + coordinator.Port)
	node2.Join(coordinator.IP + ":" + coordinator.Port)
	node3.Join(coordinator.IP + ":" + coordinator.Port)
	go coordinator.StartRPCFileService()
	time.Sleep(50 * time.Millisecond)
	address := "0.0.0.0:9401"
	sdfsfilename := "testFilename"
	addrs := node.CallLs(address, sdfsfilename)
	hashid := getHashID(sdfsfilename)
	assert(len(addrs) == 4, "wrong")
	// 8011 is the default port for FileService
	assert(hashid == 392 &&
		addrs[0] == "0.0.0.0:9401" &&
		addrs[1] == "0.0.0.0:9411" &&
		addrs[2] == "0.0.0.0:9421" &&
		addrs[3] == "0.0.0.0:9431", "wrong order")
}
