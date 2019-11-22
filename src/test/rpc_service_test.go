package test

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
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
	node0.InitMemberList()
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
	content := []byte("this is my file content")
	args := node.StoreFileArgs{master.Id, sdfsfilename, 1, content}
	node.PutFile("0.0.0.0:9321", &args, make(chan int, 4))
	var data []byte
	node.GetFile("0.0.0.0:9321", sdfsfilename, &data)
	assert(string(data) == string(content), "wrong1")
	os.Remove(master.Root_dir + "/" + sdfsfilename)
}

func getDcliClient(address string) *rpc.Client {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func TestGetFileFromClient(t *testing.T) {
	coordinator := node.CreateNode("0.0.0.0", "9510", "19510")
	master := node.CreateNode("0.0.0.0", "9511", "19511")
	coordinator.InitMemberList()
	go coordinator.MonitorInputPacket()
	go coordinator.StartRPCFileService()
	master.Join(coordinator.IP + ":" + coordinator.Port)
	go master.MonitorInputPacket()
	go master.StartRPCFileService()
	time.Sleep(50 * time.Millisecond)
	coorFsAddress := "0.0.0.0:19510"
	sdfsfilename := "testFilename"
	content := []byte("this is my file content")
	args := node.StoreFileArgs{coordinator.Id, sdfsfilename, 1, content}
	node.PutFile(coorFsAddress, &args, make(chan int, 4))
	client := getDcliClient(coorFsAddress)
	var res node.RPCResultType
	localpath := "/tmp/gotTestFile"
	err := client.Call(node.FileServiceName+coorFsAddress+".GetFileRequest", []string{sdfsfilename, localpath}, &res)
	assert(err == nil, "err")
	data, _ := ioutil.ReadFile(localpath)
	assert(string(data) == string(content), "wrong data")
	os.Remove(coordinator.Root_dir + "/" + sdfsfilename)
}

func TestPutFileFromClient(t *testing.T) {
	coordinator := node.CreateNode("0.0.0.0", "9500", "19500")
	coordinator.InitMemberList()
	go coordinator.MonitorInputPacket()
	go coordinator.StartRPCFileService()
	time.Sleep(50 * time.Millisecond)
	coorFsAddress := "0.0.0.0:19500"
	src := "/tmp/dummyrpcfile"
	random_token := rand.Int()
	content := fmt.Sprintf("this is my file content %d", random_token)
	writeDummyFile(src, content)
	defer deleteDummyFile(src)
	dest := "destfile"
	client := getDcliClient(coorFsAddress)
	var reply node.RPCResultType
	client.Call(node.FileServiceName+coorFsAddress+".PutFileRequest", node.PutFileArgs{src, dest, false}, &reply)
	data, _ := ioutil.ReadFile(coordinator.Root_dir + "/" + dest)
	assert(string(data) == content, "wrong")
}
