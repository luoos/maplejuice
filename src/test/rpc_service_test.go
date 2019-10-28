package test

import (
	"log"
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
	ts := node.CallGetTimeStamp("0.0.0.0:9310", sdfsfilename)
	assert(ts == -1, "wrong timestamp1")
	localpath := "/app/fs/testFilename"
	timestamp := 100
	masterNodeID := 128
	node0.FileList.PutFileInfo(sdfsfilename, localpath, timestamp, masterNodeID)
	ts = node.CallGetTimeStamp("0.0.0.0:9310", sdfsfilename)
	assert(ts == 100, "wrong timestamp2")
	node0.FileList.DeleteFileInfo(sdfsfilename)
	ts = node.CallGetTimeStamp("0.0.0.0:9310", sdfsfilename)
	assert(ts == -1, "wrong timestamp3")
}

func TestPutAndGetFilePRC(t *testing.T) {
	coordinator := node.CreateNode("0.0.0.0", "9200")
	master := node.CreateNode("0.0.0.0", "9201")
	go coordinator.StartRPCFileService("9300")
	go master.StartRPCFileService("9301")
	time.Sleep(50 * time.Millisecond)
	sdfsfilename := "testFilename"
	content := "this is my file content"
	node.PutFile(master.Id, 1, "0.0.0.0:9301", "/apps/files", sdfsfilename, content)
	log.Print(node.GetFile("0.0.0.0:9301", sdfsfilename))
}
