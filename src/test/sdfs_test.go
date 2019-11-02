package test

import (
	"node"
	"os"
	"testing"
	"time"
)

func TestGetIPsWithIds(t *testing.T) {
	node0 := node.CreateNode("0.0.0.0", "9500", "")
	node0.InitMemberList()
	node0.MbList.InsertNode(3, "0.0.0.3", "3", "1333", 3, "")
	node0.MbList.InsertNode(5, "0.0.0.5", "5", "1444", 3, "")
	node0.MbList.InsertNode(7, "0.0.0.7", "7", "1555", 3, "")
	ip_list := node0.GetAddressesWithIds([]int{3, 5, 7})
	assert(ip_list[0] == "0.0.0.3:1333", "wrong ip")
	assert(ip_list[1] == "0.0.0.5:1444", "wrong ip")
	assert(ip_list[2] == "0.0.0.7:1555", "wrong ip")
}

func TestSetFileDir(t *testing.T) {
	node0 := node.CreateNode("0.0.0.0", "9510", "9511")
	test_dir := "/tmp/nodetestfiledir"
	os.Remove(test_dir)
	_, err := os.Stat(test_dir)
	assert(os.IsNotExist(err), "should not exist")
	node0.SetFileDir(test_dir)
	_, err = os.Stat(test_dir)
	assert(err == nil, "should exist")
	os.Remove(test_dir)
	_, err = os.Stat(test_dir)
	assert(os.IsNotExist(err), "should not exist")
}

func TestSendFileIfNecessary(t *testing.T) {
	node0 := node.CreateNode("0.0.0.0", "9520", "9521")
	node0.SetFileDir("/tmp/node0")
	node1 := node.CreateNode("0.0.0.0", "9530", "9531")
	node1.SetFileDir("/tmp/node1")
	node2 := node.CreateNode("0.0.0.0", "9540", "9541")
	node2.SetFileDir("/tmp/node2")
	node0.InitMemberList()
	go node0.MonitorInputPacket()
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	go node0.StartRPCFileService()
	go node1.StartRPCFileService()
	go node2.StartRPCFileService()
	node1.Join(node0.IP + ":" + node0.Port)
	node2.Join(node0.IP + ":" + node0.Port)
	time.Sleep(50 * time.Millisecond)
	assert(node0.MbList.Size == 3, "wrong len")
	node0.FileList.StoreFile("sdfs1", "/tmp/node0", 87, node0.Id, []byte("hello"))
	fileInfo := node0.FileList.GetFileInfo("sdfs1")
	node0.SendFileIfNecessary(*fileInfo,
		[]string{node1.IP + ":" + node1.RPC_Port, node2.IP + ":" + node2.RPC_Port})
	time.Sleep(50 * time.Millisecond)
	fileInfo_get1 := node1.FileList.GetFileInfo("sdfs1")
	assert(fileInfo_get1 != nil, "should exist")
	fileInfo_get2 := node2.FileList.GetFileInfo("sdfs1")
	assert(fileInfo_get2 != nil, "should exist")
	_, err := os.Stat("/tmp/node1/sdfs1")
	assert(err == nil, "file should exist")
	_, err = os.Stat("/tmp/node2/sdfs1")
	assert(err == nil, "file should exist")
	os.Remove("/tmp/node0/sdfs1")
	os.Remove("/tmp/node1/sdfs1")
	os.Remove("/tmp/node2/sdfs1")
}

func TestDuplicateReplica(t *testing.T) {
	node0 := node.CreateNode("0.0.0.0", "9580", "9581") // Id 815
	node0.SetFileDir("/tmp/node0")
	node0.InitMemberList()
	node1 := node.CreateNode("0.0.0.0", "9570", "9571") // Id 823
	node1.SetFileDir("/tmp/node1")
	node2 := node.CreateNode("0.0.0.0", "9560", "9561") // Id 833
	node2.SetFileDir("/tmp/node2")
	node3 := node.CreateNode("0.0.0.0", "9550", "9551") // Id 842
	node3.SetFileDir("/tmp/node3")
	node4 := node.CreateNode("0.0.0.0", "9590", "9591") // Id 422
	node4.SetFileDir("/tmp/node4")
	go node0.MonitorInputPacket()
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	go node3.MonitorInputPacket()
	go node4.MonitorInputPacket()
	go node0.StartRPCFileService()
	go node1.StartRPCFileService()
	go node2.StartRPCFileService()
	go node3.StartRPCFileService()
	go node4.StartRPCFileService()
	node1.Join(node0.IP + ":" + node0.Port)
	node2.Join(node0.IP + ":" + node0.Port)
	node3.Join(node0.IP + ":" + node0.Port)
	node4.Join(node0.IP + ":" + node0.Port)
	node0.FileList.StoreFile("sdfs1", "/tmp/node0", 87, node0.Id, []byte("hello replica"))
	time.Sleep(50 * time.Millisecond)
	assert(node0.MbList.Size == 5, "wrong size0")
	assert(node4.MbList.Size == 5, "wrong size4")
	node0.DuplicateReplica()
	time.Sleep(50 * time.Millisecond)
	assert(node1.FileList.GetFileInfo("sdfs1") != nil, "should exist")
	assert(node3.FileList.GetFileInfo("sdfs1") != nil, "should exist")
	assert(node4.FileList.GetFileInfo("sdfs1") == nil, "should not exist")
}
