package test

import (
	"encoding/json"
	"math/rand"
	"node"
	"os"
	"testing"
	"time"
)

func TestCreateMemberList(t *testing.T) {
	mblist := node.CreateMemberList(3, 10)
	if mblist.Capacity != 10 {
		t.Fatalf("expected size to be %d, but got %d", 10, mblist.Capacity)
	}
	if mblist.Size != 0 {
		t.Fatalf("expect length of member_map to be %d", 0)
	}
	if mblist.SelfId != 3 {
		t.Fatalf("wrong SelfId expect %d, got %d", 3, mblist.SelfId)
	}
}

func TestInsertAndDeleteNodesIntoMemberList(t *testing.T) {
	mbList := node.CreateMemberList(0, 10)
	mbList.InsertNode(0, "0.0.0.0", "90", "", 1, "")
	if mbList.Size != 1 {
		t.Fatalf("length not match")
	}
	mbList.InsertNode(1, "0.0.0.0", "90", "", 1, "")
	if mbList.Size != 2 {
		t.Fatalf("length not match")
	}
	mbList.DeleteNode(1)
	if mbList.Size != 1 {
		t.Fatalf("length not match")
	}
	mbList.DeleteNode(0)
	if mbList.Size != 0 {
		t.Fatalf("length not match")
	}
}

func TestFindFreeId(t *testing.T) {
	capacity := 10
	mbList := node.CreateMemberList(0, capacity)
	id := mbList.FindLeastFreeId()
	if id != 0 {
		t.Fatalf("incorrect least free id: %d", id)
	}
	mbList.InsertNode(0, "0.0.0.0", "90", "", 1, "")
	id = mbList.FindLeastFreeId()
	if id != 1 {
		t.Fatalf("incorrect least free id: %d", id)
	}
	mbList.DeleteNode(0)
	id = mbList.FindLeastFreeId()
	if id != 0 {
		t.Fatalf("incorrect least free id: %d", id)
	}
	for i := 0; i < capacity; i++ {
		mbList.InsertNode(i, "0.0.0.0", "90", "", 1, "")
	}
	id = mbList.FindLeastFreeId()
	if id != -1 {
		t.Fatalf("incorrect least free id: %d", id)
	}
	mbList.DeleteNode(5)
	id = mbList.FindLeastFreeId()
	if id != 5 {
		t.Fatalf("incorrect least free id: %d", id)
	}
}

func TestGetNextKNodes(t *testing.T) {
	mbList := node.CreateMemberList(0, 10)
	mbList.InsertNode(0, "0.0.0.0", "90", "", 1, "")
	mbList.InsertNode(2, "0.0.0.0", "90", "", 1, "")
	mbList.InsertNode(4, "0.0.0.0", "90", "", 1, "")
	mbList.InsertNode(5, "0.0.0.0", "90", "", 1, "")
	arr := mbList.GetNextKNodes(0, 3)
	if len(arr) != 3 {
		t.Fatalf("length not match, expect %d, got %d", 3, len(arr))
	}
	if arr[0].Id != 2 {
		t.Fatal("incorrect next node")
	}
	if arr[1].Id != 4 {
		t.Fatal("incorrect next node")
	}
	if arr[2].Id != 5 {
		t.Fatal("incorrect next node")
	}
	mbList.DeleteNode(4)
	arr = mbList.GetNextKNodes(0, 3)
	if len(arr) != 2 {
		t.Fatal("length not match")
	}
	if arr[0].Id != 2 {
		t.Fatal("incorrect next node")
	}
	if arr[1].Id != 5 {
		t.Fatal("incorrect next node")
	}
	if arr[1].GetNextNode().Id != 0 {
		t.Fatal("incorrect next node")
	}
}

func TestGetPrevKNodes(t *testing.T) {
	mbList := node.CreateMemberList(0, 10)
	mbList.InsertNode(0, "192.169.163.111", "90", "", 1, "")
	mbList.InsertNode(2, "0.0.0.0", "90", "", 1, "")
	mbList.InsertNode(4, "0.0.0.0", "90", "", 1, "")
	mbList.InsertNode(5, "0.0.0.0", "90", "", 1, "")
	arr := mbList.GetPrevKNodes(5, 3)
	if len(arr) != 3 {
		t.Fatalf("length not match, expect %d, got %d", 3, len(arr))
	}
	if arr[0].Id != 4 {
		t.Fatalf("incorrect next node, got: %d", arr[0].Id)
	}
	if arr[1].Id != 2 {
		t.Fatal("incorrect next node")
	}
	if arr[2].Id != 0 {
		t.Fatal("incorrect next node")
	}
	mbList.DeleteNode(4)
	arr = mbList.GetPrevKNodes(5, 3)
	if len(arr) != 2 {
		t.Fatal("length not match")
	}
	if arr[0].Id != 2 {
		t.Fatal("incorrect next node")
	}
	if arr[1].Id != 0 {
		t.Fatal("incorrect next node")
	}
	if arr[1].GetPrevNode().Id != 5 {
		t.Fatal("incorrect next node")
	}
}

func TestNodePointer(t *testing.T) {
	mbList := node.CreateMemberList(4, 10)
	mbList.InsertNode(0, "0.0.0.0", "90", "", 1, "")
	mbList.InsertNode(2, "0.0.0.0", "90", "", 1, "")
	node0 := mbList.GetNode(0)
	if node0.GetNextNode().Id != 2 {
		t.Fatal("wrong next node")
	}
}

func TestUpdateNodeHeartbeat(t *testing.T) {
	mbList := node.CreateMemberList(0, 10)
	mbList.InsertNode(0, "0.0.0.0", "90", "", 1, "")
	node := mbList.GetNode(0)
	if node.Heartbeat_t != 1 {
		t.Fatal("wrong heartbeat")
	}
	mbList.UpdateNodeHeartbeat(0, 20)
	node = mbList.GetNode(0)
	if node.Heartbeat_t != 20 {
		t.Fatal("wrong hearbeat")
	}
}

// *** this is for passive monitoring
func TestNodeTimeOut(t *testing.T) {
	mbList := node.CreateMemberList(0, 10)
	mbList.InsertNode(0, "0.0.0.0", "90", "", 1, "")
	if mbList.NodeTimeOut(0, 0) {
		t.Fatalf("wrong heartbeat_t: %d, deadline: %d", mbList.GetNode(0).Heartbeat_t, 0)
	}
	if !mbList.NodeTimeOut(2, 0) {
		t.Fatal("wrong")
	}
}

func TestGetTimeOutNodes(t *testing.T) {
	mbList := node.CreateMemberList(0, 10)
	mbList.InsertNode(0, "0.0.0.0", "90", "", 1, "")
	mbList.InsertNode(1, "0.0.0.0", "90", "", 100, "")
	mbList.InsertNode(2, "0.0.0.0", "90", "", 100, "")
	mbList.InsertNode(3, "0.0.0.0", "90", "", 1, "")
	mbList.InsertNode(4, "0.0.0.0", "90", "", 100, "")
	timeOutNodes := mbList.GetTimeOutNodes(50, 4, 3)
	if len(timeOutNodes) != 1 && timeOutNodes[0].Id == 3 {
		t.Fatal("length not match")
	}
	timeOutNodes = mbList.GetTimeOutNodes(0, 0, 3)
	if timeOutNodes != nil {
		t.Fatal("not nil")
	}
}

func TestToJson(t *testing.T) {
	mbList := node.CreateMemberList(0, 10)
	mbList.InsertNode(0, "1.0.0.0", "91", "", 1, "")
	mbList.InsertNode(1, "0.2.0.0", "92", "", 100, "")
	mbList.InsertNode(2, "0.0.3.0", "93", "", 100, "")
	mbList.InsertNode(3, "0.0.0.4", "94", "", 1, "")
	mbList.InsertNode(4, "0.0.0.5", "95", "", 100, "")
	jsonData := mbList.ToJson()
	var copy_mbList node.MemberList
	err := json.Unmarshal(jsonData, &copy_mbList)
	if err != nil {
		t.Fatal("unmarshal error")
	}
	if !(copy_mbList.Capacity == mbList.Capacity) {
		t.Fatal("not match")
	}
	if !(copy_mbList.Size == mbList.Size) {
		t.Fatal("not match")
	}
	if !(copy_mbList.Member_map[2].Heartbeat_t ==
		mbList.Member_map[2].Heartbeat_t) {
		t.Fatal("not match")
	}
}

func TestDumpToTmpFile(t *testing.T) {
	mbList := node.CreateMemberList(0, 10)
	mbList.InsertNode(0, "192.169.163.111", "91", "", 1, "")
	// use rand to force to really run this case, otherwise maybe cached
	mbList.InsertNode(1, "0.2.0.0", "92", "", rand.Intn(100), "")
	mbList.InsertNode(2, "0.0.3.0", "93", "", 100, "")
	mbList.InsertNode(3, "0.0.0.4", "94", "", 1, "")
	mbList.InsertNode(4, "0.0.0.7", "95", "", int(time.Now().UnixNano()/1000000), "")
	mbList.DumpToTmpFile()
	_, err := os.Stat(node.MEMBER_LIST_FILE)
	if os.IsNotExist(err) {
		t.Fatal(err)
	}
	// err = os.Remove(node.MEMBER_LIST_FILE)
	// if err != nil {
	// 	t.Fatal(err)
	// }
}

func TestGetAddressesForNextKNodes(t *testing.T) {
	mbList := node.CreateMemberList(0, 10)
	mbList.InsertNode(0, "192.169.163.111", "91", "", 1, "")
	mbList.InsertNode(1, "0.2.0.0", "92", "1", rand.Intn(100), "")
	mbList.InsertNode(2, "0.0.3.0", "93", "1", 100, "")
	mbList.InsertNode(3, "0.0.0.4", "94", "1", 1, "")
	mbList.InsertNode(4, "0.0.0.7", "95", "1", int(time.Now().UnixNano()/1000000), "")
	addresses := mbList.GetRPCAddressesForNextKNodes(0, 3)
	assert(len(addresses) == 3, "wrong len")
	assert(addresses[0] == "0.2.0.0:1" &&
		addresses[2] == "0.0.0.4:1", "wrong address")
}

func TestGetSmallestNode(t *testing.T) {
	mbList := node.CreateMemberList(3, 10)
	sn := mbList.GetSmallestNode()
	assert(sn == nil, "wrong node1")
	mbList.InsertNode(3, "0.0.0.4", "94", "1", 1, "")
	sn = mbList.GetSmallestNode()
	assert(sn.Id == 3, "wrong node2")
	mbList.InsertNode(0, "192.169.163.111", "91", "", 1, "")
	sn = mbList.GetSmallestNode()
	assert(sn.Id == 0, "wrong node3")
	mbList.DeleteNode(0)
	sn = mbList.GetSmallestNode()
	assert(sn.Id == 3, "wrong node4")
}
