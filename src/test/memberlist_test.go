package test1

import (
	"encoding/json"
	"memberlist"
	"testing"
)

func TestCreateMemberList(t *testing.T) {
	mblist := memberlist.CreateMemberList(10)
	if mblist.Capacity != 10 {
		t.Fatalf("expected size to be %d, but got %d", 10, mblist.Capacity)
	}
	if mblist.Size != 0 {
		t.Fatalf("expect length of member_map to be %d", 0)
	}
}

func TestInsertAndDeleteNodesIntoMemberList(t *testing.T) {
	mbList := memberlist.CreateMemberList(10)
	mbList.InsertNode(0, "0.0.0.0", "90", 1)
	if mbList.Size != 1 {
		t.Fatalf("length not match")
	}
	mbList.InsertNode(1, "0.0.0.0", "90", 1)
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
	size := 10
	mbList := memberlist.CreateMemberList(size)
	id := mbList.FindLeastFreeId()
	if id != 0 {
		t.Fatalf("incorrect least free id: %d", id)
	}
	mbList.InsertNode(0, "0.0.0.0", "90", 1)
	id = mbList.FindLeastFreeId()
	if id != 1 {
		t.Fatalf("incorrect least free id: %d", id)
	}
	mbList.DeleteNode(0)
	id = mbList.FindLeastFreeId()
	if id != 0 {
		t.Fatalf("incorrect least free id: %d", id)
	}
	for i := 0; i < size; i++ {
		mbList.InsertNode(i, "0.0.0.0", "90", 1)
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
	size := 10
	mbList := memberlist.CreateMemberList(size)
	mbList.InsertNode(0, "0.0.0.0", "90", 1)
	mbList.InsertNode(2, "0.0.0.0", "90", 1)
	mbList.InsertNode(4, "0.0.0.0", "90", 1)
	mbList.InsertNode(5, "0.0.0.0", "90", 1)
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
	size := 10
	mbList := memberlist.CreateMemberList(size)
	mbList.InsertNode(0, "0.0.0.0", "90", 1)
	mbList.InsertNode(2, "0.0.0.0", "90", 1)
	mbList.InsertNode(4, "0.0.0.0", "90", 1)
	mbList.InsertNode(5, "0.0.0.0", "90", 1)
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
	size := 10
	mbList := memberlist.CreateMemberList(size)
	mbList.InsertNode(0, "0.0.0.0", "90", 1)
	mbList.InsertNode(2, "0.0.0.0", "90", 1)
	node0 := mbList.GetNode(0)
	if node0.GetNextNode().Id != 2 {
		t.Fatal("wrong next node")
	}
}

func TestUpdateNodeHeartbeat(t *testing.T) {
	mbList := memberlist.CreateMemberList(10)
	mbList.InsertNode(0, "0.0.0.0", "90", 1)
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

func TestGetTimeOutNodes(t *testing.T) {
	mbList := memberlist.CreateMemberList(10)
	mbList.InsertNode(0, "0.0.0.0", "90", 1)
	mbList.InsertNode(1, "0.0.0.0", "90", 100)
	mbList.InsertNode(2, "0.0.0.0", "90", 100)
	mbList.InsertNode(3, "0.0.0.0", "90", 1)
	mbList.InsertNode(4, "0.0.0.0", "90", 100)
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
	mbList := memberlist.CreateMemberList(10)
	mbList.InsertNode(0, "1.0.0.0", "91", 1)
	mbList.InsertNode(1, "0.2.0.0", "92", 100)
	mbList.InsertNode(2, "0.0.3.0", "93", 100)
	mbList.InsertNode(3, "0.0.0.4", "94", 1)
	mbList.InsertNode(4, "0.0.0.5", "95", 100)
	jsonData := mbList.ToJson()
	var copy_mbList memberlist.MemberList
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
