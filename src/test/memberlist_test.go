package test1

import (
	"memberlist"
	"testing"
)

func TestCreateMemberList(t *testing.T) {
	mblist := memberlist.CreateMemberList(10)
	if mblist.Size != 10 {
		t.Fatalf("expected size to be %d, but got %d", 10, mblist.Size)
	}
	if len(mblist.Member_map) != 0 {
		t.Fatalf("expect length of Member_map to be %d", 0)
	}
}

func TestInsertAndDeleteNodesIntoMemberList(t *testing.T) {
	mbList := memberlist.CreateMemberList(10)
	mbList.InsertNode(0, "0.0.0.0", "90", 1)
	if len(mbList.Member_map) != 1 {
		t.Fatalf("length not match")
	}
	mbList.InsertNode(1, "0.0.0.0", "90", 1)
	if len(mbList.Member_map) != 2 {
		t.Fatalf("length not match")
	}
	mbList.DeleteNode(1)
	if len(mbList.Member_map) != 1 {
		t.Fatalf("length not match")
	}
	mbList.DeleteNode(0)
	if len(mbList.Member_map) != 0 {
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
	if arr[1].Next.Id != 0 {
		t.Fatal("incorrect next node")
	}
}

func TestNodePointer(t *testing.T) {
	size := 10
	mbList := memberlist.CreateMemberList(size)
	mbList.InsertNode(0, "0.0.0.0", "90", 1)
	mbList.InsertNode(2, "0.0.0.0", "90", 1)
	node0 := mbList.Member_map[0]
	if node0.Next.Id != 2 {
		t.Fatal("wrong next node")
	}
}
