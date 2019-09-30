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
