package test

import (
	"node"
	"testing"
	"time"
)

func TestInitNode(t *testing.T) {
	node1 := node.CreateNode("0.0.0.0", "9085")
	node2 := node.CreateNode("0.0.0.0", "9086")
	node1.InitMemberList()
	if node1.MbList.Size != 1 {
		t.Fatal("wrong")
	}
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	node2.Join(node1.IP + ":" + node1.Port)
	time.Sleep(1 * time.Second)
	node1.MbList.NicePrint()
	node2.MbList.NicePrint()
	if node2.MbList.Size != 2 {
		t.Fatal("wrong")
	}
	if node1.MbList.Size != 2 {
		t.Fatal("wrong")
	}
}

func TestBroadCast(t *testing.T) {
	node1 := node.CreateNode("0.0.0.0", "9080")
	node2 := node.CreateNode("0.0.0.0", "9081")
	node3 := node.CreateNode("0.0.0.0", "9082")
	node1.InitMemberList()
	if node1.MbList.Size != 1 {
		t.Fatal("wrong1")
	}
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	go node3.MonitorInputPacket()
	node2.Join(node1.IP + ":" + node1.Port)
	time.Sleep(1 * time.Second)
	if node1.MbList.Size != 2 {
		t.Fatal("wrong2")
	}
	if node2.MbList.Size != 2 {
		t.Fatal("wrong3")
	}

	node3.Join(node1.IP + ":" + node1.Port)
	time.Sleep(1 * time.Second)

	if node2.MbList.Size != 3 {
		t.Fatal("wrong4")
	}
	if node3.MbList.Size != 3 {
		t.Fatal("wrong")
	}
	node1.MbList.NicePrint()
	node2.MbList.NicePrint()
	node3.MbList.NicePrint()
}
