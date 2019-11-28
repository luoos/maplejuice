package test

import (
	"fmt"
	"log"
	"node"
	"testing"
	"time"
)

func TestAssignFiles(t *testing.T) {
	const NODES = 10
	var nodes [NODES]*node.Node
	for i := 0; i < NODES; i++ {
		nodes[i] = node.CreateNode("0.0.0.0", fmt.Sprintf("1100%d", i), "")
		nodes[i].DisableMonitorHB = true
	}
	nodes[0].InitMemberList()
	for _, nod := range nodes {
		go nod.MonitorInputPacket()
	}
	time.Sleep(1 * time.Second)
	for i := 1; i < NODES; i++ {
		nodes[i].Join(nodes[0].IP + ":" + nodes[0].Port)
	}
	for i, nod := range nodes {
		if nod.MbList.Size != NODES {
			t.Fatalf("wrong size for nod: %d size: %d", i, nod.MbList.Size)
		}
	}
	files := []string{"file1", "file2", "file3", "file4", "file5", "file6", "file7", "file8", "file9", "file10", "file11"}
	// for _, f := range files {
	// 	fmt.Print(getHashID(f), " ")
	// }
	// log.Printf("%+v", nodes[0].AssignFiles(files, 5))
}

func TestAddAndProcessMapleTask(t *testing.T) {
	master := node.CreateNode("0.0.0.0", "11100", "21100")
	master.InitMemberList()
	go master.MonitorInputPacket()
	mj := &node.MapleJuiceService{TaskQueue: make(chan *node.MapleJuiceTaskArgs, 10), SelfNode: master}
	go master.StartRPCMapleJuiceService(mj)
	time.Sleep((100 * time.Millisecond))
	worker := node.CreateNode("0.0.0.0", "11101", "21101")
	worker.Join(master.IP + ":" + master.Port)
	time.Sleep((100 * time.Millisecond))
	assert(master.Id < worker.Id, "assert wrong")
	log.Print(master.Id, worker.Id)
	var reply node.RPCResultType
	args := &node.MapleJuiceTaskArgs{node.MapleTask, "", 1, "", "", "", ""}
	_ = mj.ForwardMapleJuiceRequest(args, &reply)
	time.Sleep((100 * time.Millisecond))
	assert(len(mj.TaskQueue) == 0, "task not processed ")
}
