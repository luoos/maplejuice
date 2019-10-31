package test

import (
	"fmt"
	"hash/fnv"
	"log"
	"node"
	. "slogger"
	"testing"
	"time"
)

func getHashID(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32()) % 1024
}

func assert(condition bool, mesg string) {
	if !condition {
		log.Fatal(mesg)
	}
}
func TestInitNode(t *testing.T) {
	node1 := node.CreateNode("0.0.0.0", "9000", "19000")
	node2 := node.CreateNode("0.0.0.0", "9001", "19001")
	node1.InitMemberList()
	if node1.MbList.Size != 1 {
		t.Fatal("wrong")
	}
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	node2.Join(node1.IP + ":" + node1.Port)
	// node1.MbList.NicePrint()
	// node2.MbList.NicePrint()
	if node2.MbList.Size != 2 {
		t.Fatal("wrong")
	}
	if node1.MbList.Size != 2 {
		t.Fatal("wrong")
	}
}

func TestBroadCast(t *testing.T) {
	node1 := node.CreateNode("0.0.0.0", "9010", "19010")
	node2 := node.CreateNode("0.0.0.0", "9011", "19011")
	node3 := node.CreateNode("0.0.0.0", "9012", "19012")
	node1.InitMemberList()
	if node1.MbList.Size != 1 {
		t.Fatal("wrong1")
	}
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	go node3.MonitorInputPacket()
	node2.Join(node1.IP + ":" + node1.Port)
	time.Sleep(500 * time.Millisecond)
	if node1.MbList.Size != 2 {
		t.Fatal("wrong2")
	}
	if node2.MbList.Size != 2 {
		t.Fatal("wrong3")
	}

	node3.Join(node1.IP + ":" + node1.Port)

	if node2.MbList.Size != 3 {
		t.Fatal("wrong4")
	}
	if node3.MbList.Size != 3 {
		t.Fatal("wrong")
	}
	// node1.MbList.NicePrint()
	// node2.MbList.NicePrint()
	// node3.MbList.NicePrint()
}

func TestLeaveAndRejoin(t *testing.T) {
	node1 := node.CreateNode("0.0.0.0", "9020", "19020")
	node2 := node.CreateNode("0.0.0.0", "9021", "19020")
	node3 := node.CreateNode("0.0.0.0", "9022", "")
	node1.InitMemberList()
	if node1.MbList.Size != 1 {
		t.Fatal("wrong1")
	}
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	go node3.MonitorInputPacket()
	node2.Join(node1.IP + ":" + node1.Port)
	if node1.MbList.Size != 2 {
		t.Fatal("wrong2")
	}
	if node2.MbList.Size != 2 {
		t.Fatal("wrong3")
	}

	node3.Join(node1.IP + ":" + node1.Port)

	if node2.MbList.Size != 3 {
		t.Fatal("wrong4")
	}
	if node3.MbList.Size != 3 {
		t.Fatal("wrong")
	}

	node2.Leave()
	time.Sleep(1 * time.Second)

	if node1.MbList.Size != 2 {
		t.Fatal("wrong4")
	}
	if node2.MbList.Size != 2 {
		t.Fatal("wrong4")
	}
	if node3.MbList.Size != 2 {
		t.Fatal("wrong")
	}

	node2.Join(node1.IP + ":" + node1.Port)
	if node1.MbList.Size != 3 {
		t.Fatal("wrong4")
	}
	if node2.MbList.Size != 3 {
		t.Fatal("wrong4")
	}
	if node3.MbList.Size != 3 {
		t.Fatal("wrong")
	}
}

func TestHeartbeat(t *testing.T) {
	SLOG.Print("Staring TESTHEARTBEAT20")
	node1 := node.CreateNode("0.0.0.0", "9030", "")
	node2 := node.CreateNode("0.0.0.0", "9031", "")
	node3 := node.CreateNode("0.0.0.0", "9032", "")
	node1.InitMemberList()
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	go node3.MonitorInputPacket()
	node2.Join(node1.IP + ":" + node1.Port)
	node3.Join(node1.IP + ":" + node1.Port)

	oldHeartBeat2 := node2.MbList.GetNode(node1.Id).Heartbeat_t
	oldHeartBeat3 := node3.MbList.GetNode(node1.Id).Heartbeat_t
	node1.SendHeartbeat()
	time.Sleep(1 * time.Second)
	newHeartBeat2 := node2.MbList.GetNode(node1.Id).Heartbeat_t
	newHeartBeat3 := node3.MbList.GetNode(node1.Id).Heartbeat_t
	if oldHeartBeat2 >= newHeartBeat2 {
		t.Fatalf("wrong2 %d and %d", oldHeartBeat2, newHeartBeat2)
	}
	if oldHeartBeat3 >= newHeartBeat3 {
		t.Fatal("wrong5", oldHeartBeat3, newHeartBeat3)
	}

	oldHeartBeat1 := node1.MbList.GetNode(node2.Id).Heartbeat_t
	oldHeartBeat3 = node3.MbList.GetNode(node2.Id).Heartbeat_t
	node2.SendHeartbeat()
	time.Sleep(500 * time.Millisecond)
	newHeartBeat1 := node1.MbList.GetNode(node2.Id).Heartbeat_t
	newHeartBeat3 = node3.MbList.GetNode(node2.Id).Heartbeat_t
	if oldHeartBeat1 >= newHeartBeat1 {
		t.Fatalf("wrong1 %d and %d", oldHeartBeat1, newHeartBeat1)
	}
	if oldHeartBeat3 >= newHeartBeat3 {
		t.Fatal("wrong3")
	}

	oldHeartBeat1 = node1.MbList.GetNode(node3.Id).Heartbeat_t
	oldHeartBeat2 = node2.MbList.GetNode(node3.Id).Heartbeat_t
	node3.SendHeartbeat()
	time.Sleep(500 * time.Millisecond)
	newHeartBeat1 = node1.MbList.GetNode(node3.Id).Heartbeat_t
	newHeartBeat2 = node2.MbList.GetNode(node3.Id).Heartbeat_t
	if oldHeartBeat1 >= newHeartBeat1 {
		t.Fatal("wrong1")
	}
	if oldHeartBeat2 >= newHeartBeat2 {
		t.Fatal("wrong2")
	}
}

// *** this is for passive monitoring
func TestCheckFailure(t *testing.T) {
	SLOG.Print("Staring TestCheckFailure.")
	node1 := node.CreateNode("0.0.0.0", "9040", "")
	node2 := node.CreateNode("0.0.0.0", "9041", "")
	node3 := node.CreateNode("0.0.0.0", "9042", "")
	node1.InitMemberList()
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	go node3.MonitorInputPacket()
	node2.Join(node1.IP + ":" + node1.Port)
	node3.Join(node1.IP + ":" + node1.Port)
	node1.SendHeartbeat()
	node2.SendHeartbeat()

	if node1.MbList.Size != 3 {
		t.Fatal("wrong4")
	}
	if node2.MbList.Size != 3 {
		t.Fatal("wrong4")
	}
	if node3.MbList.Size != 3 {
		t.Fatal("wrong4")
	}
	time.Sleep(2 * time.Second)
	node1.SendHeartbeat()
	node2.SendHeartbeat()
	time.Sleep(2 * time.Second)
	node1.SendHeartbeat()
	node2.SendHeartbeat()
	time.Sleep(2 * time.Second)

	// node3 never sends heartbeat, at least 4 second after join.
	// so node1 and node2 should find the failure and broadcast
	if node1.MbList.Size != 2 {
		t.Fatalf("wrong, size is: %d", node1.MbList.Size)
	}
	if node2.MbList.Size != 2 {
		t.Fatal("wrong4")
	}
}

func TestFindIntroducer(t *testing.T) {
	SLOG.Print("Staring TEST introducer")
	node1 := node.CreateNode("0.0.0.0", "9050", "")
	node2 := node.CreateNode("0.0.0.0", "9051", "")
	node1.InitMemberList()
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	introducer, success := node2.ScanIntroducer([]string{"0.0.0.0:9050"})
	if !success || introducer != "0.0.0.0:9050" {
		t.Fatal("wrong")
	}
	node3 := node.CreateNode("0.0.0.0", "9052", "")
	go node3.MonitorInputPacket()
	introducer, success = node3.ScanIntroducer([]string{"0.0.0.0:9055"})
	if success {
		t.Fatal("wrong")
	}
	node2.Join("0.0.0.0:9050")
	time.Sleep(1 * time.Second)
	node4 := node.CreateNode("0.0.0.0", "9053", "")
	go node4.MonitorInputPacket()
	introducer, success = node4.ScanIntroducer([]string{"0.0.0.0:9050", "0.0.0.0:9051"})
	if !success {
		t.Fatal("wrong")
	}
	node4.Join(introducer)
	time.Sleep(1 * time.Second)
	if node4.MbList.Size != 3 {
		t.Fail()
	}
}

func TestPingSelf(t *testing.T) {
	node1 := node.CreateNode("0.0.0.0", "9060", "")
	node1.InitMemberList()
	go node1.MonitorInputPacket()
	_, success := node1.ScanIntroducer([]string{"0.0.0.0:9060"})
	if success {
		t.Fatal("should no find introducer")
	}
	node2 := node.CreateNode("0.0.0.0", "9061", "")
	go node2.MonitorInputPacket()
	_, success = node1.ScanIntroducer([]string{"0.0.0.0:9061"})
	if success {
		t.Fatal("should no find introducer")
	}
	node2.InitMemberList()
	_, success = node1.ScanIntroducer([]string{"0.0.0.0:9061"})
	if !success {
		t.Fatal("should find introducer")
	}

}

func TestManyNodes(t *testing.T) {
	SLOG.Print("starting test many nodes")
	const NODES = 10
	var nodes [NODES]*node.Node
	for i := 0; i < NODES; i++ {
		nodes[i] = node.CreateNode("0.0.0.0", fmt.Sprintf("907%d", i), "")
	}
	nodes[0].InitMemberList()
	for _, nod := range nodes {
		go nod.MonitorInputPacket()
	}
	time.Sleep(1 * time.Second)
	for i := 1; i < NODES; i++ {
		nodes[i].Join(nodes[0].IP + ":" + nodes[0].Port)
	}

	// nodes[0].MbList.NicePrint()
	for i, nod := range nodes {
		if nod.MbList.Size != NODES {
			t.Fatalf("wrong size for nod: %d size: %d", i, nod.MbList.Size)
		}
	}
	for j := 0; j < 5; j++ {
		for i := 0; i < 7; i++ {
			nodes[i].SendHeartbeat()
		}
		time.Sleep(1 * time.Second)
	}
	for i := 0; i < 7; i++ {
		n := nodes[i]
		if n.MbList.Size != 7 {
			t.Fatalf("wrong sizes: %d", n.MbList.Size)
		}
	}
	// for i := 1;
	// time.Sleep(3 * time.Second)
	// for _, nod := range nodes {
	// 	if nod.MbList.Size != 0 {
	// 		t.Fatalf("wrong size: %d", nod.MbList.Size)
	// 	}
	// }
}

func TestHashID(t *testing.T) {
	node1 := node.CreateNode("0.0.0.0", "9080", "")
	node2 := node.CreateNode("0.0.0.0", "9081", "")
	node1.InitMemberList()
	if node1.MbList.Size != 1 {
		t.Fatal("wrong")
	}
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	node2.Join(node1.IP + ":" + node1.Port)
	// node1.MbList.NicePrint()
	// node2.MbList.NicePrint()
	if node2.MbList.Size != 2 {
		t.Fatal("wrong ")
	}
	if node1.MbList.Size != 2 {
		t.Fatalf("wrong: %d", node1.MbList.Size)
	}

}

func TestGetMasterID(t *testing.T) {
	node1 := node.CreateNode("0.0.0.0", "9090", "")
	node2 := node.CreateNode("0.0.0.0", "9091", "")
	node3 := node.CreateNode("0.0.0.0", "9092", "")
	node1.InitMemberList()
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	go node3.MonitorInputPacket()
	node2.Join(node1.IP + ":" + node1.Port)
	node3.Join(node1.IP + ":" + node1.Port)
	// node1.MbList.NicePrint()
	hashID := getHashID("testname1")
	masterID := node1.GetMasterID("testname1")
	assert(hashID == 917 &&
		node1.Id == 625 &&
		node2.Id == 222 &&
		node3.Id == 843, "wrong setup")
	// log.Print(masterID)
	assert(masterID == 222, "wrong algorithm")
	// log.Println(masterID, hashID)
}

func TestGetKReplica(t *testing.T) {
	node1 := node.CreateNode("0.0.0.0", "9100", "")
	node2 := node.CreateNode("0.0.0.0", "9101", "")
	node3 := node.CreateNode("0.0.0.0", "9102", "")
	node1.InitMemberList()
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	go node3.MonitorInputPacket()
	node2.Join(node1.IP + ":" + node1.Port)
	node3.Join(node1.IP + ":" + node1.Port)
	node1.MbList.NicePrint()
	hashID := getHashID("testname1")
	log.Print(hashID)
	IDs := node1.GetFirstKReplicaNodeID("testname1", 3)
	assert(hashID == 917 &&
		node1.Id == 555 &&
		node2.Id == 152 &&
		node3.Id == 337, "wrong setup")
	log.Print(IDs)
	assert(IDs[0] == 152, "wrong algorithm")
	assert(IDs[1] == 337, "wrong algorithm")
	assert(IDs[2] == 555, "wrong algorithm")
}
