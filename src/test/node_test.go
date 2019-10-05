package test

import (
	"node"
	. "slogger"
	"testing"
	"time"
)

func TestInitNode(t *testing.T) {
	node1 := node.CreateNode("0.0.0.0", "9000")
	node2 := node.CreateNode("0.0.0.0", "9001")
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
	node1 := node.CreateNode("0.0.0.0", "9010")
	node2 := node.CreateNode("0.0.0.0", "9011")
	node3 := node.CreateNode("0.0.0.0", "9012")
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
	// node1.MbList.NicePrint()
	// node2.MbList.NicePrint()
	// node3.MbList.NicePrint()
}

func TestLeaveAndRejoin(t *testing.T) {
	node1 := node.CreateNode("0.0.0.0", "9020")
	node2 := node.CreateNode("0.0.0.0", "9021")
	node3 := node.CreateNode("0.0.0.0", "9022")
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
	// node1.MbList.NicePrint()
	// node2.MbList.NicePrint()
	// node3.MbList.NicePrint()
}

func TestHeartbeat(t *testing.T) {
	SLOG.Print("Staring TESTHEARTBEAT")
	node1 := node.CreateNode("0.0.0.0", "9030")
	node2 := node.CreateNode("0.0.0.0", "9031")
	node3 := node.CreateNode("0.0.0.0", "9032")
	node1.InitMemberList()
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	go node3.MonitorInputPacket()
	node2.Join(node1.IP + ":" + node1.Port)
	node3.Join(node1.IP + ":" + node1.Port)

	oldHeartBeat2 := node2.MbList.GetNode(node1.Id).Heartbeat_t
	oldHeartBeat3 := node3.MbList.GetNode(node1.Id).Heartbeat_t
	node1.SendHeartbeat()
	time.Sleep(500 * time.Millisecond)
	newHeartBeat2 := node2.MbList.GetNode(node1.Id).Heartbeat_t
	newHeartBeat3 := node3.MbList.GetNode(node1.Id).Heartbeat_t
	if oldHeartBeat2 >= newHeartBeat2 {
		t.Fatalf("wrong2 %d and %d", oldHeartBeat2, newHeartBeat2)
	}
	if oldHeartBeat3 >= newHeartBeat3 {
		t.Fatal("wrong3")
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
// func TestCheckFailure_new(t *testing.T) {
// 	SLOG.Print("Staring TestCheckFailure")
// 	node1 := node.CreateNode("0.0.0.0", "9040")
// 	node2 := node.CreateNode("0.0.0.0", "9041")
// 	node3 := node.CreateNode("0.0.0.0", "9042")
// 	node1.InitMemberList()
// 	go node1.MonitorInputPacket()
// 	go node2.MonitorInputPacket()
// 	go node3.MonitorInputPacket()
// 	node2.Join(node1.IP + ":" + node1.Port)
// 	node3.Join(node1.IP + ":" + node1.Port)
// 	time.Sleep(1 * time.Second)
// 	node1.SendHeartbeat()
// 	node2.SendHeartbeat()

// 	if node1.MbList.Size != 3 {
// 		t.Fatal("wrong4")
// 	}
// 	if node2.MbList.Size != 3 {
// 		t.Fatal("wrong4")
// 	}
// 	if node3.MbList.Size != 3 {
// 		t.Fatal("wrong4")
// 	}

// 	node1.SendHeartbeat()
// 	node2.SendHeartbeat()
// 	time.Sleep(1 * time.Second)
// 	node1.SendHeartbeat()
// 	node2.SendHeartbeat()
// 	time.Sleep(1 * time.Second)

// 	// node3 never sends heartbeat, at least 4 second after join.
// 	// so node1 and node2 should find the failure and broadcast
// 	if node1.MbList.Size != 2 {
// 		t.Fatalf("wrong, size is: %d", node1.MbList.Size)
// 	}
// 	if node2.MbList.Size != 2 {
// 		t.Fatal("wrong4")
// 	}
// }
func TestCheckFailure(t *testing.T) {
	SLOG.Print("Staring TestCheckFailure")
	node1 := node.CreateNode("0.0.0.0", "9040")
	node2 := node.CreateNode("0.0.0.0", "9041")
	node3 := node.CreateNode("0.0.0.0", "9042")
	node1.InitMemberList()
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	go node3.MonitorInputPacket()
	node2.Join(node1.IP + ":" + node1.Port)
	node3.Join(node1.IP + ":" + node1.Port)
	time.Sleep(3 * time.Second)

	node1.SendHeartbeat()
	node2.SendHeartbeat()
	time.Sleep(1 * time.Second)
	node1.CheckFailure()
	node2.CheckFailure()
	time.Sleep(1 * time.Second)

	if node1.MbList.Size != 2 {
		t.Fatal("wrong4")
	}
	if node2.MbList.Size != 2 {
		t.Fatal("wrong4")
	}
}

func TestFindIntroducer(t *testing.T) {
	SLOG.Print("Staring TEST introducer")
	node1 := node.CreateNode("0.0.0.0", "9050")
	node2 := node.CreateNode("0.0.0.0", "9051")
	node1.InitMemberList()
	go node1.MonitorInputPacket()
	go node2.MonitorInputPacket()
	introducer, success := node2.ScanIntroducer([]string{"0.0.0.0:9050"})
	if !success || introducer != "0.0.0.0:9050" {
		t.Fatal("wrong")
	}
	node3 := node.CreateNode("0.0.0.0", "9052")
	go node3.MonitorInputPacket()
	introducer, success = node3.ScanIntroducer([]string{"0.0.0.0:9055"})
	if success {
		t.Fatal("wrong")
	}
}

func TestPingSelf(t *testing.T) {
	node1 := node.CreateNode("0.0.0.0", "9060")
	node1.InitMemberList()
	go node1.MonitorInputPacket()
	_, success := node1.ScanIntroducer([]string{"0.0.0.0:9060"})
	if success {
		t.Fatal("should no find introducer")
	}
	node2 := node.CreateNode("0.0.0.0", "9061")
	go node2.MonitorInputPacket()
	_, success = node1.ScanIntroducer([]string{"0.0.0.0:9061"})
	if success {
		t.Fatal("should no find introducer")
	}
	node2.InitMemberList()
	_, success = node1.ScanIntroducer([]string{"0.0.0.0:9061"})
	if !success {
		t.Fatal("should no find introducer")
	}

}
