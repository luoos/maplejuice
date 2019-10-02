package main

import (
	"encoding/json"
	"log"
	m "memberlist"
	"net"
	"time"
    "os"
    "strings"
)

const MAX_NODES = 10
var MONITOR_INTERVAL = 2500 * time.Millisecond
var HEARTBEAT_INTERVAL = 1000 * time.Millisecond
const PORT = ":8080"
var INTRODUCER_LIST = []string{
	"fa19-cs425-g17-01.cs.illinois.edu",
	"fa19-cs425-g17-02.cs.illinois.edu",
	"fa19-cs425-g17-03.cs.illinois.edu",
	"fa19-cs425-g17-04.cs.illinois.edu",
	"fa19-cs425-g17-05.cs.illinois.edu",
	"fa19-cs425-g17-06.cs.illinois.edu",
	"fa19-cs425-g17-07.cs.illinois.edu",
	"fa19-cs425-g17-08.cs.illinois.edu",
	"fa19-cs425-g17-09.cs.illinois.edu",
	"fa19-cs425-g17-10.cs.illinois.edu",
}

type MemberInfo struct {
	Method string
	Id     int
	Ip     string
	MbList *m.MemberList
}
type Node struct {
	Id     int
	MbList *m.MemberList
}

func (n *Node) Introduce(conn net.PacketConn, clientAddr net.Addr) {
    clientIp := strings.Split(clientAddr.String(), ":")[0]
    // create mblist if it's the first node in the ring
    log.Println("start introducing " + clientIp)
    if n.MbList == nil {
        n.MbList = m.CreateMemberList(n.Id, MAX_NODES)
    }
	clientId := n.MbList.FindLeastFreeId()
	n.MbList.InsertNode(clientId, clientIp, PORT, n.GetTime())
    joinNodeMBList := n.MbList
    joinNodeMBList.SelfId = clientId
	mbListBuf, err := json.Marshal(joinNodeMBList)
	if err != nil {
		log.Fatal(err)
	}
    _, err = conn.WriteTo(mbListBuf, clientAddr)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("written to address: ", clientAddr.String())
	n.Broadcast("add", clientId, clientIp)
}

func (n *Node) Join() {
    log.Println("trying to join the ring")
	go n.Recv()
	joinRequest := &MemberInfo{
		Method: "join",
	}
	request, err := json.Marshal(joinRequest)
	if err != nil {
		log.Fatal()
	}
	var conn net.Conn
	for _, introIp := range INTRODUCER_LIST {
		conn, err = net.Dial("udp", introIp + PORT)
		if err != nil {
			continue
		} else {
			defer conn.Close()
			break
		}
	}
	// it will at least find it self as the introducer
	if err != nil {
		log.Fatal(err)
	}
	_, err = conn.Write(request)
    if err != nil {
        log.Fatal(err)
    }
	buf := make([]byte, 4096)
	i, err := conn.Read(buf)
	var mbList m.MemberList
	json.Unmarshal(buf[:i], &mbList)
    n.MbList = m.CreateMemberList(mbList.SelfId, MAX_NODES)
    n.Id = n.MbList.SelfId
    for _, node := range mbList.Member_map {
        n.MbList.InsertNode(node.Id, node.Ip, node.Port, n.GetTime())
        log.Printf("got id: %d, ip: %s, in mbList\n", node.Id, node.Ip)
    }
	go n.MonitorHeartbeat()
	go n.SendHeartbeat()
    log.Println("joined!")
}

func (n *Node) Leave() {
    log.Println("leaving the system")
	n.Broadcast("remove", n.Id, "")
    os.Exit(0)
}

func (n *Node) UDPSend(host_ip string, msg []byte) {
	conn, err := net.Dial("udp", host_ip + PORT)
	if err != nil {
		log.Println(err)
	}
	_, err = conn.Write(msg)
    if err != nil {
        log.Fatal(err)
    }
	conn.Close()
}

func (n *Node) SendHeartbeat() {
	hbInfo := &MemberInfo{
		Method: "heartbeat",
	}
	hb, err := json.Marshal(hbInfo)
	if err != nil {
		log.Fatal(err)
	}
    log.Println("Start sending heartbeat")
	for {
		for _, monitorNode := range n.MbList.GetNextKNodes(n.Id, 3) {
			go n.UDPSend(monitorNode.Ip, hb)
		}
		time.Sleep(HEARTBEAT_INTERVAL)
	}
}

func (n *Node) Broadcast(op string, target_id int, target_ip string) {
    log.Println("Broadcasting " + op + " " + target_ip)
	memberInfo := &MemberInfo{
		Method: op,
		Id:     target_id,
		Ip:     target_ip,
		// MbList: nil
	}
	data, err := json.Marshal(memberInfo)
	if err != nil {
		log.Fatal(err)
	}
	for _, member := range n.MbList.GetNextKNodes(n.Id, n.MbList.Size-1) {
		go n.UDPSend(member.Ip, data)
	}
}

func (n *Node) Recv() {
    log.Println("start receiving requests as UDP server")
	conn, err := net.ListenPacket("udp", PORT)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	for {
		buf := make([]byte, 4096)
		i, addr, err := conn.ReadFrom(buf)
		if err != nil {
			log.Println(err)
		}
		var memberInfo MemberInfo
		json.Unmarshal(buf[:i], &memberInfo)
		if memberInfo.Method == "join" {
            log.Printf("received join request from " + addr.String())
			n.Introduce(conn, addr)
		} else if memberInfo.Method == "add" {
            log.Printf("received add request from " + addr.String())
			go n.MbList.InsertNode(memberInfo.Id, memberInfo.Ip, "8080", n.GetTime())
		} else if memberInfo.Method == "remove" {
            log.Printf("received remove request " + addr.String())
			go n.MbList.DeleteNode(memberInfo.Id)
		} else if memberInfo.Method == "heartbeat" {
            log.Printf("received heartbeat from " + addr.String())
			go n.MbList.UpdateNodeHeartbeat(memberInfo.Id, n.GetTime())
		}
	}
}

func (n *Node) MonitorHeartbeat() {
	for {
		time.Sleep(MONITOR_INTERVAL)
		lostIds := n.MbList.GetTimeOutNodes(n.GetTime()-2500, n.Id, 3)
		for _, lostNode := range lostIds {
			lostId := lostNode.Id
            log.Printf("found failue node id: %d\n", lostId)
			go n.Broadcast("remove", lostId, "")
		}
	}
}

func (n *Node) GetTime() int {
	return int(time.Now().UnixNano() / 1000000)
}

func main() {
    log.Println("initialing")
	node := Node{}
	node.Join()
	for {}
}
