package main

import (
	"encoding/json"
	"log"
	m "memberlist"
	"net"
	"time"
    "os"
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
	method string
	id     int
	ip     string
	mbList m.MemberList
}
type Node struct {
	id     int
	mbList m.MemberList
}

func (n *Node) Introduce(clientIp string) []byte {
    // create mblist for first node
    log.Println("start introducing " + clientIp)
    if &(n.mbList) == nil {
        n.mbList = *(m.CreateMemberList(MAX_NODES))
    }
	clientId := n.mbList.FindLeastFreeId()
	n.mbList.InsertNode(clientId, clientIp, PORT, n.GetTime())
	n.Broadcast("add", clientId, clientIp)
	mbListBuf, err := json.Marshal(n.mbList)
	if err != nil {
		log.Fatal(err)
	}
	return mbListBuf
}

func (n *Node) Join() {
	go n.Recv()
	joinRequest := &MemberInfo{
		method: "join",
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
	conn.Write(request)
	buf := make([]byte, 4096)
	i, err := conn.Read(buf)
	var mbList m.MemberList
	json.Unmarshal(buf[:i], &mbList)
    n.mbList = *(m.CreateMemberList(MAX_NODES))
    for _, node := range mbList.Member_map {
        n.mbList.InsertNode(node.Id, node.Ip, node.Port, n.GetTime())
    }
	go n.MonitorHeartbeat()
	go n.SendHeartbeat()
}

func (n *Node) Leave() {
    log.Println("leaving the system")
	n.Broadcast("remove", n.id, "")
    os.Exit(0)
}

func (n *Node) UDPSend(host_ip string, msg []byte) {
	conn, err := net.Dial("udp", host_ip + PORT)
	if err != nil {
		log.Println(err)
	}
	conn.Write(msg)
	conn.Close()
}
func (n *Node) SendHeartbeat() {
	hbInfo := &MemberInfo{
		method: "heartbeat",
	}
	hb, err := json.Marshal(hbInfo)
	if err != nil {
		log.Fatal(err)
	}
    log.Println("Start sending heartbeat")
	for {
		for _, monitorNode := range n.mbList.GetNextKNodes(n.id, 3) {
			go n.UDPSend(monitorNode.Ip, hb)
		}
		time.Sleep(HEARTBEAT_INTERVAL)
	}
}

func (n *Node) Broadcast(op string, target_id int, target_ip string) {
    log.Println("Broadcasting " + op + " " + target_ip)
	memberInfo := &MemberInfo{
		method: op,
		id:     target_id,
		ip:     target_ip,
		// mbList: nil
	}
	data, err := json.Marshal(memberInfo)
	if err != nil {
		log.Fatal(err)
	}
	for _, member := range n.mbList.GetNextKNodes(n.id, n.mbList.Size-1) {
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
		if memberInfo.method == "join" {
            log.Printf("received join request from " + addr.String())
			mbListBuf := n.Introduce(memberInfo.ip)
			conn.WriteTo(mbListBuf, addr)
		} else if memberInfo.method == "add" {
            log.Printf("received add request from " + addr.String())
			go n.mbList.InsertNode(memberInfo.id, memberInfo.ip, "8080", n.GetTime())
		} else if memberInfo.method == "remove" {
            log.Printf("received remove request " + addr.String())
			go n.mbList.DeleteNode(memberInfo.id)
		} else if memberInfo.method == "heartbeat" {
            log.Printf("received heartbeat from " + addr.String())
			go n.mbList.UpdateNodeHeartbeat(memberInfo.id, n.GetTime())
		}
	}
}

func (n *Node) MonitorHeartbeat() {
	for {
		time.Sleep(MONITOR_INTERVAL)
		lostIds := n.mbList.GetTimeOutNodes(n.GetTime()-2500, n.id, 3)
		for _, lostNode := range lostIds {
			lostId := lostNode.Id
            log.Println("found failue node id: " + lostId)
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
