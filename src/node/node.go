package node

import (
	"encoding/json"
	"log"
	"net"
	. "slogger"
	"time"
)

type Node struct {
	Id       int
	IP, Port string
	MbList   *MemberList
}

type Packet struct {
	Action ActionType
	Id     int
	IP     string
	Port   string
	Map    *MemberList
}

type ActionType int32

const (
	ACTION_JOIN        ActionType = 1 << 0
	ACTION_REPLY_JOIN  ActionType = 1 << 1
	ACTION_NEW_NODE    ActionType = 1 << 2
	ACTION_DELETE_NODE ActionType = 1 << 3
	ACTION_HEARTBEAT   ActionType = 1 << 4
	ACTION_PING        ActionType = 1 << 5
	ACTION_ACK         ActionType = 1 << 6

	NUM_MONITORS            int = 3
	DEADLINE_IN_MILLISECOND     = 2500
	MONITOR_INTERVAL            = DEADLINE_IN_MILLISECOND * time.Millisecond
	HEARTBEAT_INTERVAL          = 1000 * time.Millisecond
)

var ACK_INTRO = make(chan string)
var ACK_JOIN = make(chan bool)

func CreateNode(ip, port string) *Node {
	node := &Node{IP: ip, Port: port}
	return node
}

func (node *Node) InitMemberList() {
	node.MbList = CreateMemberList(0, MAX_CAPACITY)
	node.MbList.InsertNode(0, node.IP, node.Port, 100)
}

func (node *Node) ScanIntroducer(addresses []string) (string, bool) {
	pingPacket := &Packet{
		Action: ACTION_PING,
		IP:     node.IP,
		Port:   node.Port,
	}
	for _, introAddr := range addresses {
		sendPacketUDP(introAddr, pingPacket)
	}
	select {
	case res := <-ACK_INTRO:
		return res, true
	case <-time.After(time.Second):
		return "", false
	}
}

func (node *Node) Join(address string) bool {
	packet := &Packet{
		Action: ACTION_JOIN,
		IP:     node.IP,
		Port:   node.Port,
	}
	err := sendPacketUDP(address, packet)
	if err != nil {
		log.Panic(err)
	}
	select {
	case <-ACK_JOIN:
		return true
	case <-time.After(time.Second):
		return false
	}
}

func (node *Node) Leave() {
	deleteNodePacket := &Packet{
		Action: ACTION_DELETE_NODE,
		Id:     node.Id,
	}
	node.MbList.DeleteNode(node.Id)
	node.Broadcast(deleteNodePacket)
}

func (node *Node) SendHeartbeat() {
	heartbeatPacket := &Packet{
		Action: ACTION_HEARTBEAT,
		Id:     node.Id,
	}
	for _, monitorNode := range node.MbList.GetNextKNodes(node.Id, NUM_MONITORS) {
		address := monitorNode.Ip + ":" + monitorNode.Port
		sendPacketUDP(address, heartbeatPacket)
	}
}

func (node *Node) SendHeartbeatRoutine() {
	for {
		node.SendHeartbeat()
		time.Sleep(HEARTBEAT_INTERVAL)
	}
}

func (node *Node) CheckFailure() {
	lostNodes := node.MbList.GetTimeOutNodes(getMillisecond()-DEADLINE_IN_MILLISECOND, node.Id, NUM_MONITORS)
	for _, lostNode := range lostNodes {
		lostId := lostNode.Id
		SLOG.Printf("[Node %d] found failure node id: %d\n", node.Id, lostId)
		deleteNodePacket := &Packet{
			Action: ACTION_DELETE_NODE,
			Id:     lostId,
		}
		node.MbList.DeleteNode(lostId)
		node.Broadcast(deleteNodePacket)
	}
}

func (node *Node) CheckFailureRoutine() {
	for {
		time.Sleep(MONITOR_INTERVAL)
		node.CheckFailure()
	}
}

func sendPacketUDP(address string, packet *Packet) error {
	data, err := json.Marshal(packet)
	if err != nil {
		log.Println(err)
		return err
	}
	var conn net.Conn
	conn, err = net.Dial("udp", address)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.Write(data)
	return nil
}

func (node *Node) Broadcast(packet *Packet) {
	for id, member := range node.MbList.Member_map {
		if id == node.Id {
			continue
		}
		address := member.Ip + ":" + member.Port
		sendPacketUDP(address, packet)
	}
}

func (node *Node) handlePacket(packet Packet) {
	switch packet.Action {
	case ACTION_NEW_NODE:
		SLOG.Printf("[Node %d] Received ACTION_NEW_NODE (%d, %s:%s)", node.Id, packet.Id, packet.IP, packet.Port)
		node.MbList.InsertNode(packet.Id, packet.IP, packet.Port, getMillisecond())
	case ACTION_DELETE_NODE:
		SLOG.Printf("[Node %d] Received ACTION_DELETE_NODE (%d)", node.Id, packet.Id)
		node.MbList.DeleteNode(packet.Id)
	case ACTION_JOIN:
		reply_address := packet.IP + ":" + packet.Port
		freeId := node.MbList.FindLeastFreeId()
		SLOG.Printf("[Node %d] Received ACTION_JOIN from %s:%s, assign id: %d", node.Id, packet.IP, packet.Port, freeId)
		sendMemberListPacket := &Packet{
			Action: ACTION_REPLY_JOIN,
			Id:     freeId,
			Map:    node.MbList,
		}
		err := sendPacketUDP(reply_address, sendMemberListPacket)
		if err != nil {
			log.Println(err)
		}
		newNodePacket := &Packet{
			Action: ACTION_NEW_NODE,
			Id:     freeId,
			IP:     packet.IP,
			Port:   packet.Port,
		}
		node.Broadcast(newNodePacket)
		node.MbList.InsertNode(freeId, packet.IP, packet.Port, getMillisecond())
	case ACTION_REPLY_JOIN:
		node.MbList = CreateMemberList(packet.Id, MAX_CAPACITY)
		node.Id = packet.Id
		SLOG.Printf("[Node %d] Received ACTION_REPLY_JOIN assigned, member cnt: %d", node.Id, len(packet.Map.Member_map))
		for _, item := range packet.Map.Member_map {
			node.MbList.InsertNode(item.Id, item.Ip, item.Port, getMillisecond())
		}
		node.MbList.InsertNode(packet.Id, node.IP, node.Port, getMillisecond())
		ACK_JOIN <- true
	case ACTION_HEARTBEAT:
		// SLOG.Printf("[Node %d] Received ACTION_HEARTBEAT id: %d", node.Id, packet.Id)
		node.MbList.UpdateNodeHeartbeat(packet.Id, getMillisecond())
	case ACTION_PING:
		if packet.IP == node.IP && packet.Port == node.Port {
			break // self should not ack
		}
		if node.MbList == nil {
			break
		}
		SLOG.Printf("[Node %d] Received ACTION_PING from %s:%s", node.Id, packet.IP, packet.Port)
		address := packet.IP + ":" + packet.Port
		ackPacket := &Packet{
			Action: ACTION_ACK,
			IP:     node.IP,
			Port:   node.Port,
		}
		sendPacketUDP(address, ackPacket)
	case ACTION_ACK:
		SLOG.Printf("[Node x] Received ACTION_ACK from %s:%s", packet.IP, packet.Port)
		address := packet.IP + ":" + packet.Port
		ACK_INTRO <- address
	}

}

func (node *Node) MonitorInputPacket() {
	conn, err := net.ListenPacket("udp", node.IP+":"+node.Port)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	for {
		buf := make([]byte, 4096)
		length, _, err := conn.ReadFrom(buf)
		if err != nil {
			log.Println(err)
		}
		var rec_packet Packet
		json.Unmarshal(buf[:length], &rec_packet)
		node.handlePacket(rec_packet)
	}
}

func getMillisecond() int {
	return int(time.Now().UnixNano() / 1000000)
}
