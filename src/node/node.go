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
)

func CreateNode(ip, port string) *Node {
	node := &Node{IP: ip, Port: port}
	return node
}

func (node *Node) InitMemberList() {
	node.MbList = CreateMemberList(0, MAX_CAPACITY)
	node.MbList.InsertNode(0, node.IP, node.Port, 100)
}

func (node *Node) Join(address string) {
	packet := &Packet{
		Action: ACTION_JOIN,
		IP:     node.IP,
		Port:   node.Port,
	}
	err := sendPacketUDP(address, packet)
	if err != nil {
		log.Panic(err)
	}
}

func (node *Node) Leave() {
	deleteNodePacket := &Packet{
		Action: ACTION_DELETE_NODE,
		Id:     node.Id,
	}
	node.Broadcast(deleteNodePacket)
}

func sendPacketUDP(address string, packet *Packet) error {
	data, err := json.Marshal(packet)
	if err != nil {
		log.Println(err)
		return err
	}
	var conn net.Conn
	conn, err = net.Dial("udp", address)
	defer conn.Close()
	conn.Write(data)
	return nil
}

func (node *Node) Broadcast(packet *Packet) {
	for _, member := range node.MbList.Member_map {
		address := member.Ip + ":" + member.Port
		sendPacketUDP(address, packet)
	}
}

func (node *Node) handlePacket(packet Packet) {
	switch packet.Action {
	case ACTION_NEW_NODE:
		SLOG.Printf("[Node] Received ACTION_NEW_NODE (%d, %s:%s)", packet.Id, packet.IP, packet.Port)
		node.MbList.InsertNode(packet.Id, packet.IP, packet.Port, getMillisecond())
	case ACTION_DELETE_NODE:
		SLOG.Printf("[Node] Received ACTION_DELETE_NODE (%d)", packet.Id)
		node.MbList.DeleteNode(packet.Id)
	case ACTION_JOIN:
		reply_address := packet.IP + ":" + packet.Port
		freeId := node.MbList.FindLeastFreeId()
		send_packet := &Packet{
			Action: ACTION_REPLY_JOIN,
			Id:     freeId,
			Map:    node.MbList,
		}
		err := sendPacketUDP(reply_address, send_packet)
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
	case ACTION_REPLY_JOIN:
		node.MbList = CreateMemberList(packet.Id, MAX_CAPACITY)
		node.Id = packet.Id
		for _, item := range packet.Map.Member_map {
			node.MbList.InsertNode(item.Id, item.Ip, item.Port, getMillisecond())
		}
		node.MbList.InsertNode(packet.Id, node.IP, node.Port, getMillisecond())
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
