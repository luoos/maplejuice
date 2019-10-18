package node

import (
	"encoding/json"
	"log"
	"math/rand"
	"net"
	. "slogger"
	"sync"
	"time"
)

type Node struct {
	Id       int
	IP, Port string
	MbList   *MemberList
	timerMap map[int]*time.Timer
	mapLock  *sync.Mutex
}

type Packet struct {
	Action ActionType
	Id     int
	IP     string
	Port   string
	Map    *MemberList
}

type ActionType int8
type StatusType int8

const (
	ACTION_JOIN        ActionType = 1 << 0
	ACTION_REPLY_JOIN  ActionType = 1 << 1
	ACTION_NEW_NODE    ActionType = 1 << 2
	ACTION_DELETE_NODE ActionType = 1 << 3
	ACTION_HEARTBEAT   ActionType = 1 << 4
	ACTION_PING        ActionType = 1 << 5
	ACTION_ACK         ActionType = 1 << 6

	STATUS_OK   StatusType = 1 << 0
	STATUS_FAIL StatusType = 1 << 1
	STATUS_END  StatusType = 1 << 2

	LOSS_RATE              = 0.00
	NUM_MONITORS       int = 3
	HEARTBEAT_INTERVAL     = 1500 * time.Millisecond
	TIMEOUT_THRESHOLD      = 4 * time.Second
)

var ACK_INTRO = make(chan string)
var ACK_JOIN = make(chan Packet)

var HEARTBEAT_LOG_FLAG = false // debug

func CreateNode(ip, port string) *Node {
	timer_map := make(map[int]*time.Timer)
	node := &Node{IP: ip, Port: port, mapLock: &sync.Mutex{}, timerMap: timer_map}
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
		select {
		case res := <-ACK_INTRO:
			return res, true
		case <-time.After(time.Second):
			break
		}
	}
	return "", false
}

func (node *Node) Join(address string) bool {
	packet := &Packet{
		Action: ACTION_JOIN,
		IP:     node.IP,
		Port:   node.Port,
	}
	SLOG.Printf("Sending Join packet, source %s:%s, destination %s", node.IP, node.Port, address)
	err := sendPacketUDP(address, packet)
	if err != nil {
		SLOG.Panic(err)
	}
	select {
	case mblistPacket := <-ACK_JOIN:
		for _, item := range mblistPacket.Map.Member_map {
			node.MbList.InsertNode(item.Id, item.Ip, item.Port, getMillisecond())
		}
		node.MbList.InsertNode(node.Id, node.IP, node.Port, getMillisecond())
		for _, prevNode := range node.MbList.GetPrevKNodes(node.Id, NUM_MONITORS) {
			node.monitorIfNecessary(prevNode.Id)
		}
		return true
	case <-time.After(time.Second):
		SLOG.Printf("Join Time out, source: %s:%s", node.IP, node.Port)
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
		if HEARTBEAT_LOG_FLAG {
			SLOG.Printf("[Node %d] Sending ACTION_HEARTBEAT to %s", node.Id, address)
		}
		sendPacketUDP(address, heartbeatPacket)
	}
}

func (node *Node) SendHeartbeatRoutine() {
	for {
		if rand.Float64() >= LOSS_RATE { // simulate loss packet
			node.SendHeartbeat()
		}
		time.Sleep(HEARTBEAT_INTERVAL)
	}
}

func sendPacketUDP(address string, packet *Packet) error {
	data, err := json.Marshal(packet)
	if err != nil {
		SLOG.Print(err)
		return err
	}
	var conn net.Conn
	conn, err = net.Dial("udp", address)
	if err != nil {
		SLOG.Printf("Dial Failed: address: %s", address)
		SLOG.Print(err)
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
		node.monitorIfNecessary(packet.Id)
	case ACTION_DELETE_NODE:
		SLOG.Printf("[Node %d] Received ACTION_DELETE_NODE (%d), source: %s", node.Id, packet.Id, packet.IP)
		lose_heartbeat := node.isPrevKNodes(packet.Id)
		node.MbList.DeleteNode(packet.Id)
		if lose_heartbeat {
			// add other node to receive heartbeat
			for _, item := range node.MbList.GetPrevKNodes(node.Id, NUM_MONITORS) {
				if _, ok := node.timerMap[item.Id]; !ok {
					node.monitorIfNecessary(item.Id)
				}
			}
		}
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
		node.monitorIfNecessary(freeId)
	case ACTION_REPLY_JOIN:
		node.MbList = CreateMemberList(packet.Id, MAX_CAPACITY)
		node.Id = packet.Id
		SLOG.Printf("[Node %d] Received ACTION_REPLY_JOIN assigned, member cnt: %d", node.Id, len(packet.Map.Member_map))
		ACK_JOIN <- packet
	case ACTION_HEARTBEAT:
		if HEARTBEAT_LOG_FLAG {
			SLOG.Printf("[Node %d] Received ACTION_HEARTBEAT id: %d", node.Id, packet.Id)
		}
		node.MbList.UpdateNodeHeartbeat(packet.Id, getMillisecond())
		node.resetTimer(packet.Id)
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
		go node.handlePacket(rec_packet)
	}
}

func (node *Node) resetTimer(id int) {
	node.mapLock.Lock()
	if timer, ok := node.timerMap[id]; ok {
		timer.Reset(TIMEOUT_THRESHOLD)
	} else {
		SLOG.Printf("[Node %d] trying to reset a non-existed timer %d", node.Id, id)
	}

	node.mapLock.Unlock()
}

func (node *Node) monitorIfNecessary(id int) {
	if !node.isPrevKNodes(id) {
		return
	}
	node.mapLock.Lock()
	if _, ok := node.timerMap[id]; ok {
		node.timerMap[id].Stop()
		SLOG.Printf("[Node %d] Stop existed timer %d", node.Id, id)
	}
	node.timerMap[id] = time.AfterFunc(TIMEOUT_THRESHOLD, func() {
		node.nodeTimeOut(id)
	})
	node.mapLock.Unlock()
}

func (node *Node) isPrevKNodes(id int) bool {
	for _, item := range node.MbList.GetPrevKNodes(node.Id, NUM_MONITORS) {
		if item.Id == id {
			return true
		}
	}
	return false
}

func (node *Node) nodeTimeOut(id int) {
	if !node.isPrevKNodes(id) {
		return
	}
	SLOG.Printf("[Node %d] found failure node id: %d\n", node.Id, id)
	deleteNodePacket := &Packet{
		Action: ACTION_DELETE_NODE,
		Id:     id,
		IP:     node.IP,
	}
	node.Broadcast(deleteNodePacket)
	node.MbList.DeleteNode(id)
}

func getMillisecond() int {
	return int(time.Now().UnixNano() / 1000000)
}
