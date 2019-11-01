package node

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	. "slogger"
	"sort"
	"sync"
	"text/tabwriter"
	"time"
)

const MEMBER_LIST_FILE = "/tmp/member.list"
const MAX_CAPACITY = 1024

type MemberNode struct {
	Id          int
	Heartbeat_t int
	JoinTime    string
	Ip          string
	Port        string
	RPC_Port    string
	prev        *MemberNode
	next        *MemberNode
}

func CreateMemberNode(id int, ip, port, rpc_port string, heartbeat_t int) *MemberNode {
	timestamp := time.Now().Format("2006.01.02 15:04:05")
	new_node := &MemberNode{Id: id, Ip: ip, Port: port, RPC_Port: rpc_port, Heartbeat_t: heartbeat_t, JoinTime: timestamp}
	new_node.prev = new_node
	new_node.next = new_node
	return new_node
}

func (mNode *MemberNode) GetPrevNode() *MemberNode {
	return mNode.prev
}

func (mNode *MemberNode) GetNextNode() *MemberNode {
	return mNode.next
}

type MemberList struct {
	Member_map     map[int]*MemberNode
	Capacity, Size int
	SelfId         int
	lock           *sync.Mutex
}

func CreateMemberList(selfId, capacity int) *MemberList {
	Member_map := make(map[int]*MemberNode)
	memberList := &MemberList{SelfId: selfId, Member_map: Member_map, Capacity: capacity,
		lock: &sync.Mutex{}}
	SLOG.Printf("[MembershipList] Created membership list id: %d, capacity: %d", selfId, capacity)
	return memberList
}

func (mbList *MemberList) InsertNode(id int, ip, port, rpc_port string, heartbeat_t int) {
	mbList.lock.Lock()
	defer mbList.lock.Unlock()
	if _, exist := mbList.Member_map[id]; exist {
		SLOG.Printf("[MembershipList %d] trying to insert an existed id: %d", mbList.SelfId, id)
		return
	}
	new_node := CreateMemberNode(id, ip, port, rpc_port, heartbeat_t)
	SLOG.Printf("[MembershipList %d] Inserted node (%d, %s:%s, %d)", mbList.SelfId, id, ip, port, heartbeat_t)
	mbList.Member_map[id] = new_node
	mbList.Size++
	if mbList.Size == 1 {
		// No other node exists
		return
	}
	cur := (id + 1) % mbList.Capacity
	var next_node *MemberNode
	for cur != id {
		next_node = mbList.GetNode(cur)
		if next_node != nil {
			break
		}
		cur = (cur + 1) % mbList.Capacity
	}
	pre_node := next_node.prev
	pre_node.next = new_node
	new_node.prev = pre_node
	new_node.next = next_node
	next_node.prev = new_node
	mbList.DumpToTmpFile()
}

func (mbList *MemberList) FindLeastFreeId() int {
	mbList.lock.Lock()
	defer mbList.lock.Unlock()
	if mbList.Size == mbList.Capacity {
		return -1
	}
	for i := 0; i < mbList.Capacity; i++ {
		if _, exist := mbList.Member_map[i]; !exist {
			return i
		}
	}
	return -1 // Should not happend
}

func (mbList *MemberList) DeleteNode(id int) {
	mbList.lock.Lock()
	defer mbList.lock.Unlock()
	cur_node := mbList.GetNode(id)
	if cur_node == nil {
		SLOG.Printf("[MembershipList %d] trying to delete non-exist id: %d", mbList.SelfId, id)
		return
	}
	prev := cur_node.prev
	next := cur_node.next
	prev.next = next
	next.prev = prev
	delete(mbList.Member_map, id)
	mbList.Size--
	SLOG.Printf("[MembershipList %d] Deleted node %d", mbList.SelfId, id)
	mbList.DumpToTmpFile()
}

func (mbList *MemberList) UpdateNodeHeartbeat(id, heartbeat_t int) {
	node := mbList.GetNode(id)
	if node == nil {
		return
	}
	if HEARTBEAT_LOG_FLAG {
		SLOG.Printf("[MembershipList %d] Update heartbeat for node: %d, hb: %d", mbList.SelfId, id, heartbeat_t)
	}
	node.Heartbeat_t = heartbeat_t

	mbList.DumpToTmpFile()
}

func (mbList MemberList) GetNode(id int) *MemberNode {
	return mbList.Member_map[id]
}

func (mbList MemberList) GetAddress(id int) string {
	n := mbList.GetNode(id)
	return n.Ip + ":" + n.Port
}

func (mbList MemberList) GetRPCAddress(id int) string {
	n := mbList.GetNode(id)
	return n.Ip + ":" + n.RPC_Port
}

func (mbList MemberList) GetIP(id int) string {
	n := mbList.GetNode(id)
	return n.Ip
}

func (mbList MemberList) GetPrevKNodes(id, k int) []MemberNode {
	mbList.lock.Lock()
	defer mbList.lock.Unlock()
	node := mbList.GetNode(id)
	if node == nil {
		SLOG.Printf("[MembershipList %d] start id doesn't exit, node %d", mbList.SelfId, id)
		return nil
	}
	arr := make([]MemberNode, 0)
	prev := node.prev
	for i := 0; i < k && prev.Id != id; i++ {
		arr = append(arr, *prev)
		prev = prev.prev
	}
	return arr
}

func (mbList MemberList) GetNextKNodes(id, k int) []MemberNode {
	mbList.lock.Lock()
	defer mbList.lock.Unlock()
	node := mbList.GetNode(id)
	if node == nil {
		log.Panic("start id doesn't exit in node")
		return nil
	}
	arr := make([]MemberNode, 0)
	next := node.next
	for i := 0; i < k && next.Id != id; i++ {
		arr = append(arr, *next)
		next = next.next
	}
	return arr
}

// *** this is for passive monitoring
func (mbList MemberList) NodeTimeOut(deadline, id int) bool {
	mbList.lock.Lock()
	defer mbList.lock.Unlock()
	node := mbList.GetNode(id)
	if node == nil {
		log.Panic("NodeTimeOut: this node id does not exist!")
	}
	if node.Heartbeat_t < deadline {
		SLOG.Printf("[MembershipList %d] found TIMEOUT node, id: %d last heartbeat_t: %d, deadline: %d", mbList.SelfId, id, node.Heartbeat_t, deadline)
	}
	return node.Heartbeat_t < deadline
}

func (mbList MemberList) GetTimeOutNodes(deadline, id, k int) []MemberNode {
	// Check if previous k nodes (start from id) are timeout
	previousNodes := mbList.GetPrevKNodes(id, k)
	timeOutNodes := make([]MemberNode, 0)
	for _, node := range previousNodes {
		if node.Heartbeat_t < deadline {
			timeOutNodes = append(timeOutNodes, node)
		}
	}
	if len(timeOutNodes) > 0 {
		return timeOutNodes
	}
	return nil
}

func (mbList MemberList) GetRPCAddressesForNextKNodes(start, k int) []string {
	next_k_nodes := mbList.GetNextKNodes(start, k)
	addresses := make([]string, 0)
	for _, n := range next_k_nodes {
		addresses = append(addresses, mbList.GetRPCAddress(n.Id))
	}
	return addresses
}

func (mbList *MemberList) ToJson() []byte {
	bytes, _ := json.Marshal(mbList)
	return bytes
}

func (mbList *MemberList) DumpToTmpFile() {
	bytes := mbList.ToJson()
	err := ioutil.WriteFile(MEMBER_LIST_FILE, bytes, 0644)
	if err != nil {
		log.Fatal(err)
	}
}

func ConstructFromTmpFile() MemberList {
	_, e := os.Stat(MEMBER_LIST_FILE)
	if os.IsNotExist(e) {
		log.Fatalf("Membership list file (%s) doesn't exist\n", MEMBER_LIST_FILE)
	}
	dat, err := ioutil.ReadFile(MEMBER_LIST_FILE)
	checkErrorFatal(err)
	var new_mbList MemberList
	err = json.Unmarshal(dat, &new_mbList)
	checkErrorFatal(err)
	return new_mbList
}

func (mblist MemberList) NicePrint() {
	w := tabwriter.NewWriter(os.Stdout, 10, 0, 4, ' ', 0)
	var keys []int
	for k, _ := range mblist.Member_map {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	fmt.Fprintln(w, "ID\tIP\tPORT\tHeartbeat\tJoin Time")
	for _, k := range keys {
		node := mblist.Member_map[k]
		ts := time.Unix(int64(node.Heartbeat_t/1000), 0).Format("2006.01.02 15:04:05")
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\n",
			node.Id, node.Ip, node.Port, ts, node.JoinTime)
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "Self ID: %d\tSize: %d\tCapacity: %d\n",
		mblist.SelfId, mblist.Size, mblist.Capacity)
	w.Flush()
}

func checkErrorFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
