package memberlist

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"sync"
	"text/tabwriter"
)

const MEMBER_LIST_FILE = "/tmp/member.list"

type MemberNode struct {
	Id          int
	Heartbeat_t int
	Ip          string
	Port        string
	prev        *MemberNode
	next        *MemberNode
}

func CreateNode(id int, ip, port string, heartbeat_t int) *MemberNode {
	new_node := &MemberNode{Id: id, Ip: ip, Port: port, Heartbeat_t: heartbeat_t}
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
	return memberList
}

func (mbList *MemberList) InsertNode(id int, ip, port string, heartbeat_t int) {
	mbList.lock.Lock()
	defer mbList.lock.Unlock()
	if _, exist := mbList.Member_map[id]; exist {
		log.Panic("trying to insert an existed id")
	}
	new_node := CreateNode(id, ip, port, heartbeat_t)
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
		log.Panic("trying to delete non-exist id")
		return
	}
	prev := cur_node.prev
	next := cur_node.next
	prev.next = next
	next.prev = prev
	delete(mbList.Member_map, id)
	mbList.Size--

	mbList.DumpToTmpFile()
}

func (mbList *MemberList) UpdateNodeHeartbeat(id, heartbeat_t int) {
	node := mbList.GetNode(id)
	if node == nil {
		return
	}
	node.Heartbeat_t = heartbeat_t

	mbList.DumpToTmpFile()
}

func (mbList MemberList) GetNode(id int) *MemberNode {
	return mbList.Member_map[id]
}

func (mbList MemberList) GetPrevKNodes(id, k int) []MemberNode {
	mbList.lock.Lock()
	defer mbList.lock.Unlock()
	node := mbList.GetNode(id)
	if node == nil {
		log.Panic("start id doesn't exit in memberlist")
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
		log.Panic("start id doesn't exit in memberlist")
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
	fmt.Fprintln(w, "ID\tIP\tPORT\tHeartbeat")
	for _, k := range keys {
		node := mblist.Member_map[k]
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\n",
			node.Id, node.Ip, node.Port, node.Heartbeat_t)
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
