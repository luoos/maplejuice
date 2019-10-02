package memberlist

import (
	"encoding/json"
	"log"
	"sync"
)

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
}

func (mbList *MemberList) UpdateNodeHeartbeat(id, heartbeat_t int) {
	node := mbList.GetNode(id)
	if node == nil {
		return
	}
	node.Heartbeat_t = heartbeat_t
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
