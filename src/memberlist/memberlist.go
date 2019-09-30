package memberlist

import "log"

type MemberNode struct {
	Id          int
	Heartbeat_t int
	Ip          string
	Port        string
	Prev        *MemberNode
	Next        *MemberNode
}

func CreateNode(id int, ip, port string, heartbeat_t int) *MemberNode {
	new_node := &MemberNode{Id: id, Ip: ip, Port: port, Heartbeat_t: heartbeat_t}
	new_node.Prev = new_node
	new_node.Next = new_node
	return new_node
}

type MemberList struct {
	member_map     map[int]*MemberNode
	capacity, size int
}

func CreateMemberList(capacity int) *MemberList {
	member_map := make(map[int]*MemberNode)
	memberList := &MemberList{member_map: member_map, capacity: capacity}
	return memberList
}

func (mbList *MemberList) InsertNode(id int, ip, port string, heartbeat_t int) {
	if _, exist := mbList.member_map[id]; exist {
		log.Fatal("id exists")
	}
	new_node := CreateNode(id, ip, port, heartbeat_t)
	mbList.member_map[id] = new_node
	mbList.size++
	if mbList.GetSize() == 1 {
		// No other node exists
		return
	}
	cur := (id + 1) % mbList.capacity
	var next_node *MemberNode
	for cur != id {
		if _, ok := mbList.member_map[cur]; ok {
			next_node = mbList.GetNode(cur)
			break
		}
		cur = (cur + 1) % mbList.capacity
	}
	pre_node := next_node.Prev
	pre_node.Next = new_node
	new_node.Prev = pre_node
	new_node.Next = next_node
	next_node.Prev = new_node
}

func (mbList *MemberList) FindLeastFreeId() int {
	if mbList.GetSize() == mbList.capacity {
		return -1
	}
	for i := 0; i < mbList.capacity; i++ {
		if _, exist := mbList.member_map[i]; !exist {
			return i
		}
	}
	return -1 // Should not happend
}

func (mbList *MemberList) DeleteNode(id int) {
	if _, exist := mbList.member_map[id]; !exist {
		log.Panic("trying to delete non-exist id")
		return
	}
	cur_node := mbList.GetNode(id)
	prev := cur_node.Prev
	next := cur_node.Next
	prev.Next = next
	next.Prev = prev
	delete(mbList.member_map, id)
	mbList.size--
}

func (mbList *MemberList) UpdateNodeHeartbeat(id, heartbeat_t int) {
	if _, exist := mbList.member_map[id]; !exist {
		return
	}
	node := mbList.GetNode(id)
	if node == nil {
		return
	}
	node.Heartbeat_t = heartbeat_t
}

func (mbList MemberList) GetNode(id int) *MemberNode {
	return mbList.member_map[id]
}

func (mbList MemberList) GetNextKNodes(id, k int) []MemberNode {
	if _, exist := mbList.member_map[id]; !exist {
		log.Panic("start id doesn't exit in memberlist")
		return nil
	}
	arr := make([]MemberNode, 0)
	next := mbList.GetNode(id).Next
	for i := 0; i < k && next.Id != id; i++ {
		arr = append(arr, *next)
		next = next.Next
	}
	return arr
}

func (mbList MemberList) GetSize() int {
	return mbList.size
}

func (mbList MemberList) GetCapacity() int {
	return mbList.capacity
}
