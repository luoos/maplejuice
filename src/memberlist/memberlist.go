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
	Member_map map[int]*MemberNode
	Size       int
}

func CreateMemberList(size int) *MemberList {
	member_map := make(map[int]*MemberNode)
	memberList := &MemberList{Member_map: member_map, Size: size}
	return memberList
}

func (mbList *MemberList) InsertNode(id int, ip, port string, heartbeat_t int) {
	if _, exist := mbList.Member_map[id]; exist {
		log.Fatal("id exists")
	}
	new_node := CreateNode(id, ip, port, heartbeat_t)
	mbList.Member_map[id] = new_node
	if len(mbList.Member_map) == 1 {
		// No other node exists
		return
	}
	cur := (id + 1) % mbList.Size
	var next_node *MemberNode
	for cur != id {
		if _, ok := mbList.Member_map[cur]; ok {
			next_node = mbList.Member_map[cur]
			break
		}
		cur = (cur + 1) % mbList.Size
	}
	pre_node := next_node.Prev
	pre_node.Next = new_node
	new_node.Prev = pre_node
	new_node.Next = next_node
	next_node.Prev = new_node
}

func (mbList *MemberList) FindLeastFreeId() int {
	if len(mbList.Member_map) == mbList.Size {
		return -1
	}
	for i := 0; i < mbList.Size; i++ {
		if _, exist := mbList.Member_map[i]; !exist {
			return i
		}
	}
	return -1 // Should not happend
}

func (mbList *MemberList) DeleteNode(id int) {
	if _, exist := mbList.Member_map[id]; !exist {
		log.Panic("trying to delete non-exist id")
		return
	}
	cur_node := mbList.Member_map[id]
	prev := cur_node.Prev
	next := cur_node.Next
	prev.Next = next
	next.Prev = prev
	delete(mbList.Member_map, id)
}

func (mbList *MemberList) UpdateNodeHeartbeat(id, heartbeat_t int) {
	if _, exist := mbList.Member_map[id]; !exist {
		return
	}
	node := mbList.Member_map[id]
	node.Heartbeat_t = heartbeat_t
}

func (mbList MemberList) GetNextKNodes(id, k int) []MemberNode {
	if _, exist := mbList.Member_map[id]; !exist {
		log.Panic("start id doesn't exit in memberlist")
		return nil
	}
	arr := make([]MemberNode, 0)
	next := mbList.Member_map[id].Next
	for i := 0; i < k && next.Id != id; i++ {
		arr = append(arr, *next)
		next = next.Next
	}
	return arr
}
