package memberlist

import "log"

type MemberNode struct {
	id          int
	heartbeat_t int
	ip          string
	port        string
	prev        *MemberNode
	next        *MemberNode
}

func CreateNode(id int, ip, port string, heartbeat_t int) *MemberNode {
	new_node := &MemberNode{id: id, ip: ip, port: port, heartbeat_t: heartbeat_t}
	new_node.prev = new_node
	new_node.next = new_node
	return new_node
}

type MemberList struct {
	Member_map map[int]MemberNode
	Size       int
}

func CreateMemberList(size int) *MemberList {
	member_map := make(map[int]MemberNode)
	memberList := &MemberList{Member_map: member_map, Size: size}
	return memberList
}

func (mbList *MemberList) InsertNode(id int, ip, port string, heartbeat_t int) {
	if _, exist := mbList.Member_map[id]; exist {
		log.Fatal("id exists")
	}
	new_node := CreateNode(id, ip, port, heartbeat_t)
	mbList.Member_map[id] = *new_node
	if len(mbList.Member_map) == 1 {
		// No other node exists
		return
	}
	cur := (id + 1) % mbList.Size
	var next_node MemberNode
	for cur != id {
		if _, ok := mbList.Member_map[cur]; ok {
			next_node = mbList.Member_map[cur]
			break
		}
		cur = (cur + 1) % mbList.Size
	}
	pre_node := next_node.prev
	pre_node.next = new_node
	new_node.prev = pre_node
	new_node.next = &next_node
	next_node.prev = new_node
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
	prev := cur_node.prev
	next := cur_node.next
	prev.next = next
	next.prev = prev
	delete(mbList.Member_map, id)
}
