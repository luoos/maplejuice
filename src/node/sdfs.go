package node

import (
	"hash/fnv"
	"log"
)

func getHashID(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32()) % MAX_CAPACITY
}

// func (node *Node) sendFileTCP(sdfsfilename string, content string) {
// 	fileHashID := getHashID(sdfsfilename)

// }

func (node *Node) GetMasterID(sdfsfilename string) int {
	fileHashID := getHashID(sdfsfilename)
	var prevId int = -1
	for curId, _ := range node.MbList.Member_map {
		prevId = node.MbList.GetPrevKNodes(curId, 1)[0].Id
		if prevId < fileHashID && fileHashID <= curId {
			log.Printf("first case: prevId %d, fileHashID %d, curId %d", prevId, fileHashID, curId)
			return curId
		}
		if prevId > curId && (fileHashID > prevId || fileHashID <= curId) {
			log.Printf("second case: prevId %d, fileHashID %d, curId %d", prevId, fileHashID, curId)
			return curId
		}
	}
	log.Fatal("should never reach here")
	return -1
}

func (node *Node) GetFirstKReplicaNodeID(sdfsfilename string, K int) []int {
	masterID := node.GetMasterID(sdfsfilename)
	res := []int{masterID}
	cur := node.MbList.Member_map[masterID]
	for i := 0; i < K-1; i++ {
		if cur.next.Id == masterID {
			break
		}
		res = append(res, cur.next.Id)
		cur = cur.next
	}
	return res
}
