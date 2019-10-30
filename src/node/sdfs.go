package node

import (
	"hash/fnv"
	"log"
)

const DEBUG = false
const DUPLICATE_CNT = 4

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
			if DEBUG {
				log.Printf("first case: prevId %d, fileHashID %d, curId %d", prevId, fileHashID, curId)
			}
			return curId
		}
		if prevId > curId && (fileHashID > prevId || fileHashID <= curId) {
			if DEBUG {
				log.Printf("second case: prevId %d, fileHashID %d, curId %d", prevId, fileHashID, curId)
			}
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

func (node *Node) GetAddressesWithIds(ids []int) []string {
	address := make([]string, 0)
	for _, id := range ids {
		address = append(address, node.MbList.GetRPCAddress(id))
	}
	return address
}

func (node *Node) GetResponsibleAddresses(sdfsfilename string) []string {
	ids := node.GetFirstKReplicaNodeID(sdfsfilename, DUPLICATE_CNT)
	return node.GetAddressesWithIds(ids)
}

func (node *Node) GetAddressOfLatestTS(sdfsfilename string) (string, int) {
	addressList := node.GetResponsibleAddresses(sdfsfilename)
	c := make(chan Pair, 4)
	for _, address := range addressList {
		go CallGetTimeStamp(address, sdfsfilename, c)
	}
	max_timestamp := -1
	max_address := ""
	for i := 0; i < READ_QUORUM; i++ {
		pair := <-c
		address := pair.Address
		timestamp := pair.Ts
		if timestamp > max_timestamp {
			max_timestamp = timestamp
			max_address = address
		}
	}
	return max_address, max_timestamp
}
