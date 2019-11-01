package node

import (
	"hash/fnv"
	"log"
	"os"
	. "slogger"
)

const DEBUG = false
const DUPLICATE_CNT = 4

func getHashID(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32()) % MAX_CAPACITY
}

func IsInCircleRange(id, start, end int) bool {
	return (start < end && start <= id && id <= end) ||
		(start > end && (start <= id || id <= end))
}

func (node *Node) GetMasterID(sdfsfilename string) int {
	fileHashID := getHashID(sdfsfilename)
	var prevId int = -1
	for curId, _ := range node.MbList.Member_map {
		prevId = node.MbList.GetNode(curId).GetPrevNode().Id
		if prevId == curId {
			return curId
		} else if IsInCircleRange(fileHashID, prevId+1, curId) {
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
		if timestamp == -1 {
			SLOG.Print("FileNotExists in FileInfo")
		}
		if timestamp > max_timestamp {
			max_timestamp = timestamp
			max_address = address
		}
	}
	return max_address, max_timestamp
}

func (node *Node) DeleteRedundantFile() {
	prev_four_nodes := node.MbList.GetPrevKNodes(node.Id, 4)
	if len(prev_four_nodes) == 4 {
		prev_4 := prev_four_nodes[3]
		toDelete := node.FileList.DeleteFileInfosOutOfRange(prev_4.Id, node.Id)
		for _, path := range toDelete {
			err := os.Remove(path)
			if err != nil {
				SLOG.Printf("Fail to remove file %s", path)
				SLOG.Panicln(err)
			}
		}
	}
}
