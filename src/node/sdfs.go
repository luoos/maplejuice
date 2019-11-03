package node

import (
	"hash/fnv"
	"io/ioutil"
	"os"
	. "slogger"
	"time"
)

const DEBUG = false
const DUPLICATE_CNT = 4

func getHashID(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32()) % MAX_CAPACITY
}

func (node *Node) SetFileDir(dir string) {
	node.File_dir = dir
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, 0777)
	} else if err != nil {
		SLOG.Printf("Failed to create folder: %s", dir)
		os.Exit(1)
	}
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
	SLOG.Fatal("[Fatal] Fail to get master id")
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

func (node *Node) GetResponsibleHostname(sdfsName string) []string {
	ids := node.GetFirstKReplicaNodeID(sdfsName, DUPLICATE_CNT)
	res := []string{}
	for _, id := range ids {
		hostname := node.MbList.GetNode(id).Hostname
		res = append(res, hostname)
	}
	return res
}

func (node *Node) GetAddressOfLatestTS(sdfsfilename string) (string, int) {
	addressList := node.GetResponsibleAddresses(sdfsfilename)
	c := make(chan Pair, 4)
	for _, address := range addressList {
		go CallGetTimeStamp(address, sdfsfilename, c)
	}
	max_timestamp := -1
	max_address := ""
	for i := 0; i < READ_QUORUM && i < len(addressList); i++ {
		select {
		case pair := <-c:
			address := pair.Address
			timestamp := pair.Ts
			if timestamp == -1 {
				SLOG.Print("FileNotExists in FileInfo")
			}
			if timestamp > max_timestamp {
				max_timestamp = timestamp
				max_address = address
			}
		case <-time.After(2 * time.Second):
			continue
		}

	}
	return max_address, max_timestamp
}

func (node *Node) DeleteRedundantFile() {
	prev_k_nodes := node.MbList.GetPrevKNodes(node.Id, DUPLICATE_CNT)
	if len(prev_k_nodes) == DUPLICATE_CNT {
		prev_k := prev_k_nodes[DUPLICATE_CNT-1]
		toDelete := node.FileList.DeleteFileInfosOutOfRange(prev_k.Id, node.Id)
		for _, path := range toDelete {
			err := os.Remove(path)
			if err != nil {
				SLOG.Printf("Fail to remove file %s", path)
				SLOG.Panicln(err)
			}
		}
	}
}

func (node *Node) DuplicateReplica() {
	ownedFileInfos := node.FileList.GetOwnedFileInfos(node.Id)
	targetsRPCAddr := node.MbList.GetRPCAddressesForNextKNodes(node.Id, DUPLICATE_CNT-1)
	for _, info := range ownedFileInfos {
		node.SendFileIfNecessary(info, targetsRPCAddr)
	}
}

func (node *Node) SendFileIfNecessary(info FileInfo, targetRPCAddr []string) {
	L := len(targetRPCAddr)
	c := make(chan Pair, L) // TODO: rename Pair to TsPair
	for _, addr := range targetRPCAddr {
		go CallGetTimeStamp(addr, info.Sdfsfilename, c)
	}

	data, err := ioutil.ReadFile(info.Localpath)
	if err != nil {
		SLOG.Printf("[Node %d] Fail to read file: %s", node.Id, info.Localpath)
		return
	}
	dummy_chan := make(chan int, L)
	for i := 0; i < L; i++ {
		select {
		case p := <-c:
			if p.Ts < info.Timestamp {
				go PutFile(info.MasterNodeID, info.Timestamp, p.Address, info.Sdfsfilename, data, dummy_chan)
			}
		case <-time.After(1 * time.Second):
			SLOG.Printf("[Node %d] Timeout when trying to get timestamp", node.Id)
		}
	}
}

func (node *Node) TransferOwnership(newMasterId int) {
	ownedFileInfos := node.FileList.GetOwnedFileInfos(newMasterId)
	newMasterRPCAddress := node.MbList.GetRPCAddress(newMasterId)
	address := []string{newMasterRPCAddress}
	for _, info := range ownedFileInfos {
		node.SendFileIfNecessary(info, address)
	}
}
