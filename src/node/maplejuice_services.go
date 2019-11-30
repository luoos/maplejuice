/*
This file defines maple juice rpc services for a node.

Includes:

1. Client starts a maple
2. Client starts a juice
*/

package node

import (
	"net/rpc"
	. "slogger"
	"strconv"
	"strings"
)

// MapleJuiceServiceName ...
const MapleJuiceServiceName = "MapleJuiceService"

type MapleJuiceTaskType int8

// Task names
const (
	MapleTask MapleJuiceTaskType = 1
	JuiceTask MapleJuiceTaskType = 2
)

//MapleJuiceTaskArgs ...
type MapleJuiceTaskArgs struct {
	TaskType   MapleJuiceTaskType // "MapleTask" or "JuiceTask"
	Exe        string
	NumWorkers int
	Prefix     string
	Path       string
	ClientAddr string
}

type MapleJuiceService struct {
	TaskQueue chan *MapleJuiceTaskArgs
	SelfNode  *Node
}

func (node *Node) RegisterMapleJuiceService(address string, mjService *MapleJuiceService) error {
	return rpc.RegisterName(MapleJuiceServiceName+address, mjService)
}

func (node *Node) RegisterRPCMapleJuiceService() {
	mjService := &MapleJuiceService{TaskQueue: make(chan *MapleJuiceTaskArgs, 10), SelfNode: node}
	go mjService.processMapleJuiceTasks()
	address := node.IP + ":" + node.RPC_Port
	rpc.RegisterName(MapleJuiceServiceName+address, mjService)
}

/*****
 * Non-Master: Receive From Dcli -> Send to Master
 *****/

// handle maple/juice request from Dcli send request to Master
func (mj *MapleJuiceService) ForwardMapleJuiceRequest(args *MapleJuiceTaskArgs, result *RPCResultType) error {
	mbList := mj.SelfNode.MbList
	masterNode := mbList.GetNode(mbList.smallestId)
	address := masterNode.Ip + ":" + masterNode.RPC_Port
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		SLOG.Printf("[ForwardMJ] Dial failed, address: %s", address)
		return err
	}
	defer client.Close()
	var reply RPCResultType
	send_err := client.Call(MapleJuiceServiceName+address+".AddMapleJuiceTask", args, &reply)
	if send_err != nil {
		SLOG.Println("[ForwardMJ] call rpc err:", send_err)
	}
	SLOG.Printf("[FowardMJ] forward request to master: %d", masterNode.Id)
	*result = RPC_SUCCESS
	return err
}

/*****
 * Master:
 * 1. handle Maple
 * 2. handle Juice
 *****/

// add maple juice task to queue
func (mj *MapleJuiceService) AddMapleJuiceTask(args *MapleJuiceTaskArgs, result *RPCResultType) error {
	SLOG.Print("task added to TaskQueue")
	mj.TaskQueue <- args
	*result = RPC_SUCCESS
	return nil
}

func (mj *MapleJuiceService) processMapleJuiceTasks() {
	for {
		task := <-mj.TaskQueue
		if task.TaskType == MapleTask {
			mj.dispatchMapleTask(task)
		} else if task.TaskType == JuiceTask {
			mj.dispatchJuiceTask(task)
		}
	}
}

func (mj *MapleJuiceService) dispatchMapleTask(args *MapleJuiceTaskArgs) {
	liveNodeCount := len(mj.SelfNode.MbList.Member_map)
	mj.SelfNode.FailureNodeChan = make(chan int, liveNodeCount)
	// 1. split input files TBD

	// 2. get all filenames in the dir
	SLOG.Printf("[MAPLE] starting maple task with exe: %s, src_dir: %s", args.Exe, args.Path)
	files := mj.SelfNode.ListFileInDirRequest(args.Path)
	// 3. assign files to machines,func PartitionFiles(files, num_workers) -> map of {int(node_id):string(files)}
	//    TODO: based on data locality:
	worker_and_files := mj.SelfNode.PartitionFiles(files, args.NumWorkers, "hash")
	SLOG.Printf("[dispatchMapleTask] worker and files: %+v", worker_and_files)
	// 4. goroutine each call one worker to start maple task
	waitChan := make(chan int, args.NumWorkers)
	for workerID, filesList := range worker_and_files {
		if len(filesList) > 0 {
			workerNode := mj.SelfNode.MbList.GetNode(workerID)
			workerAddress := workerNode.Ip + ":" + workerNode.RPC_Port
			SLOG.Printf("[dispatchMapleTask] telling workderID: %d to process files: %+q", workerID, filesList)
			go CallMapleRequest(workerID, workerAddress, filesList, args, waitChan)
		} else {
			waitChan <- workerID
		}
	}

	// 5. wait for ack using success channel and fail channel
	completeTaskCount := args.NumWorkers
	for completeTaskCount > 0 {
		select {
		case workerID := <-waitChan:
			completeTaskCount -= 1
			SLOG.Printf("[DispatchMapleTask] work done! workerID: %d, Files: %+q ... %d/%d remaining", workerID, worker_and_files[workerID], completeTaskCount, args.NumWorkers)
		case failureWorkerID := <-mj.SelfNode.FailureNodeChan:
			SLOG.Printf("[DispatchMapleTask] work from workerid: %d has failed, finding a new worker!", failureWorkerID)
			mj.reDispatchMapleTask(failureWorkerID, worker_and_files, waitChan, args)
		}
	}
	// 6. send success message to client
	// NotYetImplemented
	SLOG.Print("[DispatchMapleTask] Success!")
}

// TODO: test this
func (mj *MapleJuiceService) reDispatchMapleTask(failureWorkerID int, worker_and_files map[int][]string, waitChan chan int, args *MapleJuiceTaskArgs) {
	newWorkerId := -1
	for nodeId, _ := range mj.SelfNode.MbList.Member_map {
		if _, exists := worker_and_files[nodeId]; !exists {
			newWorkerId = nodeId
			break
		}
	}
	if newWorkerId == -1 {
		SLOG.Fatal("unexpected no worker available situation")
		// TODO: consider same node receive two maple tasks, what should we do?
		// 1. we can make two jobs concurrent, but we can't use same dir to store imtermedia files.
		// 2. we can make two jobs sequential
	}
	worker_and_files[newWorkerId] = worker_and_files[failureWorkerID]
	delete(worker_and_files, failureWorkerID)
	newWorkerNode := mj.SelfNode.MbList.GetNode(newWorkerId)
	newWorkerAddress := newWorkerNode.Ip + ":" + newWorkerNode.RPC_Port
	go CallMapleRequest(newWorkerId, newWorkerAddress, worker_and_files[newWorkerId], args, waitChan)
}

func CallMapleRequest(workerID int, workerAddress string, files []string, args *MapleJuiceTaskArgs, waitChan chan int) {
	taskID := getHashID(strings.Join(files[:], ","))
	mapleTaskDescription := &MapleTaskDescription{strconv.Itoa(taskID), args.Exe, args.Prefix, files}
	client, err := rpc.Dial("tcp", workerAddress)
	if err != nil {
		SLOG.Printf("[ForwardMJ] Dial failed, address: %s", workerAddress)
		return
	}
	defer client.Close()
	var reply RPCResultType
	sendErr := client.Call(MapleJuiceServiceName+workerAddress+".StartMapleTask", mapleTaskDescription, &reply)
	if sendErr != nil {
		SLOG.Println("[ForwardMJ] call MapleTask err:", sendErr)
	}
	waitChan <- workerID
}

func (node *Node) PartitionFiles(files []string, numWorkers int, partitionMethod string) map[int][]string {
	workerMap := make(map[int][]string)
	if partitionMethod == "hash" {
		partitionedFiles := make([][]string, numWorkers)
		for i, _ := range partitionedFiles {
			partitionedFiles[i] = []string{}
		}
		for _, file := range files {
			hashIndex := getHashID(file) % numWorkers
			partitionedFiles[hashIndex] = append(partitionedFiles[hashIndex], file)
		}
		workerId := node.MbList.GetNode(node.Id).next.Id
		for i := 0; i < numWorkers; i++ {
			workerMap[workerId] = partitionedFiles[i]
			workerId = node.MbList.GetNode(workerId).next.Id
		}
	} else if partitionMethod == "range" {
		minFiles := len(files) / numWorkers
		extra := len(files) % numWorkers
		workerId := node.MbList.GetNode(node.Id).next.Id
		file_i := 0
		for i := 0; i < numWorkers; i++ {
			for j := 0; j < minFiles; j++ {
				workerMap[workerId] = append(workerMap[workerId], files[file_i])
				file_i++
			}
			if extra > 0 {
				workerMap[workerId] = append(workerMap[workerId], files[file_i])
				file_i++
				extra--
			}
			workerId = node.MbList.GetNode(workerId).next.Id
		}
		if file_i != len(files) {
			SLOG.Fatal("[PartitionFiles] assertion error")
		}
	} else {
		SLOG.Fatal("wrong partition moethod")
	}
	return workerMap
	// Data locality partition:
	// for _, file := range files {
	// 	assigned := false
	// 	for workerID, workerFiles := range workerMap {
	// 		var K int
	// 		if len(node.MbList.Member_map) < DUPLICATE_CNT {
	// 			K = len(node.MbList.Member_map)
	// 		} else {
	// 			K = DUPLICATE_CNT
	// 		}
	// 		log.Print(len(node.MbList.GetNextKNodes(workerID, K)))
	// 		nextKID := node.MbList.GetNextKNodes(workerID, K-1)[K-1].Id
	// 		hashid := getHashID(file)
	// 		if IsInCircleRange(hashid, workerID+1, nextKID) && len(workerFiles) < max_files_per_worker {
	// 			workerMap[workerID] = append(workerFiles, file)
	// 			assigned = true
	// 			break
	// 		}
	// 	}
	// 	if !assigned {
	// 		for workerID, workerFiles := range workerMap {
	// 			if len(workerFiles) < max_files_per_worker {
	// 				workerMap[workerID] = append(workerFiles, file)
	// 				break
	// 			}
	// 		}
	// 	}
	// }
	return workerMap
}

func (mj *MapleJuiceService) dispatchJuiceTask(args *MapleJuiceTaskArgs) {
	/** TODO:
	 * 1. collect intermediate files based on prefix
	 * 2. partition files to reducers range or hash (similar to assign file)
	 * 3.
	 **/
}

/*****
 * Worker:
 *****/
func (mj *MapleJuiceService) StartMapleTask(des *MapleTaskDescription, result *RPCResultType) error {
	*result = RPC_DUMMY
	return mj.SelfNode.StartMapleTask(des)
}
