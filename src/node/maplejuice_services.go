/*
This file defines maple juice rpc services for a node.

Includes:

1. Client starts a maple
2. Client starts a juice
*/

package node

import (
	"net"
	"net/rpc"
	"path/filepath"
	. "slogger"
)

// MapleJuiceServiceName ...
const MapleJuiceServiceName = "MapleJuiceService"

// Task names
const (
	MapleTask = 1
	JuiceTask = 2
)

//MapleJuiceTaskArgs ...
type MapleJuiceTaskArgs struct {
	TaskName   int // "MapleTask" or "JuiceTask"
	Exe        string
	NumWorkers int
	Prefix     string
	Path       string
	ClientIP   string
	ClientPort string
}

type MapleArgs struct {
	MapleExe string
	Prefix   string
	SrcFiles []string
}

// type JuiceArgs struct {
// 	JuiceExe     string
// 	NumJuices    int
// 	Prefix       string
// 	DestFilename string
// }

type MapleJuiceService struct {
	TaskQueue chan *MapleJuiceTaskArgs
	SelfNode  *Node
}

func (node *Node) RegisterMapleJuiceService(address string, mjService *MapleJuiceService) error {
	return rpc.RegisterName(MapleJuiceServiceName+address, mjService)
}

func (node *Node) StartRPCMapleJuiceService(mjService *MapleJuiceService) *MapleJuiceService {
	mjService = &MapleJuiceService{TaskQueue: make(chan *MapleJuiceTaskArgs, 10), SelfNode: node}
	go mjService.processMapleJuiceTasks()
	node.RegisterMapleJuiceService(node.IP+":"+node.RPC_Port, mjService)
	listener, err := net.Listen("tcp", "0.0.0.0:"+node.RPC_Port)
	if err != nil {
		SLOG.Fatal("ListenTCP error:", err)
	}
	rpc.Accept(listener)
	return mjService
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
	return nil
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
		if task.TaskName == MapleTask {
			mj.dispatchMapleTask(task)
		} else if task.TaskName == JuiceTask {
			mj.dispatchJuiceTask(task)
		}
	}
}

func (mj *MapleJuiceService) dispatchMapleTask(args *MapleJuiceTaskArgs) {
	liveNodeCount := len(mj.SelfNode.MbList.Member_map)
	mj.SelfNode.FailureNodeChan = make(chan int, liveNodeCount)
	// 1. split input files TBD

	// 2. get all filenames in the dir
	SLOG.Printf("[MAPLE] starting maple task with exe: %s", args.Exe)
	files, err := filepath.Glob(filepath.Join(args.Path, "*"))
	if err != nil {
		SLOG.Fatal(err)
	}
	// 3. assign files to machines based on data locality: func AssignFiles(files, num_workers) -> map of {int(node_id):string(files)}
	worker_and_files := mj.SelfNode.AssignFiles(files, args.NumWorkers)

	// 4. goroutine each call one worker to start maple task
	waitChan := make(chan int, args.NumWorkers)
	for workerID, filesList := range worker_and_files {
		workerNode := mj.SelfNode.MbList.GetNode(workerID)
		workerAddress := workerNode.Ip + ":" + workerNode.Port
		go CallMapleRequest(workerID, workerAddress, filesList, args, waitChan)
	}

	// 5. wait for ack using success channel and fail channel
	completeTaskCount := args.NumWorkers
	for completeTaskCount > 0 {
		select {
		case workerID := <-waitChan:
			completeTaskCount -= 1
			SLOG.Printf("[DispatchMapleTask] work done! workerID: %d, %d/%d remaining", workerID, completeTaskCount, args.NumWorkers)
		case failureWorkerID := <-mj.SelfNode.FailureNodeChan:
			mj.reDispatchMapleTask(failureWorkerID, worker_and_files, waitChan, args)
		}
	}
	// 6. send success message to client
}

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
	newWorkerAddress := newWorkerNode.Ip + ":" + newWorkerNode.Port
	go CallMapleRequest(newWorkerId, newWorkerAddress, worker_and_files[newWorkerId], args, waitChan)
}

func CallMapleRequest(workerID int, workerAddress string, files []string, args *MapleJuiceTaskArgs, waitChan chan int) {
	// mapleArgs := &MapleArgs{args.Exe, args.Prefix, files}
	// client, err := rpc.Dial("tcp", workerAddress)
	// if err != nil {
	// 	SLOG.Printf("[ForwardMJ] Dial failed, address: %s", workerAddress)
	// 	return
	// }
	// defer client.Close()
	// var reply RPCResultType
	// sendErr := client.Call(MapleJuiceServiceName+workerAddress+".StartMapleTask", mapleArgs, &reply)
	// if sendErr != nil {
	// 	SLOG.Println("[ForwardMJ] call rpc err:", sendErr)
	// }
	waitChan <- workerID
}

func (node *Node) AssignFiles(files []string, numWorkers int) map[int][]string {
	workerId := node.MbList.GetNode(node.Id).next.Id
	workerMap := make(map[int][]string)
	for i := 0; i < numWorkers; i++ {
		workerMap[workerId] = []string{}
		workerId = node.MbList.GetNode(workerId).next.Id
	}
	i := 0
	for i < len(files) {
		for workerID, workerFiles := range workerMap {
			workerMap[workerID] = append(workerFiles, files[i])
			i++
			if i == len(files) {
				break
			}
		}
	}
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

}
