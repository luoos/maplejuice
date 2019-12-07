/*
This file defines maple juice rpc services for a node.

Includes:

1. Client starts a maple
2. Client starts a juice
*/

package node

import (
	"fmt"
	"net"
	"net/rpc"
	. "slogger"
	"strconv"
)

// MapleJuiceServiceName ...
const MapleJuiceServiceName = "MapleJuiceService"
const JuicePartitionMethod = "range"
const DcliReceiverPort = "8013"

type MapleJuiceTaskType int8

// Task names
const (
	MapleTask MapleJuiceTaskType = 1
	JuiceTask MapleJuiceTaskType = 2
)

//MapleJuiceTaskArgs ...
type MapleJuiceTaskArgs struct {
	TaskType    MapleJuiceTaskType // "MapleTask" or "JuiceTask"
	Exe         string
	NumWorkers  int
	InputPath   string // sdfs_src_dir for maple, prefix for juice
	OutputPath  string // prefix for maple, sdfs_dest_filename for juice
	ClientAddr  string
	DeleteInput bool
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
		mj.dispatchMapleJuiceTask(task)
	}
}

func (mj *MapleJuiceService) dispatchMapleJuiceTask(args *MapleJuiceTaskArgs) {
	/*****
	 * 1. split input files
	 * 2. collect intermediate files based on prefix
	 * 3. partition files to reducers range or hash (similar to assign file)
	 * 4. goroutine each call one worker to start juice task
	 * 5. wait for ack using success channel and fail channel
	 * 6. merge received files
	 * 7. delete input if neccessary
	 * 8. send success message to client
	 *****/
	// handle failure using a channel from failure detector
	liveNodeCount := len(mj.SelfNode.MbList.Member_map)
	mj.SelfNode.FailureNodeChan = make(chan int, liveNodeCount)

	// 1.
	// tell everyone to set state
	mj.SelfNode.SetMJState(args.TaskType)

	// 2.
	files := mj.SelfNode.ListFileInDirRequest(args.InputPath)
	if args.TaskType == MapleTask {
		SLOG.Printf("[MAPLE] starting maple task with exe: %s, src_dir: %s", args.Exe, args.InputPath)
	} else {
		SLOG.Printf("[JUICE] starting juice task with exe: %s, src_prefix: %s", args.Exe, args.InputPath)
	}

	// 3.
	partitionMethod := "range"
	if args.TaskType == JuiceTask {
		partitionMethod = JuicePartitionMethod
	}
	worker_and_files := mj.SelfNode.PartitionFiles(files, args.NumWorkers, partitionMethod)
	if args.TaskType == MapleTask {
		SLOG.Printf("[dispatchMapleJuiceTask] worker and files: %+v", worker_and_files)
	}
	// 4.
	waitChan := make(chan int, args.NumWorkers)
	workerTaskID := make(map[int]int)
	taskId := 1
	for workerID, filesList := range worker_and_files {
		workerTaskID[workerID] = taskId
		if len(filesList) > 0 {
			workerNode := mj.SelfNode.MbList.GetNode(workerID)
			workerAddress := workerNode.Ip + ":" + workerNode.RPC_Port
			if args.TaskType == MapleTask {
				SLOG.Printf("[dispatchMapleJuiceTask] telling workderID: %d to process files: %+q", workerID, filesList)
			} else {
				SLOG.Printf("[dispatchMapleJuiceTask] telling workderID: %d to process files", workerID)
			}
			go CallMapleJuiceRequest(taskId, workerID, workerAddress, filesList, args, waitChan)
		} else {
			waitChan <- workerID
		}
		taskId++
	}

	// 5.
	completeTaskCount := args.NumWorkers
	for completeTaskCount > 0 {
		select {
		case workerID := <-waitChan:
			completeTaskCount--
			if args.TaskType == MapleTask {
				SLOG.Printf("[DispatchMapleJuiceTask] work done! workerID: %d, Files: %+q ... %d/%d remaining", workerID, worker_and_files[workerID], completeTaskCount, args.NumWorkers)
			} else {
				SLOG.Printf("[DispatchMapleJuiceTask] work done! workerID: %d, ... %d/%d remaining", workerID, completeTaskCount, args.NumWorkers)
			}
			delete(worker_and_files, workerID)
		case failureWorkerID := <-mj.SelfNode.FailureNodeChan:
			SLOG.Printf("[DispatchMapleJuiceTask] work from workerid: %d has failed, finding a new worker!", failureWorkerID)
			intermediate_dir_name := FormatTempDirName("output", strconv.Itoa(workerTaskID[failureWorkerID]), args.OutputPath)
			SLOG.Printf("[DispatchMapleJuiceTask] deleting all intermediate files written by taskID: %d, the dir name is: %s", workerTaskID[failureWorkerID], intermediate_dir_name)
			mj.SelfNode.DeleteSDFSDirRequest(intermediate_dir_name)
			mj.reDispatchMapleJuiceTask(args.TaskType, failureWorkerID, worker_and_files, workerTaskID, waitChan, args)
		}
	}

	// 6.

	SLOG.Printf("[DispatchMapleJuiceTask] tell every to merge temp files")
	mj.SelfNode.MergeDirRequest(args.OutputPath)

	// 7.
	if args.DeleteInput {
		if args.TaskType == JuiceTask {
			mj.SelfNode.DeleteSDFSDirRequest(args.InputPath)
		} else {
			SLOG.Print("unexpected deleteInput")
		}
	}

	// tell everyone to duplicate for failured files
	// if args.TaskType == MapleTask {
	// 	mj.SelfNode.DuplicateReplicaRequest()
	// }

	// 8.
	msg := "[Maple Task] Finished!"
	if args.TaskType == JuiceTask {
		msg = "[Juice Task] Finished!"
	}
	ReplyTaskResultToDcli(msg, args.ClientAddr)
}

func ReplyTaskResultToDcli(message, clientAddress string) {
	conn, err := net.Dial("tcp", clientAddress)
	if err != nil {
		SLOG.Println(err)
		return
	}
	fmt.Fprintf(conn, message+"\n")
	conn.Close()
	SLOG.Print(message)
}

// TODO: test this
func (mj *MapleJuiceService) reDispatchMapleJuiceTask(taskType MapleJuiceTaskType, failureWorkerID int, worker_and_files map[int][]string, workerTaskID map[int]int, waitChan chan int, args *MapleJuiceTaskArgs) {
	newWorkerId := -1
	for nodeId, _ := range mj.SelfNode.MbList.Member_map {
		if _, exists := worker_and_files[nodeId]; !exists {
			newWorkerId = nodeId
			break
		}
	}
	if newWorkerId == -1 {
		SLOG.Fatal("unexpected no worker available situation")
	}
	worker_and_files[newWorkerId] = worker_and_files[failureWorkerID]
	workerTaskID[newWorkerId] = workerTaskID[failureWorkerID]
	delete(worker_and_files, failureWorkerID)
	delete(workerTaskID, failureWorkerID)
	newWorkerNode := mj.SelfNode.MbList.GetNode(newWorkerId)
	newWorkerAddress := newWorkerNode.Ip + ":" + newWorkerNode.RPC_Port
	if taskType == MapleTask {
		SLOG.Printf("[reDispatchMapleJuiceTask] telling new workderID: %d to process files: %+q", newWorkerId, worker_and_files[newWorkerId])
	} else {
		SLOG.Printf("[reDispatchMapleJuiceTask] telling new workderID: %d to process files", newWorkerId)
	}
	go CallMapleJuiceRequest(workerTaskID[newWorkerId], newWorkerId, newWorkerAddress, worker_and_files[newWorkerId], args, waitChan)
}

func CallMapleJuiceRequest(taskID, workerID int, workerAddress string, files []string, args *MapleJuiceTaskArgs, waitChan chan int) {
	// taskID = strconv.Itoa(getHashID(strings.Join(files[:], ",")))
	taskDescription := &TaskDescription{
		TaskType:   args.TaskType,
		TaskID:     strconv.Itoa(taskID),
		ExeFile:    args.Exe,
		InputFiles: files,
		OutputPath: args.OutputPath,
	}
	client, err := rpc.Dial("tcp", workerAddress)
	if err != nil {
		SLOG.Printf("[CallMapleJuiceRequest] Dial failed, address: %s", workerAddress)
		return
	}
	defer client.Close()
	var reply RPCResultType
	sendErr := client.Call(MapleJuiceServiceName+workerAddress+".StartMapleJuiceTask", taskDescription, &reply)
	if sendErr != nil {
		SLOG.Println("[CallMapleJuiceRequest] call MapleTask err:", sendErr)
		return
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
			workerMap[workerId] = []string{}
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
}

/*****
 * Worker:
 *****/
func (mj *MapleJuiceService) StartMapleJuiceTask(des *TaskDescription, result *RPCResultType) error {
	*result = RPC_DUMMY
	return mj.SelfNode.StartMapleJuiceTask(des)
}
