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
	"strings"
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
	 * 6. delete input if neccessary
	 * 7. send success message to client
	 *****/
	// handle failure using a channel from failure detector
	liveNodeCount := len(mj.SelfNode.MbList.Member_map)
	mj.SelfNode.FailureNodeChan = make(chan int, liveNodeCount)

	// 1. TBD

	// 2.
	var files []string
	if args.TaskType == MapleTask {
		files = mj.SelfNode.ListFileInDirRequest(args.InputPath)
		SLOG.Printf("[MAPLE] starting maple task with exe: %s, src_dir: %s", args.Exe, args.InputPath)
	} else {
		files = mj.SelfNode.ListFilesWithPrefixRequest(args.InputPath)
		SLOG.Printf("[JUICE] starting juice task with exe: %s, src_prefix: %s", args.Exe, args.InputPath)
	}

	// 3.
	partitionMethod := "hash"
	if args.TaskType == JuiceTask {
		partitionMethod = JuicePartitionMethod
	}
	worker_and_files := mj.SelfNode.PartitionFiles(files, args.NumWorkers, partitionMethod)
	SLOG.Printf("[dispatchMapleJuiceTask] worker and files: %+v", worker_and_files)

	// 4.
	waitChan := make(chan int, args.NumWorkers)
	for workerID, filesList := range worker_and_files {
		if len(filesList) > 0 {
			workerNode := mj.SelfNode.MbList.GetNode(workerID)
			workerAddress := workerNode.Ip + ":" + workerNode.RPC_Port
			SLOG.Printf("[dispatchMapleJuiceTask] telling workderID: %d to process files: %+q", workerID, filesList)
			go CallMapleJuiceRequest(workerID, workerAddress, filesList, args, waitChan)
		} else {
			waitChan <- workerID
		}
	}

	// 5.
	completeTaskCount := args.NumWorkers
	for completeTaskCount > 0 {
		select {
		case workerID := <-waitChan:
			completeTaskCount--
			SLOG.Printf("[DispatchMapleJuiceTask] work done! workerID: %d, Files: %+q ... %d/%d remaining", workerID, worker_and_files[workerID], completeTaskCount, args.NumWorkers)
			delete(worker_and_files, workerID)
		case failureWorkerID := <-mj.SelfNode.FailureNodeChan:
			SLOG.Printf("[DispatchMapleJuiceTask] work from workerid: %d has failed, finding a new worker!", failureWorkerID)
			mj.reDispatchMapleJuiceTask(args.TaskType, failureWorkerID, worker_and_files, waitChan, args)
		}
	}

	// 6.
	if args.DeleteInput {
		if args.TaskType == JuiceTask {
			mj.SelfNode.DeleteSDFSDirRequest(args.InputPath)
		} else {
			SLOG.Print("unexpected deleteInput")
		}
	}

	// Ask receiver to merge
	allRPCAddress := mj.SelfNode.MbList.GetAllRPCAddresses()
	CallNodesMergeTmpFiles(allRPCAddress)

	// 7.
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
func (mj *MapleJuiceService) reDispatchMapleJuiceTask(taskType MapleJuiceTaskType, failureWorkerID int, worker_and_files map[int][]string, waitChan chan int, args *MapleJuiceTaskArgs) {
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
	delete(worker_and_files, failureWorkerID)
	newWorkerNode := mj.SelfNode.MbList.GetNode(newWorkerId)
	newWorkerAddress := newWorkerNode.Ip + ":" + newWorkerNode.RPC_Port
	go CallMapleJuiceRequest(newWorkerId, newWorkerAddress, worker_and_files[newWorkerId], args, waitChan)
}

func CallMapleJuiceRequest(workerID int, workerAddress string, files []string, args *MapleJuiceTaskArgs, waitChan chan int) {
	taskID := strconv.Itoa(getHashID(strings.Join(files[:], ",")))
	taskDescription := &TaskDescription{
		TaskType:   args.TaskType,
		TaskID:     taskID,
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

func CallNodesMergeTmpFiles(receiverAddress []string) {
	ts := GetMillisecond()
	c := make(chan int, len(receiverAddress))
	for _, address := range receiverAddress {
		go CallSingleNodeMergeTmpFiles(address, ts, c)
	}
	ack_cnt := 0
	for ack_cnt < len(receiverAddress) {
		<-c
		ack_cnt++
	}
}

func CallSingleNodeMergeTmpFiles(address string, ts int, c chan int) {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		SLOG.Printf("[CallNodeMergeTmpFiles] Dial failed, address: %s", address)
		return
	}
	defer client.Close()
	var reply RPCResultType
	err = client.Call(MapleJuiceServiceName+address+".MergeTmpFiles", ts, &reply)
	if err != nil {
		SLOG.Printf("[CallNodeMergeTmpFiles] call err, address: %s", address)
	}
	c <- 1
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
 * Worker
 *****/
func (mj *MapleJuiceService) StartMapleJuiceTask(des *TaskDescription, result *RPCResultType) error {
	*result = RPC_DUMMY
	return mj.SelfNode.StartMapleJuiceTask(des)
}

func (mj *MapleJuiceService) MergeTmpFiles(ts int, result *RPCResultType) error {
	n := mj.SelfNode
	n.FileList.MergeTmpFiles(n.Root_dir+"/tmp", n.Root_dir, ts)
	prevNodeId := n.MbList.GetNode(n.Id).prev.Id
	n.FileList.UpdateMasterID(n.Id, func(fileInfo *FileInfo) bool {
		return IsInCircleRange(fileInfo.HashID, prevNodeId+1, n.Id)
	})
	go n.DuplicateReplica()
	*result = RPC_SUCCESS
	return nil
}
