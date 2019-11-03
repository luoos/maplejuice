/*
This file defines rpc services for a node.

Includes:

1. Client puts a file
2. Client deletes a file
3. Client uses command "ls"
*/

package node

import (
	"io/ioutil"
	"net"
	"net/rpc"
	"os"
	. "slogger"
	"time"
)

const FileServiceName = "SimpleFileService"
const FILE_SERVICE_DEFAULT_PORT = "8011"
const READ_QUORUM = 2
const WRITE_QUORUM = 3
const MIN_UPDATE_INTERVAL = 60 * 1000

type RPCResultType int8

type Pair struct {
	Address string
	Ts      int // Timestamp
}

type PutFileArgs struct {
	LocalName   string
	SdfsName    string
	ForceUpdate bool
}

type StoreFileArgs struct {
	MasterNodeId int
	SdfsName     string
	Ts           int
	Content      []byte
}

const (
	RPC_SUCCESS     RPCResultType = 1 << 0
	RPC_DUMMY       RPCResultType = 1 << 1
	RPC_FAIL        RPCResultType = 1 << 2
	RPC_PROMPT      RPCResultType = 1 << 3
	LOCAL_PATH_ROOT               = "/apps/files"
)

type FileService struct {
	node *Node
}

type FileServiceInterface = interface {
	PutFileRequest(args PutFileArgs, code *RPCResultType) error
	GetTimeStamp(sdfsFileName string, timestamp *int) error
	StoreFileToLocal(args StoreFileArgs, result *RPCResultType) error
}

func (node *Node) RegisterFileService(address string, svc FileServiceInterface) error {
	return rpc.RegisterName(FileServiceName+address, svc)
}

func (node *Node) StartRPCFileService() {
	node.RegisterFileService(node.IP+":"+node.RPC_Port, &FileService{node: node})
	listener, err := net.Listen("tcp", "0.0.0.0:"+node.RPC_Port)
	if err != nil {
		SLOG.Fatal("ListenTCP error:", err)
	}
	node.file_service_on = true
	for {
		conn, err := listener.Accept()
		if err != nil {
			SLOG.Fatal("Accept error:", err)
		}

		go rpc.ServeConn(conn)
	}
}

/* Callee begin */
func (fileService *FileService) PutFileRequest(args PutFileArgs, result *RPCResultType) error {
	fstat, err := os.Stat(args.LocalName)
	if err != nil {
		SLOG.Print(err)
		return err
	}
	if fstat.IsDir() {
		files, err := ioutil.ReadDir(args.LocalName)
		if err != nil {
			SLOG.Printf("err ReadDir: ", args.LocalName)
			return err
		}
		for _, file := range files {
			localFilename := args.LocalName + "/" + file.Name()
			sdfsFileName := file.Name()
			err := fileService.individualPutFileRequest(sdfsFileName, localFilename, true, result)
			if err != nil {
				SLOG.Printf("err individual put")
				return err
			}
		}
		return nil
	} else {
		return fileService.individualPutFileRequest(args.SdfsName, args.LocalName, args.ForceUpdate, result)
	}
}
func (fileService *FileService) individualPutFileRequest(sdfsName, localName string, forceUpdate bool, result *RPCResultType) error {
	_, ts := fileService.node.GetAddressOfLatestTS(sdfsName)
	if !forceUpdate && ((GetMillisecond() - ts) < MIN_UPDATE_INTERVAL) {
		*result = RPC_PROMPT
		return nil
	}
	targetAddresses := fileService.node.GetResponsibleAddresses(sdfsName)
	masterId := fileService.node.GetMasterID(sdfsName)

	ts = GetMillisecond()
	data, err := ioutil.ReadFile(localName)
	if err != nil {
		SLOG.Println(err)
		*result = RPC_FAIL
		return err
	}
	c := make(chan int, DUPLICATE_CNT)
	for _, addr := range targetAddresses {
		go PutFile(masterId, ts, addr, sdfsName, data, c)
	}
	for i := 0; i < WRITE_QUORUM && i < len(targetAddresses); i++ {
		select {
		case <-c:
			continue
		case <-time.After(10 * time.Second):
			SLOG.Printf("[WTF] waiting too long when putting file: %s", localName)
			*result = RPC_FAIL
			return err
		}
	}
	*result = RPC_SUCCESS
	return err
}

// Executed in coordinator
func (fileService *FileService) GetFileRequest(args []string, result *RPCResultType) error {
	sdfsName := args[0]
	localPath := args[1]
	file_addr, _ := fileService.node.GetAddressOfLatestTS(sdfsName)
	var data []byte
	err := GetFile(file_addr, sdfsName, &data)
	if err != nil {
		*result = RPC_FAIL
		return err
	}
	err = ioutil.WriteFile(localPath, data, 0777)
	if err != nil {
		SLOG.Println(localPath, err)
		*result = RPC_FAIL
		return err
	}
	*result = RPC_DUMMY
	return nil
}

func (fileService *FileService) DeleteFileRequest(sdfsName string, result *RPCResultType) error {
	targetAddresses := fileService.node.GetResponsibleAddresses(sdfsName)
	c := make(chan string, DUPLICATE_CNT)
	for _, addr := range targetAddresses {
		go DeleteFile(addr, sdfsName, c)
	}
	received := []string{}
	for i := 0; i < DUPLICATE_CNT && i < len(targetAddresses); i++ {
		select {
		case addr := <-c:
			received = append(received, addr)
			continue
		case <-time.After(5 * time.Second):
			SLOG.Printf("[WTF] waiting too long when deleting file: %s, responding servers: %v", sdfsName, received)
			*result = RPC_FAIL
			return nil
		}
	}
	*result = RPC_SUCCESS
	return nil

}

func (fileService *FileService) Ls(sdfsfilename string, hostnames *[]string) error {
	addressList := fileService.node.GetResponsibleAddresses(sdfsfilename)
	c := make(chan Pair, 4)
	for _, address := range addressList {
		CallGetTimeStamp(address, sdfsfilename, c)
	}
	ids_contains_file := []int{}
	for i := 0; i < len(addressList); i++ {
		pair := <-c
		if pair.Ts > -1 {
			ids_contains_file = append(ids_contains_file, getHashID(pair.Address))
		}
	}
	hostList := []string{}
	for _, id := range ids_contains_file {
		hostname := fileService.node.MbList.GetNode(id).Hostname
		hostList = append(hostList, hostname)
	}
	*hostnames = hostList
	return nil
}

func (fileService *FileService) GetTimeStamp(sdfsFileName string, timestamp *int) error {
	*timestamp = fileService.node.FileList.GetTimeStamp(sdfsFileName)
	return nil
}

func (fileService *FileService) StoreFileToLocal(args StoreFileArgs, result *RPCResultType) error {
	err := fileService.node.FileList.StoreFile(args.SdfsName, fileService.node.File_dir, args.Ts, args.MasterNodeId, args.Content)
	if err != nil {
		SLOG.Println(err)
		*result = RPC_FAIL
	} else {
		*result = RPC_SUCCESS
	}
	return err
}

func (fileService *FileService) ServeLocalFile(sdfsfilename string, result *[]byte) error {
	fileinfo := fileService.node.FileList.GetFileInfo(sdfsfilename)
	data, err := ioutil.ReadFile(fileinfo.Localpath)
	*result = data
	return err
}

func (fileService *FileService) DeleteLocalFile(sdfsName string, result *RPCResultType) error {

	isSuccess := fileService.node.FileList.DeleteFileAndInfo(sdfsName)
	if isSuccess {
		*result = RPC_SUCCESS
	} else {
		*result = RPC_FAIL
	}
	return nil
}

/* Callee end */

/* Caller begin */

func PutFile(masterNodeID int, timestamp int, address, sdfsfilename string, content []byte, c chan int) {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		SLOG.Printf("[PutFile] Dial failed, address: %s", address)
		return
	}
	var reply RPCResultType
	args := StoreFileArgs{masterNodeID, sdfsfilename, timestamp, content}
	send_err := client.Call(FileServiceName+address+".StoreFileToLocal", args, &reply)
	if send_err != nil {
		SLOG.Panic("send_err:", send_err)
	}
	SLOG.Printf("[PutFile] destination: %s, filename: %s", address, sdfsfilename)
	c <- 1
}

func GetFile(address, sdfsfilename string, data *[]byte) error {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return err
	}
	send_err := client.Call(FileServiceName+address+".ServeLocalFile", sdfsfilename, data)
	if send_err != nil {
		SLOG.Panic("send_err:", send_err)
		return send_err
	}
	return nil
}

func DeleteFile(address, sdfsName string, c chan string) error {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return err
	}
	var result RPCResultType
	err = client.Call(FileServiceName+address+".DeleteLocalFile", sdfsName, &result)
	if err != nil {
		SLOG.Printf("Delete File Failure, address: %s, sdfsName: %s", address, sdfsName)
		return err
	}
	c <- address
	return err
}

func CallGetTimeStamp(address, sdfsFileName string, c chan Pair) {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		SLOG.Printf("[CallGetTimeStamp] fail to dial %s", address)
		return
	}
	var timestamp int
	err = client.Call(FileServiceName+address+".GetTimeStamp", sdfsFileName, &timestamp)
	if err != nil {
		SLOG.Fatal(err)
	}
	c <- Pair{address, timestamp}
}

/* Caller end */
