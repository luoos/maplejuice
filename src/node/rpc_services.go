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
	"log"
	"net"
	"net/rpc"
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
		log.Fatal("ListenTCP error:", err)
	}
	node.file_service_on = true
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Accept error:", err)
		}

		go rpc.ServeConn(conn)
	}
}

/* Callee begin */
func (fileService *FileService) PutFileRequest(args PutFileArgs, result *RPCResultType) error {
	// args should have three elements: [localFilePath, sdfsFileName, forceUpdate]
	// _, sdfsFileName := args[0], args[1] // TODO: use localFilePath, (args[0])
	// _, err := strconv.ParseBool("true") // TODO: use forceUpdate
	// if err != nil {
	// 	SLOG.Fatal(err)
	// }
	// addressList := fileService.node.GetAddressWithSDFSFileName(sdfsFileName)

	// filePath, sdfsFileName := args[0], args[1]
	// filename := filepath.Base(filePath)
	// hashId := getHashID(filename)
	/*TODO:
	1. find machines responsible for this file
	2. collect timestamp from above machines through RPC
		2.1 if timestamp is not nil and and last update is within 60s, return RPC_CAUTION
		2.2 otherwise transfer the file to responsible machines and wait for 3 ACK, return RPC_SUCCESS
	*/
	// if GetMillisecond-file_ts < MIN_UPDATE_INTERVAL {
	// TODO:	promp to user
	// }
	targetAddresses := fileService.node.GetResponsibleAddresses(args.SdfsName)
	masterId := fileService.node.GetMasterID(args.SdfsName)
	ts := GetMillisecond()
	data, err := ioutil.ReadFile(args.LocalName)
	if err != nil {
		SLOG.Println(err)
		*result = RPC_FAIL
		return err
	}
	c := make(chan int, DUPLICATE_CNT)
	for _, addr := range targetAddresses {
		go PutFile(masterId, ts, addr, args.SdfsName, data, c)
	}

	for i := 0; i < WRITE_QUORUM && i < len(targetAddresses); i++ {
		select {
		case <-c:
			continue
		case <-time.After(10 * time.Second):
			SLOG.Printf("[WTF] waiting too long when putting file: %s", args.LocalName)
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

func (fileService *FileService) Ls(sdfsfilename string, addrs *[]string) error {
	*addrs = fileService.node.GetResponsibleHostname(sdfsfilename)
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
		log.Fatal("send_err:", send_err)
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
		log.Fatal("send_err:", send_err)
	}
	return send_err
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
