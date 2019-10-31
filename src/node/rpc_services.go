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
)

const FileServiceName = "SimpleFileService"
const FILE_SERVICE_DEFAULT_PORT = "8011"
const READ_QUORUM = 2
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
	masterNodeId int
	SdfsName     string
	Ts           int
	Content      string
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

func DialFileService(address string) *rpc.Client {
	// address: IP + Port, such as "0.0.0.0:8011"
	c, err := rpc.Dial("tcp", address)
	if err != nil {
		SLOG.Fatal(err)
	}
	return c
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

	*result = RPC_SUCCESS
	return nil
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
		*result = RPC_FAIL
		return err
	}
	*result = RPC_DUMMY
	return nil
}

func (fileService *FileService) Ls(sdfsfilename string, addrs *[]string) error {
	*addrs = fileService.node.GetResponsibleAddresses(sdfsfilename)
	return nil
}

func (fileService *FileService) GetTimeStamp(sdfsFileName string, timestamp *int) error {
	*timestamp = fileService.node.FileList.GetTimeStamp(sdfsFileName)
	return nil
}

func (fileService *FileService) StoreFileToLocal(args StoreFileArgs, result *RPCResultType) error {
	filePath := LOCAL_PATH_ROOT + "/" + args.SdfsName
	fileService.node.FileList.PutFileInfo(args.SdfsName, LOCAL_PATH_ROOT, args.Ts, args.masterNodeId)
	content_bytes := []byte(args.Content)
	err := ioutil.WriteFile(filePath, content_bytes, 0777)
	if err != nil {
		SLOG.Print(err)
	}
	*result = RPC_SUCCESS
	return nil
}

func (fileService *FileService) ServeLocalFile(sdfsfilename string, result *[]byte) error {
	fileinfo := fileService.node.FileList.GetFileInfo(sdfsfilename)
	data, err := ioutil.ReadFile(fileinfo.Localpath)
	*result = data
	return err
}

/* Callee end */

/* Caller begin */

func CallLs(address, sdfsfilename string) []string {
	clien := DialFileService(address)
	var addresses []string
	err := clien.Call(FileServiceName+address+".Ls", sdfsfilename, &addresses)
	if err != nil {
		SLOG.Fatal(err)
	}
	return addresses
}

/** from coordinator **/
func PutFile(masterNodeID int, timestamp int, address, sdfsfilename, content string) {
	client := DialFileService(address)
	var reply RPCResultType
	args := StoreFileArgs{masterNodeID, sdfsfilename, timestamp, content}
	send_err := client.Call(FileServiceName+address+".StoreFileToLocal", args, &reply)
	if send_err != nil {
		log.Fatal("send_err:", send_err)
	}
}

func GetFile(address, sdfsfilename string, data *[]byte) error {
	sender := DialFileService(address)
	send_err := sender.Call(FileServiceName+address+".ServeLocalFile", sdfsfilename, data)
	if send_err != nil {
		log.Fatal("send_err:", send_err)
	}
	return send_err
}

func CallGetTimeStamp(address, sdfsFileName string, c chan Pair) {
	sender := DialFileService(address)
	var timestamp int
	err := sender.Call(FileServiceName+address+".GetTimeStamp", sdfsFileName, &timestamp)
	if err != nil {
		SLOG.Fatal(err)
	}
	c <- Pair{address, timestamp}
}

/* Caller end */
