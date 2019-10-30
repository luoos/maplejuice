/*
This file defines rpc services for a node.

Includes:

1. Client puts a file
2. Client deletes a file
3. Client uses command "ls"
*/

package node

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	. "slogger"
	"strconv"
	"strings"
)

const FileServiceName = "SimpleFileService"
const FILE_SERVICE_DEFAULT_PORT = "8011"
const READ_QUORUM = 2
const MIN_UPDATE_INTERVAL = 60 * 1000

type RPCResultType int8

const (
	RPC_SUCCESS     RPCResultType = 1 << 0
	LOCAL_PATH_ROOT               = "/apps/files"
)

type FileService struct {
	node *Node
}

type FileServiceInterface = interface {
	PutFileRequest(args []string, code *RPCResultType) error
	GetTimeStamp(sdfsFileName string, timestamp *int) error
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

func (node *Node) StartRPCFileService(port string) {
	node.RegisterFileService(node.IP+":"+port, &FileService{node: node})
	listener, err := net.Listen("tcp", "0.0.0.0:"+port)
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

func (fileService *FileService) getAddressOfLatestTS(sdfsfilename string) (string, int) {
	ip_list := fileService.node.GetResponsibleIPs(sdfsfilename)
	c := make(chan string, 4)
	for _, ip := range ip_list {
		address := ip + ":" + FILE_SERVICE_DEFAULT_PORT
		go CallGetTimeStamp(address, sdfsfilename, c)
	}
	max_timestamp := -1
	max_address := ""
	for i := 0; i < READ_QUORUM; i++ {
		val := <-c
		vals := strings.Split(val, " ")
		address := vals[0]
		timestamp, err := strconv.Atoi(vals[1])
		if err != nil {
			log.Fatal(err)
		}
		if timestamp > max_timestamp {
			max_timestamp = timestamp
			max_address = address
		}
	}
	return max_address, max_timestamp
}

/* Callee begin */
func (fileService *FileService) PutFileRequest(args []string, result *RPCResultType) error {
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

func (fileService *FileService) GetFileRequest(sdfsfilename string, result *string) error {
	file_addr, _ := fileService.getAddressOfLatestTS(sdfsfilename)
	*result = GetFile(file_addr, sdfsfilename)
	return nil
}

func (fileService *FileService) Ls(sdfsfilename string, addrs *[]string) error {
	ids := fileService.node.GetFirstKReplicaNodeID(sdfsfilename, 4)
	res := []string{}
	for _, id := range ids {
		mem_node := fileService.node.MbList.GetNode(id)
		res = append(res, mem_node.Ip+":"+mem_node.Port)
	}
	*addrs = res
	return nil
}

func (fileService *FileService) GetTimeStamp(sdfsFileName string, timestamp *int) error {
	*timestamp = fileService.node.FileList.GetTimeStamp(sdfsFileName)
	return nil
}

func (fileService *FileService) StoreFileToLocal(args []string, result *bool) error {
	masterNodeID, err := strconv.Atoi(args[0])
	sdfsfilename := args[1]
	local_path_root := args[2]
	timestamp, err := strconv.Atoi(args[3])
	content := args[4]
	fileService.node.FileList.PutFileInfo(sdfsfilename, local_path_root, timestamp, masterNodeID)
	content_bytes := []byte(content)
	err = ioutil.WriteFile(local_path_root+"/"+sdfsfilename, content_bytes, 0777)
	if err != nil {
		SLOG.Print(err)
	}
	*result = true
	return nil
}

func (fileService *FileService) ServeLocalFile(sdfsfilename string, result *string) error {
	fileinfo := fileService.node.FileList.GetFileInfo(sdfsfilename)
	data, err := ioutil.ReadFile(fileinfo.Localpath)
	if err != nil {
		log.Fatal(err)
	}
	*result = string(data)
	return nil
}

/* Callee end */

/* Caller begin */

/** from client **/
func CallPutFileRequest(address, src, dest string, forceUpdate bool) RPCResultType {
	/* If forceUpdate is false,
	 */
	client := DialFileService(address)
	var reply RPCResultType
	err := client.Call(FileServiceName+address+".PutFileRequest", []string{src, dest, strconv.FormatBool(forceUpdate)}, &reply)
	if err != nil {
		SLOG.Fatal(err)
	}
	return reply
}

/** from coordinator **/
func PutFile(masterNodeID int, timestamp int, address, local_path_root, sdfsfilename, content string) {
	sender := DialFileService(address)
	reply := false
	send_err := sender.Call(FileServiceName+address+".StoreFileToLocal", []string{strconv.Itoa(masterNodeID), sdfsfilename, local_path_root, strconv.Itoa(timestamp), content}, &reply)
	if !reply || send_err != nil {
		log.Fatal("send_err:", send_err)
	}
}

func GetFile(address, sdfsfilename string) string {
	sender := DialFileService(address)
	var file_content string
	send_err := sender.Call(FileServiceName+address+".ServeLocalFile", sdfsfilename, &file_content)
	if send_err != nil {
		log.Fatal("send_err:", send_err)
	}
	return file_content
}

func CallGetTimeStamp(address, sdfsFileName string, c chan string) {
	sender := DialFileService(address)
	var timestamp int
	err := sender.Call(FileServiceName+address+".GetTimeStamp", sdfsFileName, &timestamp)
	if err != nil {
		SLOG.Fatal(err)
	}
	c <- fmt.Sprintf("%s %d", address, timestamp)
}

/* Caller end */
