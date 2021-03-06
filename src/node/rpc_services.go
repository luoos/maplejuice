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
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	. "slogger"
	"strings"
	"time"
)

const FileServiceName = "SimpleFileService"
const RPC_DEFAULT_PORT = "8011"
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
	Appending   bool
	Tmp         bool
}

type StoreFileArgs struct {
	MasterNodeId int
	SdfsName     string
	Ts           int
	Content      []byte
	Appending    bool
	Tmp          bool
}

const (
	RPC_SUCCESS    RPCResultType = 1 << 0
	RPC_DUMMY      RPCResultType = 1 << 1
	RPC_FAIL       RPCResultType = 1 << 2
	RPC_PROMPT     RPCResultType = 1 << 3
	FILES_ROOT_DIR               = "/apps/files"
)

type FileService struct {
	node *Node
}

func (node *Node) RegisterFileService(address string) error {
	return rpc.RegisterName(FileServiceName+address, &FileService{node: node})
}

func (node *Node) StartRPCService() {
	node.RegisterFileService(node.IP + ":" + node.RPC_Port)
	node.RegisterRPCMapleJuiceService()
	// go node.StartTCPService()
	listener, err := net.Listen("tcp", "0.0.0.0:"+node.RPC_Port)
	if err != nil {
		SLOG.Fatal("ListenTCP error:", err)
	}
	node.file_service_on = true
	// for {
	rpc.Accept(listener)
	// conn, err := listener.Accept()
	// if err != nil {
	// 	SLOG.Fatal("Accept error:", err)
	// }

	// go rpc.ServeConn(conn)
	// }
}

/* Callee begin */
func (fileService *FileService) PutFileRequest(args *PutFileArgs, result *RPCResultType) error {
	return fileService.node.PutFileRequest(args, result)
}
func (node *Node) PutFileRequest(args *PutFileArgs, result *RPCResultType) error {
	fstat, err := os.Stat(args.LocalName)
	if err != nil {
		SLOG.Print(err)
		return err
	}
	if fstat.IsDir() {
		files, err := ioutil.ReadDir(args.LocalName)
		if err != nil {
			SLOG.Printf("err ReadDir: %s", args.LocalName)
			return err
		}
		for _, file := range files {
			localFilename := filepath.Join(args.LocalName, file.Name())
			var sdfsFileName string
			if args.Tmp {
				sdfsFileName = file.Name()
			} else {
				sdfsFileName = filepath.Join(args.SdfsName, file.Name()) // Now we need to store a dir in SDFS
			}
			err := node.IndividualPutFileRequest(sdfsFileName, localFilename, true, args.Appending, args.Tmp, result)
			if err != nil {
				SLOG.Printf("err individual put")
				return err
			}
		}
		return nil
	} else {
		return node.IndividualPutFileRequest(args.SdfsName, args.LocalName, args.ForceUpdate, args.Appending, args.Tmp, result)
	}
}

func (node *Node) IndividualPutFileRequest(sdfsName, localName string, forceUpdate, appending, tmp bool, result *RPCResultType) error {
	if !forceUpdate {
		_, ts := node.GetAddressOfLatestTS(sdfsName)
		if (GetMillisecond() - ts) < MIN_UPDATE_INTERVAL {
			*result = RPC_PROMPT
			return nil
		}
	}
	toHash := sdfsName
	if tmp {
		// if it's tmp file, we need to truncate the tail, which is a metadata
		splitted := strings.Split(sdfsName, "___")
		if len(splitted) != 2 {
			SLOG.Printf("[Error] unexpected tmp filename: %s", sdfsName)
		}
		toHash = splitted[0]
	}
	targetAddresses := node.GetResponsibleAddresses(toHash)
	masterId := node.GetMasterID(toHash)

	ts := GetMillisecond()
	data, err := ioutil.ReadFile(localName)
	if err != nil {
		SLOG.Println(err)
		*result = RPC_FAIL
		return err
	}
	args := &StoreFileArgs{masterId, sdfsName, ts, data, appending, tmp}
	c := make(chan int, DUPLICATE_CNT)
	for _, addr := range targetAddresses {
		// TCPAddr := strings.Split(addr, ":")[0] + ":" + TCP_FILE_PORT
		PutFile(addr, args, c)
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
	return fileService.node.GetFileRequest(args, result)
}
func (node *Node) GetFileRequest(args []string, result *RPCResultType) error {
	sdfsName := args[0]
	localPath := args[1]
	file_addr, _ := node.GetAddressOfLatestTS(sdfsName)
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

func (fileService *FileService) ListFileInDirRequest(sdfsDir string, res *[]string) error {
	*res = fileService.node.ListFileInDirRequest(sdfsDir)
	return nil
}

func (node *Node) ListFileInDirRequest(sdfsDir string) []string {
	fileSet := make(map[string]bool)
	for _, memNode := range node.MbList.Member_map {
		address := memNode.Ip + ":" + memNode.RPC_Port
		filelists := ListFileInSDFSDir(address, sdfsDir)
		for _, f := range filelists {
			fileSet[f] = true
		}
	}
	res := []string{}
	for filepath, _ := range fileSet {
		res = append(res, filepath)
	}
	return res
}

func (node *Node) ListFilesWithPrefixRequest(prefix string) []string {
	fileSet := make(map[string]bool)
	for _, memNode := range node.MbList.Member_map {
		address := memNode.Ip + ":" + memNode.RPC_Port
		fileLists := ListFilesWithPrefixInNode(address, prefix)
		for _, f := range fileLists {
			fileSet[f] = true
		}
	}

	res := []string{}
	for sdfsName, _ := range fileSet {
		res = append(res, sdfsName)
	}
	return res
}

func (fileService *FileService) DeleteSDFSDirRequest(sdfsdir string, result *RPCResultType) error {
	*result = RPC_SUCCESS
	return fileService.node.DeleteSDFSDirRequest(sdfsdir)
}

func (node *Node) DeleteSDFSDirRequest(sdfsdir string) error {
	for _, memNode := range node.MbList.Member_map {
		address := memNode.Ip + ":" + memNode.RPC_Port
		err := DeleteSDFSDir(address, sdfsdir)
		if err != nil {
			SLOG.Println("[DeleteSDFSDirRequest] err: ", err)
			return err
		}
	}
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
	hosts := []string{}
	for _, addr := range addressList {
		host := CheckFile(sdfsfilename, addr)
		if host != "" {
			hosts = append(hosts, host)
		}
	}
	*hostnames = hosts
	return nil
}

func (fileService *FileService) GetTimeStamp(sdfsFileName string, timestamp *int) error {
	*timestamp = fileService.node.FileList.GetTimeStamp(sdfsFileName)
	return nil
}

func (fileService *FileService) StoreFileToLocal(args *StoreFileArgs, result *RPCResultType) error {
	var err error
	if args.Tmp {
		err = fileService.node.FileList.StoreTmpFile(args.SdfsName, fileService.node.Root_dir, args.Ts, args.MasterNodeId, args.Content)
	} else if args.Appending {
		err = fileService.node.FileList.AppendFile(args.SdfsName, fileService.node.Root_dir, args.Ts, args.MasterNodeId, args.Content)
	} else {
		err = fileService.node.FileList.StoreFile(args.SdfsName, fileService.node.Root_dir, args.Ts, args.MasterNodeId, args.Content)
	}

	if err != nil {
		SLOG.Println(err)
		*result = RPC_FAIL
	} else {
		*result = RPC_SUCCESS
	}
	return err
}

func (fileService *FileService) CheckFileExists(sdfsfilename string, hostname *string) error {
	if fileService.node.FileList.GetFileInfo(sdfsfilename) != nil {
		*hostname = fileService.node.Hostname
		return nil
	}
	return fmt.Errorf("file not exitst in CheckFileExists")
}

func (fileService *FileService) ServeLocalFile(sdfsfilename string, result *[]byte) error {
	data, err := fileService.node.FileList.ServeFile(sdfsfilename)
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

func (fileService *FileService) ListFileInLocalDir(dir string, result *[]string) error {
	*result = fileService.node.FileList.ListFileInDir(dir)
	return nil
}

func (fileService *FileService) ListFilesWithPrefix(prefix string, result *[]string) error {
	*result = fileService.node.FileList.ListFilesWithPrefix(prefix)
	return nil
}

func (fileService *FileService) DeleteSDFSDir(dir string, result *RPCResultType) error {
	fileService.node.FileList.DeleteSDFSDir(dir)
	*result = RPC_SUCCESS
	return nil
}

/* Callee end */

/* Caller begin */

func ListFileInSDFSDir(address, dir string) []string {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		SLOG.Printf("[ListFileInSDFSDir] Dial failed, address: %s", address)
		return []string{}
	}
	defer client.Close()
	var result []string
	send_err := client.Call(FileServiceName+address+".ListFileInLocalDir", dir, &result)
	if send_err != nil {
		SLOG.Println("send_err:", send_err)
	}
	return result
}

func ListFilesWithPrefixInNode(address, prefix string) []string {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		SLOG.Printf("[ListFilesWithPrefix] Dial failed, address: %s", address)
		return []string{}
	}
	defer client.Close()
	var result []string
	send_err := client.Call(FileServiceName+address+".ListFilesWithPrefix", prefix, &result)
	if send_err != nil {
		SLOG.Println("[ListFilesWithPrefix] send_err:", send_err)
	}
	return result
}

func PutFile(address string, args *StoreFileArgs, c chan int) {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		SLOG.Printf("[PutFile] Dial failed, address: %s", address)
		c <- 1
		// when two node failed, some putfile may not be able to dial, so file is not send to them.
		// but duplicate will fix this issue so we still send ack to channel and pretend it succeed
		// so other put job will proceed.
		return
	}
	defer client.Close()
	var reply RPCResultType
	send_err := client.Call(FileServiceName+address+".StoreFileToLocal", args, &reply)
	if send_err != nil {
		SLOG.Println("send_err:", send_err)
	}
	// SLOG.Printf("[PutFile] destination: %s, filename: %s", address, args.SdfsName)
	c <- 1
}

// func PutFile(address string, args *StoreFileArgs, c chan int) {
// 	conn, err := net.Dial("tcp", address)
// 	if err != nil {
// 		SLOG.Printf("[PutFile] Dial failed, address: %s", address)
// 		return
// 	}
// 	defer conn.Close()
// 	conn.Write([]byte("PUT\n"))
// 	conn.Write([]byte(fmt.Sprintf("%d\n", args.MasterNodeId)))
// 	conn.Write([]byte(args.SdfsName + "\n"))
// 	conn.Write([]byte(fmt.Sprintf("%d\n", args.Ts)))
// 	conn.Write([]byte(fmt.Sprintf("%t\n", args.Appending)))
// 	for i := 0; i < len(args.Content); i += TCPBufferSize {
// 		end := i + TCPBufferSize
// 		if i+TCPBufferSize > len(args.Content) {
// 			end = len(args.Content)
// 		}
// 		sendBuffer := args.Content[i:end]
// 		conn.Write(sendBuffer)
// 		if end >= len(args.Content) {
// 			break
// 		}
// 	}
// 	// SLOG.Printf("[PutFile] destination: %s, filename: %s", address, args.SdfsName)
// 	c <- 1
// }

func CheckFile(sdfsfilename, address string) string {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		SLOG.Printf("[CheckFile] Dial failed, address: %s", address)
		return ""
	}
	defer client.Close()
	var hostname string
	err = client.Call(FileServiceName+address+".CheckFileExists", sdfsfilename, &hostname)
	if err != nil {
		SLOG.Println("Call CHECK FILE EXITS err: ", err)
		return ""
	}
	return hostname
}

func GetFile(address, sdfsfilename string, data *[]byte) error {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer client.Close()
	send_err := client.Call(FileServiceName+address+".ServeLocalFile", sdfsfilename, data)
	if send_err != nil {
		SLOG.Println("send_err:", send_err)
	}
	return send_err
}

func DeleteFile(address, sdfsName string, c chan string) error {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer client.Close()
	var result RPCResultType
	err = client.Call(FileServiceName+address+".DeleteLocalFile", sdfsName, &result)
	if err != nil || result == RPC_FAIL {
		SLOG.Printf("Delete File Failure, address: %s, sdfsName: %s", address, sdfsName)
		return err
	}
	c <- address
	return err
}

func DeleteSDFSDir(address, dir string) error {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer client.Close()
	var result RPCResultType
	err = client.Call(FileServiceName+address+".DeleteSDFSDir", dir, &result)
	return err
}

func CallGetTimeStamp(address, sdfsFileName string, c chan Pair) {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		SLOG.Printf("[CallGetTimeStamp] fail to dial %s", address)
		return
	}
	defer client.Close()
	var timestamp int
	err = client.Call(FileServiceName+address+".GetTimeStamp", sdfsFileName, &timestamp)
	if err != nil {
		SLOG.Fatal(err)
	}
	c <- Pair{address, timestamp}
}

/* Caller end */
