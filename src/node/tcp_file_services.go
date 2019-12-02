package node

import (
	"io"
	"net"
	. "slogger"
	"strconv"
	"strings"
)

const TCPBufferSize = 1024
const TCP_FILE_PORT = "8012"

const (
	GETRequest string = "GET"
	PUTRequest string = "PUT"
)

func (node *Node) StartTCPService() {
	listener, err := net.Listen("tcp", "0.0.0.0:"+TCP_FILE_PORT)
	if err != nil {
		SLOG.Fatal("ListenTCP error:", err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			SLOG.Fatal("Accept error:", err)
		}
		go node.HandleTCPFileRequest(conn)
	}
}

func (node *Node) HandleTCPFileRequest(conn net.Conn) {
	/***** API:
	 ** Similar to StoreFileToLocal
	 * PUT\n
	 * MasterNodeId\n
	 * SdfsName\n
	 * Ts\n
	 * Appending\n
	 * contents
	 *****
	 ** Similar to serveLocalFile
	 * GET\n
	 * SdfsName
	 *****/
	defer conn.Close()
	content := string(ReadContent(conn))
	lines := strings.Split(content, "\n")
	requestType := lines[0]
	if requestType == PUTRequest {
		args, err := ParsePutArgs(lines)
		if err != nil {
			SLOG.Print("[HandleTCPFILERequest] err ParsePutArgs: ", err)
			return
		}
		node.StoreFileToLocal(args)
		if err != nil {
			SLOG.Print("[HandleTCPFILERequest] err StoreFileToLocal: ", err)
			return
		}
	} else if requestType == GETRequest {

	}
}

func (node *Node) StoreFileToLocal(args *StoreFileArgs) error {
	var err error
	if args.Appending {
		err = node.FileList.AppendFile(args.SdfsName, node.Root_dir, args.Ts, args.MasterNodeId, args.Content)
	} else {
		err = node.FileList.StoreFile(args.SdfsName, node.Root_dir, args.Ts, args.MasterNodeId, args.Content)
	}

	if err != nil {
		SLOG.Println(err)
	}
	return err
}

func ParsePutArgs(lines []string) (*StoreFileArgs, error) {
	masterNodeId, err := strconv.Atoi(lines[1])
	if err != nil {
		return nil, err
	}
	sdfsName := lines[2]
	ts, err := strconv.Atoi(lines[3])
	if err != nil {
		return nil, err
	}
	appending := lines[4] == "true"
	content := []byte(strings.Join(lines[5:], "\n"))
	return &StoreFileArgs{
		MasterNodeId: masterNodeId,
		SdfsName:     sdfsName,
		Ts:           ts,
		Appending:    appending,
		Content:      content,
	}, nil
}

func ReadContent(conn net.Conn) []byte {
	res := []byte{}
	for {
		buffer := make([]byte, TCPBufferSize)
		n, err := conn.Read(buffer)
		if err != nil {
			if err != io.EOF {
				SLOG.Println("read error:", err)
			}
			break
		}
		res = append(res, buffer[:n]...)
	}
	SLOG.Println("[ReadContent] got content")
	return res
}
