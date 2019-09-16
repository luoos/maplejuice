package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"os/exec"
	"strconv"
	"fmt"
)

type CmdService struct{}

func (p *CmdService) Exec(cmd_str string, output *string) error {
	log.Println("Exec: " + cmd_str)
	out, err := exec.Command("bash", "-c", cmd_str).CombinedOutput()
	*output = string(out)
	if err != nil && err.Error() != "exit status 1" {  // ignore exit status 1 (a warning exit code for pattern not found)
		*output = fmt.Sprint(err) + ": " + string(out)
	}
	return nil
}

var port = flag.Int("port", 8000, "The port to receive connect; defaults to 8000.")

func main() {
	flag.Parse()

	address := ":" + strconv.Itoa(*port)
	rpc.RegisterName("CmdService", new(CmdService))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal("ListenTCP error: ", err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Accept error:", err)
		}

		go rpc.ServeConn(conn)
	}
}
