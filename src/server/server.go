package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"os/exec"
	"strconv"
	"strings"
)

type CmdService struct{}

func (p *CmdService) Exec(cmd_str string, output *string) error {
	log.Println("Exec: " + cmd_str)
	parts := strings.Fields(cmd_str)
	head := parts[0]
	args := parts[1:]
	out, err := exec.Command(head, args...).Output()
	*output = string(out)
	if err != nil {
		log.Panic(err)
		return err
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
