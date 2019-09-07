package main

import (
	"bufio"
	"flag"
	"log"
	"net"
	"net/rpc"
	"os"
	"regexp"
	. "result"
	"strconv"
)

type GrepService struct{}

func (p *GrepService) Grep(reg string, machineResult *MachineResult) error {
	filename := "sample_logs/sample.log" // TODO: this is a test file
	fileResult := FileResult{Name: filename}
	matchFile(&fileResult, reg)

	*machineResult = MachineResult{Name: "MachineName", Files: []FileResult{fileResult}}
	return nil
}

func matchFile(fileResult *FileResult, reg string) {
	filename := fileResult.Name
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
		fileResult.Err = err.Error()
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := make([][]string, 0)
	i := 0
	r, _ := regexp.Compile(reg) // TODO: handle err
	for scanner.Scan() {
		l := scanner.Text()
		if r.MatchString(l) {
			lines = append(lines, []string{strconv.Itoa(i), l})
		}
		i++
	}
	fileResult.Lines = lines
}

var port = flag.Int("port", 8000, "The port to receive connect; defaults to 8000.")

func main() {
	flag.Parse()

	address := ":" + strconv.Itoa(*port)
	rpc.RegisterName("GrepService", new(GrepService))
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
