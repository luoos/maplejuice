package node

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"plugin"
	. "slogger"
	"strings"
)

type TaskDescription struct {
	TaskType        MapleJuiceTaskType
	TaskID          string
	ExeFile         string
	InputFiles      []string
	OutputPath      string
	MasterAddresses []string // includes backup master
}

func (node *Node) StartMapleJuiceTask(des *TaskDescription) error {
	// 1. Retrieve files and exe into a local tmp dir
	// 1.1 Recreate dir: task_id+prefix as dir name
	input_sub_path := "input___" + des.TaskID + "___" + des.OutputPath
	local_input_path := filepath.Join("/tmp", input_sub_path)
	output_sub_path := "output___" + des.TaskID + "___" + des.OutputPath
	local_output_path := filepath.Join("/tmp", output_sub_path)
	if des.TaskType == JuiceTask {
		local_output_path = filepath.Join("/tmp", des.OutputPath)
	}
	os.RemoveAll(local_input_path)
	os.MkdirAll(local_input_path, 0777)
	if des.TaskType == MapleTask {
		os.RemoveAll(local_output_path)
		os.MkdirAll(local_output_path, 0777)
	}
	// 1.2 Retrieve files from SDFS and store files into above dir
	err := node.GetFilesFromSDFS(des.InputFiles, local_input_path)
	if err != nil {
		SLOG.Println("[GetFilesFromSDFS] files", err)
		return err
	}
	// 1.3 Retrieve exe file into /tmp
	err = node.GetFilesFromSDFS([]string{des.ExeFile}, "/tmp")
	if err != nil {
		SLOG.Println("[GetFilesFromSDFS] exe", err)
		return err
	}
	exe_path := filepath.Join("/tmp", des.ExeFile)

	// 3. load func from exec
	p, _ := plugin.Open(exe_path)
	var f plugin.Symbol
	if des.TaskType == MapleTask {
		f, err = p.Lookup("Maple")
	} else {
		f, err = p.Lookup("Juice")
	}
	if err != nil {
		SLOG.Println(err)
		return err
	}

	// 4. process each file and store tmp result to local dir
	if des.TaskType == MapleTask {
		node.HandleMapleTask(local_input_path, local_output_path, f)
	} else {
		node.HandleJuiceTask(local_input_path, local_output_path, f)
	}

	// 5. append files to SDFS
	args := &PutFileArgs{local_output_path, des.OutputPath, true, true}
	var result RPCResultType
	err = node.PutFileRequest(args, &result)
	if err != nil {
		log.Printf("call PutFileRequest return err")
		return err
	}

	// 6. delete local dir
	os.RemoveAll(local_input_path)
	os.RemoveAll(local_output_path)
	os.RemoveAll(exe_path)
	return nil
}

func (node *Node) HandleMapleTask(input_dir, output_dir string, f plugin.Symbol) {
	mapleFunc := f.(func([]string) map[string]string)
	var files []string
	err := filepath.Walk(input_dir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		SLOG.Println(err)
	}
	for _, file := range files {
		fd, err := os.Open(file)
		if err != nil {
			SLOG.Println(err)
		}
		reader := bufio.NewReader(fd)
		var lines []string
		for i := 0; ; i++ {
			line, err := reader.ReadString('\n')
			lines = append(lines, line)
			if i == 9 || err == io.EOF {
				kvpair := mapleFunc(lines)
				WriteMaplePairToLocal(output_dir, kvpair)
				i = 0
				lines = make([]string, 0)
			}
			if err == io.EOF {
				break
			}
		}
		fd.Close()
	}
}

func WriteMaplePairToLocal(dir string, kvpair map[string]string) {
	// TODO: check special character in key for valid filename
	for k, v := range kvpair {
		output_path := filepath.Join(dir, k)
		f, err := os.OpenFile(output_path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
		if err != nil {
			SLOG.Printf("Fail to open file: %s", output_path)
			return
		}
		_, err = f.WriteString(v + "\n")
		if err != nil {
			SLOG.Printf("Fail to append file: %s", output_path)
			return
		}
		f.Close()
	}
}

func (node *Node) HandleJuiceTask(input_dir, output_file string, f plugin.Symbol) {
	juiceFunc := f.(func(string, []string) map[string]string)
	var files []string
	err := filepath.Walk(input_dir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		SLOG.Println(err)
	}
	for _, file := range files {
		fd, err := os.Open(file)
		key := filepath.Base(file)
		if err != nil {
			SLOG.Println(err)
		}
		reader := bufio.NewReader(fd)
		var lines []string
		var line string
		for err == nil {
			line, err = reader.ReadString('\n')
			line = strings.Trim(line, " \n")
			if line != "" { // discard empty lines
				lines = append(lines, line)
			}
		}
		if err != io.EOF {
			SLOG.Print("[HandleJuiceTask] error reading file ", err)
			return
		}
		kvpair := juiceFunc(key, lines)
		WriteJuicePairToLocal(output_file, kvpair)
		fd.Close()
	}
}

func WriteJuicePairToLocal(outputfile string, kvpair map[string]string) {
	for k, v := range kvpair {
		f, err := os.OpenFile(outputfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
		if err != nil {
			SLOG.Printf("Fail to open file: %s", outputfile)
			return
		}
		_, err = f.WriteString(k + " " + v + "\n")
		if err != nil {
			SLOG.Printf("Fail to append file: %s", outputfile)
			return
		}
		f.Close()
	}
}

func (node *Node) GetFilesFromSDFS(sdfsfiles []string, dir string) error {
	for _, sdfsPath := range sdfsfiles {
		filename := filepath.Base(sdfsPath)
		localPath := filepath.Join(dir, filename)
		file_addr, _ := node.GetAddressOfLatestTS(sdfsPath)
		var data []byte
		err := GetFile(file_addr, sdfsPath, &data)
		if err != nil {
			SLOG.Println(err)
			return err
		}
		err = ioutil.WriteFile(localPath, data, 0777)
		if err != nil {
			SLOG.Println(localPath, err)
			return err
		}
	}
	return nil
}

func dialLocalNode() (*rpc.Client, string) {
	hostname, _ := os.Hostname()
	addr_raw, err := net.LookupIP(hostname)
	if err != nil {
		fmt.Println("Unknown host")
	}
	ip := fmt.Sprintf("%s", addr_raw[0])
	address := ip + ":" + RPC_DEFAULT_PORT
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	return client, address
}
