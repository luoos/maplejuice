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
)

type TaskDescription struct {
	TaskType        MapleJuiceTaskType // Maple or Juice
	TaskID          string             // TODO: backup
	Prefix          string
	ExeFile         string
	Files           []string
	MasterAddresses []string // includes backup master
}

type MapleTaskDescription struct {
	TaskID  string
	ExeFile string
	Prefix  string
	Files   []string
	// MasterAddresses []string
}

func (node *Node) StartMapleTask(des *MapleTaskDescription) error {
	// 1. Retrieve files and exe into a local tmp dir
	// 1.1 Recreate dir: task_id+prefix as dir name
	input_dir_name := "input___" + des.TaskID + "___" + des.Prefix
	input_dir_path := filepath.Join("/tmp", input_dir_name)
	output_dir_name := "output___" + des.TaskID + "___" + des.Prefix
	output_dir_path := filepath.Join("/tmp", output_dir_name)
	_ = os.RemoveAll(input_dir_path)
	_ = os.MkdirAll(input_dir_path, 0777)
	_ = os.RemoveAll(output_dir_path)
	_ = os.MkdirAll(output_dir_path, 0777)
	// 1.2 Retrieve files from SDFS and store files into above dir
	err := node.GetFilesFromSDFS(des.Files, input_dir_path)
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

	p, _ := plugin.Open(exe_path)
	// 3. load func from exec
	f, err := p.Lookup("Maple")
	if err != nil {
		SLOG.Println(err)
		return err
	}
	// 4. process each file and store tmp result to local dir
	node.HandleMapleTask(input_dir_path, output_dir_path, f)
	client, address := dialLocalNode()
	defer client.Close()
	var reply RPCResultType
	// 5. append files to SDFS
	err = client.Call(FileServiceName+address+".PutFileRequest", PutFileArgs{output_dir_path, des.Prefix, true, true}, &reply)
	if err != nil {
		log.Printf("call PutFileRequest return err")
		return err
	}
	// 6. delete local dir
	_ = os.RemoveAll(input_dir_path)
	_ = os.RemoveAll(output_dir_path)
	_ = os.RemoveAll(exe_path)
	return nil
}

// func (node *Node) StartProcessTask(des TaskDescription) {
// 	// 1. Retrieve files and exe into a local tmp dir
// 	// 1.1 Recreate dir: task_id+prefix as dir name
// 	input_dir_name := "input___" + des.TaskID + "___" + des.Prefix
// 	input_dir_path := filepath.Join("/tmp", input_dir_name)
// 	output_dir_name := "output___" + des.TaskID + "___" + des.Prefix
// 	output_dir_path := filepath.Join("/tmp", output_dir_name)

// 	_ = os.RemoveAll(input_dir_path)
// 	_ = os.MkdirAll(input_dir_path, 0777)
// 	_ = os.RemoveAll(output_dir_path)
// 	_ = os.MkdirAll(output_dir_path, 0777)
// 	err := node.GetFilesFromSDFS(des.Files, input_dir_path)
// 	if err != nil {
// 		SLOG.Println(err)
// 		return
// 	}
// 	// 1.2 Retrieve files from SDFS and store files into above dir
// 	// 1.3 Retrieve exe file into /tmp
// 	err = node.GetFilesFromSDFS([]string{des.ExeFile}, "/tmp")
// 	if err != nil {
// 		SLOG.Println(err)
// 		return
// 	}
// 	exe_path := filepath.Join("/tmp", des.ExeFile)

// 	p, _ := plugin.Open(exe_path)
// 	// 2. determine the task type, maple or juice
// 	if des.TaskType == MapleTask {
// 		// 3. load func from exec
// 		f, err := p.Lookup("Maple")
// 		if err != nil {
// 			SLOG.Println(err)
// 			return
// 		}
// 		// 4. process each file and store tmp result to local dir
// 		node.HandleMapleTask(input_dir_path, output_dir_path, f)
// 		client, address := dialLocalNode()
// 		defer client.Close()
// 		var reply RPCResultType
// 		err = client.Call(FileServiceName+address+".PutFileRequest", PutFileArgs{output_dir_path, des.Prefix, true, true}, &reply)
// 		if err != nil {
// 			log.Printf("call PutFileRequest return err")
// 			log.Fatal(err)
// 		}
// 	} else {
// 		// 3. load func from exec
// 		// f, err := p.Lookup("Juice")
// 		// TODO: 4.
// 	}

// 	// 5. append files to SDFS

// 	// 6. delete local dir
// }

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
				node.WritePairToLocal(output_dir, kvpair)
				log.Printf("%+v", kvpair)
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

func (node *Node) WritePairToLocal(dir string, kvpair map[string]string) {
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

// func (node *Node) HandleJuiceTask(dir string, f plugin.Symbol) string {}

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
