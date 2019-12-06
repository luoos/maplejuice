package node

import (
	"io/ioutil"
	"os"
	"path/filepath"
	. "slogger"
	"strings"
	"sync"
)

const FILE_LIST_FILE = "/tmp/file.list"

type FileInfo struct {
	HashID       int
	Sdfsfilename string
	Localpath    string
	Timestamp    int
	MasterNodeID int
	FileLock     *sync.Mutex
	Tmp          bool
}

type FileList struct {
	ID       int
	FileMap  map[string]*FileInfo // Key: sdfsfilename, value: fileinfo
	ListLock *sync.Mutex
}

func CreateFileList(selfID int) *FileList {
	return &FileList{ID: selfID, FileMap: make(map[string]*FileInfo), ListLock: &sync.Mutex{}}
}

func (fl *FileList) ServeFile(sdfsfilename string) ([]byte, error) {
	fileinfo := fl.GetFileInfo(sdfsfilename)
	fileinfo.FileLock.Lock()
	defer fileinfo.FileLock.Unlock()
	data, err := ioutil.ReadFile(fileinfo.Localpath)
	return data, err
}

// PutFileInfoObject is used For testing
func (fl *FileList) PutFileInfoObject(sdfsfilename string, fi *FileInfo) {
	if fl.GetFileInfo(sdfsfilename) != nil {
		SLOG.Printf("%s already exist, updating all metainfo", sdfsfilename)
	}
	fl.FileMap[sdfsfilename] = fi
}

func (fl *FileList) PutFileInfo( // TODO: looks like it's not used??
	sdfsName string,
	path string,
	timestamp int,
	masterNodeID int) {
	if fl.GetFileInfo(sdfsName) != nil {
		SLOG.Printf("%s already exist, updating all metainfo", sdfsName)
	}
	hashid := getHashID(sdfsName)
	fl.PutFileInfoBase(hashid, sdfsName, path, timestamp, masterNodeID, false)
}

func (fl *FileList) PutFileInfoBase(
	hashId int,
	sdfsfilename string,
	abs_path string,
	timestamp int,
	masterNodeID int,
	tmp bool) {
	if _, exist := fl.FileMap[sdfsfilename]; exist {
		fl.FileMap[sdfsfilename].HashID = hashId
		fl.FileMap[sdfsfilename].Localpath = abs_path
		fl.FileMap[sdfsfilename].Timestamp = timestamp
		fl.FileMap[sdfsfilename].MasterNodeID = masterNodeID
		fl.FileMap[sdfsfilename].Tmp = tmp
	} else {
		if _, exist := fl.FileMap[sdfsfilename]; !exist {
			fl.FileMap[sdfsfilename] = &FileInfo{
				HashID:       hashId,
				Sdfsfilename: sdfsfilename,
				Localpath:    abs_path,
				Timestamp:    timestamp,
				MasterNodeID: masterNodeID,
				FileLock:     &sync.Mutex{},
				Tmp:          tmp,
			}
		}
	}
}

func (fl *FileList) StoreFile(
	sdfsName string,
	root_dir string,
	timestamp int,
	masterNodeID int,
	data []byte) error {

	hashId := getHashID(sdfsName)
	return fl.StoreFileBase(hashId, sdfsName, root_dir, timestamp, masterNodeID, data, false, false)
}

func (fl *FileList) StoreTmpFile(
	sdfsName string,
	root_dir string,
	timestamp int,
	masterNodeID int,
	data []byte) error {
	toHash := strings.Split(sdfsName, "___")[0] // like: prefix_key____123, the 123 the maple worker id
	hashId := getHashID(toHash)
	return fl.StoreFileBase(hashId, sdfsName, root_dir, timestamp, masterNodeID, data, false, true)
}

func (fl *FileList) AppendFile(
	sdfsName string,
	root_dir string,
	timestamp int,
	masterNodeID int,
	data []byte) error {
	hashId := getHashID(sdfsName)
	return fl.StoreFileBase(hashId, sdfsName, root_dir, timestamp, masterNodeID, data, true, false)
}

// This should only be used in test
func (fl *FileList) StoreFileBase(
	hashId int,
	sdfsName string,
	root_dir string,
	timestamp int,
	masterNodeID int,
	data []byte,
	appending bool,
	tmp bool) error {

	if tmp {
		root_dir = root_dir + "/tmp"
	}
	abs_path := filepath.Join(root_dir, sdfsName)
	dir, _ := filepath.Split(abs_path)
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		SLOG.Printf("Fail to create dir: %s", dir)
		return err
	}
	if _, exist := fl.FileMap[sdfsName]; !exist {
		fl.ListLock.Lock()
		fl.PutFileInfoBase(hashId, sdfsName, abs_path, timestamp, masterNodeID, tmp)
		fl.ListLock.Unlock()
	}
	fl.FileMap[sdfsName].FileLock.Lock()
	defer fl.FileMap[sdfsName].FileLock.Unlock()
	fl.PutFileInfoBase(hashId, sdfsName, abs_path, timestamp, masterNodeID, tmp)
	if appending {
		f, err := os.OpenFile(abs_path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
		if err != nil {
			SLOG.Printf("Fail to open file: %s", abs_path)
			return err
		}
		_, err = f.Write(data)
		if err != nil {
			SLOG.Printf("Fail to append file: %s", abs_path)
			return err
		}
		f.Close()
	} else {
		err = ioutil.WriteFile(abs_path, data, 0777)
		if err != nil {
			SLOG.Printf("Fail to write file: %s", abs_path)
			return err
		}
	}
	return nil
}

func (fl *FileList) DeleteFileInfo(sdfsfilename string) bool {
	if fl.GetFileInfo(sdfsfilename) == nil {
		SLOG.Printf("File not found %s", sdfsfilename)
		return false
	}
	fl.ListLock.Lock()
	defer fl.ListLock.Unlock()

	delete(fl.FileMap, sdfsfilename)
	return true
}

func (fl *FileList) DeleteFileAndInfo(sdfsName string) bool {
	info := fl.GetFileInfo(sdfsName)
	if info == nil {
		SLOG.Printf("trying to delete a non-exist sdfa file: %s", sdfsName)
		return false
	}
	err := os.RemoveAll(info.Localpath)
	if err != nil {
		SLOG.Printf("Fail to delete local file: %s", info.Localpath)
		return false
	}
	fl.DeleteFileInfo(sdfsName)
	return true
}

func (fl *FileList) GetFileInfo(sdfsfilename string) *FileInfo {
	return fl.FileMap[sdfsfilename]
}

// func (fl *FileList) GetAllFileInfo() []*FileInfo {
// 	res := make([]*FileInfo, 0)
// 	for _, innerMap := range fl.FileMap {
// 		for _, fileinfo := range innerMap {
// 			res = append(res, fileinfo)
// 		}
// 	}
// 	return res
// }

func (fl *FileList) GetTimeStamp(sdfsfilename string) int {
	if fl.FileMap[sdfsfilename] == nil {
		return -1
	}
	return fl.FileMap[sdfsfilename].Timestamp
}

func (fl *FileList) GetFilesInRange(startID, endID int) []string {
	res := []string{}
	fl.ListLock.Lock()
	defer fl.ListLock.Unlock()
	for _, fi := range fl.FileMap {
		if IsInCircleRange(fi.HashID, startID+1, endID) {
			res = append(res, fi.Sdfsfilename)
		}
	}
	return res
}

func (fl *FileList) DeleteFileInfosOutOfRange(start, end int) []string {
	res := []string{}
	toDelete := []string{}
	fl.ListLock.Lock()
	for _, fi := range fl.FileMap {
		if !IsInCircleRange(fi.HashID, start+1, end) {
			res = append(res, fi.Localpath)
			toDelete = append(toDelete, fi.Sdfsfilename)
		}
	}
	fl.ListLock.Unlock()
	for _, n := range toDelete {
		fl.DeleteFileInfo(n)
	}
	return res
}

func (fl *FileList) UpdateMasterID(new_master_id int, needUpdate func(fileInfo *FileInfo) bool) {
	fl.ListLock.Lock()
	defer fl.ListLock.Unlock()
	for _, fileInfo := range fl.FileMap {
		if needUpdate(fileInfo) {
			fileInfo.MasterNodeID = new_master_id
		}
	}
}

func (fl *FileList) GetOwnedFileInfos(masterId int) []FileInfo {
	res := make([]FileInfo, 0)
	fl.ListLock.Lock()
	defer fl.ListLock.Unlock()
	for _, fileInfo := range fl.FileMap {
		if fileInfo.MasterNodeID == masterId {
			res = append(res, *fileInfo)
		}
	}
	return res
}

func (fl *FileList) ListFileInDir(dir string) []string {
	res := []string{}
	fl.ListLock.Lock()
	defer fl.ListLock.Unlock()
	for sdfsfilename, _ := range fl.FileMap {
		if filepath.Dir(sdfsfilename) == dir {
			res = append(res, sdfsfilename)
		}
	}
	SLOG.Printf("[ListFileInDir] Found these files: %+q", res)
	return res
}

func (fl *FileList) DeleteSDFSDir(dirName string) {
	// delete all files under this dir
	files := fl.ListFileInDir(dirName)
	if len(files) == 0 {
		return
	}

	// Get path of dir in file system
	head := fl.GetFileInfo(files[0])
	abs_dir_path := filepath.Dir(head.Localpath)

	// delete all Fileinfo under this dir
	for _, sdfsfilename := range files {
		fl.DeleteFileInfo(sdfsfilename)
	}

	// delete this dir
	_ = os.RemoveAll(abs_dir_path)
}

func (fl *FileList) MergeTmpFiles(tmpDir, desDir string, ts int) {
	for sdfsName, info := range fl.FileMap {
		if !info.Tmp {
			continue
		}
		targetName := strings.Split(sdfsName, "___")[0]
		data, _ := fl.ServeFile(sdfsName)
		fl.AppendFile(targetName, desDir, ts, info.MasterNodeID, data)
		fl.DeleteFileAndInfo(sdfsName)
	}
	err := os.RemoveAll(tmpDir)
	if err != nil {
		SLOG.Println(err)
	}
}

func (fl *FileList) MergeDirectoryWithSurfix(surffix string) {
	fl.ListLock.Lock()
	targetFileInfos := []*FileInfo{}
	for sdfsName, fInfo := range fl.FileMap {
		if isTargetFile(sdfsName, surffix) {
			targetFileInfos = append(targetFileInfos, fInfo)
		}
	}
	fl.ListLock.Unlock()
	for _, fInfo := range targetFileInfos {
		sdfsName := fInfo.Sdfsfilename
		// Read from file, append to new dir
		data, err := fl.ServeFile(sdfsName)
		if err != nil {
			SLOG.Fatal("[MergeDirectoryWithSurfix] err ", err)
		}
		basename := filepath.Base(sdfsName)
		newsdfsName := filepath.Join(surffix, basename)
		path_split := strings.Split(fInfo.Localpath, "/")
		root_dir := "/" + filepath.Join(path_split[1], path_split[2]) //  [1]/[2] could be apps/files for production and tmp/test_merge_dir for test
		fl.AppendFile(newsdfsName, root_dir, fInfo.Timestamp, fInfo.MasterNodeID, data)
	}
	targetDirSet := make(map[string]bool)
	for _, fInfo := range targetFileInfos {
		sdfsDir := filepath.Dir(fInfo.Sdfsfilename)
		targetDirSet[sdfsDir] = true
	}
	for dir, _ := range targetDirSet {
		fl.DeleteSDFSDir(dir)
	}
}

func isTargetFile(sdfsName, surffix string) bool {
	dir := filepath.Dir(sdfsName)
	split := strings.Split(dir, "___")
	if len(split) < 3 {
		return false
	}
	return split[2] == surffix
}
