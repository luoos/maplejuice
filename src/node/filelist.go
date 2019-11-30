package node

import (
	"io/ioutil"
	"os"
	"path/filepath"
	. "slogger"
)

const FILE_LIST_FILE = "/tmp/file.list"

type FileInfo struct {
	HashID       int
	Sdfsfilename string
	Localpath    string
	Timestamp    int
	MasterNodeID int
}

type FileList struct {
	ID      int
	FileMap map[string]*FileInfo // Key: sdfsfilename, value: fileinfo
}

func CreateFileInfo(hashID int,
	sdfsfilename string,
	localpath string,
	timestamp int,
	masterNodeID int) *FileInfo {
	new_fileinfo := &FileInfo{
		HashID:       hashID,
		Sdfsfilename: sdfsfilename,
		Localpath:    localpath,
		Timestamp:    timestamp,
		MasterNodeID: masterNodeID,
	}
	return new_fileinfo
}

func CreateFileList(selfID int) *FileList {
	return &FileList{ID: selfID, FileMap: make(map[string]*FileInfo)}
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
	fl.PutFileInfoBase(hashid, sdfsName, path, timestamp, masterNodeID)
}

func (fl *FileList) PutFileInfoBase(
	hashId int,
	sdfsfilename string,
	abs_path string,
	timestamp int,
	masterNodeID int) {
	fl.FileMap[sdfsfilename] = &FileInfo{
		HashID:       hashId,
		Sdfsfilename: sdfsfilename,
		Localpath:    abs_path,
		Timestamp:    timestamp,
		MasterNodeID: masterNodeID,
	}
}

func (fl *FileList) StoreFile(
	sdfsName string,
	root_dir string,
	timestamp int,
	masterNodeID int,
	data []byte) error {

	hashId := getHashID(sdfsName)
	return fl.StoreFileBase(hashId, sdfsName, root_dir, timestamp, masterNodeID, data, false)
}

func (fl *FileList) AppendFile(
	sdfsName string,
	root_dir string,
	timestamp int,
	masterNodeID int,
	data []byte) error {

	hashId := getHashID(sdfsName)
	return fl.StoreFileBase(hashId, sdfsName, root_dir, timestamp, masterNodeID, data, true)
}

// This should only be used in test
func (fl *FileList) StoreFileBase(
	hashId int,
	sdfsName string,
	root_dir string,
	timestamp int,
	masterNodeID int,
	data []byte,
	appending bool) error {

	abs_path := filepath.Join(root_dir, sdfsName)
	dir, _ := filepath.Split(abs_path)
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		SLOG.Printf("Fail to create dir: %s", dir)
		return err
	}
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
	fl.PutFileInfoBase(hashId, sdfsName, abs_path, timestamp, masterNodeID)
	return nil
}

func (fl *FileList) DeleteFileInfo(sdfsfilename string) bool {
	if fl.GetFileInfo(sdfsfilename) == nil {
		SLOG.Printf("File not found %s", sdfsfilename)
		return false
	}
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
	for _, fi := range fl.FileMap {
		if !IsInCircleRange(fi.HashID, start+1, end) {
			res = append(res, fi.Localpath)
			toDelete = append(toDelete, fi.Sdfsfilename)
		}
	}
	for _, n := range toDelete {
		fl.DeleteFileInfo(n)
	}
	return res
}

func (fl *FileList) UpdateMasterID(new_master_id int, needUpdate func(fileInfo *FileInfo) bool) {
	for _, fileInfo := range fl.FileMap {
		if needUpdate(fileInfo) {
			fileInfo.MasterNodeID = new_master_id
		}
	}
}

func (fl *FileList) GetOwnedFileInfos(masterId int) []FileInfo {
	res := make([]FileInfo, 0)
	for _, fileInfo := range fl.FileMap {
		if fileInfo.MasterNodeID == masterId {
			res = append(res, *fileInfo)
		}
	}
	return res
}

func (fl *FileList) ListFileInDir(dir string) []string {
	res := []string{}
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
		fl.DeleteFileAndInfo(sdfsfilename)
	}

	// delete this dir
	_ = os.RemoveAll(abs_dir_path)
}
