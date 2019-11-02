package node

import (
	"io/ioutil"
	"log"
	"os"
	. "slogger"
)

type FileInfo struct {
	HashID       int
	Sdfsfilename string
	Localpath    string
	Timestamp    int
	MasterNodeID int
}

type FileList struct {
	ID      int
	FileMap map[string]*FileInfo
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

func (fl *FileList) PutFileInfo(
	sdfsName string,
	filePath string,
	timestamp int,
	masterNodeID int) {
	if fl.GetFileInfo(sdfsName) != nil {
		SLOG.Printf("%s already exist, updating all metainfo", sdfsName)
	}
	hashid := getHashID(sdfsName)
	fl.PutFileInfoBase(hashid, sdfsName, filePath, timestamp, masterNodeID)
}

func (fl *FileList) PutFileInfoBase(
	hashId int,
	sdfsfilename string,
	filepath string,
	timestamp int,
	masterNodeID int) {
	fl.FileMap[sdfsfilename] = &FileInfo{
		HashID:       hashId,
		Sdfsfilename: sdfsfilename,
		Localpath:    filepath,
		Timestamp:    timestamp,
		MasterNodeID: masterNodeID,
	}
}

func (fl *FileList) StoreFile(
	sdfsName string,
	file_dir string,
	timestamp int,
	masterNodeID int,
	data []byte) error {

	hashId := getHashID(sdfsName)
	return fl.StoreFileBase(hashId, sdfsName, file_dir, timestamp, masterNodeID, data)
}

// This should only be used in test
func (fl *FileList) StoreFileBase(
	hashId int,
	sdfsName string,
	file_dir string,
	timestamp int,
	masterNodeID int,
	data []byte) error {

	filepath := file_dir + "/" + sdfsName
	err := ioutil.WriteFile(filepath, data, 0777)
	if err != nil {
		SLOG.Printf("Fail to write file: %s", filepath)
		return err
	}
	fl.PutFileInfoBase(hashId, sdfsName, filepath, timestamp, masterNodeID)
	return nil
}

func (fl *FileList) DeleteFileInfo(sdfsfilename string) bool {
	if fl.GetFileInfo(sdfsfilename) == nil {
		log.Fatal("File not found")
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
	err := os.Remove(info.Localpath)
	fl.DeleteFileInfo(sdfsName)
	if err != nil {
		SLOG.Printf("Fail to delete local file: %s", info.Localpath)
		return false
	}
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
