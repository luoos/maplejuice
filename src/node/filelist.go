package node

import (
	"log"
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
	sdfsfilename string,
	localpath string,
	timestamp int,
	masterNodeID int) {
	if fl.GetFileInfo(sdfsfilename) != nil {
		SLOG.Printf("%s already exist, updating all metainfo", sdfsfilename)
	}
	hashid := getHashID(sdfsfilename)
	fl.FileMap[sdfsfilename] = &FileInfo{
		HashID:       hashid,
		Sdfsfilename: sdfsfilename,
		Localpath:    localpath + "/" + sdfsfilename,
		Timestamp:    timestamp,
		MasterNodeID: masterNodeID,
	}
}

func (fl *FileList) DeleteFileInfo(sdfsfilename string) bool {
	if fl.GetFileInfo(sdfsfilename) == nil {
		log.Fatal("File not found")
	}
	delete(fl.FileMap, sdfsfilename)
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

func (fl *FileList) GetResponsibleFileWithID(startID, endID int) []string {
	res := []string{}
	for _, fi := range fl.FileMap {
		if IsInCircleRange(fi.HashID, startID+1, endID) {
			res = append(res, fi.Sdfsfilename)
		}
	}
	return res
}
