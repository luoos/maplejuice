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

func CreateFileList() *FileList {
	return &FileList{FileMap: make(map[string]*FileInfo)}
}

func (fl *FileList) PutFileInfoObject(sdfsfilename string, fi *FileInfo) {
	if fl.FileMap[sdfsfilename] != nil {
		SLOG.Printf("%s already exist, updating all metainfo", sdfsfilename)
	}
	fl.FileMap[sdfsfilename] = fi
}
func (fl *FileList) PutFileInfo(hashID int,
	sdfsfilename string,
	localpath string,
	timestamp int,
	masterNodeID int) {
	if fl.FileMap[sdfsfilename] != nil {
		SLOG.Printf("%s already exist, updating all metainfo", sdfsfilename)
	}
	fl.FileMap[sdfsfilename] = &FileInfo{
		HashID:       hashID,
		Sdfsfilename: sdfsfilename,
		Localpath:    localpath,
		Timestamp:    timestamp,
		MasterNodeID: masterNodeID,
	}
}

func (fl *FileList) DeleteFileInfo(sdfsfilename string) bool {
	if fl.FileMap[sdfsfilename] == nil {
		log.Fatal("File not found")
	}
	delete(fl.FileMap, sdfsfilename)
	return true
}

func (fl *FileList) GetFileInfo(sdfsfilename string) *FileInfo {
	return fl.FileMap[sdfsfilename]
}

func (fl *FileList) UpdateFileTimeStamp(sdfsfilename string, timestamp int) {
	if fl.FileMap[sdfsfilename] == nil {
		log.Fatal("File not found")
	}
	fl.FileMap[sdfsfilename].Timestamp = timestamp
}
