package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// helper functions

/*MakeMessage will return a message struct filled with content*/
func MakeMessage(msgContent MessageType, msgSource string, memberList []Member) Message {
	msg := Message{msgContent, msgSource, memberList}
	return msg
}

/*GetIPAddr will return the ip address of the current machine
reference: https://blog.csdn.net/wangshubo1989/article/details/78066344
*/
func GetIPAddr() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "Error getting IP address"
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ip, ok := address.(*net.IPNet); ok && !ip.IP.IsLoopback() {
			if ip.IP.To4() != nil {
				return ip.IP.String()
			}
		}
	}
	return ""
}

/*GenerateID will return one unique ID*/
func GenerateID() string {
	timeString := time.Now().Format("2006-01-02/15:04:05")
	NodeName = IPNodeNameMap[IP]
	ID = NodeName + "#" + timeString
	return ID
}

/*GetNodeNameByIP will get get the name according to IP*/
func GetNodeNameByIP(IP string) string {
	return IPNodeNameMap[IP]
}

/*DialUDPWithLog will return the UPDAddr pointer */
func DialUDPWithLog(target string) (*net.UDPConn, error) {
	targetUDPAddr := GetUDPAddr(target + ":" + portNumber)
	connection, err := net.DialUDP("udp", nil, targetUDPAddr)
	// nil means local UDP addr
	if err != nil {
		fmt.Println("UDP Dial error, from ", NodeName, " to ", GetNodeNameByIP(target), " with ", err)
		return nil, err
	}
	return connection, nil
}

/*GetUDPAddr will return the UPDAddr pointer */
func GetUDPAddr(addr string) *net.UDPAddr {
	UDPAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		fmt.Println("Resolve UDP addr of ", addr, " error with ", err)
		return nil
	}
	return UDPAddr
}

/*SerializeMessage will Serialize the data*/
func SerializeMessage(msg Message) []byte {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(msg)
	if err != nil {
		fmt.Println("Serialize error happened during serialize message ", msg)
	}
	return buf.Bytes()
}

/*UnserializeMessage will Serialize the data*/
func UnserializeMessage(buf []byte) Message {
	msg := Message{}
	decoder := gob.NewDecoder(bytes.NewReader(buf))
	err := decoder.Decode(&msg)
	if err != nil {
		fmt.Println("UnSerialize Error during ", buf)
	}
	return msg
}

// util
func dialTCPWithLog(targetIP string, port string) (net.Conn, error) {
	connection, err := net.Dial("tcp", targetIP+":"+port)
	if err != nil {
		fmt.Println("TCP Dial error, from ", NodeName, " to ", GetNodeNameByIP(targetIP), " with ", err)
		return nil, err
	}
	return connection, nil
}

func makeSDFSMessage(content SDFSMessageType, fileInfo FileInfo, fileList []FileInfo) FileTransferMessage {
	return FileTransferMessage{content, fileInfo, fileList}
}

func serializeSDFSMessage(msg FileTransferMessage) []byte {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(msg)
	if err != nil {
		fmt.Println("Serialize error happened during serialize message ", msg)
	}
	return buf.Bytes()
}

func unserializeSDFSMessage(buf []byte) (FileTransferMessage, error) {
	msg := FileTransferMessage{}
	decoder := gob.NewDecoder(bytes.NewReader(buf))
	err := decoder.Decode(&msg)
	if err != nil {
		fmt.Println("UnSerialize Error")
	}
	return msg, err
}

func getFilesFromDirectory(dirPath string) []FileInfo {
	files := []FileInfo{}
	for _, f := range globalFiles {
		// add 5 because sdfsFileName = 'sdfs_' + dirPath + '_' + filename
		if len(f.SdfsFileName) >= len(dirPath)+5 && f.SdfsFileName[5:len(dirPath)+5] == dirPath {
			files = append(files, f)
		}
	}
	return files
}

func getLastUnderscorePos(filename string) int {
	lastUnderscorePos := 0
	for i := len(filename) - 1; i >= 0; i-- {
		if filename[i] == '_' {
			lastUnderscorePos = i
			break
		}
	}
	return lastUnderscorePos
}

func generateCompressFiles(outPutCompressedFileName string, inputFilesDir string) error {
	compressCmd := "tar"
	compressParameters := "-zcf"

	cmd := exec.Command(compressCmd, compressParameters, outPutCompressedFileName, inputFilesDir)
	cmdOutput, err := cmd.CombinedOutput()
	if err != nil {
		// encounter error
		fmt.Println("command run fail in compress\n", err)
		fmt.Println(string(cmdOutput))
		return err
	}
	return nil
}

func uncompressFiles(compressedFileName string) error {
	uncompressCmd := "tar"
	uncompressParameters := "-zxf"

	cmd := exec.Command(uncompressCmd, uncompressParameters, compressedFileName)
	cmdOutput, err := cmd.CombinedOutput()
	if err != nil {
		// encounter error
		fmt.Println("command run fail in uncompress\n", err)
		fmt.Println(string(cmdOutput))
		return err
	}
	return nil
}

func removeFilesInDirectory(dirName string) {
	files, err := filepath.Glob("./" + dirName + "*")
	if err == nil {
		for _, f := range files {
			removeFilesOnDisk(f)
		}
	}
}

func removeDirectory(dirName string) {
	err := os.RemoveAll("./" + dirName)
	if err != nil {
		fmt.Println(err)
	}
}

func findFilesWithPrefix(prefix string) []string {
	targetFiles := []string{}
	allFiles, err := filepath.Glob("./*")
	// files should in the same path with go
	if err == nil {
		prefixLen := len(prefix)
		for _, f := range allFiles {
			if (len(f) > prefixLen) && (f[0:prefixLen] == prefix) {
				targetFiles = append(targetFiles, f)
			}
		}
	}
	return targetFiles
}

func moveFilesInDirectoryToDirectory(srcDir string, dstDir string) {
	files, err := filepath.Glob("./" + srcDir + "*")
	if err != nil {
		fmt.Println("error in move files ", err)
		return
	}
	for _, file := range files {
		err = moveFilesToDirectory(file, dstDir)
		if err != nil {
			fmt.Println("error in move files ", err)
			return
		}
	}
}

func moveFilesToDirectory(fullPathFileName string, dirName string) error {
	FirstSlashPos := 0
	for i := 0; i < len(fullPathFileName); i++ {
		if fullPathFileName[i] == '/' {
			FirstSlashPos = i
			break
		}
	}
	fileName := fullPathFileName[FirstSlashPos+1:]
	err := os.Rename(fullPathFileName, dirName+"/"+fileName)
	if err != nil {
		return err
	}
	return nil
}
