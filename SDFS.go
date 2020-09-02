package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

/*SDFSMessageType defines the SDFS message's type*/
type SDFSMessageType string

const (
	//PutFileMsg will put a file to the SDFS system
	PutFileMsg SDFSMessageType = "PutFile"
	// GetFileMsg will get a file from SDFS system
	GetFileMsg SDFSMessageType = "GetFile"
	// DeleteFileMsg will delete a file from SDFS system
	DeleteFileMsg SDFSMessageType = "DeleteFile"
	// GlobalFileAddMsg will generate when the target file will be added or updated
	GlobalFileAddMsg SDFSMessageType = "GlobalFileAdd"
	// GlobalFileRemoveMsg will generate when the target file will be added
	GlobalFileRemoveMsg SDFSMessageType = "GlobalFileRemove"
	// QueryGlobalFileMsg will be sent when a new node join the group to fetch file list
	QueryGlobalFileMsg SDFSMessageType = "QueryGlobalFile"
	// ReadyToTransferMsg will be sent from receiver when the receiver is ready to receive
	ReadyToTransferMsg SDFSMessageType = "ReadyToTransfer"
)

/*FileInfo stores the information of an sdfsFile*/
type FileInfo struct {
	SdfsFileName string
	FileSize     int64
}

/*FileTransferMessage contrain file information*/
type FileTransferMessage struct {
	Content  SDFSMessageType
	Info     FileInfo
	FileList []FileInfo
}

type ringItem struct {
	Hash   string
	member Member
}

// used to sort
type ringType []ringItem

func (r ringType) Len() int {
	return len(r)
}
func (r ringType) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
func (r ringType) Less(i, j int) bool {

	return r[i].Hash < r[j].Hash
}

const masterListenPort string = "14159"
const normalNodeListenPort string = "26535"
const replicaCnt = 3

var (
	myMasterFiles []FileInfo
	// the files stores in the node as a master
	globalFiles []FileInfo
	// all the files in the sdfs system
	sdfsFiles []FileInfo
	// the nodes which are my replica
	replicaNodes         []Member
	ring                 ringType
	fileTransferListener net.Listener

	myMasterFilesMutex sync.Mutex
	globalFilesMutex   sync.Mutex
	sdfsFilesMutex     sync.Mutex
	replicaNodesMutex  sync.Mutex
	ringMutex          sync.Mutex
)

// SDFS helper
func generateHash(input string) string {
	hash := sha1.New()
	hash.Write([]byte(input))
	sha := hash.Sum(nil)
	shaStr := hex.EncodeToString(sha)
	return shaStr
}

func getNodePositionInRing(member Member) int {
	nodeHash := generateHash(member.NodeName)
	for i := 0; i < len(ring); i++ {
		if ring[i].Hash == nodeHash {
			return i
		}
	}
	fmt.Println("Cannot find ", member.NodeName, " with hash ", nodeHash, " in the ring")
	return -1
}

func getFilePositionInRing(fileName string) int {
	pos := 0
	fileHash := generateHash(fileName)
	for i := 0; i < len(ring); i++ {
		if ring[i].Hash > fileHash {
			pos = i
			break
		}
	}
	return pos
}

func getFileMaster(fileName string) Member {
	ringMutex.Lock()
	pos := getFilePositionInRing(fileName)
	master := ring[pos].member
	ringMutex.Unlock()
	return master
}

func getFileStoreNodes(fileName string) []Member {
	members := []Member{}
	fileExists := false
	for _, f := range globalFiles {
		if f.SdfsFileName == fileName {
			fileExists = true
			break
		}
	}
	if !fileExists {
		fmt.Println(fileName, " does not exist anywhere in the sdfs file system")
		return []Member{}
	}
	firstPos := getFilePositionInRing(fileName)
	members = append(members, ring[firstPos].member)

	// replica
	for i := 1; i < replicaCnt+1; i++ {
		members = append(members, ring[(firstPos+i)%len(ring)].member)
	}
	return members
}

func getMyReplicaNodes() []Member {
	members := []Member{}
	myPos := getNodePositionInRing(self)
	// replica
	for i := 1; i < replicaCnt+1; i++ {
		members = append(members, ring[(myPos+i)%len(ring)].member)
	}
	return members
}

// when join remove all files in sdfs path
func cleanUp() {
	files, err := filepath.Glob("./sdfs*")
	if err == nil {
		for _, f := range files {
			removeFilesOnDisk(f)
		}
	}
	files, err = filepath.Glob("./" + "./temp_*")
	if err == nil {
		for _, f := range files {
			removeFilesOnDisk(f)
		}
	}
	files, err = filepath.Glob("./" + "./temp/*")
	if err == nil {
		for _, f := range files {
			removeFilesOnDisk(f)
		}
	}
	cmd := exec.Command("rm -r s*")
	cmd.CombinedOutput()
}

func ringInit() {
	ringMutex.Lock()
	for _, member := range MemberList {
		nodeHash := generateHash(member.NodeName)
		ring = append(ring, ringItem{nodeHash, member})
	}
	// sort by hash value
	sort.Sort(ring)
	ringMutex.Unlock()
}

func masterFilesInit() {
	myMasterFilesMutex.Lock()
	myMasterFiles = getMyMasterFiles()
	myMasterFilesMutex.Unlock()
}

func replicaNodesInit() {
	replicaNodesMutex.Lock()
	replicaNodes = getMyReplicaNodes()
	replicaNodesMutex.Unlock()
}

func getMyMasterFiles() []FileInfo {
	fileList := []FileInfo{}
	for _, file := range globalFiles {
		fileMaster := getFileMaster(file.SdfsFileName)
		if fileMaster.MemberID == ID {
			// I am the master about the file
			fileList = append(fileList, file)
		}
	}
	fmt.Println("the current master files are ", fileList)
	return fileList
}

// check if file exist in globalFiles or sdfsFiles
func checkFileExist(sdfsFileName string, global bool) bool {
	if global {
		for _, f := range globalFiles {
			if f.SdfsFileName == sdfsFileName {
				return true
			}
		}
		return false
	}
	_, err := os.Stat(sdfsFileName)
	return err == nil || os.IsExist(err)
}

func removeFilesOnDisk(sdfsFileName string) {
	//sdfsFileStorePath
	if checkFileExist(sdfsFileName, false) {
		err := os.Remove(sdfsFileName)
		if err != nil {
			fmt.Println("file remove error during ", sdfsFileName, " ", err)
		}
	}
}

func makeFileInfo(localFileName string, sdfsFileName string) FileInfo {
	file, err := os.Open(localFileName)
	if err != nil {
		fmt.Println("make file info error, cannot open ", file)
		return FileInfo{"Error", 0}
	}
	fileinfo, err := file.Stat()
	return FileInfo{sdfsFileName, fileinfo.Size()}
}

func checkFileIntegrity(fileName string, supposedSize int64) int {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("open file error")
		return -1
	}
	fileInfo, _ := file.Stat()
	if fileInfo.Size() > supposedSize {
		return 1
	} else if fileInfo.Size() < supposedSize {
		return -1
	}
	return 0
}

func getDistanceInRingFromTarget(member1 Member, member2 Member) int {
	pos1 := getNodePositionInRing(member1)
	pos2 := getNodePositionInRing(member2)

	if pos1 < pos2 {
		pos1 += len(ring)
	}
	return pos1 - pos2
}

func getFileFromSocket(conn *net.Conn, fileName string) {
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Println("error opening file: ", err)
	}
	_, err = io.Copy(file, *conn)
	if err != nil {
		fmt.Println("copy file from socket err ", err)
		return
	}
	(*conn).Close()
	file.Close()
}

func sendFileToSocket(conn *net.Conn, fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("send file error, cannot open ", file)
		(*conn).Close()
		return err
	}
	_, err = io.Copy(*conn, file)
	if err != nil {
		fmt.Println("io copy error ", err)
		(*conn).Close()
		return err
	}
	file.Close()
	(*conn).Close()
	return nil
}

func getFileInfoWithFileName(filename string) FileInfo {
	var sdfsFile FileInfo
	for _, f := range globalFiles {
		if f.SdfsFileName == filename {
			sdfsFile = f
			break
		}
	}
	return sdfsFile
}

// SDFS logic

func sendSDFSMessageToTarget(targetIP string, port string, msg FileTransferMessage) (*net.Conn, error) {
	SerializedMsg := serializeSDFSMessage(msg)
	conn, err := dialTCPWithLog(targetIP, port)
	if err != nil {
		return nil, err
	}
	_, e := conn.Write(SerializedMsg)
	if e != nil {
		fmt.Println("write to connection error during send msg ", msg, " from ", NodeName, " to ", GetNodeNameByIP(targetIP), " with error", e)
		return nil, err
	}
	// fmt.Println("send msg ", msg, " from ", NodeName, " to ", GetNodeNameByIP(targetIP))
	logger.Println("Member ", NodeName, " send ", msg.Content, " to member: ", GetNodeNameByIP(targetIP))
	return &conn, nil

}

func broadcastMessage(port string, msg FileTransferMessage) {
	for _, member := range MemberList {
		// only need to send to nodes that aren't myself
		if member.MemberID != ID {
			sendSDFSMessageToTarget(member.MemberIP, port, msg)
		}
	}
}

func sendFilesToReplica(replica Member, Files []FileInfo) {
	for _, file := range Files {
		msg := makeSDFSMessage(PutFileMsg, file, []FileInfo{})
		conn, err := sendSDFSMessageToTarget(replica.MemberIP, normalNodeListenPort, msg)
		logger.Println("Send put file request to replica ", replica, " with file ", file)
		if err != nil {
			fmt.Println("build request connection error ", err)
			return
		}

		//wait for respond msg

		bufRecv := make([]byte, bufSize)
		_, err = (*conn).Read(bufRecv)
		if err != nil {
			fmt.Println("read error with 1", err)
		} else {
			respondMsg, e := unserializeSDFSMessage(bufRecv)
			if e != nil {
				return
			}
			if respondMsg.Content == ReadyToTransferMsg {
				sendFileToSocket(conn, file.SdfsFileName)
				logger.Println("send ", file.SdfsFileName, " to ", msg.Content, " to replica: ", replica)
			}
		}
		(*conn).Close()
	}
}

func updateRing(member Member, add bool) {
	ringMutex.Lock()
	if add {
		nodeHash := generateHash(member.NodeName)
		ring = append(ring, ringItem{nodeHash, member})
		sort.Sort(ring)
		logger.Println("ring changed to ", ring)
	} else {
		targetNodeIndex := -1
		for i := 0; i < len(ring); i++ {
			if ring[i].member.MemberID == member.MemberID {
				targetNodeIndex = i
			}
		}
		if targetNodeIndex == -1 {
			fmt.Println("remove member at ring error, cannot find member ", member.NodeName, " in the ring")
		} else {
			var tmp ringType
			tmp = append(ring[:targetNodeIndex], ring[targetNodeIndex+1:]...)
			ring = tmp
			fmt.Println("Remove ", member.NodeName, " in the ring")
			logger.Println("ring changed to ", ring)
		}
	}
	ringMutex.Unlock()
}

func getFileInListPos(info FileInfo, list []FileInfo) int {
	for i, item := range list {
		if item.SdfsFileName == info.SdfsFileName {
			return i
		}
	}
	return -1
}

func changeFileList(sdfsFile FileInfo, put bool, filelist string) {
	var newfilelist []FileInfo
	switch filelist {
	case "sdfsfilelist":
		{
			sdfsFilesMutex.Lock()
			newfilelist = sdfsFiles[:]
		}
	case "globalfilelist":
		{
			globalFilesMutex.Lock()
			newfilelist = globalFiles[:]
		}

	case "masterfilelist":
		{
			myMasterFilesMutex.Lock()
			newfilelist = myMasterFiles[:]
		}
	}

	index := -1
	if filelist == "sdfsfilelist" {
		index = getFileInListPos(sdfsFile, sdfsFiles)
	} else if filelist == "globalfilelist" {
		index = getFileInListPos(sdfsFile, globalFiles)
	} else if filelist == "masterfilelist" {
		masterFileCopy := myMasterFiles[:]
		index = getFileInListPos(sdfsFile, masterFileCopy)
	}

	if put {
		if index != -1 {
			// already exist just update
			newfilelist[index] = sdfsFile
		} else {
			newfilelist = append(newfilelist, sdfsFile)
		}
	} else {
		// delete
		if index != -1 {
			tmp := []FileInfo{}
			tmp = append(newfilelist[:index], newfilelist[index+1:]...)
			newfilelist = tmp[:]
		} else {
			fmt.Println(filelist, " remove error in remove ", sdfsFile.SdfsFileName)
		}
	}

	switch filelist {
	case "sdfsfilelist":
		{
			//logger.Println("sdfsfilelist changed from ", sdfsFiles, " to ", newfilelist)
			sdfsFiles = newfilelist[:]
			sdfsFilesMutex.Unlock()
		}
	case "globalfilelist":
		{
			//logger.Println("globalfilelist changed from ", globalFiles, " to ", newfilelist)
			globalFiles = newfilelist[:]
			globalFilesMutex.Unlock()
		}
	case "masterfilelist":
		{
			//logger.Println("masterfilelist changed from ", myMasterFiles, " to ", newfilelist)
			myMasterFiles = newfilelist[:]
			myMasterFilesMutex.Unlock()
		}
	}
}

func getSupposedFileList() []string {
	fileList := []string{}
	for _, file := range globalFiles {
		fileMaster := getFileMaster(file.SdfsFileName)
		distance := getDistanceInRingFromTarget(self, fileMaster)
		if distance <= 3 {
			fileList = append(fileList, file.SdfsFileName)
		}
	}
	return fileList
}

func getIndexInList(target string, list []string) int {
	for i := 0; i < len(list); i++ {
		if list[i] == target {
			return i
		}
	}
	return -1
}

func removeUnsupposedFiles() {
	supposedFileList := getSupposedFileList()
	sdfsFilesCopy := make([]FileInfo, len(sdfsFiles))
	copy(sdfsFilesCopy, sdfsFiles)
	// need deep copy
	for _, file := range sdfsFilesCopy {
		if getIndexInList(file.SdfsFileName, supposedFileList) == -1 {
			// target file does not exist in the supposed list, just remove
			changeFileList(file, false, "sdfsfilelist")
			removeFilesOnDisk(file.SdfsFileName)
		}
	}
}

func updateSDFSVariablesWhenMemberAdd(newMember Member) {
	updateRing(newMember, true)
	distanceFromNewMember := getDistanceInRingFromTarget(self, newMember)
	if distanceFromNewMember == 1 {
		// only the node next to the new node needs to recalculate my master files
		masterFilesInit()
		fmt.Println("new member add, master files changes to ", myMasterFiles)
	}
	// recalculate replica nodes
	replicaNodesInit()
	for _, member := range replicaNodes {
		if member.MemberID == newMember.MemberID {
			// new member is the replica of the current node
			time.Sleep(1 * time.Second)
			// wait the new node to receive data and start listening
			sendFilesToReplica(newMember, myMasterFiles)
		}
	}
	removeUnsupposedFiles()
}

func updateSDFSVariablesWhenMemberRemove(failedMember Member) {
	// in old ring
	// check if I need to update replica
	distance := getDistanceInRingFromTarget(failedMember, self)
	// check if the failed member is the node behind me
	if getDistanceInRingFromTarget(self, failedMember) == 1 {
		// check if the failed member is the node behind me
		updateRing(failedMember, false)
		masterFilesInit()
		replicaNodesInit()
		for _, replica := range replicaNodes {
			sendFilesToReplica(replica, myMasterFiles)
		}
	} else {
		// just update ring
		updateRing(failedMember, false)
		// replica cnt == 3
		if distance <= 3 {
			if len(ring) < 4 {
				fmt.Println("the node count in the system is less the min requirement, stop operation")
				return
			}
			selfPos := getNodePositionInRing(self)
			newReplica := ring[(selfPos+3)%len(ring)]
			// send my masterfiles to my newReplica
			sendFilesToReplica(newReplica.member, myMasterFiles)
		}
	}
}

// will only be generated by the file master
func fileChangeBroadCast(sdfsFile FileInfo, put bool) {
	msg := makeSDFSMessage(GlobalFileAddMsg, sdfsFile, []FileInfo{})
	if !put {
		msg = makeSDFSMessage(GlobalFileRemoveMsg, sdfsFile, []FileInfo{})
	}
	broadcastMessage(normalNodeListenPort, msg)
}

func putUpdateFileMsgHandlerMaster(msg FileTransferMessage, conn *net.Conn) {
	sdfsFile := msg.Info

	// send file transfer ready msg
	readyMsg := makeSDFSMessage(ReadyToTransferMsg, sdfsFile, []FileInfo{})
	serializedMsg := serializeSDFSMessage(readyMsg)
	_, e := (*conn).Write(serializedMsg)
	if e != nil {
		fmt.Println("write to connection error during send readyMsg ", readyMsg, " with error", e)
	}

	supposedFileSize := sdfsFile.FileSize

	files, err := filepath.Glob("./sdfs*")
	if err == nil {
		for _, f := range files {
			if f == sdfsFile.SdfsFileName {
				removeFilesOnDisk(f)
			}
		}
	}

	getFileFromSocket(conn, sdfsFile.SdfsFileName)

	file, err := os.Open(sdfsFile.SdfsFileName)
	if err != nil {
		fmt.Println("open file error")
	}
	fileInfo, _ := file.Stat()

	res := checkFileIntegrity(sdfsFile.SdfsFileName, supposedFileSize)
	if res < 0 {
		fmt.Println("at putUpdateFileMsgHandlerMaster : Get incomplete file with ", sdfsFile.SdfsFileName)
		removeFilesOnDisk(sdfsFile.SdfsFileName)
		return
	} else if res > 0 {
		file, _ := os.Open(sdfsFile.SdfsFileName)
		fileInfo, _ := file.Stat()
		fmt.Println("at putUpdateFileMsgHandlerMaster : Get larger file than expected with ", sdfsFile.SdfsFileName, " size is ", fileInfo.Size())
		removeFilesOnDisk(sdfsFile.SdfsFileName)
		return
	} else {
		fmt.Println("recv file complete")
	}

	fmt.Println("recv complete file ", msg.Info.SdfsFileName, " with size ", fileInfo.Size())

	changeFileList(sdfsFile, true, "sdfsfilelist")
	changeFileList(sdfsFile, true, "globalfilelist")
	changeFileList(sdfsFile, true, "masterfilelist")
	fileChangeBroadCast(sdfsFile, true)

	// send put file to replicas
	replicas := getMyReplicaNodes()
	for _, replica := range replicas {
		fmt.Println("send ", sdfsFile, " to ", replica)
		sendFilesToReplica(replica, []FileInfo{sdfsFile})
	}
	logger.Println(NodeName, " sent file ", sdfsFile.SdfsFileName, " to its replicas: ", replicas)
	logger.Println(NodeName, " finished put/update file process and replied the request node")
}

func getFileMsgHandlerMaster(msg FileTransferMessage, conn *net.Conn) {
	//logger.Println("Member ", NodeName, " received the getFileMsg for file ", msg.Info.SdfsFileName)
	sendFileToSocket(conn, msg.Info.SdfsFileName)
	//logger.Println(NodeName, " sent file ", msg.Info.SdfsFileName, " to the request node")
}

func deleteFileMsgHandlerMaster(msg FileTransferMessage, conn *net.Conn) {
	// find the sdfs FileInfo
	sdfsFile := getFileInfoWithFileName(msg.Info.SdfsFileName)

	removeFilesOnDisk(sdfsFile.SdfsFileName)
	//logger.Println(NodeName, " deleted file: ", sdfsFile.SdfsFileName)

	changeFileList(sdfsFile, false, "sdfsfilelist")
	changeFileList(sdfsFile, false, "globalfilelist")
	changeFileList(sdfsFile, false, "masterfilelist")

	//broadcast delete file msg
	fileChangeBroadCast(sdfsFile, false)

	replicas := getMyReplicaNodes()
	for _, replica := range replicas {
		msg = makeSDFSMessage(DeleteFileMsg, sdfsFile, []FileInfo{})
		sendSDFSMessageToTarget(replica.MemberIP, normalNodeListenPort, msg)
	}

	//logger.Println(NodeName, " broadcasted the delete file: ", sdfsFile.SdfsFileName)
}

func handleSDFSMessageMaster(msg FileTransferMessage, conn *net.Conn) {
	// fmt.Println("handle master msg with content ", msg)
	switch msg.Content {
	case PutFileMsg:
		putUpdateFileMsgHandlerMaster(msg, conn)
	case GetFileMsg:
		getFileMsgHandlerMaster(msg, conn)
	case DeleteFileMsg:
		deleteFileMsgHandlerMaster(msg, conn)
	}
	(*conn).Close()
}

func sdfsMasterLoop() {
	listener, err := net.Listen("tcp", ":"+masterListenPort)
	if err != nil {
		fmt.Println("Listen error with ", err)
		return
	}
	fmt.Println("master start listening at ", masterListenPort)
	buf := make([]byte, bufSize)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Accept error!")
			continue
		}
		_, err = conn.Read(buf)
		if err != nil {
			fmt.Println("read error with ", err)
			continue
		}
		msg, e := unserializeSDFSMessage(buf)
		if e != nil {
			continue
		}
		handleSDFSMessageMaster(msg, &conn)
	}
}

func putUpdateFileMsgHandlerNormalNode(msg FileTransferMessage, conn *net.Conn) {
	// send file transfer ready msg
	readyMsg := makeSDFSMessage(ReadyToTransferMsg, msg.Info, []FileInfo{})
	serializedMsg := serializeSDFSMessage(readyMsg)
	_, err := (*conn).Write(serializedMsg)

	if err != nil {
		fmt.Println("connection write error")
		(*conn).Close()
		return
	}

	sdfsFile := msg.Info
	supposedFileSize := sdfsFile.FileSize

	files, err := filepath.Glob("./sdfs*")
	if err == nil {
		for _, f := range files {
			if f == sdfsFile.SdfsFileName {
				removeFilesOnDisk(f)
			}
		}
	}

	getFileFromSocket(conn, sdfsFile.SdfsFileName)

	res := checkFileIntegrity(sdfsFile.SdfsFileName, supposedFileSize)
	if res < 0 {
		fmt.Println("at putUpdateFileMsgHandlerNormalNode : Get incomplete file with ", sdfsFile.SdfsFileName)
		removeFilesOnDisk(sdfsFile.SdfsFileName)
		return
	} else if res > 0 {
		file, _ := os.Open(sdfsFile.SdfsFileName)
		fileInfo, _ := file.Stat()
		fmt.Println("at putUpdateFileMsgHandlerNormalNode : Get larger file than expected with ", sdfsFile.SdfsFileName, " size is ", fileInfo.Size())
		removeFilesOnDisk(sdfsFile.SdfsFileName)
		return
	} else {
		fmt.Println("recv file complete")
	}
	fmt.Println("recv file ", sdfsFile.SdfsFileName, " as replica node")
	changeFileList(sdfsFile, true, "sdfsfilelist")
}

// get file from replica
func getFileMsgHandlerNormalNode(msg FileTransferMessage, conn *net.Conn) {
	fmt.Println("Member (as replica) ", NodeName, " received the getFileMsg for file ", msg.Info.SdfsFileName)
	//logger.Println("Member (as replica) ", NodeName, " received the getFileMsg for file ", msg.Info.SdfsFileName)
	sendFileToSocket(conn, msg.Info.SdfsFileName)
	fmt.Println("get request handle complete, send file complete")
	//logger.Println(NodeName, " (as replica) sent file ", msg.Info.SdfsFileName, " to the request node")
}

func deleteFileMsgHandlerNormalNode(msg FileTransferMessage, conn *net.Conn) {
	// fmt.Println("receive delete msg ", msg)
	changeFileList(msg.Info, false, "sdfsfilelist")
	removeFilesOnDisk(msg.Info.SdfsFileName)
}

func queryGlobalFileMsgHandlerNormalNode(msg FileTransferMessage, conn *net.Conn) {
	globalFileMsg := makeSDFSMessage(QueryGlobalFileMsg, msg.Info, globalFiles)
	serializedMsg := serializeSDFSMessage(globalFileMsg)
	(*conn).Write(serializedMsg)
}

func handleSDFSMessageNormalNode(msg FileTransferMessage, conn *net.Conn) {
	// fmt.Println("handle normal node msg with content ", msg)
	switch msg.Content {
	case PutFileMsg:
		putUpdateFileMsgHandlerNormalNode(msg, conn)
	case GetFileMsg:
		getFileMsgHandlerNormalNode(msg, conn)
	case DeleteFileMsg:
		deleteFileMsgHandlerNormalNode(msg, conn)
	case GlobalFileAddMsg:
		changeFileList(msg.Info, true, "globalfilelist")
	case GlobalFileRemoveMsg:
		changeFileList(msg.Info, false, "globalfilelist")
	case QueryGlobalFileMsg:
		queryGlobalFileMsgHandlerNormalNode(msg, conn)
	}
	(*conn).Close()
}

func sdfsNormalNodeLoop() {
	listener, err := net.Listen("tcp", ":"+normalNodeListenPort)
	if err != nil {
		fmt.Println("Listen error with ", err)
		return
	}
	fmt.Println("normal node start listening at ", normalNodeListenPort)
	buf := make([]byte, bufSize)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Accept error!")
			continue
		}
		_, err = conn.Read(buf)
		if err != nil {
			fmt.Println("read error with ", err)
			continue
		}
		msg, e := unserializeSDFSMessage(buf)
		if e != nil {
			continue
		}
		go handleSDFSMessageNormalNode(msg, &conn)
	}
}

func queryGlobalFilesRequest() {
	emptyInfo := FileInfo{"", 0}
	msg := makeSDFSMessage(QueryGlobalFileMsg, emptyInfo, []FileInfo{})
	for _, member := range MemberList {
		if member.MemberID != ID {
			// not self
			conn, err := sendSDFSMessageToTarget(member.MemberIP, normalNodeListenPort, msg)
			if err != nil {
				// dial error, move to the next one
				continue
			}
			buf := make([]byte, bufSize)
			_, err = (*conn).Read(buf)
			if err != nil {
				fmt.Println("read error with ", err)
				continue
			}
			recvMsg, e := unserializeSDFSMessage(buf)
			if e != nil {
				continue
			}
			globalFiles = recvMsg.FileList
			fmt.Println("recv global file list, content is ", globalFiles)
			break
		}
	}
}

func putDirRequest(dirPath string) {
	files, err := filepath.Glob(dirPath + "/*")
	if err != nil {
		fmt.Println("glob file error: ", err)
	}
	for _, f := range files {
		putUpdateFileRequest(f, "sdfs_"+dirPath+"_"+f[len(dirPath)+1:])
	}
}

func putUpdateFileRequest(localFileName string, SDFSFileName string) {
	// get file info
	fileInfo := makeFileInfo(localFileName, SDFSFileName)
	if fileInfo.SdfsFileName == "Error" {
		// file open error
		return
	}
	msg := makeSDFSMessage(PutFileMsg, fileInfo, []FileInfo{})

	fileMaster := getFileMaster(SDFSFileName)

	conn, err := sendSDFSMessageToTarget(fileMaster.MemberIP, masterListenPort, msg)
	if err != nil {
		fmt.Println("error during send put request ", err)
		return
	}

	//wait for respond msg
	bufRecv := make([]byte, bufSize)
	_, err = (*conn).Read(bufRecv)
	if err != nil {
		fmt.Println("read error with ", err)
		return
	}
	respondMsg, e := unserializeSDFSMessage(bufRecv)
	if e != nil {
		return
	}
	if respondMsg.Content == ReadyToTransferMsg {
		// ready to send
		sendFileToSocket(conn, localFileName)
		//logger.Println("send file complete")
	}
}

func getFileRequest(SDFSFileName string, localFileName string) bool {

	if !checkFileExist(SDFSFileName, true) {
		fmt.Println(SDFSFileName, " does not exist anywhere in out sdfs file system. ")
		return false
	}

	fileInfo := FileInfo{SDFSFileName, 0}
	msg := makeSDFSMessage(GetFileMsg, fileInfo, []FileInfo{})
	fileMaster := getFileMaster(SDFSFileName)
	conn, err := sendSDFSMessageToTarget(fileMaster.MemberIP, masterListenPort, msg)
	if err != nil {
		return false
	}
	getFileFromSocket(conn, localFileName)
	supposedSize := (int64)(0)
	for _, file := range globalFiles {
		if file.SdfsFileName == SDFSFileName {
			supposedSize = file.FileSize
		}
	}

	res := checkFileIntegrity(localFileName, supposedSize)
	if res < 0 {
		fmt.Println("at get file request : Get incomplete file with ", localFileName)
		return false
	} else if res > 0 {
		file, _ := os.Open(localFileName)
		fileInfo, _ := file.Stat()
		fmt.Println("at get file request : Get larger file than expected with ", localFileName, " size is ", fileInfo.Size())
	} else {
		fmt.Println("recv file complete")
	}

	return true
}

func deleteFileRequest(SDFSFileName string) {
	fileInfo := FileInfo{SDFSFileName, 0}
	msg := makeSDFSMessage(DeleteFileMsg, fileInfo, []FileInfo{})
	fileMaster := getFileMaster(SDFSFileName)
	sendSDFSMessageToTarget(fileMaster.MemberIP, masterListenPort, msg)
}

func fetchMasterFilesWhenNodeJoin() {
	// all my master file will exist the node that next to the new join node
	selfPos := getNodePositionInRing(self)
	oldMaster := ring[(selfPos+1)%len(ring)].member
	for _, SDFSFile := range myMasterFiles {
		fileInfo := FileInfo{SDFSFile.SdfsFileName, 0}
		msg := makeSDFSMessage(GetFileMsg, fileInfo, []FileInfo{})
		conn, err := sendSDFSMessageToTarget(oldMaster.MemberIP, normalNodeListenPort, msg)
		if err != nil {
			return
		}

		supposedSize := (int64)(0)
		for _, file := range globalFiles {
			if file.SdfsFileName == SDFSFile.SdfsFileName {
				supposedSize = file.FileSize
			}
		}
		getFileFromSocket(conn, SDFSFile.SdfsFileName)

		res := checkFileIntegrity(SDFSFile.SdfsFileName, supposedSize)
		if res < 0 {
			fmt.Println("at fetch master files: Get incomplete file with ", SDFSFile.SdfsFileName)
		} else if res > 0 {
			file, _ := os.Open(SDFSFile.SdfsFileName)
			fileInfo, _ := file.Stat()
			fmt.Println("at fetch master files: Get larger file than expected with ", SDFSFile.SdfsFileName, " size is ", fileInfo.Size())
		} else {
			fmt.Println("recv file complete")
		}
	}
}

// init data when join the sdfs system
func normalNodeSDFSInit() {
	// If a node fails and rejoins, remove all files
	cleanUp()
	// get global files list from other
	queryGlobalFilesRequest()
	// init ring
	ringInit()
	// init master file list
	masterFilesInit()
	// init replica nodes
	replicaNodesInit()
	go sdfsMasterLoop()
	go sdfsNormalNodeLoop()
	// get master files
	fetchMasterFilesWhenNodeJoin()

}

// SDFSInit the main logic of sdfs
func introducerSDFSInit() {
	cleanUp()
	ringInit()
	masterFilesInit()
	replicaNodesInit()
	go sdfsMasterLoop()
	go sdfsNormalNodeLoop()
}
