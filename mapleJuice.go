package main

import (
	"bufio"
	"fmt"
	"math"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"plugin"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*MapleRequest used to invoke maple task, will be sent from client to the master*/
type MapleRequest struct {
	TaskCommand            string     // the maple task
	IntermediateFilePrefix string     // the intermediate file prefix
	SourceFileDirectory    string     // the input source file directory
	TaskInputFiles         []FileInfo // the input file that will be used as input
	WorkerCount            int
	TaskID                 string
}

/*MapleTask used to store the information about maple task*/
type MapleTask struct {
	TaskCommand            string     // the maple task
	IntermediateFilePrefix string     // the intermediate file prefix
	SourceFileDirectory    string     // the input source file directory
	TaskInputFiles         []FileInfo // the intermediate file prefix
	TaskID                 string
}

/*MapleTaskResult defines the maple task result*/
type MapleTaskResult struct {
	ReplyResult MapleReplyType
	ResultList  [][]string
}

// MapleReplyType define the maple task's reply
type MapleReplyType string

const (
	// MapleSuccessReply will be sent from worker to the master when the task is finished
	MapleSuccessReply MapleReplyType = "MapleSuccessReply"
	// MapleDataNotExistErrorReply will be sent from worker to the master when the task's input does not exist
	MapleDataNotExistErrorReply MapleReplyType = "MapleDataNotExistErrorReply"
	// MapleProgramErrorReply will be sent from worker to the master when the task's program has problem
	MapleProgramErrorReply MapleReplyType = "MapleProgramErrorReply"
	// MapleDataErrorReply will be sent from worker to the master when the task's data has problem
	MapleDataErrorReply MapleReplyType = "MapleDataErrorReply"
)

// MapleResultType define the maple phrase result
type MapleResultType string

const (
	// MapleSuccessResult will be generate when the all task finished without error
	MapleSuccessResult MapleResultType = "MapleSuccessResult"
	// MapleFailedResult will be generate when the task cannot finished due to some problems
	MapleFailedResult MapleResultType = "MapleFailedResult"
)

/*MapleTaskChannelMessage used to store the information about maple task and the related worker*/
type MapleTaskChannelMessage struct {
	Worker     Member
	Task       MapleTask
	TaskResult MapleTaskResult
	Error      error
}

/*MapleResultChannelMessage used to store the information about the result after maple phrase is finished*/
type MapleResultChannelMessage struct {
	FinalResultList [][]string
	Result          MapleResultType
	Error           error
}

/*JuiceRequest used to invoke juice task, will be sent from client to the master*/
type JuiceRequest struct {
	TaskCommand                   string // the juice task
	IntermediateFilePrefix        string // the intermediate file prefix
	DestinationFileName           string
	TaskInputFiles                []FileInfo // the intermediate file prefix
	DeleteInterMediateFilesOption bool
	WorkerCount                   int
	TaskID                        string
}

/*JuiceTask used to store the information about juice task*/
type JuiceTask struct {
	TaskCommand                   string // the juice task
	IntermediateFilePrefix        string // the intermediate file prefix
	DestinationFileName           string
	TaskInputFiles                []FileInfo // the intermediate file prefix
	DeleteInterMediateFilesOption bool
	TaskID                        string
}

/*JuiceTaskResult defines the juice task result*/
type JuiceTaskResult struct {
	ReplyResult JuiceReplyType
	ResultMap   map[string]string
}

// JuiceReplyType define the juice task's reply
type JuiceReplyType string

const (
	// JuiceSuccessReply will be sent from worker to the master when the task is finished
	JuiceSuccessReply JuiceReplyType = "JuiceSuccessReply"
	// JuiceDataNotExistErrorReply will be sent from worker to the master when the task's input does not exist
	JuiceDataNotExistErrorReply JuiceReplyType = "JuiceDataNotExistErrorReply"
	// JuiceProgramErrorReply will be sent from worker to the master when the task's program has problem
	JuiceProgramErrorReply JuiceReplyType = "JuiceProgramErrorReply"
	// JuiceDataErrorReply will be sent from worker to the master when the task's data has problem
	JuiceDataErrorReply JuiceReplyType = "JuiceDataErrorReply"
)

// JuiceResultType define the juice phrase result
type JuiceResultType string

const (
	// JuiceSuccessResult will be generate when the all task finished without error
	JuiceSuccessResult JuiceResultType = "JuiceSuccessResult"
	// JuiceFailedResult will be generate when the task cannot finished due to some problems
	JuiceFailedResult JuiceResultType = "JuiceFailedResult"
)

/*JuiceTaskChannelMessage used to store the information about juice task and the related worker*/
type JuiceTaskChannelMessage struct {
	Worker     Member
	Task       JuiceTask
	TaskResult JuiceTaskResult
	Error      error
}

/*JuiceResultChannelMessage used to store the information about the result after juice phrase is finished*/
type JuiceResultChannelMessage struct {
	Result         JuiceResultType
	FinalResultMap map[string]string
	Error          error
}

const intermediateFilePrefix = "sdfs_intermediate_"
const tempIntermediateFilePrefix = "temp_intermediate_"
const juiceOutPrefix = "juice_output_"
const maplePort string = "23846"
const juicePort string = "26433"

var (
	mapleTaskChannel         chan MapleTaskChannelMessage
	maplePhraseResultChannel chan MapleResultChannelMessage
	juiceTaskChannel         chan JuiceTaskChannelMessage
	juicePhraseResultChannel chan JuiceResultChannelMessage
	taskMutex                sync.Mutex
)

// MapleService will used as RPC
type MapleService struct {
}

// JuiceService will used as RPC
type JuiceService struct {
}

// Mapper interface
type Mapper interface {
	Map(MapleRequest) bool
}

func mapleInit() {
	mapleTaskChannel = make(chan MapleTaskChannelMessage, 10)
	maplePhraseResultChannel = make(chan MapleResultChannelMessage, 10)
	go mapleMainLoop()
}

func mapleMainLoop() {
	rpc.RegisterName("MapleService", new(MapleService))
	listener, err := net.Listen("tcp", ":"+maplePort)
	if err != nil {
		fmt.Println("ListenTCP error:", err)
	}
	fmt.Println("start listen at port ", maplePort)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
		}

		go rpc.ServeConn(conn)
	}
}

func reassignFailedMapleTask(failedTask MapleTask, failedMember Member, workerCount int, workerInputFilesMap map[Member][]FileInfo) {
	fmt.Println("failed task ", failedTask, " failedMember ", failedMember)

	fileList := workerInputFilesMap[failedMember]
	foundNoWorkNodes := false
	var worker Member

	for _, member := range MemberList {
		_, exist := workerInputFilesMap[member]
		if !exist {
			// found a node which is not working
			worker = member
			foundNoWorkNodes = true
			break
		}
	}

	if !foundNoWorkNodes {
		// all alive nodes are working
		for _, member := range MemberList {
			if member.MemberID != failedMember.MemberID {
				// just find an alive node
				worker = member
			}
		}
	}

	request := MapleRequest{
		failedTask.TaskCommand,
		failedTask.IntermediateFilePrefix,
		failedTask.SourceFileDirectory,
		fileList,
		workerCount,
		failedTask.TaskID,
	}
	workerInputFilesMap[worker] = fileList
	fmt.Println("reassign task ", failedTask, " to ", worker)
	go sendMapleRequestToWorker(request, worker)
	delete(workerInputFilesMap, failedMember)
}

func mapleTaskSchedule(workerCount int, workerInputFilesMap map[Member][]FileInfo) {
	var msg MapleTaskChannelMessage
	unfinishedJobCount := workerCount
	result := [][]string{}
	var resultListMutex sync.Mutex
	fmt.Println(time.Now().Format("2006-01-02 15:04:05.000000"))
	fmt.Println("There are ", unfinishedJobCount, " worker needs to respond")
	for {
		select {
		case msg = <-mapleTaskChannel:
			if msg.TaskResult.ReplyResult == MapleSuccessReply {
				fmt.Println(time.Now().Format("2006-01-02 15:04:05.000000"))
				fmt.Println("succ at node ", msg.Worker, " with task ", msg.Task)
				unfinishedJobCount--
				fmt.Println(time.Now().Format("2006-01-02 15:04:05.000000"))
				fmt.Println("current unrespond workers left ", unfinishedJobCount)

				resultListMutex.Lock()
				result = append(result, msg.TaskResult.ResultList...)
				resultListMutex.Unlock()

				if unfinishedJobCount == 0 {
					// all jobs are done
					fmt.Println("all jobs are done write to result channel")
					res := MapleResultChannelMessage{result, MapleSuccessResult, nil}
					maplePhraseResultChannel <- res
					return
				}
			} else {
				if msg.Error != nil {
					// rpc dial error, which means the worker has failed
					fmt.Println(time.Now().Format("2006-01-02 15:04:05.000000"))
					fmt.Println("fail at node ", msg.Worker, " with task ", msg.Task)
					fmt.Println(time.Now().Format("2006-01-02 15:04:05.000000"))
					fmt.Println("failure detected by rpc!")
					// detect member fail, and this may earilier than heartbeat detect since rpc is sync invoke

					time.Sleep(15 * time.Second)
					fmt.Println("start reassign task")
					go reassignFailedMapleTask(msg.Task, msg.Worker, workerCount, workerInputFilesMap)
				} else if msg.TaskResult.ReplyResult != MapleSuccessReply {
					// todo, need to know the error types of the rpc call
					fmt.Println("reply error with content ", msg.TaskResult.ReplyResult)
				}
			}
		}
	}
}

func mapleTaskSuccessAction(request MapleRequest, worker Member, result MapleTaskResult, err error) {
	task := MapleTask{request.TaskCommand, request.IntermediateFilePrefix, request.SourceFileDirectory, request.TaskInputFiles, request.TaskID}
	msg := MapleTaskChannelMessage{worker, task, result, err}
	fmt.Println(time.Now().Format("2006-01-02 15:04:05.000000"))
	fmt.Println("maple task success, member ", worker, "finished ", task)
	mapleTaskChannel <- msg
}

func mapleTaskFailedAction(request MapleRequest, worker Member, result MapleTaskResult, err error) {
	task := MapleTask{request.TaskCommand, request.IntermediateFilePrefix, request.SourceFileDirectory, request.TaskInputFiles, request.TaskID}
	msg := MapleTaskChannelMessage{worker, task, result, err}
	fmt.Println(time.Now().Format("2006-01-02 15:04:05.000000"))
	fmt.Println("maple task failed, member ", worker, "fail to finish ", task)
	mapleTaskChannel <- msg
}

func sendMapleRequestToWorker(request MapleRequest, worker Member) {
	client, err := rpc.Dial("tcp", worker.MemberIP+":"+maplePort)
	if err != nil {
		fmt.Println("dialing:", err)
	}
	var result MapleTaskResult
	err = client.Call("MapleService.StartMapleTask", request, &result)

	if err != nil {
		fmt.Println(err)
		fmt.Println("rpc call error ", err)
		mapleTaskFailedAction(request, worker, result, err)
		return
	}
	if result.ReplyResult != MapleSuccessReply {
		fmt.Println("task failed with result ", result.ReplyResult)
		mapleTaskFailedAction(request, worker, result, err)
		return
	}
	mapleTaskSuccessAction(request, worker, result, err)
}

func assignFileToWorkerRange(file FileInfo, workerInputFilesMap map[Member][]FileInfo, workerCount int, filesCount int) {
	for _, member := range MemberList {
		_, ok := workerInputFilesMap[member]
		// current member not in the map
		if !ok {
			workerInputFilesMap[member] = append(workerInputFilesMap[member], file)
			return
		}
	}
	// everyone has task, find least one
	var workerWithLeastInput Member
	leastInputFileCount := math.MaxInt32
	maxFilesPerNode := (int)(math.Ceil((float64)(filesCount) / (float64)(workerCount)))

	for i := 0; i < workerCount; i++ {
		if len(workerInputFilesMap[ring[i].member]) < maxFilesPerNode {
			if len(workerInputFilesMap[ring[i].member]) < leastInputFileCount {
				workerWithLeastInput = ring[i].member
				leastInputFileCount = len(workerInputFilesMap[workerWithLeastInput])
			}
		}
	}
	workerInputFilesMap[workerWithLeastInput] = append(workerInputFilesMap[workerWithLeastInput], file)
}

func assignFileToWorker(file FileInfo, workerInputFilesMap map[Member][]FileInfo, workerCount int, filesCount int) {
	maxFilesPerNode := (int)(math.Ceil((float64)(filesCount) / (float64)(workerCount)))
	fileMaster := getFileMaster(file.SdfsFileName)
	var worker Member
	worker = fileMaster
	_, ok := workerInputFilesMap[worker]
	// current member not in the map
	if !ok {
		workerInputFilesMap[worker] = append(workerInputFilesMap[worker], file)
		return
	} else {
		for _, member := range MemberList {
			_, ok := workerInputFilesMap[member]
			// current member not in the map
			if !ok {
				workerInputFilesMap[member] = append(workerInputFilesMap[member], file)
				return
			}
		}
		var workerWithLeastInput Member
		leastInputFileCount := math.MaxInt32
		for i := 0; i < workerCount; i++ {
			if len(workerInputFilesMap[ring[i].member]) < maxFilesPerNode {
				if len(workerInputFilesMap[ring[i].member]) < leastInputFileCount {
					workerWithLeastInput = ring[i].member
					leastInputFileCount = len(workerInputFilesMap[workerWithLeastInput])
				}
			}
		}
		workerInputFilesMap[workerWithLeastInput] = append(workerInputFilesMap[workerWithLeastInput], file)
	}

}

//AllocateMapleTask will be invoke from client and be handled by the master to allocate maple task
func (p *MapleService) AllocateMapleTask(request MapleRequest, result *MapleResultType) error {
	dirPath := request.SourceFileDirectory
	inputFiles := []FileInfo{}
	for _, f := range globalFiles {
		// add 5 because sdfsFileName = 'sdfs_' + dirPath + '_' + filename

		if len(f.SdfsFileName) >= len(dirPath)+5 && f.SdfsFileName[5:len(dirPath)+5] == dirPath {
			inputFiles = append(inputFiles, f)
		}
	}
	workerInputFilesMap := make(map[Member][]FileInfo)
	filesCount := len(inputFiles)
	if filesCount == 0 {
		fmt.Println("input files empty for maple!")
		*result = MapleFailedResult
		return nil
	}

	fmt.Println("before allocate the maple task , global files are ", globalFiles)
	fmt.Println("before allocate the maple task , input files are ", inputFiles)
	fmt.Println("before allocate the maple task , member list are ", MemberList)
	fmt.Println("before allocate the maple task , ring are ", ring)
	for _, file := range inputFiles {
		assignFileToWorker(file, workerInputFilesMap, request.WorkerCount, filesCount)
	}
	fmt.Println("the worker input files map is ", workerInputFilesMap)
	go mapleTaskSchedule(len(workerInputFilesMap), workerInputFilesMap)
	cnt := 0
	//task id
	for worker, files := range workerInputFilesMap {
		request := MapleRequest{
			request.TaskCommand,
			request.IntermediateFilePrefix,
			request.SourceFileDirectory,
			files,
			request.WorkerCount,
			strconv.Itoa(cnt),
		}
		fmt.Println(time.Now().Format("2006-01-02 15:04:05.000000"))
		fmt.Println("send start maple task to worker ", worker, "with request ", request)
		go sendMapleRequestToWorker(request, worker)
		cnt++
	}
	res := <-maplePhraseResultChannel
	fmt.Println("reply result: ", res.Result)
	if res.Result == MapleSuccessResult {
		fmt.Println("intermediate file step one finished")

		mergeResult := make(map[string][]string)
		for _, pair := range res.FinalResultList {
			if len(pair) < 2 {
				fmt.Println("input pair length error")
				return nil
			} else if pair[0] == "" {
				fmt.Println("input pair first element error")
				return nil
			} else if pair[1] == "" {
				fmt.Println("input pair second element error")
				return nil
			} else {
				mergeResult[pair[0]] = append(mergeResult[pair[0]], pair[1])
			}
		}

		dirnames := writeMergeResultToFolders(mergeResult, request.IntermediateFilePrefix)

		for _, dir := range dirnames {
			compressedFileName := dir + ".tar.gz"
			generateCompressFiles("local_"+compressedFileName, dir)
			removeFilesInDirectory(dir + "/")
			removeDirectory(dir)
			putUpdateFileRequest("local_"+compressedFileName, compressedFileName)
			os.Remove("local_" + compressedFileName)
		}

		*result = MapleSuccessResult
		return nil
	} else {
		*result = MapleFailedResult
		return res.Error
	}
}

//StartMapleTask will be invoke from master to client(worker) to start the maple task
func (p *MapleService) StartMapleTask(request MapleRequest, result *MapleTaskResult) error {
	taskMutex.Lock()
	inputfileNames := []string{}
	for _, file := range request.TaskInputFiles {
		// check if file exists in globalFiles
		if !checkFileExist(file.SdfsFileName, true) {
			(*result).ReplyResult = MapleDataNotExistErrorReply
			return nil
		}
		// get file if file not in sdfsFiles
		if !checkFileExist(file.SdfsFileName, false) {
			for {
				fileLocalName := "temp_maple_" + file.SdfsFileName
				if getFileRequest(file.SdfsFileName, fileLocalName) {
					inputfileNames = append(inputfileNames, fileLocalName)
					break
				}
			}
		} else {
			inputfileNames = append(inputfileNames, file.SdfsFileName)
		}
	}

	var mod string
	var funcName string
	switch request.TaskCommand {
	case "wc":
		mod = "wc_map.so"
		funcName = "Map"
	case "rwl":
		mod = "rwl_map.so"
		funcName = "MapRwl"
	default:
		fmt.Println("no such function")
		return nil
	}

	plug, err := plugin.Open(mod)
	if err != nil {
		(*result).ReplyResult = MapleProgramErrorReply
		return err
	}

	symbol, err := plug.Lookup(funcName)
	if err != nil {
		(*result).ReplyResult = MapleProgramErrorReply
		return err
	}

	mapper, ok := symbol.(func([]string) ([][]string, error))
	if !ok {
		(*result).ReplyResult = MapleProgramErrorReply
		return nil
	}

	fmt.Println("input files are ", inputfileNames)
	pairs, err := mapper(inputfileNames)
	if err != nil {
		(*result).ReplyResult = MapleProgramErrorReply
		return err
	}
	(*result).ResultList = pairs
	(*result).ReplyResult = MapleSuccessReply
	fmt.Println("maple task finished")
	taskMutex.Unlock()
	return nil
}

func mapleAllocateTaskRequest(cmd string, interFilesPrefix string, srcFilesDir string, workerCnt int) {
	req := MapleRequest{cmd, interFilesPrefix, srcFilesDir, []FileInfo{}, workerCnt, ""}
	client, err := rpc.Dial("tcp", introducerAddr+":"+maplePort)
	if err != nil {
		fmt.Println("dialing:", err)
	}
	fmt.Println("send allocate request with content ", req)
	var result MapleResultType
	err = client.Call("MapleService.AllocateMapleTask", req, &result)
	if err != nil {
		fmt.Println(err)
		return
	}
	if result != MapleSuccessResult {
		fmt.Println("maple failed with result ", result)
		return
	}
	fmt.Println("maple success")
}

func writeMergeResultToFolders(mergeResult map[string][]string, requestPrefix string) []string {
	// prefix string, dirnames []string
	dirnames := []string{}
	fmt.Println(len(mergeResult))
	numFile := (int)(math.Ceil((float64)(len(mergeResult)) / 10.0))
	count := 0

	for i := 0; i < 10; i++ {
		dirname := intermediateFilePrefix + requestPrefix + "_" + strconv.Itoa(i)
		if _, err := os.Stat(dirname); os.IsNotExist(err) {
			os.Mkdir(dirname, 0777)
			fmt.Println("make dir success: ", dirname)
		}
		removeFilesInDirectory(dirname + "/")
		dirnames = append(dirnames, dirname)
	}

	for k, v := range mergeResult {
		folderIndex := count / numFile
		dirname := dirnames[folderIndex]

		outputFileName := tempIntermediateFilePrefix + k
		outputFile, _ := os.OpenFile(dirname+"/"+outputFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		for _, val := range v {
			outputFile.WriteString(val + "\n")
		}
		outputFile.Close()
		count++
	}

	return dirnames
}

func juiceInit() {
	juiceTaskChannel = make(chan JuiceTaskChannelMessage, 10)
	juicePhraseResultChannel = make(chan JuiceResultChannelMessage, 10)
	go juiceMainLoop()
}

//AllocateJuiceTask will be invoke from client and be handled by the master to allocate juice task
func (p *JuiceService) AllocateJuiceTask(request JuiceRequest, result *JuiceResultType) error {
	inputFiles := []FileInfo{}
	for _, f := range globalFiles {
		// sdfs_intermediate_prefix_i
		prefix := intermediateFilePrefix + request.IntermediateFilePrefix
		if len(f.SdfsFileName) >= len(prefix) && f.SdfsFileName[:len(prefix)] == prefix {
			inputFiles = append(inputFiles, f)
		}
	}

	if len(inputFiles) == 0 {
		fmt.Println("input files empty for juice!")
		*result = JuiceFailedResult
		return nil
	}
	workerInputFilesMap := make(map[Member][]FileInfo)
	filesCount := len(inputFiles)
	fmt.Println("before allocate the juice task , global files are ", globalFiles)
	fmt.Println("before allocate the juice task , input files are ", inputFiles)
	fmt.Println("before allocate the juice task , member list are ", MemberList)
	fmt.Println("before allocate the juice task , ring are ", ring)
	for _, file := range inputFiles {
		assignFileToWorker(file, workerInputFilesMap, request.WorkerCount, filesCount)
	}
	fmt.Println("the worker input files map is ", workerInputFilesMap)
	go juiceTaskSchedule(len(workerInputFilesMap), workerInputFilesMap)
	cnt := 0
	for worker, files := range workerInputFilesMap {
		request := JuiceRequest{
			request.TaskCommand,
			request.IntermediateFilePrefix,
			request.DestinationFileName,
			files,
			request.DeleteInterMediateFilesOption,
			request.WorkerCount,
			strconv.Itoa(cnt),
		}
		cnt++
		fmt.Println("send start juice task to worker ", worker, "with request ", request)
		go sendJuiceRequestToWorker(request, worker)
	}
	res := <-juicePhraseResultChannel
	fmt.Println("reply result: ", res.Result)
	if res.Result == JuiceSuccessResult {
		resultMap := res.FinalResultMap
		var keys []string
		for k := range resultMap {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		outputResultFileName := request.DestinationFileName
		ResultFilePtr, _ := os.OpenFile(outputResultFileName, os.O_CREATE|os.O_WRONLY, 0644)
		for _, k := range keys {
			s := k + "    " + resultMap[k] + "\n"
			ResultFilePtr.WriteString(s)
		}
		ResultFilePtr.Close()
		putUpdateFileRequest(outputResultFileName, "sdfs_"+outputResultFileName)

		if request.DeleteInterMediateFilesOption == true {
			files := getFilesFromDirectory(request.IntermediateFilePrefix)
			for _, file := range files {
				deleteFileRequest(file.SdfsFileName)
			}
		}

		*result = JuiceSuccessResult
		return nil
	} else {
		*result = JuiceSuccessResult
		return res.Error
	}
}

// StartJuiceTask will start the juice task
func (p *JuiceService) StartJuiceTask(request JuiceRequest, result *JuiceTaskResult) error {
	inputCompressedFileList := []string{}
	for _, file := range request.TaskInputFiles {
		// check if file exists in globalFiles
		if !checkFileExist(file.SdfsFileName, true) {
			(*result).ReplyResult = JuiceDataNotExistErrorReply
			return nil
		}
		// get file if file not in sdfsFiles
		if !checkFileExist(file.SdfsFileName, false) {
			for {
				fileLocalName := "temp_juice_" + file.SdfsFileName
				if getFileRequest(file.SdfsFileName, fileLocalName) {
					inputCompressedFileList = append(inputCompressedFileList, fileLocalName)
					break
				}
			}
		} else {
			inputCompressedFileList = append(inputCompressedFileList, file.SdfsFileName)
		}
	}
	fmt.Println("Juice input comressedFileList are: ", inputCompressedFileList)

	var mod string
	var funcName string
	switch request.TaskCommand {
	case "wc":
		mod = "wc_reduce.so"
		funcName = "Reduce"
	case "rwl":
		mod = "rwl_reduce.so"
		funcName = "ReduceRwl"
	default:
		fmt.Println("no such function")
		(*result).ReplyResult = JuiceDataNotExistErrorReply
		return nil
	}

	plug, err := plugin.Open(mod)
	if err != nil {
		fmt.Println("open plugin error")
		(*result).ReplyResult = JuiceDataNotExistErrorReply
		return err
	}

	symbol, err := plug.Lookup(funcName)
	if err != nil {
		fmt.Println("lookup symbol error")
		(*result).ReplyResult = JuiceDataNotExistErrorReply
		return err
	}

	reducer, ok := symbol.(func([]string) (map[string]string, error))
	if !ok {
		fmt.Println("ok fail")
		(*result).ReplyResult = JuiceDataNotExistErrorReply
		return nil
	}

	inputFiles := []string{}
	for _, compressedFile := range inputCompressedFileList {
		err := uncompressFiles(compressedFile)
		if err != nil {
			fmt.Println("error in uncompress: ", err)
			(*result).ReplyResult = JuiceDataNotExistErrorReply
			return err
		}

		dirName := compressedFile[:len(compressedFile)-len(".tar.gz")]
		if dirName[:len("temp_juice_")] == "temp_juice_" {
			dirName = dirName[len("temp_juice_"):]
		}
		allFilesInTheDir, err := filepath.Glob("./" + dirName + "/*")
		if err != nil {
			fmt.Println("error in get files in dir: ", err)
			(*result).ReplyResult = JuiceDataNotExistErrorReply
			return err
		}
		fmt.Println("Extracted ", len(allFilesInTheDir), " files in unzipped folder: ", dirName)
		inputFiles = append(inputFiles, allFilesInTheDir...)
	}
	fmt.Println("JUICE total input file length: ", len(inputFiles))

	resultMap, err := reducer(inputFiles)
	if err != nil {
		fmt.Println("plugin run error")
		(*result).ReplyResult = JuiceDataNotExistErrorReply
		return err
	}

	(*result).ResultMap = resultMap
	(*result).ReplyResult = JuiceSuccessReply
	fmt.Println("juice task finished")
	return nil
}

func juiceTaskSuccessAction(request JuiceRequest, worker Member, result JuiceTaskResult, err error) {
	task := JuiceTask{request.TaskCommand, request.IntermediateFilePrefix, request.DestinationFileName, request.TaskInputFiles, request.DeleteInterMediateFilesOption, request.TaskID}
	msg := JuiceTaskChannelMessage{worker, task, result, err}
	fmt.Println("juice task success, member ", worker, "finished ", task)
	juiceTaskChannel <- msg
}

func juiceTaskFailedAction(request JuiceRequest, worker Member, result JuiceTaskResult, err error) {
	task := JuiceTask{request.TaskCommand, request.IntermediateFilePrefix, request.DestinationFileName, request.TaskInputFiles, request.DeleteInterMediateFilesOption, request.TaskID}
	msg := JuiceTaskChannelMessage{worker, task, result, err}
	fmt.Println("juice task failed, member ", worker, "fail to finish ", task)
	juiceTaskChannel <- msg
}

func sendJuiceRequestToWorker(request JuiceRequest, worker Member) {
	client, err := rpc.Dial("tcp", worker.MemberIP+":"+juicePort)
	if err != nil {
		fmt.Println("dialing:", err)
	}
	var result JuiceTaskResult
	err = client.Call("JuiceService.StartJuiceTask", request, &result)

	if err != nil {
		fmt.Println(err)
		fmt.Println("rpc call error ", err)
		juiceTaskFailedAction(request, worker, result, err)
		return
	}
	if result.ReplyResult != JuiceSuccessReply {
		fmt.Println("juice task failed with result ", result)
		juiceTaskFailedAction(request, worker, result, err)
		return
	}
	juiceTaskSuccessAction(request, worker, result, err)
}

func reassignFailedJuiceTask(failedTask JuiceTask, failedMember Member, workerCount int, workerInputFilesMap map[Member][]FileInfo) {
	fileList := workerInputFilesMap[failedMember]
	foundNoWorkNodes := false
	var worker Member

	for _, member := range MemberList {
		_, exist := workerInputFilesMap[member]
		if !exist {
			// found a node which is not working
			worker = member
			foundNoWorkNodes = true
			break
		}
	}

	if !foundNoWorkNodes {
		// all alive nodes are working
		for _, member := range MemberList {
			if member.MemberID != failedMember.MemberID {
				// just find an alive node
				worker = member
			}
		}
	}

	request := JuiceRequest{
		failedTask.TaskCommand,
		failedTask.IntermediateFilePrefix,
		failedTask.DestinationFileName,
		failedTask.TaskInputFiles,
		failedTask.DeleteInterMediateFilesOption,
		workerCount,
		failedTask.TaskID,
	}
	workerInputFilesMap[worker] = fileList
	go sendJuiceRequestToWorker(request, worker)
	delete(workerInputFilesMap, failedMember)
}

func juiceTaskSchedule(workerCount int, workerInputFilesMap map[Member][]FileInfo) {
	var msg JuiceTaskChannelMessage
	unfinishedJobCount := workerCount
	resultMap := make(map[string]string)
	var resultMapMutex sync.Mutex
	fmt.Println("There are ", unfinishedJobCount, " needs to be done")
	for {
		select {
		case msg = <-juiceTaskChannel:
			if msg.TaskResult.ReplyResult == JuiceSuccessReply {
				fmt.Println("juice succ at node ", msg.Worker, " with task ", msg.Task)
				resultMapMutex.Lock()
				for k := range msg.TaskResult.ResultMap {
					resultMap[k] = msg.TaskResult.ResultMap[k]
				}
				resultMapMutex.Unlock()
				unfinishedJobCount--
				fmt.Println("current unfinished job left ", unfinishedJobCount)
				if unfinishedJobCount == 0 {
					// all jobs are done
					res := JuiceResultChannelMessage{JuiceSuccessResult, resultMap, nil}
					juicePhraseResultChannel <- res
					return
				}
			} else {
				if msg.Error != nil {
					// rpc dial error, which means the worker has failed
					fmt.Println("juice fail at node ", msg.Worker, " with task ", msg.Task)
					fmt.Println("juice failure detected by rpc!")
					// detect member fail, and this may earilier than heartbeat detect since rpc is sync invoke
					time.Sleep(8 * time.Second)
					go reassignFailedJuiceTask(msg.Task, msg.Worker, workerCount, workerInputFilesMap)
				} else if msg.TaskResult.ReplyResult != JuiceSuccessReply {
					// todo, need to know the error types of the rpc call
					fmt.Println("reply error with content ", msg.TaskResult.ReplyResult)
				}
			}
		}
	}
}

func juiceAllocateTaskRequest(cmd string, interFilesPrefix string, dstFileName string, deleteInterOption bool, workerCnt int) {
	req := JuiceRequest{cmd, interFilesPrefix, dstFileName, []FileInfo{}, deleteInterOption, workerCnt, ""}
	client, err := rpc.Dial("tcp", introducerAddr+":"+maplePort)
	if err != nil {
		fmt.Println("dialing:", err)
	}
	fmt.Println("send allocate juice request with content ", req)
	var result JuiceResultType
	err = client.Call("JuiceService.AllocateJuiceTask", req, &result)
	if err != nil {
		fmt.Println(err)
		return
	}
	if result != JuiceSuccessResult {
		fmt.Println("juice failed with result ", result)
		return
	}	
	fmt.Println("juice success")
	if deleteInterOption {
		for _, file := range globalFiles {
			prefix := intermediateFilePrefix + interFilesPrefix
			if len(file.SdfsFileName) > len(prefix) {
				if file.SdfsFileName[:4] == "temp" || file.SdfsFileName[:len(prefix)] == prefix {
					deleteFileRequest(file.SdfsFileName)
				}
			}
		}
		fmt.Println("Deleted all intermediate files")
	}
}

func juiceMainLoop() {
	rpc.RegisterName("JuiceService", new(JuiceService))

	listener, err := net.Listen("tcp", ":"+juicePort)
	if err != nil {
		fmt.Println("ListenTCP error:", err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
		}

		go rpc.ServeConn(conn)
	}
}

func mapleJuiceMainLoop() {
	go mapleMainLoop()
	go juiceMainLoop()
}

func mainLoop() {
	reader := bufio.NewReader(os.Stdin)
	//var cmd string
	for {
		fmt.Println("Please input command: \n",
			"1: Print current member list\n",
			//"2: Print selfID()\n",
			"2: start juice\n",
			"3: Join the Group\n",
			//"4: Query logfile\n",
			"4: start maple\n",
			"5: put/update file\n",
			"6: get file\n",
			"7: delete file\n",
			"8: list file (machines that have this file)\n",
			"9: store (list all files in this machine)\n",
			"0: put Dir",
		)
		cmd, _ := reader.ReadString('\n')
		cmd = cmd[:len(cmd)-1]
		if len(cmd) == 0 {
			// just return character ignore
			fmt.Println("Invalid command")
			continue
		}
		// remove /n at last
		switch cmd[0] {
		case '1':
			{
				PrintMemberList()
			}
		case '2':
			{
				//PrintSelfID()
				cmds := strings.Split(cmd, " ")
				if len(cmds) != 6 {
					fmt.Println("Usage for juice: 2 juice juice_exe number_juices sdfs_intermediate_filename_prefix sdfs_dest_file delete_intermediate{0, 1} ")
					continue
				}

				WorkerCnt, _ := strconv.Atoi(cmds[2])
				if WorkerCnt <= 0 {
					fmt.Println("invaild worker count")
					continue
				}

				deleteOptionInt, _ := strconv.Atoi(cmds[5])
				var option bool
				if deleteOptionInt != 0 && deleteOptionInt != 1 {
					fmt.Println("invaild delete option")
					continue
				} else if deleteOptionInt == 1 {
					option = true
				} else {
					option = false
				}

				juiceAllocateTaskRequest(cmds[1], cmds[3], cmds[4], option, WorkerCnt)
			}
		case '3':
			{
				if IP == introducerAddr || len(MemberList) > 1 {
					fmt.Println("You have already joined the group!")
					break
				} else {
					JoinGroup()
				}
			}
		case '4':
			{
				cmds := strings.Split(cmd, " ")
				if len(cmds) != 5 {
					fmt.Println("Usage for maple: 4 maple maple_exe number_maples sdfs_intermediate_filename_prefix sdfs_src_directory>")
					continue
				}
				cnt, _ := strconv.Atoi(cmds[2])
				if cnt <= 0 {
					fmt.Println("invaild worker count")
					continue
				}
				mapleAllocateTaskRequest(cmds[1], cmds[3], cmds[4], cnt)
				//GrepLogfiles()

			}
		case '5':
			{
				cmds := strings.Split(cmd, " ")
				if len(cmds) != 3 {
					fmt.Println("Usage for put/update file: 5 localfilename sdfsfilename")
					continue
				}
				putUpdateFileRequest(cmds[1], cmds[2])
			}
		case '6':
			{
				cmds := strings.Split(cmd, " ")
				if len(cmds) != 3 {
					fmt.Println("Usage for get file: 6 sdfsfilename localfilename")
					continue
				}
				getFileRequest(cmds[1], cmds[2])
			}
		case '7':
			{
				cmds := strings.Split(cmd, " ")
				if len(cmds) != 2 {
					fmt.Println("Usage for delete file: 7 sdfsfilename")
					continue
				}
				deleteFileRequest(cmds[1])
			}
		case '8':
			{
				cmds := strings.Split(cmd, " ")
				if len(cmds) != 2 {
					fmt.Println("Usage for list: 8 sdfsfilename")
					continue
				}
				fmt.Println(getFileStoreNodes(cmds[1]))
			}
		case '9':
			{
				fmt.Println(sdfsFiles)
			}
		case '0':
			{
				cmds := strings.Split(cmd, " ")
				if len(cmds) != 2 {
					fmt.Println("Usage for put Dir: 0 localDirPath")
					continue
				}
				putDirRequest(cmds[1])
			}
		default:
			{
				fmt.Println("Invalid command")
			}
		}
	}
}

func main() {
	Membership()
	mainLoop()
}
