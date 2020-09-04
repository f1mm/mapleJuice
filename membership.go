
package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"sort"
	"sync"
	"time"
)

/*MessageType defines the message's type*/
type MessageType string

const (
	//MemberJoinMsg will be sent from new node to the introducer node
	MemberJoinMsg MessageType = "MemberJoin"
	// HeartBeatMsg will be sent from one node to its neighbors
	HeartBeatMsg MessageType = "HeartBeat"
	// MemberFailMsg will be sent from one node to its neighbors when the current node detects failure
	MemberFailMsg MessageType = "MemberFail"
	// NewMemberMsg will be sent from the introducer node to all the other nodes when there is an new node
	NewMemberMsg MessageType = "NewMember"
	// PushMemberListMsg will be sent from the introducer node to other nodes when new node join
	PushMemberListMsg MessageType = "PushMemberList"
	// EndHeartbeatMsg will be sent to the member's monitors when group member changes
	EndHeartbeatMsg MessageType = "EndHeartbeat"
)

/*Message the Content stores the msg content, the host stores the host IP*/
type Message struct {
	Content   MessageType
	MsgSource string
	// used by the JoinMsg NewNodeMsg and PushMemberListMsg
	Members []Member
}

/*Member stores the info about the member*/
type Member struct {
	MemberID string
	MemberIP string
	NodeName string
}

const bufSize int = 2048
const heartBeatMonitorsCnt int = 3
const gossipTargetCnt int = 3
const portNumber string = "10087"
const introducerAddr string = "172.22.152.176" // fa19-cs425-g52-01.cs.illinois.edu
const heartBeatTimeOutValue = time.Second * 8
const heartBeatPeriodValue = time.Second * 2

// IPNodeNameMap stores the IP of the nodes
var IPNodeNameMap = map[string]string{
	"172.22.152.176": "vm1",
	"172.22.154.172": "vm2",
	"172.22.156.172": "vm3",
	"172.22.152.177": "vm4",
	"172.22.154.173": "vm5",
	"172.22.156.173": "vm6",
	"172.22.152.178": "vm7",
	"172.22.154.174": "vm8",
	"172.22.156.174": "vm9",
	"172.22.152.179": "vm10",
}

var (
	//IP is the ip addr of the current machine
	IP string
	//ID is a time-related unique identifier
	ID string
	//NodeName looks like vm1 vm2
	NodeName string
	//MemberList stores all members in the group
	self Member
	// MemberList comment
	MemberList []Member
	// HeartBeatSenderTicker comment
	HeartBeatSenderTicker *time.Ticker
	// HeartBeatReceiverTimersMap comment
	HeartBeatReceiverTimersMap map[string]*time.Timer

	// MemberListIndex index
	MemberListIndex int

	// MemberListMutex Mutex
	MemberListMutex sync.Mutex
	// GossipTargetsMutex Mutex
	GossipTargetsMutex sync.Mutex
	// HeartBeatMonitorsMutex Mutex
	HeartBeatMonitorsMutex sync.Mutex
	// ModifyTimerMutex Mutex
	ModifyTimerMutex sync.Mutex

	// GossipTargets comment
	GossipTargets []string
	// JoinedGroup comment
	JoinedGroup bool

	// logger comment
	logger *log.Logger
	// logfile comment
	logfile *os.File
)

// memberlist data operation functions

/*SortMemberListByIP will sort the members according to the name*/
func SortMemberListByIP() {
	sort.SliceStable(MemberList, func(i, j int) bool {
		return MemberList[i].MemberID < MemberList[j].MemberID
	})
}

/*GetTargetMemberListIndex will calculate the target member's index in the memberList*/
func GetTargetMemberListIndex(targetID string) int {
	for index, member := range MemberList {
		if member.MemberID == targetID {
			return index
		}
	}
	return -1
}

/*UpdateMemberListIndex will calculate the current node's index in the memberList*/
func UpdateMemberListIndex() {
	for index, member := range MemberList {
		if member.MemberID == ID {
			// fmt.Println("node ", NodeName, "change index from ", MemberListIndex, " to ", index)
			MemberListIndex = index
			return
		}
	}
	// fmt.Println("Update member list error, cannot find the index of the current machine")
	MemberListIndex = -1
}

/*UpdateMemberList will update the local memberlist*/
func UpdateMemberList(List []Member, add bool) {
	MemberListMutex.Lock()
	if add == true {
		// add member to the list
		if len(List) == 1 {
			// introducer received join requests and add the new member to its list, or other receive new member msg from introducer
			MemberList = append(MemberList, List[0])
		} else {
			// other member receive full member list from introducer when member first join
			MemberList = List
		}
	} else {
		// remove target member
		RemoveTargetMember(List[0].MemberID)
	}
	SortMemberListByIP()
	UpdateMemberListIndex()
	UpdateGossipTargets()
	MemberListMutex.Unlock()
}

/*RemoveTargetMember will remove the target member*/
func RemoveTargetMember(targetMemberID string) {
	for index, member := range MemberList {
		if member.MemberID == targetMemberID {
			MemberList = append(MemberList[:index], MemberList[index+1:]...)
			fmt.Println("remove member with ID ", targetMemberID)
			return
		}
	}
	fmt.Println("remove target member with ID ", targetMemberID, " error, cannot find this member")
}

/*UpdateGossipTargets will update the gossip targets*/
func UpdateGossipTargets() {
	GossipTargetsMutex.Lock()
	cnt := 0
	old := GossipTargets

	msg := MakeMessage(EndHeartbeatMsg, ID, []Member{})
	SendMessageToTargets(old, msg)

	for i := MemberListIndex + 1; i < MemberListIndex+gossipTargetCnt+1; i++ {
		index := i % len(MemberList)
		if len(MemberList) == 1 {
			//to deal with edge case that there is only one member (debug use)
			break
		}
		GossipTargets[cnt] = MemberList[index].MemberIP
		logger.Println("Member ", NodeName, " added member ", GetNodeNameByIP(MemberList[index].MemberIP), " to its gossiptargets")
		cnt = cnt + 1
		if cnt >= len(MemberList)-1 {
			//to deal with edge case that the total member cnt is less than 4 (debug use)
			break
		}
	}

	//fmt.Println("Gossip target at ", NodeName, "changed from ", old, " to ", GossipTargets)
	GossipTargetsMutex.Unlock()
}

/*SendMessageToTargets will make a UDP dial to nodes in the targetAddr slice and send msg to them*/
func SendMessageToTargets(targets []string, msg Message) {
	for i := 0; i < len(targets); i++ {
		target := targets[i]
		if target == "" {
			continue
		}
		SendMessageToTarget(target, msg)
	}
}

/*SendMessageToTarget will make a UDP dial to the single target and send msg to it*/
func SendMessageToTarget(target string, msg Message) {
	SerializedMsg := SerializeMessage(msg)
	conn, err := DialUDPWithLog(target)
	if err != nil {
		return
	}
	_, e := conn.Write(SerializedMsg)
	if e != nil {
		fmt.Println("write to connection error during send msg ", msg, " from ", NodeName, " to ", GetNodeNameByIP(target), " with error", e)
	} else {
		if msg.Content != HeartBeatMsg {
			// fmt.Println("send msg ", msg, " from ", NodeName, " to ", GetNodeNameByIP(target))
		}
		logger.Println("Member ", NodeName, " send ", msg.Content, " to member: ", GetNodeNameByIP(target))
	}
}

// timer functions

/*HeartBeatTimeoutCallBack will handle when found some members have been detected as failure*/
func HeartBeatTimeoutCallBack(timeOutMemberID string) {
	// the failed memberID
	//fmt.Println("HeartBeat timeout at member ", timeOutMemberID)
	targetIndex := GetTargetMemberListIndex(timeOutMemberID)
	// log this detected failure
	fmt.Println("Member ", timeOutMemberID, " failed detected by member ", NodeName)
	logger.Println("Member ", timeOutMemberID, " failed detected by member ", NodeName)
	if targetIndex == -1 {
		// already removed this member from gossip from other member
		RemoveTargetHeartBeatReceiveTimer(timeOutMemberID)

	} else {
		failedMember := MemberList[GetTargetMemberListIndex(timeOutMemberID)]
		failedMemberList := []Member{failedMember}
		RemoveTargetHeartBeatReceiveTimer(timeOutMemberID)
		// remove the timeout member
		UpdateMemberList(failedMemberList, false)
		msg := MakeMessage(MemberFailMsg, ID, failedMemberList)
		SendMessageToTargets(GossipTargets, msg)

		// update the sdfs file system
		go updateSDFSVariablesWhenMemberRemove(failedMember)
		logger.Println("Member ", NodeName, " update the sdfs after ", msg.Members[0].NodeName, " failed.")
	}
}

/*AddMemberToHeartBeatReceiverMap will add a timer in the map whose key is the target*/
func AddMemberToHeartBeatReceiverMap(targetID string) {
	ModifyTimerMutex.Lock()
	newTimer := time.AfterFunc(heartBeatTimeOutValue, func() {
		HeartBeatTimeoutCallBack(targetID)
	})
	HeartBeatReceiverTimersMap[targetID] = newTimer
	//fmt.Println("Add timer for ", targetID)
	logger.Println("Member ", NodeName, " added timer for ", targetID)
	ModifyTimerMutex.Unlock()
}

/*RemoveTargetHeartBeatReceiveTimer will remove the timer in the map according to the ID*/
func RemoveTargetHeartBeatReceiveTimer(targetID string) {
	ModifyTimerMutex.Lock()
	if targetTimer, ok := HeartBeatReceiverTimersMap[targetID]; ok {
		targetTimer.Stop()
		delete(HeartBeatReceiverTimersMap, targetID)
		logger.Println("Member ", NodeName, " removed member ", targetID, " from its HeartbeatReceiverMap")
	}
	ModifyTimerMutex.Unlock()
}

/*ResetTargetHeartBeatReceiverTimer will reset the timer in the map according to the ID*/
func ResetTargetHeartBeatReceiverTimer(targetID string) {
	ModifyTimerMutex.Lock()
	targetTimer := HeartBeatReceiverTimersMap[targetID]
	targetTimer.Stop()
	HeartBeatReceiverTimersMap[targetID] = time.AfterFunc(heartBeatTimeOutValue, func() {
		HeartBeatTimeoutCallBack(targetID)
	})
	ModifyTimerMutex.Unlock()
}

/*PeriodicallySendHeartBeat will send heartbeat msg to gossip targets*/
func PeriodicallySendHeartBeat() {
	for {
		select {
		case <-HeartBeatSenderTicker.C:
			{
				if len(MemberList) == 1 || !JoinedGroup {
					// when the length is 1, there only exist introducer.And when the node does not join in group, do nothing
				} else {
					// send heartbeat to others
					HeartBeatRequest()
				}
			}
		}
	}

}

// msg listening and handle functions

/*StartListeningMessage will start listen the msg port and do different thing according to the msg
some code reference to https://colobu.com/2016/10/19/Go-UDP-Programming/
*/
func StartListeningMessage() {
	localUDPAddr := GetUDPAddr(IP + ":" + portNumber)
	if localUDPAddr == nil {
		return
	}
	conn, err := net.ListenUDP("udp", localUDPAddr)
	if err != nil {
		fmt.Println("Listen error with ", err)
		return
	}
	// fmt.Println("start listening at ", portNumber)
	buf := make([]byte, bufSize)
	for {
		_, remote, err := conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Println("read from ", remote, "error with ", err)
		}
		msg := UnserializeMessage(buf)
		logger.Println(NodeName, " received msg: ", msg)
		HandleMessageReceive(msg)
	}
}

/*HandleMessageReceive defines the behavior to handle the incoming msg*/
func HandleMessageReceive(msg Message) {
	if msg.Content != HeartBeatMsg {
		// fmt.Println("Handle msg ", msg)
	}
	switch msg.Content {
	case MemberJoinMsg:
		// this msg will only be able to receive in the introducer node
		{
			HandleMemberJoinMsg(msg)
		}
	case HeartBeatMsg:
		// receive heartbeat
		{
			HandleHeartBeatMsg(msg)
		}
	case PushMemberListMsg:
		// this msg will only be able to receive in the new joining node
		{
			HandlePushMemberListMsg(msg)
		}
	case NewMemberMsg:
		// this msg will only be generated by the introducer
		{
			HandleNewMemberMsg(msg)
		}
	case MemberFailMsg:
		// this msg will send when a member detects timeout or receive fail msg from others
		{
			HandleMemberFailMsg(msg)
		}
	case EndHeartbeatMsg:
		// this msg will send when a member remove it's heartbeat sender
		{
			HandleEndHeartbeatMsg(msg)
		}
	}
}

/*HandleMemberFailMsg will handle the msg*/
func HandleMemberFailMsg(msg Message) {
	index := GetTargetMemberListIndex(msg.Members[0].MemberID)
	if index == -1 {
		// this member has been already removed in the group, just return, don't gossip anymore
		return
	}
	// receive the msg for the first time. update list and then gossip
	logger.Println("Member ", NodeName, " received the failure of ", msg.Members[0].NodeName)
	UpdateMemberList(msg.Members, false)
	RemoveTargetHeartBeatReceiveTimer(msg.Members[0].MemberID)
	SendMessageToTargets(GossipTargets, msg)

	// update the sdfs file system
	updateSDFSVariablesWhenMemberRemove(msg.Members[0])
	logger.Println("Member ", NodeName, " update the sdfs after ", msg.Members[0].NodeName, " failed.")
}

/*HandleMemberJoinMsg will handle the msg only introducer can receive this*/
func HandleMemberJoinMsg(msg Message) {
	NewMember := msg.Members[0]
	index := GetTargetMemberListIndex(NewMember.MemberID)
	if index != -1 {
		// already in the group, just return, don't gossip anymore
		return
	}
	// receive the msg for the first time. update list and then gossip
	logger.Println("Introducer ", NodeName, " received the join of ", GetNodeNameByIP(NewMember.MemberIP))
	UpdateMemberList(msg.Members, true)

	// send full memberlist to the new member
	newmsg := MakeMessage(PushMemberListMsg, ID, MemberList)
	logger.Println("Introducer ", NodeName, " send full memberlist to new member ", GetNodeNameByIP(NewMember.MemberIP))
	SendMessageToTarget(NewMember.MemberIP, newmsg)

	// gossip this newmembermsg to its targets
	newMemberList := []Member{NewMember}
	newmsg = MakeMessage(NewMemberMsg, ID, newMemberList)
	SendMessageToTargets(GossipTargets, newmsg)

	// update the sdfs file system
	updateSDFSVariablesWhenMemberAdd(NewMember)
	logger.Println("Member ", NodeName, " update the sdfs after ", msg.Members[0].NodeName, " joined.")
}

/*HandleHeartBeatMsg will handle the msg*/
func HandleHeartBeatMsg(msg Message) {
	if len(HeartBeatReceiverTimersMap) <= heartBeatMonitorsCnt {
		logger.Println("Member ", NodeName, " received heartbeat from ", msg.Members[0].NodeName)
		//current receiver map is smaller than the target cnt, just add
		if _, ok := HeartBeatReceiverTimersMap[msg.MsgSource]; ok {
			// already exist
			ResetTargetHeartBeatReceiverTimer(msg.MsgSource)
			logger.Println("Member ", NodeName, " Reset Target HeartBeat Receiver Timer for member", msg.Members[0].NodeName)
		} else {
			// new timer
			AddMemberToHeartBeatReceiverMap(msg.MsgSource)
			logger.Println("Member ", NodeName, " Add Member ", msg.Members[0].NodeName, " To HeartBeatReceiverMap ")
		}
	} else {
		// in this case, we just ingore the new request until the failed member has been removed
		fmt.Println("Warning, receive timer map has more cnt than assume, this may due to some member fails")
	}
}

/*HandlePushMemberListMsg will handle the msg*/
func HandlePushMemberListMsg(msg Message) {
	logger.Println("Newly joined member ", NodeName, " received the full membership list: ", msg.Members)
	fmt.Println("Newly joined member ", NodeName, " received the full membership list: ", msg.Members)
	UpdateMemberList(msg.Members, true)
	JoinedGroup = true

	// join sdfs file system
	normalNodeSDFSInit()
	mapleInit()
	juiceInit()
}

/*HandleNewMemberMsg will handle the msg*/
func HandleNewMemberMsg(msg Message) {
	index := GetTargetMemberListIndex(msg.Members[0].MemberID)
	if index != -1 || msg.Members[0].MemberID == ID {
		// already in the group, just return, don't gossip anymore
		return
	}
	logger.Println("Member ", NodeName, " received the join of ", msg.Members[0].NodeName)
	// receive the msg for the first time. update list and then gossip
	if msg.Members[0].MemberID != ID {
		UpdateMemberList(msg.Members, true)
	}
	SendMessageToTargets(GossipTargets, msg)

	// update the sdfs file system
	updateSDFSVariablesWhenMemberAdd(msg.Members[0])
	logger.Println("Member ", NodeName, " update the sdfs after ", msg.Members[0].NodeName, " joined.")
}

/*HandleEndHeartbeatMsg will remove it's heartbeat sender from its heartbeatreceivermap */
func HandleEndHeartbeatMsg(msg Message) {
	if _, ok := HeartBeatReceiverTimersMap[msg.MsgSource]; ok {
		RemoveTargetHeartBeatReceiveTimer(msg.MsgSource)
		//fmt.Println("remove heartbeat timer for ", msg.MsgSource)
	} else {
		//fmt.Println("failed to  heartbeat timer for ", msg.MsgSource, "do not have timer for it")
	}
}

// generate request functions

/*JoinGroupRequest will make a request to the introducer to join the group*/
func JoinGroupRequest() {
	msg := MakeMessage(MemberJoinMsg, ID, MemberList)
	logger.Println("New member ", NodeName, " send a join request to introducer")
	SendMessageToTarget(introducerAddr, msg)
}

/*HeartBeatRequest will make a heartbeat msg to its monitors*/
func HeartBeatRequest() {
	msg := MakeMessage(HeartBeatMsg, ID, []Member{self})
	SendMessageToTargets(GossipTargets, msg)
}

// Init functions

/*MemberShipInit will initialize all the needed data*/
func MemberShipInit() {
	IP = GetIPAddr()
	ID = GenerateID()
	MemberList = make([]Member, 0)
	self = Member{ID, IP, NodeName}
	// add self to the member list
	MemberList = append(MemberList, self)
	GossipTargets = make([]string, gossipTargetCnt)
	HeartBeatReceiverTimersMap = make(map[string]*time.Timer)
	HeartBeatSenderTicker = time.NewTicker(heartBeatPeriodValue)
	if IP == introducerAddr {
		// the current node is introducer
		JoinedGroup = true
		introducerSDFSInit()
		mapleInit()
		juiceInit()
	} else {
		JoinedGroup = false
	}
	InitLogOutput()
}

/*InitLogOutput will init the log output*/
func InitLogOutput() {
	logfile, err := os.OpenFile("logfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}

	logger = log.New(logfile, "logger: ", log.Lshortfile|log.Lmicroseconds)
	logger.Println("Initialized log file for member: ", NodeName)
}

//input cmd functions

/*PrintMemberList will print the memberlist*/
func PrintMemberList() {
	fmt.Println("The current member list is")
	fmt.Println(MemberList)
}

/*PrintSelfID will print the id of the current machine*/
func PrintSelfID() {
	fmt.Println("The current ID of this machine is ", ID)
}

/*JoinGroup will make a join request to the introducer*/
func JoinGroup() {
	fmt.Println("Send join request to introducer")
	JoinGroupRequest()
}

/*GrepLogfiles grep log files from other members*/
func GrepLogfiles() {
	var vm string
	var pat string
	fmt.Println("Which VMs do you want to grep? please separate by comma (e.g. '1,2,3')")
	fmt.Scan(&vm)

	fmt.Println("Please input a pattern: ")
	fmt.Scan(&pat)

	cmd := exec.Command("go", "run", "client/client.go", vm, pat)
	out, _ := cmd.CombinedOutput()
	fmt.Println(string(out))
}

/*PrintReceiveTimers will print the timers map*/
func PrintReceiveTimers() {
	fmt.Println("the timers at ", NodeName, " are", HeartBeatReceiverTimersMap)
}

/*PrintGossipTarget will print the gossip targets*/
func PrintGossipTarget() {
	fmt.Println("the gossip targets at ", NodeName, " are", GossipTargets)
}

/*Membership will init membership*/
func Membership() {
	MemberShipInit()
	go StartListeningMessage()
	go PeriodicallySendHeartBeat()
	defer logfile.Close()
}
