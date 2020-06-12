package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// create a new Raft server.
//		rf = Make(...)
// start agreement on a new log entry
//		rf.Start(command interface{}) (index, term, isleader)
// ask a Raft for its current term, and whether it thinks it is leader
//		rf.GetState() (term, isLeader)
// each time a new entry is committed to the log, each Raft peer should send
// an ApplyMsg to the service (or tester) in the same server.
//		ApplyMsg
//

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//TODO: need to check the command
type Log struct {
	Term    int
	Index   int
	Command interface{}
}

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

const HeartBeatInterval = 100 * time.Millisecond
const LeaderPeerTickInterval = 10 * time.Millisecond
const commitIdleInterval = 25 * time.Millisecond

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	applyCh chan ApplyMsg // Channel for the commit to the state machine

	// Your data here (3, 4).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []Log

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	//append rpc
	leaderId int

	//additional
	lastHeartBeat    time.Time
	state            string
	appendSignalChan []chan struct{}
}

//
// return currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
	//var term int
	//var isleader bool

	// Your code here (3).
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader

}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

type RaftPersistence struct {
	CurrentTerm int
	Log         []Log
	VotedFor    int
	LastApplied int
}

func (rf *Raft) persist() {
	// Your code here (4).
	// Example:
	buf := new(bytes.Buffer)
	gob.NewEncoder(buf).Encode(
		RaftPersistence{
			CurrentTerm: rf.currentTerm,
			Log:         rf.log,
			VotedFor:    rf.votedFor,
		})

	rf.persister.SaveRaftState(buf.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	obj := RaftPersistence{}
	d.Decode(&obj)

	rf.votedFor, rf.currentTerm, rf.log = obj.VotedFor, obj.CurrentTerm, obj.Log

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3, 4).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (3).
	Term        int
	VoteGranted bool
}

func (reply *RequestVoteReply) countVote() int {

	if reply.VoteGranted {
		return 1
	}
	return 0
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3, 4).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//reply.Id = rf.id
	//fmt.Println("argument term ", args.Term, " term ", rf.currentTerm, " ME ", rf.me)
	lastIndex, lastTerm := rf.fetchDetailOfLastEntry()

	logUpToDate := func() bool {
		if lastTerm == args.LastLogTerm {
			return lastIndex <= args.LastLogIndex
		}
		return lastTerm < args.LastLogTerm
	}()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false

		reply.Term = rf.currentTerm
		return

	}

	if args.Term > rf.currentTerm {
		rf.changeStateToFollower(args.Term)

	}
	reply.VoteGranted = false
	if (rf.votedFor == -1 || args.CandidateId == rf.votedFor) && logUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeartBeat = time.Now()
	}
	reply.Term = rf.currentTerm

	rf.persist()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVoteToServer(server int, args *RequestVoteArgs, reply *RequestVoteReply, successChan chan bool) {
	//ok := rf.sendRequestVote(server, args, reply)
	ok := false
	for i := 0; i < 3; i++ {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			break
		}
	}

	successChan <- ok

}

func (rf *Raft) sendRequestVoteToServerHandler(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteChan chan int) {
	timeOutRPC := func() time.Duration {
		return (50) * time.Millisecond
	}

	RPCTimeout := timeOutRPC()
	rpcChan := make(chan bool, 1)

	go rf.sendRequestVoteToServer(server, args, reply, rpcChan)

	select {
	case ok := <-rpcChan:
		if ok {
			voteChan <- server
		} else {
			voteChan <- -1
		}
	case <-time.After(RPCTimeout):
		//RPCDebug("Request attempt %d timed out", requestName, attempts+1)
		voteChan <- -1
	}

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := false
	for i := 0; i < 3; i++ {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			break
		}
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Println("Inside start ", rf.me)

	//function to give index of  log entry
	index := func() int {
		if len(rf.log) > 0 {
			return rf.log[len(rf.log)-1].Index + 1
		}
		return 1
	}()

	entry := Log{Index: index, Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)
	//fmt.Println("New entry appended to leader's log: ", entry)

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	//Your code here, if desired.
	//fmt.Println("Killed ", rf.me)

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	//fmt.Println("Inside Make")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3, 4).
	rf.leaderId = -1
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	go rf.startLeaderElectionProcess()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.applyLog(applyCh)

	return rf
}

func (rf *Raft) applyLog(applyChan chan ApplyMsg) {
	//rf.mu.Lock()
	//fmt.Println("Starting commit process -  ", rf.me," Last log applied: " ,rf.lastApplied)

	//rf.mu.Unlock()

	for {
		rf.mu.Lock()

		if rf.commitIndex >= 0 && rf.commitIndex > rf.lastApplied {

			startLogIndex := rf.lastApplied
			endLogIndex := -1
			for i := startLogIndex; i < len(rf.log); i++ {
				if rf.log[i].Index <= rf.commitIndex {
					endLogIndex = i
				}
			}

			if endLogIndex >= 0 {
				entries := make([]Log, endLogIndex-startLogIndex+1)
				copy(entries, rf.log[startLogIndex:endLogIndex+1])

				rf.mu.Unlock()

				for _, entry := range entries {
					applyChan <- ApplyMsg{CommandIndex: entry.Index, Command: entry.Command, CommandValid: true}
				}

				rf.mu.Lock()
				rf.lastApplied += len(entries)
			}
			rf.mu.Unlock()

		} else {
			rf.mu.Unlock()
			<-time.After(commitIdleInterval)
		}
	}
}

func (rf *Raft) startLeaderElectionProcess() {

	timeOutForElection := func() time.Duration {
		return (350 + time.Duration(rand.Intn(100))) * time.Millisecond
	}

	reElectionTimeOut := timeOutForElection()
	// After waits for the duration to elapse and then sends the current time
	// on the returned channel.

	timeJustAfterElectionTimeOut := <-time.After(reElectionTimeOut)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Start election if I am not leader and I haven't received heartbeat
	if rf.state != Leader && timeJustAfterElectionTimeOut.Sub(rf.lastHeartBeat) >= reElectionTimeOut {
		go rf.startElection()
	}

	go rf.startLeaderElectionProcess()

}

func (rf *Raft) fetchDetailOfLastEntry() (int, int) {
	if len(rf.log) > 0 {
		entry := rf.log[len(rf.log)-1]
		return entry.Index, entry.Term
	}
	return 0, 0
}

func (rf *Raft) startElection() {

	rf.mu.Lock()
	//fmt.Println("Election started by ", rf.me)
	rf.changeStateToCandidate()
	rf.lastHeartBeat = time.Now()

	// Request votes from peers
	lastIndex, lastTerm := rf.fetchDetailOfLastEntry()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  lastTerm,
		LastLogIndex: lastIndex,
	}

	votingReplies := make([]RequestVoteReply, len(rf.peers))

	votingChannel := make(chan int, len(rf.peers))

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVoteToServerHandler(i, &args, &votingReplies[i], votingChannel)
		}
	}
	rf.persist()
	rf.mu.Unlock()

	//rf.mu.Lock()
	votes := 1
	i := 1
	flag := false
	for ; i < len(rf.peers); i++ {

		server := <-votingChannel
		rf.mu.Lock()

		if server == -1 {
			if i == len(rf.peers)-1 {
				rf.persist()

			}
			rf.mu.Unlock()
			continue

		}

		reply := votingReplies[server]

		if reply.Term > rf.currentTerm {
			rf.changeStateToFollower(reply.Term)
			flag = true
			rf.persist()

			break
		} else if votes += reply.countVote(); votes > len(rf.peers)/2 { // Has majority vote
			//fmt.Println("Has Majority vote", rf.me)
			flag = true
			if rf.state == Candidate && args.Term == rf.currentTerm {
				//fmt.Println("Election won. Vote: ", votes, " peers :", len(rf.peers), " me", rf.me, "term ", rf.currentTerm)
				go rf.promoteToLeader()
				//rf.mu.Unlock()
				rf.persist()

				break
			} else {

				//fmt.Println("Election for term ended", args.Term)
				//rf.mu.Unlock()
				rf.persist()

				break
			}

		}

		if i == len(rf.peers)-1 {

			rf.persist()

		}

		rf.mu.Unlock()

	}

	if flag {

		rf.mu.Unlock()
	}

}

func (rf *Raft) changeStateToCandidate() {
	//rf.SetStateType(Candidate)
	//fmt.Println("transition to canditate ", rf.me, " term ", rf.currentTerm)

	rf.state = Candidate
	// Increment currentTerm and vote for self
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) changeStateToFollower(newTerm int) {

	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
}

func (rf *Raft) promoteToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Leader
	rf.leaderId = rf.me

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.appendSignalChan = make([]chan struct{}, len(rf.peers))

	for i := range rf.peers {
		if i != rf.me {
			rf.nextIndex[i] = len(rf.log) + 1
			rf.matchIndex[i] = 0

			rf.appendSignalChan[i] = make(chan struct{}, 1)

			go rf.startLeaderPeerInteractionProcess(i, rf.appendSignalChan[i])
		}
	}

}

func (rf *Raft) startLeaderPeerInteractionProcess(peerIndex int, appendSignalChan chan struct{}) {
	timer := time.NewTicker(LeaderPeerTickInterval)

	//fmt.Println("Leader peer process started", rf.state, " Me", rf.me)

	rf.sendAppendEntries(peerIndex, appendSignalChan)
	lastHeartBeatSent := time.Now()

	for {

		rf.mu.Lock()
		if rf.state != Leader {
			timer.Stop()
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		//fmt.Println("Inside for loop append", rf.me)

		//whenever get a append signal then send append entry
		//when doing nothing then send heartbeat in fixed interval
		select {
		case <-appendSignalChan:
			lastHeartBeatSent = time.Now()
			rf.sendAppendEntries(peerIndex, appendSignalChan)
		case now := <-timer.C:
			if now.Sub(lastHeartBeatSent) >= HeartBeatInterval {
				//fmt.Println("Inside heartbeat", rf.me)

				lastHeartBeatSent = time.Now()
				rf.sendAppendEntries(peerIndex, appendSignalChan)
				//fmt.Println("After heartbeat", rf.me)

			}
		}

		//now := <-timer.C //  send a heartbeat
		//if now.Sub(lastHeartBeatSent) >= HeartBeatInterval {

		//	lastHeartBeatSent = time.Now()
		//rf.sendAppendEntries(peerIndex)
		//}

	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictingTerm  int // Term of the conflicting entry, if any
	ConflictingIndex int // First index of the log for the above conflicting term
}

func (rf *Raft) sendAppendEntries(peerIndex int, appendSignalChan chan struct{}) {

	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	var entries []Log = []Log{}
	var previousLogIndex, previousLogTerm int = 0, 0
	lastLogIndex, _ := rf.fetchDetailOfLastEntry()

	if lastLogIndex > 0 && lastLogIndex >= rf.nextIndex[peerIndex] {

		for i, v := range rf.log {
			if v.Index == rf.nextIndex[peerIndex] {
				if i > 0 {
					lastEntry := rf.log[i-1]
					previousLogIndex, previousLogTerm = lastEntry.Index, lastEntry.Term
				}
				entries = make([]Log, len(rf.log)-i)
				copy(entries, rf.log[i:])
				break
			}
		}
		//fmt.Println("Sending log entries of length", len(entries)," to ", peerId)

	} else {
		if len(rf.log) > 0 {
			lastEntry := rf.log[len(rf.log)-1]
			previousLogIndex, previousLogTerm = lastEntry.Index, lastEntry.Term
		}

	}

	reply := AppendEntriesReply{}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: previousLogIndex,
		PrevLogTerm:  previousLogTerm,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	rf.mu.Unlock()

	//fmt.Println("Me ", rf.me, " before sending request")

	ok := rf.sendAppendEntryRequestHandler(peerIndex, &args, &reply)
	//fmt.Println("Me ", rf.me, " after sending request")

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		//fmt.Println("Me ", rf.me, " no response", peerIndex)
	} else if reply.Term > rf.currentTerm {
		//fmt.Println("Switching to follower as in append", peerIndex," " ,reply.Term)

		rf.changeStateToFollower(reply.Term)
	} else if rf.state != Leader || reply.Term != rf.currentTerm {
		//fmt.Println("Do nothing for this condition")
	} else if reply.Success {
		if len(entries) > 0 {
			lastReplicated := entries[len(entries)-1]
			rf.matchIndex[peerIndex] = lastReplicated.Index
			rf.nextIndex[peerIndex] = lastReplicated.Index + 1
			rf.handleCommitIndexModification()
		} else {

		}
	} else {

		rf.nextIndex[peerIndex] = FindMax(reply.ConflictingIndex-1, 1)
		appendSignalChan <- struct{}{}

	}

	rf.persist()

}
func FindMax(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func (rf *Raft) handleCommitIndexModification() {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if v := rf.log[i]; v.Term == rf.currentTerm && v.Index > rf.commitIndex {
			count := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= v.Index {
					if count++; count > len(rf.peers)/2 {
						rf.commitIndex = v.Index
						break
					}
				}
			}
		} else {
			break
		}
	}
}
func (rf *Raft) sendAppendEntryRequestHandler(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := false
	timeOutRPC := func() time.Duration {
		return (50) * time.Millisecond
	}
	RPCTimeout := timeOutRPC()
	rpcChan := make(chan bool, 1)

	go rf.sendAppendEntryRequest(server, args, reply, rpcChan)

	select {
	case ok := <-rpcChan:
		return ok
	case <-time.After(RPCTimeout):
		//RPCDebug("Request attempt %d timed out", requestName, attempts+1)
		return false
	}
	return ok
}

func (rf *Raft) sendAppendEntryRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, successChan chan bool) {
	ok := false

	for i := 0; i < 3; i++ {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			break
		}
	}
	successChan <- ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm

		reply.Success = false
		return
	} else {
		rf.changeStateToFollower(args.Term)
		rf.leaderId = args.LeaderId
		rf.lastHeartBeat = time.Now()

	}

	previousLogIndex := -1
	for i, v := range rf.log {
		if v.Index == args.PrevLogIndex {
			if v.Term == args.PrevLogTerm {
				previousLogIndex = i
				break
			} else {
				reply.ConflictingTerm = v.Term
			}
		}
	}

	IsPreviousTheBeginningOfLog := args.PrevLogIndex == 0 && args.PrevLogTerm == 0

	if previousLogIndex >= 0 || IsPreviousTheBeginningOfLog {

		indexOfEntries := 0
		for i := previousLogIndex + 1; i < len(rf.log); i++ {

			entryConsistent := func() bool {
				myEntry, receivedEntry := rf.log[i], args.Entries[indexOfEntries]
				return myEntry.Index == receivedEntry.Index && myEntry.Term == receivedEntry.Term
			}

			if indexOfEntries >= len(args.Entries) || !entryConsistent() {
				rf.log = rf.log[:i]
				break
			} else {
				indexOfEntries++
			}

		}

		if indexOfEntries < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[indexOfEntries:]...)
		}

		if args.LeaderCommit > rf.commitIndex {
			var lastNewIndex = 0
			if len(rf.log) > 0 {
				lastNewIndex = rf.log[len(rf.log)-1].Index
			}

			if args.LeaderCommit < lastNewIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = lastNewIndex
			}
		}
		reply.Success = true
	} else {

		if reply.ConflictingTerm == 0 && len(rf.log) > 0 {
			reply.ConflictingTerm = rf.log[len(rf.log)-1].Term
		}

		for _, v := range rf.log {
			if v.Term == reply.ConflictingTerm {
				reply.ConflictingIndex = v.Index
				break
			}
		}

		reply.Success = false
	}
	rf.persist()
}
