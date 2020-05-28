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
	Command interface{}
}

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

const HeartBeatInterval = 100 * time.Millisecond
const LeaderPeerTickInterval = 10 * time.Millisecond

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
	lastHeartBeat  time.Time
	state          string
	sendAppendChan []chan struct{}
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
func (rf *Raft) persist() {
	// Your code here (4).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

func (reply *RequestVoteReply) VoteCount() int {

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

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false

	} else if args.Term == rf.currentTerm && rf.votedFor != args.CandidateId {

		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		rf.changeStateToFollower(args.Term)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}

	reply.Term = rf.currentTerm
	rf.persist()
	//fmt.Printf("Vote requested for: %s on term: %d. Log up-to-date? %v. Vote granted? %v", rf, args.CandidateId, args.Term, logUpToDate, reply.VoteGranted)
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
func (rf *Raft) sendRequestVoteToServer(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteChan chan int) {
	//ok := rf.sendRequestVote(server, args, reply)
	ok := false
	for i := 0; i < 3; i++ {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			break
		}
	}
	//fmt.Println("Vote Request to", server, " status " , ok)
	if ok {
		voteChan <- server
	} else {
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
	index := -1
	//term := -1
	//isLeader := true
	term, isLeader := rf.GetState()

	// Your code here (4).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3, 4).
	rf.leaderId = -1
	rf.state = Follower
	rf.currentTerm = -1
	rf.votedFor = -1
	go rf.startLeaderElectionProcess()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) startLeaderElectionProcess() {

	timeOutForElection := func() time.Duration {
		return (300 + time.Duration(rand.Intn(200))) * time.Millisecond
	}

	reElectionTimeOut := timeOutForElection()
	timeJustAfterElectionTimeOut := <-time.After(reElectionTimeOut)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Start election if I am not leader and I haven't received heartbeat
	if rf.state != Leader && timeJustAfterElectionTimeOut.Sub(rf.lastHeartBeat) >= reElectionTimeOut {
		go rf.startElection()
	}
	go rf.startLeaderElectionProcess()

}

func (rf *Raft) startElection() {

	rf.mu.Lock()
	//fmt.Println("Election started by ", rf.me)
	rf.changeStateToCandidate()

	// Request votes from peers

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  0,
		LastLogIndex: 0,
	}

	votingReplies := make([]RequestVoteReply, len(rf.peers))

	votingChannel := make(chan int, len(rf.peers))

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVoteToServer(i, &args, &votingReplies[i], votingChannel)
		}
	}
	rf.persist()
	rf.mu.Unlock()

	//rf.mu.Lock()
	votes := 1
	i := 0
	for ; i < len(rf.peers)-1; i++ {
		//serverReplied := <-votingChannel
		//reply := votingReplies[<-votingChannel]
		//fmt.Println("server replied ", serverReplied)

		server := <-votingChannel
		if server == -1 {
			continue
		}

		reply := votingReplies[server]
		rf.mu.Lock()

		votes += reply.VoteCount()
		if reply.Term > rf.currentTerm {
			rf.changeStateToFollower(reply.Term)
			//rf.mu.Unlock()
			break
		} else if votes > len(rf.peers)/2 { // Has majority vote
			// Ensure that we're still a candidate and that another election did not interrupt
			//fmt.Println("Has Majority vote", rf.me)
			if rf.state == Candidate && args.Term == rf.currentTerm {
				//fmt.Println("Election won. Vote: ", votes, " peers :", len(rf.peers), " me", rf.me, "term ", rf.currentTerm)
				go rf.promoteToLeader()
				//rf.mu.Unlock()
				break
			} else {
				//fmt.Println("Election for term ended", args.Term)
				//rf.mu.Unlock()
				break
			}
		}
		rf.mu.Unlock()
	}

	rf.persist()
	if i != len(rf.peers)-1 {
		rf.mu.Unlock()
	}

}

func (rf *Raft) changeStateToCandidate() {
	//rf.SetStateType(Candidate)
	//fmt.Println("transition to canditate ", rf.me)
	rf.state = Candidate
	// Increment currentTerm and vote for self
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) changeStateToFollower(newTerm int) {
	//rf.SetStateType(Follower)

	//fmt.Println("transition to Follower", rf.me)

	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
}

func (rf *Raft) promoteToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Leader
	rf.leaderId = rf.me

	for i := range rf.peers {
		if i != rf.me {
			go rf.startLeaderPeerInteractionProcess(i)
		}
	}

}

func (rf *Raft) startLeaderPeerInteractionProcess(peerIndex int) {
	timer := time.NewTicker(LeaderPeerTickInterval)

	// Initial heartbeat
	//fmt.Println("Leader peer process started", rf.state, " Me", rf.me)
	rf.sendAppendEntries(peerIndex)
	lastHeartBeatSent := time.Now()

	for {

		rf.mu.Lock()
		if rf.state != Leader {
			timer.Stop()
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		currentTime := <-timer.C //  send a heartbeat
		if currentTime.Sub(lastHeartBeatSent) >= HeartBeatInterval {

			lastHeartBeatSent = time.Now()
			rf.sendAppendEntries(peerIndex)
		}

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
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(peerIndex int) {

	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	reply := AppendEntriesReply{}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 0,
	}
	rf.mu.Unlock()
	ok := rf.sendAppendEntryRequest(peerIndex, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		//fmt.Println("Me ", rf.me, " Communication error: AppendEntries() RPC failed", peerIndex)
	} else if rf.state != Leader || args.Term != rf.currentTerm {
		//fmt.Println("Node state has changed it's state since request was sent. Discarding response", rf.me)
	} else if reply.Success {
		//if len(args.entries) > 0 {
		//append entries later
		//} else {
		//fmt.Println("Successful heartbeat from ", peerIndex)

		//}
	}

	if !reply.Success && reply.Term > rf.currentTerm {
		//fmt.Println("Switching to follower ", reply.Term)

		rf.changeStateToFollower(reply.Term)
	}

	rf.persist()

}

func (rf *Raft) sendAppendEntryRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := false
	for i := 0; i < 3; i++ {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			break
		}
	}

	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.changeStateToFollower(args.Term)
		rf.leaderId = args.LeaderId
		reply.Success = true

	} else if args.Term == rf.currentTerm {
		rf.votedFor = -1
		reply.Success = true
	}
	if rf.leaderId == args.LeaderId {
		rf.lastHeartBeat = time.Now()
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	rf.persist()
}
