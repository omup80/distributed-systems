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
	"fmt"
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
const CommitApplyIdleCheckInterval = 25 * time.Millisecond

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
	rf.lastApplied = rf.lastApplied
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
	lastIndex, lastTerm := rf.getLastEntryInfo()

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

	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Println("Inside start ", rf.me)

	nextIndex := func() int {
		if len(rf.log) > 0 {
			return rf.log[len(rf.log)-1].Index + 1
		}
		return 1
	}()

	entry := Log{Index: nextIndex, Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)
	//fmt.Println("New entry appended to leader's log: ", entry)

	return nextIndex, term, isLeader
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
	fmt.Println("Inside Make")
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
	go rf.startLocalApplyProcess(applyCh)

	return rf
}
func (rf *Raft) findLogIndex(logIndex int) (int, bool) {
	for i, e := range rf.log {
		if e.Index == logIndex {
			return i, true
		}
	}
	return -1, false
}
func (rf *Raft) startLocalApplyProcess(applyChan chan ApplyMsg) {
	rf.mu.Lock()
	//fmt.Println("Starting commit process -  ", rf.me," Last log applied: " ,rf.lastApplied)

	rf.mu.Unlock()

	for {
		rf.mu.Lock()

		if rf.commitIndex >= 0 && rf.commitIndex > rf.lastApplied {

			startIndex, _ := rf.findLogIndex(rf.lastApplied + 1)
			startIndex = Max(startIndex, 0)

			endIndex := -1
			for i := startIndex; i < len(rf.log); i++ {
				if rf.log[i].Index <= rf.commitIndex {
					endIndex = i
				}
			}

			if endIndex >= 0 { // We have some entries to locally commit
				entries := make([]Log, endIndex-startIndex+1)
				copy(entries, rf.log[startIndex:endIndex+1])

				rf.mu.Unlock()

				for _, v := range entries { // Hold no locks so that slow local applies don't deadlock the system
					applyChan <- ApplyMsg{CommandIndex: v.Index, Command: v.Command, CommandValid: true}
				}

				rf.mu.Lock()
				rf.lastApplied += len(entries)
			}
			rf.mu.Unlock()

		} else {
			rf.mu.Unlock()
			<-time.After(CommitApplyIdleCheckInterval)
		}
	}
}

func (rf *Raft) startLeaderElectionProcess() {

	timeOutForElection := func() time.Duration {
		return (350 + time.Duration(rand.Intn(200))) * time.Millisecond
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

func (rf *Raft) getLastEntryInfo() (int, int) {
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
	lastIndex, lastTerm := rf.getLastEntryInfo()

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

		if reply.Term > rf.currentTerm {
			rf.changeStateToFollower(reply.Term)
			//rf.mu.Unlock()
			break
		} else if votes += reply.VoteCount(); votes > len(rf.peers)/2 { // Has majority vote
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

	if i != len(rf.peers)-1 {
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()

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
	//rf.SetStateType(Follower)

	//fmt.Println("transition to Follower", rf.me, " term ", rf.currentTerm)

	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
}

func (rf *Raft) promoteToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println("Inside promote to leader ", rf.me, " term ", rf.currentTerm)

	rf.state = Leader
	rf.leaderId = rf.me

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.sendAppendChan = make([]chan struct{}, len(rf.peers))

	for i := range rf.peers {
		if i != rf.me {
			rf.nextIndex[i] = len(rf.log) + 1 // Should be initialized to leader's last log index + 1
			rf.matchIndex[i] = 0              // Index of highest log entry known to be replicated on server
			rf.sendAppendChan[i] = make(chan struct{}, 1)

			// Start routines for each peer which will be used to monitor and send log entries
			go rf.startLeaderPeerInteractionProcess(i, rf.sendAppendChan[i])
		}
	}

}

func (rf *Raft) startLeaderPeerInteractionProcess(peerIndex int, sendAppendChan chan struct{}) {
	timer := time.NewTicker(LeaderPeerTickInterval)

	// Initial heartbeat
	//fmt.Println("Leader peer process started", rf.state, " Me", rf.me)
	rf.sendAppendEntries(peerIndex, sendAppendChan)
	lastHeartBeatSent := time.Now()

	for {

		rf.mu.Lock()
		if rf.state != Leader {
			timer.Stop()
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		select {
		case <-sendAppendChan: // Signal that we should send a new append to this peer
			lastHeartBeatSent = time.Now()
			rf.sendAppendEntries(peerIndex, sendAppendChan)
		case currentTime := <-timer.C: // If traffic has been idle, we should send a heartbeat
			if currentTime.Sub(lastHeartBeatSent) >= HeartBeatInterval {
				lastHeartBeatSent = time.Now()
				rf.sendAppendEntries(peerIndex, sendAppendChan)
			}
		}

		//currentTime := <-timer.C //  send a heartbeat
		//if currentTime.Sub(lastHeartBeatSent) >= HeartBeatInterval {

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
	Term                int
	Success             bool
	ConflictingLogTerm  int // Term of the conflicting entry, if any
	ConflictingLogIndex int // First index of the log for the above conflicting term
}

func (rf *Raft) sendAppendEntries(peerIndex int, sendAppendChan chan struct{}) {

	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	var entries []Log = []Log{}
	var prevLogIndex, prevLogTerm int = 0, 0
	lastLogIndex, _ := rf.getLastEntryInfo()

	if lastLogIndex > 0 && lastLogIndex >= rf.nextIndex[peerIndex] {

		for i, v := range rf.log { // Need to send logs beginning from index `rf.nextIndex[peerIndex]`
			if v.Index == rf.nextIndex[peerIndex] {
				if i > 0 {
					lastEntry := rf.log[i-1]
					prevLogIndex, prevLogTerm = lastEntry.Index, lastEntry.Term
				}
				entries = make([]Log, len(rf.log)-i)
				copy(entries, rf.log[i:])
				break
			}
		}
		//fmt.Println("Sending log entries of length", len(entries)," to ", peerId)

	} else { // We're just going to send a heartbeat
		if len(rf.log) > 0 {
			lastEntry := rf.log[len(rf.log)-1]
			prevLogIndex, prevLogTerm = lastEntry.Index, lastEntry.Term
		}

	}

	reply := AppendEntriesReply{}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	rf.mu.Unlock()

	ok := rf.sendAppendEntryRequest(peerIndex, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		//fmt.Println("Me ", rf.me, " Communication error: AppendEntries() RPC failed", peerIndex)
	} else if reply.Term > rf.currentTerm {
		//fmt.Println("Switching to follower as in append", peerIndex," " ,reply.Term)

		rf.changeStateToFollower(reply.Term)
	} else if rf.state != Leader || reply.Term != rf.currentTerm {
		//fmt.Println("Node state has changed it's state since request was sent. Discarding response", rf.me)
	} else if reply.Success {
		if len(entries) > 0 {
			lastReplicated := entries[len(entries)-1]
			rf.matchIndex[peerIndex] = lastReplicated.Index
			rf.nextIndex[peerIndex] = lastReplicated.Index + 1
			rf.updateCommitIndex()
		} else {
			//fmt.Prinl("Successful heartbeat from ", peerId)
		}
	} else {

		// Log deviation, we should go back to `ConflictingLogIndex - 1`, lowest value for nextIndex[peerIndex] is 1.
		rf.nextIndex[peerIndex] = Max(reply.ConflictingLogIndex-1, 1)
		sendAppendChan <- struct{}{} // Signals to leader-peer process that appends need to occur

	}

	rf.persist()

}
func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func (rf *Raft) updateCommitIndex() {
	// §5.3/5.4: If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	for i := len(rf.log) - 1; i >= 0; i-- {
		if v := rf.log[i]; v.Term == rf.currentTerm && v.Index > rf.commitIndex {
			replicationCount := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= v.Index {
					if replicationCount++; replicationCount > len(rf.peers)/2 { // Check to see if majority of nodes have replicated this
						//fmt.Println("Updating commit index ", rf, rf.commitIndex, v.Index, replicationCount, len(rf.peers))
						rf.commitIndex = v.Index // Set index of this entry as new commit index
						break
					}
				}
			}
		} else {
			break
		}
	}
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

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else {

		//else if args.Term >= rf.currentTerm  && rf.state==Leader{
		rf.changeStateToFollower(args.Term)
		rf.leaderId = args.LeaderId
		rf.lastHeartBeat = time.Now()

	}

	//if rf.leaderId == args.LeaderId {
	//	rf.lastHeartBeat = time.Now()
	//}

	prevLogIndex := -1
	for i, v := range rf.log {
		if v.Index == args.PrevLogIndex {
			if v.Term == args.PrevLogTerm {
				prevLogIndex = i
				break
			} else {
				reply.ConflictingLogTerm = v.Term
			}
		}
	}

	PrevIsBeginningOfLog := args.PrevLogIndex == 0 && args.PrevLogTerm == 0

	if prevLogIndex >= 0 || PrevIsBeginningOfLog {
		if len(args.Entries) > 0 {
			//fmt.Println("Appending entries of length", len(args.LogEntries), " from ",args.LeaderID)
		}

		// Remove any inconsistent logs and find the index of the last consistent entry from the leader

		entriesIndex := 0
		for i := prevLogIndex + 1; i < len(rf.log); i++ {

			entryConsistent := func() bool {
				localEntry, leadersEntry := rf.log[i], args.Entries[entriesIndex]
				return localEntry.Index == leadersEntry.Index && localEntry.Term == leadersEntry.Term
			}

			if entriesIndex >= len(args.Entries) || !entryConsistent() {
				// Additional entries must be inconsistent, so let's delete them from our local log
				rf.log = rf.log[:i]
				break
			} else {
				entriesIndex++
			}

		}

		// Append all entries that are not already in our log
		if entriesIndex < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[entriesIndex:]...)
		}

		// Update the commit index
		if args.LeaderCommit > rf.commitIndex {
			var latestLogIndex = 0
			if len(rf.log) > 0 {
				latestLogIndex = rf.log[len(rf.log)-1].Index
			}

			if args.LeaderCommit < latestLogIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = latestLogIndex
			}
		}
		reply.Success = true
	} else {
		// §5.3: When rejecting an AppendEntries request, the follower can include the term of the
		//	 	 conflicting entry and the first index it stores for that term.

		// If there's no entry with `args.PreviousLogIndex` in our log. Set conflicting term to that of last log entry
		if reply.ConflictingLogTerm == 0 && len(rf.log) > 0 {
			reply.ConflictingLogTerm = rf.log[len(rf.log)-1].Term
		}

		for _, v := range rf.log { // Find first log index for the conflicting term
			if v.Term == reply.ConflictingLogTerm {
				reply.ConflictingLogIndex = v.Index
				break
			}
		}

		reply.Success = false
	}
	rf.persist()
}
