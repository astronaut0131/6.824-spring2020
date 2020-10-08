package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const electionTimeoutMin int = 400
const electionTimeoutMax int = 600
const heartBeatInterval int = 100

func getRandomTimeout() int{
	return electionTimeoutMin + rand.Intn(electionTimeoutMax - electionTimeoutMin)
}

type Entry struct {
	Term int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/*----------------------------------*/
	/* Persistent state on all servers: */
	/*----------------------------------*/
	// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm int
	// candidateId that received vote in current term (or null if none)
	votedFor int
	// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	log []Entry

	/*----------------------------------*/

	/*--------------------------------*/
	/* Volatile state on all servers: */
	/*--------------------------------*/
	// index of highest log entry known to be committed (initialized to 0, increases
	// monotonically)
	commitIndex int
	// to record whether heartbeats packets are heard in the period or not
	heard bool
	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastApplied int
	// 0 for follower, 1 for candidate, 2 for leader
	state	int
	/*--------------------------------*/

	/*--------------------------------*/
	/* Volatile state on leaders:	  */
	/*--------------------------------*/
	// for each server, index of the next log entry to send to that server (initialized to leader
	// last log index + 1)
	nextIndex []int

	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIndex []int
	/*--------------------------------*/
}

type AppendEntriesArgs struct {
	// leader’s term
	Term int
	// so follower can redirect clients
	LeaderId int
	// index of log entry immediately preceding new ones
	PrevLogIndex int
	// term of prevLogIndex entry
	PrevLogTerm int
	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	Entries[] int
	// leader’s commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	// currentTerm, for leader to update itself
	Term int
	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
}

func (rf *Raft) SendHeartBeats() {
	for {
		if rf.killed() {
			break
		}
		time.Sleep(time.Duration(heartBeatInterval) * time.Millisecond)
		// send vote requests to all other followers
		appendEntriesArgs := AppendEntriesArgs{}
		rf.mu.Lock()
		appendEntriesArgs.LeaderId = rf.me
		appendEntriesArgs.Term = rf.currentTerm
		all := len(rf.peers)
		term := rf.currentTerm
		rf.mu.Unlock()
		maxTerm := 0
		var mu sync.Mutex
		cond := sync.NewCond(&mu)
		finished := 1
		timeUp := false
		timer := func() {
			time.Sleep(time.Duration(heartBeatInterval) * time.Millisecond)
			mu.Lock()
			defer mu.Unlock()
			timeUp = true
			cond.Broadcast()
		}
		sender := func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.sendAppendEntries(server, args, reply)
			mu.Lock()
			defer mu.Unlock()
			finished++
			if ok && reply.Term > maxTerm {
				maxTerm = reply.Term
			}
			cond.Broadcast()
		}
		go timer()
		for idx := 0; idx < all; idx++ {
			appendEntriesReply := AppendEntriesReply{}
			if idx != rf.me {
				go sender(idx,&appendEntriesArgs,&appendEntriesReply)
			}
		}
		mu.Lock()
		for maxTerm <= term && finished < all && !timeUp {
			cond.Wait()
		}
		if maxTerm > term {
			println("leader turns back to follower ",rf.me)
			mu.Unlock()
			// turn to follower
			rf.mu.Lock()
			rf.currentTerm = maxTerm
			rf.state = 0
			rf.votedFor = -1
			rf.mu.Unlock()
			go rf.electionTimeOut()
			break
		}
		mu.Unlock()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	if len(args.Entries) == 0{
		// heart beat packet
		println("heart beat from to",args.LeaderId,rf.me)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.heard = true
		reply.Term = rf.currentTerm
		if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.state == 1){
			if rf.state == 2 {
				println("spawn election timeout")
				go rf.electionTimeOut()
			}
			// turn to follower, update term
			rf.state = 0
			rf.currentTerm = args.Term
			rf.votedFor = -1
			return
		}

	} else {
		// todo
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == 2
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
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
	// Your code here (2C).
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
	// Your data here (2A, 2B).
	// candidate’s term
	Term int
	// candidate requesting vote
	CandidateId int
	// index of candidate’s last log entry
	LastLogIndex int
	// term of candidate’s last log entry
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// currentTerm, for candidate to update itself
	Term int
	// true means candidate received vote
	VoteGranted bool
}

// this function must be called under locked context
func (rf *Raft) GetLastLogTermAndIndex() (int,int) {
	n := len(rf.log)
	if n == 0{
		return 0,0
	} else {
		return rf.log[n-1].Term, n-1
	}
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.heard = true
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	lastLogTerm,lastLogIndex := rf.GetLastLogTermAndIndex()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		args.LastLogTerm >= lastLogTerm && args.LastLogIndex >= lastLogIndex {
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
		}
		rf.currentTerm = args.Term
		reply.VoteGranted = true
	} else {
		println("x already voted y",rf.me,rf.votedFor)
		reply.VoteGranted = false
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionTimeOut(){
	for
	{
		if rf.killed() {
			break
		}
		timeout := getRandomTimeout()
		time.Sleep(time.Duration(timeout) * time.Millisecond)
		rf.mu.Lock()
		println("heard ",rf.me,rf.heard)
		if rf.heard == true {
			rf.heard = false
			rf.mu.Unlock()
			continue
		}
		rf.currentTerm++
		println("election starting ...", rf.me,rf.currentTerm)
		var lastLogTerm int
		if len(rf.log) == 0 {
			lastLogTerm = 0
		} else {
			lastLogTerm = rf.log[len(rf.log) - 1].Term
		}
		// start an election
		rf.state = 1
		voteGrantedNum := 1
		finished := 1
		all := len(rf.peers)
		rf.votedFor = rf.me
		majority := int(math.Ceil(float64(all) / 2.0))
		var mu sync.Mutex
		timeUp := false
		cond := sync.NewCond(&mu)
		requestVoteArgs := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log),
			LastLogTerm:  lastLogTerm,
		}
		rf.mu.Unlock()
		timer := func() {
			time.Sleep(time.Duration(timeout) * time.Millisecond)
			mu.Lock()
			defer mu.Unlock()
			timeUp = true
			cond.Broadcast()
		}
		sender := func(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
			rf.sendRequestVote(server,args,reply)
			mu.Lock()
			if reply.VoteGranted {
				println("vote from to",server,rf.me)
				voteGrantedNum++
			}
			finished++
			mu.Unlock()
			cond.Broadcast()
		}
		// send vote requests to all other peers
		go timer()
		for idx := 0; idx < all; idx++ {
			if idx != rf.me {
				requestVoteReply :=RequestVoteReply{}
				go sender(idx,&requestVoteArgs,&requestVoteReply)
			}
		}
		mu.Lock()
		for voteGrantedNum < majority && finished < all && !timeUp{
			cond.Wait()
		}
		rf.mu.Lock()
		if voteGrantedNum >= majority || rf.state == 0{
			// successfully elected as a leader
			// start sending heart beats to other servers
			rf.state = 2
			mu.Unlock()
			rf.mu.Unlock()
			println("become leader successfully ",rf.me)
			go rf.SendHeartBeats()

			// break the loop to stop the election timeout goroutine
			break
		}
		println("election failed ",rf.me)
		rf.votedFor = -1
		mu.Unlock()
		rf.mu.Unlock()
	}
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
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		mu:          sync.Mutex{},
		peers:       peers,
		persister:   persister,
		me:          me,
		dead:        0,
		currentTerm: 0,
		votedFor:    -1,
		log:         nil,
		commitIndex: 0,
		heard:       false,
		lastApplied: 0,
		state:       0,
		nextIndex:   nil,
		matchIndex:  nil,
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.electionTimeOut()
	return rf
}
