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
	"bytes"
	"math"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"
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
	applyCh chan ApplyMsg
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
	Entries[] Entry
	// leader’s commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	// currentTerm, for leader to update itself
	Term int
	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
	// the conflicting entry term, -1 for none
	XTerm int
	// index of the first entry for Xterm, -1 for none
	XIndex int
	// len of follower's log
	XLen int

}
// this function should be called under rf's mu locked
func (rf *Raft) TryApplyLog() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		// println("x apply y",rf.me,rf.lastApplied)
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied-1].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
}
func (rf *Raft) SendAppendEntriesToAll() {
	for {
		rf.mu.Lock()
		isLeader := rf.state == 2
		rf.mu.Unlock()
		if rf.killed() || !isLeader {
			break
		}
		time.Sleep(time.Duration(heartBeatInterval) * time.Millisecond)
		// send vote requests to all other followers
		rf.mu.Lock()
		if rf.state != 2 {
			rf.mu.Unlock()
			break
		}
		all := len(rf.peers)
		term := rf.currentTerm
		rf.mu.Unlock()
		turnedToFollower := false
		maxIndex := 0
		var mu sync.Mutex
		cond := sync.NewCond(&mu)
		finished := 1
		timeUp := false
		timer := func() {
			time.Sleep(time.Duration(heartBeatInterval/2) * time.Millisecond)
			mu.Lock()
			defer mu.Unlock()
			timeUp = true
			cond.Broadcast()
		}
		sender := func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply,lastIndex int) {
			ok := rf.sendAppendEntries(server, args, reply)
			mu.Lock()
			defer mu.Unlock()
			if turnedToFollower || timeUp {
				return
			}
			finished++
			if ok {
				if reply.Term > term {
					rf.mu.Lock()
					rf.TurnToFollower(reply.Term)
					rf.mu.Unlock()
					turnedToFollower = true
					cond.Broadcast()
					return
				}
				if reply.Success {
					rf.mu.Lock()
					if len(args.Entries) != 0 {
						rf.nextIndex[server] = lastIndex + 1
						rf.matchIndex[server] = rf.nextIndex[server] - 1
						if rf.matchIndex[server] > maxIndex {
							maxIndex = rf.matchIndex[server]
						}
					}
					rf.mu.Unlock()
				} else {
					rf.mu.Lock()
					if reply.XTerm != -1 {
						has := false
						i := len(rf.log)-1
						for ; i >= 0; i-- {
							if rf.log[i].Term == reply.XTerm {
								has = true
								break
							}
						}
						if has {
							rf.nextIndex[server] = i+1
						} else {
							rf.nextIndex[server] = reply.XIndex
						}
					} else {
						rf.nextIndex[server] = reply.XLen
					}
					if rf.nextIndex[server] == 0 {
						rf.nextIndex[server] = 1
					}
					rf.mu.Unlock()
				}
			}
			cond.Broadcast()
		}
		go timer()
		for idx := 0; idx < all; idx++ {
			var entries []Entry
			rf.mu.Lock()
			_,lastIndex := rf.GetLastLogTermAndIndex()
			for i := rf.nextIndex[idx]; i >= 1 && i <= lastIndex; i++ {
				entries = append(entries, rf.log[i-1])
			}
			var prevLogTerm int
			prevLogIndex := rf.nextIndex[idx] - 1
			if prevLogIndex == 0 {
				prevLogTerm = 0
			} else {
				prevLogTerm = rf.log[prevLogIndex - 1].Term
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			if idx != rf.me {
				go sender(idx,&args,&reply,lastIndex)
			}
		}
		mu.Lock()
		for !turnedToFollower && finished < all && !timeUp {
			cond.Wait()
		}

		if turnedToFollower {
			mu.Unlock()
			break
		}

		rf.mu.Lock()
		majority := int(math.Ceil(float64(all) / 2.0))
		_,lastIndex := rf.GetLastLogTermAndIndex()
		for i := lastIndex; i > rf.commitIndex; i-- {
			cnt := 1
			for j := 0; j < all; j++ {
				if j != rf.me && rf.matchIndex[j] >= i {
					cnt++
				}
			}
			if cnt >= majority && rf.log[i-1].Term == rf.currentTerm {
				// // println("cnt",cnt)
				// println("master term",rf.currentTerm)
				//for i:= 0; i < len(rf.log); i++ {
				//	var cmd int
				//	if rf.log[i].Command != nil {
				//		cmd = rf.log[i].Command.(int)
				//	} else {
				//		cmd = -1
				//	}
				//	// println("master ",cmd,rf.log[i].Term, i)
				//}
				rf.commitIndex = i
				// println("leader commitId turn to",rf.commitIndex)
				break
			}
		}
		rf.TryApplyLog()
		rf.mu.Unlock()
		mu.Unlock()
	}
}
// this function should be called under locked context
func (rf *Raft) TurnToFollower (Term int){
	state := rf.state
	rf.state = 0
	rf.currentTerm = Term
	rf.votedFor = -1
	rf.persist()
	rf.matchIndex = nil
	rf.nextIndex = nil
	_,lastIndex := rf.GetLastLogTermAndIndex()
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex = append(rf.matchIndex,0)
		rf.nextIndex = append(rf.nextIndex,lastIndex+1)
	}
	// println("turn to follower next index",lastIndex + 1)
	if state == 2 {
		// println("leader turn to follower", rf.me)
		go rf.electionTimeOut()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.heard = true
	reply.XLen = len(rf.log)
	reply.XTerm = -1
	reply.XIndex = -1
	// println("x heard from y",rf.me,args.LeaderId,rf.currentTerm,args.Term)
	// println("entry len", len(args.Entries))
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.state == 1){
		rf.TurnToFollower(args.Term)
	}
	_,lastIndex := rf.GetLastLogTermAndIndex()
	if args.Term < rf.currentTerm || lastIndex < args.PrevLogIndex{
		// println("reply false")
		reply.Success = false
		return
	} else if args.PrevLogIndex != 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		idx := args.PrevLogIndex-1
		reply.XTerm = rf.log[idx].Term
		for i:= 0; i < len(rf.log); i++ {
			if rf.log[i].Term == reply.XTerm {
				reply.XIndex = i+1
				break
			}
		}
		// println("reply Xterm XIndex PrevLogIndex PrevLogTerm",reply.XTerm,reply.XIndex,args.PrevLogIndex,args.PrevLogTerm)
		reply.Success = false
		return
	}
	i := 0

	//for i:= 0; i < len(rf.log); i++ {
	//	cmd := rf.log[i].Command.(int)
	//	// println("before ",cmd,rf.log[i].Term)
	//}
	//
	//for i:= 0; i < len(args.Entries); i++ {
	//	cmd := args.Entries[i].Command.(int)
	//	// println("entry ",cmd,args.Entries[i].Term)
	//}
	//
	//// println(args.PrevLogIndex, lastIndex)
	//oldLen := len(rf.log)
	for ; i < len(args.Entries) && args.PrevLogIndex+i < lastIndex; i++ {
		idx := args.PrevLogIndex + i
		if rf.log[idx].Term != args.Entries[i].Term {
			// println("don't agree idx", idx+1)
			// delete all the entries >= i
			rf.log = rf.log[:idx]
			break
		}
	}
	// println(i, lastIndex, args.PrevLogIndex)
	for ; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
	}
	rf.persist()
	//if len(args.Entries) != 0 {
	//	// println("x len turn from to ", rf.me, oldLen, len(rf.log))
	//	for i:= 0; i < len(rf.log); i++ {
	//		var cmd int
	//		if rf.log[i].Command != nil {
	//			cmd = rf.log[i].Command.(int)
	//		} else {
	//			cmd = -1
	//		}
	//		// println("fuck ",cmd,rf.log[i].Term, i)
	//	}
	//}
	reply.Success = true
	// update commit
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log)
		}
		rf.TryApplyLog()
		// println("commitIndex for x turn to y", rf.me, rf.commitIndex)
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
// this function should be called under locked context
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil || d.Decode(&log) != nil{
		// println("decoding failed!!!")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = voteFor
	  rf.log = log
	}
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
		return rf.log[n-1].Term, n
	}
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// println("x heard vote from y ",rf.me,args.CandidateId)
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.TurnToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	lastLogTerm,lastLogIndex := rf.GetLastLogTermAndIndex()
	// println(args.CandidateId,args.LastLogTerm,args.LastLogIndex,args.Term)
	// println(rf.me,lastLogTerm,lastLogIndex,rf.currentTerm)
	if args.Term < rf.currentTerm {
		// println("term candidate term self",args.CandidateId,args.Term,rf.me,rf.currentTerm)
		reply.VoteGranted = false
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		rf.heard = true
		reply.VoteGranted = true
	} else {
		// println("x already voted y",rf.me,rf.votedFor)
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
	isLeader := false
	// Your code here (2B).

	rf.mu.Lock()
	index = len(rf.log) + 1
	term = rf.currentTerm
	isLeader = rf.state == 2
	if isLeader {
		entry := Entry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log,entry)
		rf.persist()
		// println("master len",len(rf.log))
	}
	rf.mu.Unlock()
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
		timeout := getRandomTimeout()
		time.Sleep(time.Duration(timeout) * time.Millisecond)
		if rf.killed() {
			// println("killed", rf.me)
			break
		}
		rf.mu.Lock()
		// println("heard ",rf.me,rf.heard)
		if rf.heard == true {
			rf.heard = false
			rf.mu.Unlock()
			continue
		}
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
		// println("election starting ...", rf.me,rf.currentTerm)
		lastTerm,lastIndex := rf.GetLastLogTermAndIndex()
		// start an election
		rf.state = 1
		voteGrantedNum := 1
		finished := 1
		all := len(rf.peers)
		majority := int(math.Ceil(float64(all) / 2.0))
		var mu sync.Mutex
		timeUp := false
		cond := sync.NewCond(&mu)
		turnedToFollower := false
		requestVoteArgs := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
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
			ok := rf.sendRequestVote(server,args,reply)
			mu.Lock()
			if turnedToFollower || timeUp {
				mu.Unlock()
				return
			}
			if ok {
				if reply.Term > args.Term {
					turnedToFollower = true
					// println("vote rejected && turn to follower",server,args.CandidateId)
					rf.mu.Lock()
					rf.TurnToFollower(reply.Term)
					rf.mu.Unlock()
					cond.Broadcast()
					mu.Unlock()
					return
				}
				if reply.VoteGranted {
					// println("vote from to", server, args.CandidateId)
					voteGrantedNum++
				} else {
					// println("vote rejected from to",server,args.CandidateId)
				}
			} else {
				// println("fail to call x from y",server,args.CandidateId)
			}
			finished++
			cond.Broadcast()
			mu.Unlock()
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
		for !turnedToFollower && voteGrantedNum < majority && finished < all && !timeUp{
			// println("waiting...",rf.me)
			cond.Wait()
		}
		// println("fuck here ",rf.me,turnedToFollower,timeUp)
		if turnedToFollower {
			mu.Unlock()
			// println("turn to follower from election and continue", rf.me)
			continue
		}
		rf.mu.Lock()
		if voteGrantedNum >= majority && rf.state == 1{
			// successfully elected as a leader
			// start sending heart beats to other servers
			rf.state = 2
			rf.matchIndex = nil
			rf.nextIndex = nil
			// insert blank no op entry
			//if len(rf.log) != 0 {
			//	emptyEntry := Entry{
			//		Term:    rf.currentTerm,
			//		Command: nil,
			//	}
			//	rf.log = append(rf.log, emptyEntry)
			//}
			_,lastIndex := rf.GetLastLogTermAndIndex()
			for i := 0; i < len(rf.peers); i++ {
				rf.matchIndex = append(rf.matchIndex,0)
				rf.nextIndex = append(rf.nextIndex,lastIndex+1)
			}

			mu.Unlock()
			rf.mu.Unlock()
			// println("become leader successfully, term",rf.me,rf.currentTerm)
			go rf.SendAppendEntriesToAll()

			// break the loop to stop the election timeout goroutine
			break
		}
		// println("election failed ",rf.me)
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
		applyCh: applyCh,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	_,lastIndex := rf.GetLastLogTermAndIndex()
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex = append(rf.matchIndex,0)
		rf.nextIndex = append(rf.nextIndex,lastIndex+1)
	}
	go rf.electionTimeOut()
	return rf
}
