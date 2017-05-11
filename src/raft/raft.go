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
	"sync"
	"labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "encoding/gob"


// States of leadership: leader, candidate, and follower
const (
	LEADER    = iota
	CANDIDATE
	FOLLOWER

	HEART_BEAT_INTERVAL = 50 * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// persistent state on all servers
	currentTerm int               // latest term server has seen
	votedFor    int               // candidateId that received vote in current term
	log         []LogEntry

	// volatile state on all servers
	commitIndex int               // index of highest log entry known to be committed
	lastApplied int               // index of highest log entry applied to state machine

	// volatile state on leaders
	state        int
	nextIndex    []int            // for each server, index of the next log entry to send to that server
	matchIndex   []int            // for each server, index of highest log entry known to be replicated on server
	majoritySize int
	voteCount    int

	// channels
	requestVoteCh chan RequestVoteArgs
	applyCh       chan ApplyMsg
	heartbeatCh   chan bool
	leaderCh      chan bool
}

func (rf *Raft) getLastLogTermAndIndex() (int, int) {
	if len(rf.log) == 0 {
		return 0, 0
	}
	lastLog := rf.log[len(rf.log) - 1]
	return lastLog.Term, lastLog.Index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidates received vote
}

//
// example RequestVote RPC handler.
// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("Server %d received a vote request from %d\n", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		DPrintf("Server %d rejected server %d's request\n", rf.me, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	}

	// compare log
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	logUpToDate := lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)

	if ( rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logUpToDate { // log is up-to-date
		DPrintf("Server %d voted for server %d\n", rf.me, args.CandidateId)
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.heartbeatCh <- true
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
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) broadcastRequestVote() {
	args := RequestVoteArgs{}
	rf.mu.Lock()
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.LastLogTerm, args.LastLogIndex = rf.getLastLogTermAndIndex()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {
			go func(peer int, curtRf *Raft) {
				for curtRf.state == CANDIDATE {
					reply := RequestVoteReply{}
					DPrintf("Server %d sends vote request to %d\n", curtRf.me, peer)
					ok := curtRf.sendRequestVote(peer, &args, &reply)
					if ok {
						if reply.VoteGranted {
							curtRf.mu.Lock()
							curtRf.voteCount++
							if curtRf.voteCount >= curtRf.majoritySize {
								DPrintf("Server %d becomes the leader --------- \n", curtRf.me)
								curtRf.leaderCh <- true
							}
							curtRf.mu.Unlock()
						}
						break
					}
				}
			}(i, rf)
		}
	}
}

// log structure
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// appendEntry, used for log and heartbeats
type AppendEntryArg struct {
	Term              int         // leader’s term
	LeaderId          int         // so follower can redirect clients
	PreLogIndex       int         // index of log entry immediately preceding new ones
	PreLogTerm        int         // term of PrevLogIndex entry
	Entries           []LogEntry  // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommitIndex int         // leader’s commitIndex
}

type AppendEntryReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntryArg, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Entries == nil {// heartbeat
		if args.Term >= rf.currentTerm {
			reply.Success = true
			rf.state = FOLLOWER
			rf.heartbeatCh <- true
		} else {
			DPrintf("Server %d rejected heartbeat from server %d\n", rf.me, args.LeaderId)
			reply.Success = false
		}
	} else {

	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArg, reply *AppendEntryReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) broadcastHeartbeats() {
	args := AppendEntryArg{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me

	for i := range rf.peers {
		if i != rf.me {
			DPrintf("Server %d sends a heartbeat to server %d\n", rf.me, i)
			go func(crf *Raft, peer int) {
				reply := AppendEntryReply{}
				ok := crf.sendAppendEntries(peer, &args, &reply)
				if !ok {
					DPrintf("Server %d failed to send hb to server %d\n", crf.me, peer)
				}
			}(rf, i)
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.majoritySize = len(peers) / 2 + 1
	rf.applyCh = applyCh
	rf.heartbeatCh = make(chan bool, 100)
	rf.leaderCh = make(chan bool, 100)

	rf.log = make([]LogEntry, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func(curtRf *Raft) {
		for {
			switch curtRf.state {
			case FOLLOWER:
				select {
				case <- curtRf.heartbeatCh:
					curtRf.state = FOLLOWER
					DPrintf("Server %d receives a heartbeat\n", curtRf.me)
				case <- time.After(time.Duration(rand.Float32() * 150 + 150) * time.Millisecond):
					curtRf.state = CANDIDATE
				}
			case CANDIDATE:
				curtRf.mu.Lock()
				curtRf.currentTerm++
				curtRf.votedFor = curtRf.me
				curtRf.voteCount = 1
				curtRf.mu.Unlock()
				go curtRf.broadcastRequestVote()
				select {
				case <- curtRf.leaderCh:
					curtRf.state = LEADER
				case <- curtRf.heartbeatCh:
					curtRf.state = FOLLOWER
				case <- time.After(time.Duration(rand.Float32() * 150 + 150) * time.Millisecond):
				}
			case LEADER:
				curtRf.broadcastHeartbeats()
				time.Sleep(HEART_BEAT_INTERVAL)
			}
		}
	}(rf)

	return rf
}
