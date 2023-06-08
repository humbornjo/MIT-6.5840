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
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	currTerm    int32      // current term number
	votedFor    int        // server id to be chosen as the leader
	logEntries  []logEntry // log entries
	commitIndex int32
	state       int
	isTimeout   bool
}

// 2A
type logEntry struct {
	Command interface{}
	Term    int32
	Index   int32
}

// 2A
const (
	Follower  int = 0
	Candidate int = 1
	Leader    int = 2
)

// 2A
type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int
	PrevLogIndex int32
	PrevLogTerm  int32
	Entries      []logEntry
	LeaderCommit int32
}

// 2A
type AppendEntriesReply struct {
	Term    int32
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.LogLock()
	defer rf.LogUnlock()

	reply.Term = rf.currTerm

	if rf.currTerm > args.Term {
		reply.Success = false
		return
	}

	if rf.logEntries != nil && rf.logEntries[args.PrevLogIndex].Term != args.Term {
		reply.Success = false
		return
	}

	for idx, entry := range rf.logEntries {
		if entry.Index == args.PrevLogIndex {
			rf.logEntries = rf.logEntries[:idx]
			break
		}
	}

	if args.Entries != nil {
		rf.logEntries = append(rf.logEntries, args.Entries...)
	}
	//  else {
	// 	DPrintf("S%d accept heartbeat...\n", rf.me)
	// }

	if args.LeaderCommit > rf.commitIndex {
		lastIdx := len(args.Entries)
		min := func(a, b int32) int32 {
			if a > b {
				return b
			}
			return a
		}
		if args.Entries != nil {
			rf.commitIndex = min(args.LeaderCommit, args.Entries[lastIdx-1].Index)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	reply.Success = true
	switch rf.state {
	case Leader:
	case Candidate:
		rf.state = Follower
		DebugLog(dVote, "S%d convert to follower...\n", rf.me)
		// rf.votedFor = -1
	case Follower:
		rf.currTerm = args.Term
		rf.isTimeout = false
	default:
		break
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DebugLog(dLog, "S%d -> S%d T: %d, send {PLI: %d, PLT: %d}", rf.me, server, rf.currTerm, args.PrevLogIndex, args.PrevLogTerm)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.LogLock()
	term = int(rf.currTerm)
	isleader = rf.state == Leader
	rf.LogUnlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32
	CandidateId  int
	LastLogIndex int32
	LastLogTerm  int32
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// 2A
	// Reply false if term < currentTerm
	rf.LogLock()
	defer rf.LogUnlock()
	reply.Term = rf.currTerm
	if rf.currTerm > args.Term {
		reply.VoteGranted = false
		DebugLog(dVote, "S%d -> S%d vote false, T: %d, CT: %d\n", rf.me, args.CandidateId, rf.currTerm, args.Term)
		return
	} else if rf.currTerm == args.Term {
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			DebugLog(dVote, "S%d -> S%d vote true, same term\n", rf.me, args.CandidateId)
			return
		} else {
			reply.VoteGranted = false
			DebugLog(dVote, "S%d -> S%d vote false, VT: %d\n", rf.me, args.CandidateId, rf.votedFor)
			return
		}
	} else {
		rf.currTerm = args.Term
		rf.isTimeout = false
		rf.state = Follower
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		DebugLog(dVote, "S%d -> S%d vote true, from T: %d to CT: %d\n", rf.me, args.CandidateId, reply.Term, rf.currTerm)
		return
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DebugLog(dVote, "S%d C%d ask for vote, T: %d", server, rf.commitIndex, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) heartBeat() {
	DebugLog(dLeader, "S%d convert to leader at T: %d\n", rf.me, rf.currTerm)

	for {
		rf.LogLock()
		if rf.state != Leader {
			rf.LogUnlock()
			return
		}
		if rf.killed() {
			rf.state = Follower
			rf.LogUnlock()
			return
		}
		args := AppendEntriesArgs{
			Term:         rf.currTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: 0,
		}
		rf.LogUnlock()

		rf.isTimeout = false
		for peerId := 0; peerId < len(rf.peers); peerId++ {
			if peerId == rf.me {
				continue
			}
			go func(id int) {
				reply := AppendEntriesReply{}
				for ok := rf.sendAppendEntries(id, &args, &reply); !ok; {
				}
				if reply.Term > rf.currTerm {
					rf.state = Follower
				}
			}(peerId)
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (rf *Raft) StartElection() {
	DebugLog(dTimer, "S%d convert to candidate, calling election T: %d\n", rf.me, rf.currTerm)

	rf.LogLock()
	rf.currTerm += 1    //先将 Term 自增1
	rf.votedFor = rf.me // 给自己投票
	//本来应该reset ElectionTimeout的，由于并发执行，在外侧实现

	// lastIndex := len(rf.logEntries) - 1
	lastLogTerm, lastLogIndex := rf.LastLogInfo()
	args := RequestVoteArgs{
		rf.currTerm,  //Term
		rf.me,        // CandidateId
		lastLogIndex, // LastLogIndex
		lastLogTerm,  // LastLogTerm
	}
	rf.LogUnlock()

	var nVoter int32 = 1 // 本身就是一个投票者
	var once sync.Once   //保证在成为Leader后心跳广播的协程只存在一个

	// 给所有节点发送RequestVote RPCs
	for peerId := 0; peerId < len(rf.peers); peerId++ {
		if peerId == rf.me {
			continue
		}

		go func(id int) {

			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(id, &args, &reply)

			rf.LogLock()
			defer rf.LogUnlock()

			if rf.state == Follower { // 已经收到了其他Leader的心跳，直接返回
				return
			} else if ok && reply.VoteGranted { // 被选举了
				atomic.AddInt32(&nVoter, 1)
				if int(nVoter) > len(rf.peers)/2 {
					rf.state = Leader

					go once.Do(rf.heartBeat)
				}
			}
		}(peerId)
	}
	return
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.state != Leader {
			DebugLog(dTimer, "S%d not leader, election timeout...\n", rf.me)

			rf.isTimeout = true // 不加锁，后面会睡眠，所以不会有问题
			Electiontimeout()   // Electiontimeout(), 睡眠200-400ms
			rf.LogLock()        // 加锁，因为后面会修改rf的状态
			//会超时，自己的状态只有可能是Follower或Candidate，统一转化为Candidate
			if rf.isTimeout { // && rf.state == Follower

				rf.state = Candidate
				rf.votedFor = -1
				go rf.StartElection() //并发执行选举, 满足 If election timeout elapses: start new election
			}
			rf.LogUnlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	// DPrintf("deploying %d server...\n", me)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	// CurrTerm init 0, Do nothing
	rf.votedFor = -1 // VotedFor init -1, for server index start from 0
	// LogEntries init empty slice
	// isTimeout init false, reset true in ticker(), Do nothing
	// state init Follower, Do nothing
	// commitIndex TODO
	DebugLog(dClient, "S%d started at T:%d LLI:%d\n", rf.me, rf.currTerm, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
