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

	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	currTerm    int        // current term number
	votedFor    int        // server id to be chosen as the leader
	logEntries  []logEntry // log entries
	commitIndex int
	state       int
	isTimeout   bool

	// 2B
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg
	applyCond   sync.Cond

	// 2D
	snapshot Snapshot
}

// 2A
type logEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// 2A
const (
	Follower  int = 0
	Candidate int = 1
	Leader    int = 2
)

// 2A
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

// 2A
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// DebugLog(dError, "S%d recv append from S%d, T: %d, ENTRIES: %v\n", rf.me, args.LeaderId, args.Term, args.Entries)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currTerm

	// 如果leader的term比自己小，说明leader的信息不是最新的，拒绝
	// 如果leader中有着自己没有的信息，这个操作会将leader的Term更新到与自己相同
	// leader keep up 以后，只会存在自己向leader投票的情况，且会因为投票而重置timeout
	if rf.currTerm > args.Term {
		reply.Success = false
		DebugLog(dVote, "S%d append fail, T: %d large term\n", rf.me, rf.currTerm)
		return
	}

	// 从现在开始，leader的term大于等于自己的term
	if rf.currTerm < args.Term {
		rf.currTerm = args.Term //persist
		rf.state = Follower
	}

	if rf.snapshot.LastIncludedIndex > args.PrevLogIndex {
		reply.Success = true
		DebugLog(dVote, "S%d append success, snapshot interrupt...\n", rf.me)
		return
	}

	// 如果在prevLogIndex处不存在一个term相同的entry，返回false
	// DebugLog(dLog2, "S%d -> S%d recv append, prevLogIndex: %d, prevLogTerm: %d, SS: %v\n",
	// 	rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm,
	// 	rf.snapshot)
	if args.PrevLogIndex > rf.logEntries[len(rf.logEntries)-1].Index ||
		rf.logEntries[args.PrevLogIndex-rf.snapshot.LastIncludedIndex].Term != args.PrevLogTerm { // ATTENTION 这里可能会越界，要减去lastincludedindex
		reply.Success = false
		DebugLog(dVote, "S%d append fail, diff term\n", rf.me)
		return
	}

	// 偷懒写法，标准实现需要比较每个entry的term，删除第一个出现问题的entry之后的所有entry，再append
	if len(args.Entries) == 0 {
		DebugLog(dLog, "S%d -> S%d recv heartbeat\n", rf.me, args.LeaderId, rf.currTerm, len(rf.logEntries))
	} else {
		rfLastLogIndex, rfLastLogTerm := rf.LogInfoByIndex(len(rf.logEntries) - 1)
		argLastLogIndex, argLastLogTerm := args.Entries[len(args.Entries)-1].Index, args.Entries[len(args.Entries)-1].Term
		if rfLastLogTerm < argLastLogTerm || (rfLastLogTerm == argLastLogTerm && rfLastLogIndex < argLastLogIndex) {
			rf.logEntries = rf.logEntries[:args.PrevLogIndex+1-rf.snapshot.LastIncludedIndex] // ATTENTION
			rf.logEntries = append(rf.logEntries, args.Entries...)                            // persist
			rf.persist()
		}

		DebugLog(dLog2, "S%d -> S%d append ok T: %d, LENLOG: %d\n", rf.me, args.LeaderId, rf.currTerm, len(rf.logEntries))
	}

	reply.Success = true
	// switch rf.state {
	// case Leader:
	// case Candidate:
	rf.state = Follower
	// DebugLog(dVote, "S%d convert to follower...\n", rf.me)
	// rf.votedFor = -1
	// case Follower:
	rf.isTimeout = false
	// default:
	// 	break
	// }

	// 最后一层判断，如果leaderCommit大于自己的commitIndex，更新自己的commitIndex
	if args.LeaderCommit > rf.commitIndex {
		lastIdx := rf.logEntries[len(rf.logEntries)-1].Index
		rf.commitIndex = min(args.LeaderCommit, rf.logEntries[lastIdx-rf.snapshot.LastIncludedIndex].Index)
		rf.applyCond.Signal()
		DebugLog(dCommit, "S%d commit {CI: %d, LENLOG: %d}\n", rf.me, rf.commitIndex, len(rf.logEntries))
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	// USELESS - Lock
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// if ok {
	// 	if len(args.Entries) == 0 {
	// 		DebugLog(dLog, "S%d -> S%d T: %d, send heartbeat\n", rf.me, server, rf.currTerm)
	// 	} else {
	// 		DebugLog(dLog, "S%d -> S%d T: %d, send {PLI: %d, PLT: %d, CI: %d, BEGINLOGIDX: %d, ENDLOGIDX: %d, LOG: %v}\n",
	// 			rf.me, server, rf.currTerm, args.PrevLogIndex, args.PrevLogTerm,
	// 			args.LeaderCommit, args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index,
	// 			args.Entries)
	// 	}
	// } else {
	// 	DebugLog(dError, "S%d -> S%d T: %d, fail send {PLI: %d, PLT: %d, CI: %d}\n", rf.me, server, rf.currTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	// }
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()

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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.snapshot)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currTerm int
	var votedFor int
	var logEntries []logEntry
	var snapshot Snapshot

	if d.Decode(&currTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil || d.Decode(&snapshot) != nil {
		return
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currTerm = currTerm
		rf.votedFor = votedFor
		rf.logEntries = logEntries
		rf.snapshot = snapshot
	}
}

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
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
	rf.mu.Lock()

	if index <= rf.snapshot.LastIncludedIndex {
		DebugLog(dSnap, "S%d snapshot too small, FIRLOGIDX: %d, IDX: %d\n", rf.me, rf.logEntries[0].Index, index)
		rf.mu.Unlock()
		return
	}

	// 不允许生成超过自己当前日志长度的快照
	if index > rf.logEntries[len(rf.logEntries)-1].Index {
		DebugLog(dSnap, "S%d snapshot out of order, LENLOG: %d, IDX: %d\n", rf.me, len(rf.logEntries), index)
		rf.mu.Unlock()
		return
	}

	rf.logEntries = rf.logEntries[index-rf.snapshot.LastIncludedIndex:]
	rf.snapshot.LastIncludedIndex = index
	rf.snapshot.LastIncludedTerm = rf.logEntries[index-rf.snapshot.LastIncludedIndex].Term
	// USELESS ?
	rf.persist()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.snapshot)
	state := w.Bytes()

	rf.persister.SaveStateAndSnapshot(state, snapshot)
	rf.mu.Unlock()
	DebugLog(dSnap, "S%d create snapshot LII:%d, LENLOG: %d, T: %d\n",
		rf.me, index, len(rf.logEntries), rf.currTerm)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DebugLog(dSnap, "S%d start install snapshot...\n", rf.me)
	logs := []logEntry{}

	rf.mu.Lock()

	reply.Term = rf.currTerm
	// 如果我的Term比你的要大，不用搞了
	if rf.currTerm > args.Term {
		rf.mu.Unlock()
		return
	}

	if args.LastIncludedIndex <= rf.snapshot.LastIncludedIndex ||
		args.LastIncludedIndex <= rf.lastApplied {
		rf.mu.Unlock()
		return
	}

	// 从现在开始，args的Term >= Follower的Term
	if rf.currTerm == args.Term { // 如何处理votefor？
		// snapshot有延迟，与当前的log不存在交集
		if args.LastIncludedIndex < rf.logEntries[0].Index {
			DebugLog(dSnap, "S%d old snapshot, firstLogIndex: %d, SSLastLogIndex: %d, drop...\n",
				rf.me, rf.logEntries[0].Index, args.LastIncludedIndex)
			rf.mu.Unlock()
			return
		} else if args.LastIncludedIndex <= rf.logEntries[len(rf.logEntries)-1].Index { // 存在交集
			logs = append(logs, rf.logEntries[args.LastIncludedIndex-rf.logEntries[0].Index:]...)
			logs[0].Command = nil
			logs[0].Term = args.LastIncludedTerm
			DebugLog(dSnap, "S%d partial snapshot, firstLogIndex: %d, lastLogIndex: %d, SSLastLogIndex: %d, drop...\n",
				rf.me, rf.logEntries[0].Index, rf.logEntries[len(rf.logEntries)-1].Index, args.LastIncludedIndex)
		} else { // snapshot包含了server所有的log，全部discard
			logs = append(logs, logEntry{nil, args.LastIncludedTerm, args.LastIncludedIndex})
			DebugLog(dSnap, "S%d full snapshot, firstLogIndex: %d, lastLogIndex: %d, SSLastLogIndex: %d, drop...\n",
				rf.me, rf.logEntries[0].Index, rf.logEntries[len(rf.logEntries)-1].Index, args.LastIncludedIndex)
		}
	} else { //有更大的term。根据普适原则，需要调整term，convert to Follower
		logs = append(logs, logEntry{nil, args.LastIncludedTerm, args.LastIncludedIndex})

		rf.currTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.state = Follower
		DebugLog(dLeader, "S%d in installSnapshot transfer to T: %d, convert to Follower\n", rf.me, args.Term)
	}

	rf.logEntries = logs

	rf.persist()

	// Reset state machine using snapshot contents
	rf.snapshot = Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
	}

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)

	e.Encode(args.LastIncludedIndex)
	e.Encode(args.LastIncludedTerm)
	state := w.Bytes()

	rf.persister.SaveStateAndSnapshot(state, args.Data)

	applyMsg := ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		SnapshotValid: true,
		Snapshot:      args.Data,
		// to be fixed
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.mu.Unlock()
	rf.applyCh <- applyMsg
	DebugLog(dSnap, "S%d C%d install snapshot, IDX: %d T: %d, start from %d to %d\n", rf.me, rf.commitIndex,
		args.LastIncludedIndex, args.Term, rf.lastApplied, rf.logEntries[len(rf.logEntries)-1].Index)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DebugLog(dSnap, "S%d -> S%d send snapshot, T: %d", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// 2A
	// Reply false if term < currentTerm
	// 2B TODO
	// respond according to args.lastLogIndex and args.lastLogTerm
	// refer to last para in 5.4.1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currTerm
	if rf.currTerm > args.Term {
		reply.VoteGranted = false
		DebugLog(dVote, "S%d -> S%d vote false, T: %d, CT: %d\n", rf.me, args.CandidateId, rf.currTerm, args.Term)
		return
	}

	if rf.currTerm == args.Term {
		// 没投票，可以投，但是要判断
		if rf.votedFor == -1 {
			// 先判断
			rfLastLogIndex, rfLastLogTerm := rf.LogInfoByIndex(len(rf.logEntries) - 1)
			if rfLastLogTerm > args.LastLogTerm {
				reply.VoteGranted = false
				DebugLog(dVote, "S%d -> S%d vote false, legacy log\n", rf.me, args.CandidateId)
				return
			}
			if rfLastLogTerm == args.LastLogTerm && rfLastLogIndex > args.LastLogIndex {
				reply.VoteGranted = false
				DebugLog(dVote, "S%d -> S%d vote false, old log\n", rf.me, args.CandidateId)
				return
			}

			reply.VoteGranted = true
			rf.isTimeout = false
			rf.votedFor = args.CandidateId
			rf.persist()
			DebugLog(dVote, "S%d -> S%d vote true, same term\n", rf.me, args.CandidateId)
			return
		}

		// 投了票，看看是不是自己人，不用管Index和term了
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.isTimeout = false
			DebugLog(dVote, "S%d -> S%d vote true, same term\n", rf.me, args.CandidateId)
			return
		} else {
			reply.VoteGranted = false
			DebugLog(dVote, "S%d -> S%d vote false, VT: %d\n", rf.me, args.CandidateId, rf.votedFor)
			return
		}
	}

	// 好大的term，可以投票，但是要判断
	defer rf.persist()
	if rf.currTerm < args.Term {
		rf.currTerm = args.Term
		rf.state = Follower

		// 先判断
		rfLastLogIndex, rfLastLogTerm := rf.LogInfoByIndex(len(rf.logEntries) - 1)
		if rfLastLogTerm > args.LastLogTerm {
			reply.VoteGranted = false
			rf.votedFor = -1
			DebugLog(dVote, "S%d -> S%d vote false, legacy log\n", rf.me, args.CandidateId)
			return
		}
		if rfLastLogTerm == args.LastLogTerm && rfLastLogIndex > args.LastLogIndex {
			reply.VoteGranted = false
			rf.votedFor = -1
			DebugLog(dVote, "S%d -> S%d vote false, old log\n", rf.me, args.CandidateId)
			return
		}

		reply.VoteGranted = true
		rf.isTimeout = false
		rf.votedFor = args.CandidateId

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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1
	isLeader = true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		isLeader = false
		return
	}

	index = rf.logEntries[len(rf.logEntries)-1].Index + 1
	term = rf.currTerm
	rf.logEntries = append(rf.logEntries, logEntry{command, term, index})
	rf.persist()

	go rf.broadcastAppendEntries()

	DebugLog(dClient, "S%d recv command, LENLOG: %d, T: %d\n", rf.me, index, term)
	return
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
	// 如何在合适的时机关闭goroutine
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) heartBeat() {
	DebugLog(dLeader, "S%d convert to leader at T: %d\n", rf.me, rf.currTerm)

	for {
		rf.broadcastAppendEntries()
		time.Sleep(time.Millisecond * 100)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	if rf.killed() {
		rf.state = Follower
		rf.mu.Unlock()
		return
	}

	rf.isTimeout = false
	rf.mu.Unlock()

	for peerId := 0; peerId < len(rf.peers); peerId++ {
		if peerId == rf.me {
			continue
		}

		go func(id int) {
			rf.mu.Lock()
			// 需要注意的是，当rf.nextIndex[id] == rf.logEntries[len(rf.logEntries)-1].Index+1，对应的是发送heartbeat的情况
			if rf.nextIndex[id] > rf.logEntries[len(rf.logEntries)-1].Index+1 {
				rf.mu.Unlock()
				return
			}

			if rf.nextIndex[id] <= rf.snapshot.LastIncludedIndex {

				DebugLog(dError, "S%d -> S%d broadcast snapshot {IDX: %d, LENLOG: %d, LOG: %v}, NEXT: %v, SS: %v",
					rf.me, id, rf.nextIndex[id]-1, len(rf.logEntries), rf.logEntries[len(rf.logEntries)-1],
					rf.nextIndex, rf.snapshot)

				args := InstallSnapshotArgs{
					Term:              rf.currTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.snapshot.LastIncludedIndex,
					LastIncludedTerm:  rf.snapshot.LastIncludedTerm,
					Data:              rf.persister.snapshot,
					//Offset:            0,
					//Done:              true,
				}
				rf.mu.Unlock()
				reply := InstallSnapshotReply{}

				if ok := rf.sendInstallSnapshot(id, &args, &reply); !ok {
					DebugLog(dError, "S%d send snapshot to S%d failed\n", rf.me, id)
					return
				}

				rf.mu.Lock()
				if reply.Term > rf.currTerm {
					DebugLog(dLeader, "S%d in broadcast transfer to T: %d, convert to Follower\n", rf.me, reply.Term)
					rf.state = Follower
					rf.votedFor = -1
					rf.currTerm = reply.Term
					rf.persist()
				} else {
					rf.matchIndex[id] = max(rf.snapshot.LastIncludedIndex, rf.matchIndex[id])
					rf.nextIndex[id] = max(rf.matchIndex[id]+1, rf.nextIndex[id])
				}
				rf.mu.Unlock()
			} else {
				DebugLog(dError, "S%d -> S%d LogInfo {IDX: %d, LENLOG: %d, LOG: %v}, NEXT: %v, SS: %v",
					rf.me, id, rf.nextIndex[id]-1, len(rf.logEntries), rf.logEntries[len(rf.logEntries)-1],
					rf.nextIndex, rf.snapshot)
				prevLogIndex, prevLogTerm := rf.LogInfoByIndex(rf.nextIndex[id] - 1 - rf.snapshot.LastIncludedIndex)
				beginOfIndex := rf.nextIndex[id] - rf.snapshot.LastIncludedIndex
				endOfIndex := rf.logEntries[len(rf.logEntries)-1].Index + 1
				// DebugLog(dError, "S%d send to S%d, ENTRIES: %v\n", rf.me, id,
				// 	rf.logEntries[beginOfIndex:])

				args := AppendEntriesArgs{
					rf.currTerm,                  // Term
					rf.me,                        // LeaderId
					prevLogIndex,                 // PrevLogIndex
					prevLogTerm,                  // PrevLogTerm
					rf.logEntries[beginOfIndex:], // Entries —— heartbeat entry always nil
					rf.commitIndex,               // LeaderCommit
				}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}

				if ok := rf.sendAppendEntries(id, &args, &reply); ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currTerm {
						rf.state = Follower
						rf.votedFor = -1
						rf.currTerm = reply.Term
						rf.persist()
					} else {
						if reply.Success {
							if rf.nextIndex[id] < endOfIndex {
								rf.nextIndex[id] = endOfIndex
								rf.matchIndex[id] = endOfIndex - 1
								DebugLog(dError, "S%d -> S%d update success, NEXT: %v, MATCH: %v\n", rf.me, id, rf.nextIndex, rf.matchIndex)
								rf.updateCommitIndex()
							}
							// 讲道理，更新成功就要试试能不能update commitIndex
						} else {
							rf.nextIndex[id] = max(1, rf.nextIndex[id]/2)
							DebugLog(dError, "S%d -> S%d update failed, NEXT: %d\n", rf.me, id, rf.nextIndex[id])
						}
						// }
					}
				}
			}
		}(peerId)
	}
}

func (rf *Raft) updateCommitIndex() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	var dupMatchIndex []int
	dupMatchIndex = append(dupMatchIndex, rf.matchIndex...)
	dupMatchIndex[rf.me] = rf.logEntries[len(rf.logEntries)-1].Index
	sort.Ints(dupMatchIndex)
	DebugLog(dError, "S%d get N: %d\n", rf.me, dupMatchIndex[(len(rf.peers)-1)/2])

	var N int = dupMatchIndex[(len(rf.peers)-1)/2]
	if N > rf.commitIndex && rf.logEntries[N-rf.snapshot.LastIncludedIndex].Term == rf.currTerm {
		rf.commitIndex = N
		rf.applyCond.Signal()
		DebugLog(dError, "S%d update commitIndex CI: %d\n", rf.me, rf.commitIndex)
	}
}

func (rf *Raft) Apply() {
	//Your code may have loops that repeatedly check for certain events.
	//Don't have these loops execute continuously without pausing,
	//since that will slow your implementation enough that it fails tests.
	//Use Go's condition variables, or insert a time.Sleep(10 * time.Millisecond) in each loop iteration.
	for !rf.killed() {
		rf.mu.Lock()

		cid := max(rf.lastApplied, rf.snapshot.LastIncludedIndex) + 1
		for cid <= rf.commitIndex {
			DebugLog(dError, "S%d, LENLOG: %d, LA: %d apply msg {CI: %d} \n",
				rf.me, len(rf.logEntries), rf.lastApplied, cid)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logEntries[cid-rf.snapshot.LastIncludedIndex].Command,
				CommandIndex: cid,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			rf.lastApplied = cid
			cid = max(rf.lastApplied, rf.snapshot.LastIncludedIndex) + 1
		}

		rf.lastApplied = rf.commitIndex
		// block := rf.commitIndex == rf.lastApplied
		rf.mu.Unlock()

		rf.applyCond.L.Lock()
		rf.applyCond.Wait()
		rf.applyCond.L.Unlock()
	}
}

func (rf *Raft) StartElection() {
	DebugLog(dTimer, "S%d convert to candidate, calling election T: %d\n", rf.me, rf.currTerm)

	rf.mu.Lock()
	rf.currTerm += 1    //先将 Term 自增1
	rf.votedFor = rf.me // 给自己投票
	rf.persist()
	//本来应该reset ElectionTimeout的，由于并发执行，在外侧实现

	// lastIndex := len(rf.logEntries) - 1
	lastLogIndex, lastLogTerm := rf.LogInfoByIndex(len(rf.logEntries) - 1)
	args := RequestVoteArgs{
		rf.currTerm,  // Term
		rf.me,        // CandidateId
		lastLogIndex, // LastLogIndex
		lastLogTerm,  // LastLogTerm
	}
	rf.mu.Unlock()

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

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state == Follower { // 已经收到了其他Leader的心跳，直接返回
				return
			}

			if ok {
				if reply.VoteGranted { // 被选举了
					atomic.AddInt32(&nVoter, 1)
					if int(nVoter) > len(rf.peers)/2 {
						once.Do(func() {
							rf.state = Leader
							for i := range rf.peers {
								rf.nextIndex[i] = rf.logEntries[len(rf.logEntries)-1].Index + 1
								rf.matchIndex[i] = 0
							}

							go rf.heartBeat()
						})

						// go once.Do(rf.heartBeat)
					}
				} else if reply.Term > rf.currTerm { // 有更高的Term，转为Follower
					rf.state = Follower
					rf.votedFor = -1
					rf.currTerm = reply.Term
					rf.persist()
				} else {
					rf.isTimeout = false
				}
			} else {
				DebugLog(dError, "S%d sendRequestVote to S%d failed\n", rf.me, id)
			}
		}(peerId)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.state != Leader {
			// DebugLog(dTimer, "S%d not leader, election timeout...\n", rf.me)

			rf.isTimeout = true // 不加锁，后面会睡眠，所以不会有问题
			ElectionTimeout()   // Electiontimeout(), 睡眠200-400ms
			rf.mu.Lock()        // 加锁，因为后面会修改rf的状态
			//会超时，自己的状态只有可能是Follower或Candidate，统一转化为Candidate
			if rf.isTimeout { // && rf.state == Follower

				rf.state = Candidate
				go rf.StartElection() //并发执行选举, 满足 If election timeout elapses: start new election
			}
			rf.mu.Unlock()
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

	// LogEntries init
	rf.logEntries = []logEntry{{Command: "init server"}}
	rf.applyCh = applyCh
	rf.applyCond = sync.Cond{L: &sync.Mutex{}}
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for idx := range rf.peers {
		rf.nextIndex[idx] = 1
	}
	// isTimeout init false, reset true in ticker(), Do nothing
	// state init Follower, Do nothing
	// commitIndex init as 0, Do nothing
	// lastApplied init as 0, Do nothing

	// Reinitialized after election: nextIndex[] and matchIndex[]

	DebugLog(dClient, "S%d started at T:%d LLI:%d\n", rf.me, rf.currTerm, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start apply goroutine to apply log entries
	go rf.Apply()

	return rf
}
