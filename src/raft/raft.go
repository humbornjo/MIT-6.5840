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

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

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
	rf.state = Follower
	rf.isTimeout = false

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
	return ok
}

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

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.snapshot)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

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

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

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
	logs := []logEntry{}
	rf.mu.Lock()

	reply.Term = rf.currTerm
	// 如果我的Term比你的要大，不用搞了
	if rf.currTerm > args.Term || args.LastIncludedIndex <= rf.snapshot.LastIncludedIndex ||
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
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.mu.Unlock()
	rf.applyCh <- applyMsg
	DebugLog(dSnap, "S%d C%d install snapshot, IDX: %d T: %d, start from %d to %d\n", rf.me, rf.commitIndex,
		args.LastIncludedIndex, args.Term, rf.lastApplied, rf.logEntries[len(rf.logEntries)-1].Index)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 2A
	// Reply false if term < currentTerm
	// 2B
	// respond according to args.lastLogIndex and args.lastLogTerm
	// refer to last para in 5.4.1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currTerm
	if rf.currTerm > args.Term {
		reply.VoteGranted = false
		return
	}

	if rf.currTerm == args.Term {
		// 没投票，可以投，但是要判断
		if rf.votedFor == -1 {
			// 先判断
			rfLastLogIndex, rfLastLogTerm := rf.LogInfoByIndex(len(rf.logEntries) - 1)
			if rfLastLogTerm > args.LastLogTerm ||
				(rfLastLogTerm == args.LastLogTerm && rfLastLogIndex > args.LastLogIndex) {
				reply.VoteGranted = false
				return
			}
			reply.VoteGranted = true
			rf.isTimeout = false
			rf.votedFor = args.CandidateId
			rf.persist()
			return
		}

		// 投了票，看看是不是自己人，不用管Index和term了
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.isTimeout = false
			return
		} else {
			reply.VoteGranted = false
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
		if rfLastLogTerm > args.LastLogTerm ||
			(rfLastLogTerm == args.LastLogTerm && rfLastLogIndex > args.LastLogIndex) {
			reply.VoteGranted = false
			rf.votedFor = -1
			return
		}
		reply.VoteGranted = true
		rf.isTimeout = false
		rf.votedFor = args.CandidateId
		return
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DebugLog(dVote, "S%d C%d ask for vote, T: %d", server, rf.commitIndex, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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

	for rf.state == Leader && rf.killed() == false {
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
					}
				}
			}
		}(peerId)
	}
}

func (rf *Raft) updateCommitIndex() {
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		if rf.state != Leader {
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

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// 2A
	rf.votedFor = -1 // VotedFor init -1, for server index start from 0

	rf.logEntries = []logEntry{logEntry{Command: nil, Term: 0, Index: 0}}
	rf.applyCh = applyCh
	rf.applyCond = sync.Cond{L: &sync.Mutex{}}
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for idx := range rf.peers {
		rf.nextIndex[idx] = 1
	}

	DebugLog(dClient, "S%d started at T:%d LLI:%d\n", rf.me, rf.currTerm, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start apply goroutine to apply log entries
	go rf.Apply()

	return rf
}
