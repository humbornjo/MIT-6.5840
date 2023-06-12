package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Debugging
const Debug = true
const LockDebug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf("CLNT "+format, a...)
	}
	return
}

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DebugLog(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

const (
	minElectionTimeout = 150
	maxElectionTimeout = 300
)

func ElectionTimeout() {
	rand.Seed(time.Now().UnixNano())
	sleepTime := time.Duration(rand.Intn(maxElectionTimeout-minElectionTimeout+1) + minElectionTimeout)
	time.Sleep(time.Millisecond * sleepTime)
}

func (rf *Raft) LogInfoByIndex(idx int) (index int, term int) {
	// 初始化中，logEntries不为空了，为每个server加了一个初始log entry
	// if len(rf.logEntries) == 0 {
	// 	return 0, 0
	// }
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	DebugLog(dError, "S%d LogInfo {IDX: %d, LENLOG: %d, LOG: %v}", rf.me, idx, len(rf.logEntries), rf.logEntries[len(rf.logEntries)-1])
	t := &testing.T{}
	assert.Conditionf(t, func() bool { return idx >= 0 && idx < len(rf.logEntries) }, "idx out of range")
	index, term = rf.logEntries[idx].Index, rf.logEntries[idx].Term
	return
}

func (rf *Raft) LogLock() {
	if LockDebug {
		DPrintf("try LogLock: %d\n", rf.me)
	}
	rf.mu.Lock()
	if LockDebug {
		DPrintf("get LogLock: %d\n", rf.me)
	}
}

func (rf *Raft) LogUnlock() {
	if LockDebug {
		DPrintf("try Release LogLock: %d\n", rf.me)
	}
	rf.mu.Unlock()
}
