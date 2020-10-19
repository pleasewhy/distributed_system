package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, CurrentTerm, isleader)
//   start agreement on a new log entry
// rf.GetState() (CurrentTerm, isLeader)
//   ask a Raft for its current CurrentTerm, and whether it thinks it is Leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

import "../labgob"

const (
	Follower = iota
	Candidate
	Leader
	DEATH = 1
)

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
	CommandValid  bool
	Command       interface{}
	CommandIndex  int
	Kvmap         map[string]string
	NeedLock      bool
	ApplySnapshot bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	identifier         int64
	mu                 sync.Mutex          // Lock to protect shared access to this peer's state
	peers              []*labrpc.ClientEnd // RPC end points of all peers
	persister          *Persister          // Object to hold this peer's persisted state
	me                 int                 // this peer's index into peers[]
	applyCh            chan ApplyMsg       // channel to connect with client
	dead               int32               // set by Kill()
	heartBeat          int64               // last heartBeat time
	CurrentTerm        int                 // CurrentTerm number
	LogManager         *LogManager
	nextIndex          []int
	matchIndex         []int
	commitIndex        int
	lastApplied        int
	VoteFor            int
	state              int
	timeout            int64
	sending            sync.Map // server-> whether the server is sending
	discardLogNeedLock bool
	lastIncludedIndex  int
	lastIncludedTerm   int
}

// return CurrentTerm and whether this server
// believes it is the Leader.
func (rf *Raft) GetState() (int, bool) {
	// TODO safety?
	return rf.CurrentTerm, rf.state == Leader
}

// return commitIndex and CurrentTerm
func (rf *Raft) GetIndexAndTerm() (int, int) {
	return rf.commitIndex, rf.CurrentTerm
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.LogManager.Logs)
	e.Encode(rf.LogManager.Offset)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) GetStateData() ([]byte, error) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.LogManager.Logs)
	e.Encode(rf.LogManager.Offset)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	data := w.Bytes()
	return data, nil
}

//
// restore previously persisted state.
//
func (rf *Raft) applyRaftState(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var Offset int
	var Logs []*Entry
	var CurrentTerm int
	var voteFor int

	err := d.Decode(&Logs)
	if err != nil {
		fmt.Println(err)
	}

	err = d.Decode(&Offset)
	if err != nil {
		fmt.Println(err)
	}

	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&voteFor) != nil {
		panic("error")
	} else {
		rf.LogManager = &LogManager{Logs: Logs, Offset: Offset}
		rf.CurrentTerm = CurrentTerm
		rf.VoteFor = voteFor
	}
}

func (rf *Raft) applySnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	bf := bytes.NewBuffer(data)
	d := labgob.NewDecoder(bf)
	var kvmap map[string]string
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&kvmap) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic("decode snapshot error")
	}

	rf.sendSnapshotToKV(kvmap)
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

// unsafe need lock
func (rf *Raft) appendLog(previousIndex int, entries []*Entry) {
	for i, j := previousIndex+1, 0; j < len(entries); i, j = i+1, j+1 {
		if i >= rf.LogManager.length() {
			rf.LogManager.appendLog(entries[j])
		} else if entries[j].Term != rf.LogManager.get(i).Term {
			rf.LogManager.Logs = rf.LogManager.Slice(rf.LogManager.Offset, i)
			rf.LogManager.appendLog(entries[j])
		}
	}
	rf.persist()
}

func (rf *Raft) changeTerm(term int, locked bool) {
	if !locked {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	rf.CurrentTerm = term
	rf.VoteFor = -1
	rf.persist()
}

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	rf.changeTerm(rf.CurrentTerm+1, true)
	rf.VoteFor = rf.me
	rf.state = Candidate
	rf.heartBeat = time.Now().UnixNano()
	rf.timeout = getRandTimeout()
	rf.persist()
	//log.Printf("[%d] converting to candidate, currentTerm=%d, lastLogTerm=%d,log.length=%d\n",
	//	rf.me, rf.CurrentTerm, rf.LogManager.get(rf.LogManager.length()-1).Term, rf.LogManager.length())
	rf.mu.Unlock()
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	for i, _ := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.LogManager.length()
	}
	//log.Printf("[%d] converting to leader, currentTerm=%d, lastLogTerm=%d,log.length=%d\n",
	//	rf.me, rf.CurrentTerm, rf.LogManager.get(rf.LogManager.length()-1).Term, rf.LogManager.length())
	rf.state = Leader
	rf.mu.Unlock()
	//rf.AppendRPCToAllServer()
	rf.Start(nil)
}

func (rf *Raft) convertToFollower(term int, locked bool) {
	if !locked {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	rf.state = Follower
	if rf.CurrentTerm < term {
		rf.VoteFor = -1
		rf.changeTerm(term, true)
	}

	rf.timeout = getRandTimeout()
	//log.Printf("[%d] converting to follower, currentTerm=%d, lastLogTerm=%d,log.length=%d\n",
	//	rf.me, rf.CurrentTerm, rf.LogManager.get(rf.LogManager.length()-1).Term, rf.LogManager.length())
}

func (rf *Raft) sendSnapshotToKV(kvMap map[string]string) {
	msg := ApplyMsg{
		CommandValid: false,
		Command:      nil,
		CommandIndex: 0,
		Kvmap:        kvMap,
		ApplySnapshot: true,
	}
	rf.applyCh <- msg
}

// send log command to kv server
func (rf *Raft) sendCommandToKV(command interface{}, commandValid bool, commandIndex int, needLock bool) {
	if command == nil {
		commandValid = false
	}
	msg := ApplyMsg{
		CommandValid: commandValid,
		Command:      command,
		CommandIndex: commandIndex,
		Kvmap:        nil,
		NeedLock:     needLock,
	}
	rf.applyCh <- msg
}

//
// check whether the heartbeat message sent by the Leader has timeout
//
func (rf *Raft) checkHeartBeat() {
	for !rf.killed() {
		rf.mu.Lock()
		ok := time.Now().UnixNano()-rf.heartBeat > rf.timeout && rf.state != Leader
		//fmt.Printf("[%d] heartBeart %v %v\n", rf.me, time.Now().UnixNano()-rf.heartBeat, rf.state)
		rf.mu.Unlock()
		if ok {
			rf.AttemptRequestVote()
		}
		time.Sleep(time.Millisecond * 20)
	}
}

//
// send heart beat message to all server periodically
//
func (rf *Raft) sendHeartBeatPeriodically() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 100)
			continue
		}
		rf.mu.Unlock()
		rf.AppendRPCToAllServer()
		time.Sleep(time.Millisecond * 100)
	}
}

//
// call the AppendEntries RPC of all servers.
//
// return the number of AppendEntriesReply.success==true
//
func (rf *Raft) AppendRPCToAllServer() int {
	wg := sync.WaitGroup{}

	successNum := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		server := i
		wg.Add(1)
		go func() {
			_, ok := rf.callAppendEntries(server)
			if ok {
				successNum++
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return successNum
}

//
//  check whether the leader can connect the majority server
//
func (rf *Raft) IsConnectMajority() bool {
	return rf.AppendRPCToAllServer() > len(rf.peers)/2
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the Leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the Leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// CurrentTerm. the third return value is true if this server believes it is
// the Leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}
	//fmt.Printf("command:%v\n", command)
	entry := &Entry{Command: command, Term: rf.CurrentTerm}
	index := rf.LogManager.length()
	//rf.appendLog(len(rf.log)-1, []*Entry{entry})
	rf.LogManager.appendLog(entry)
	rf.persist()
	term := rf.CurrentTerm
	rf.mu.Unlock()
	go rf.AppendRPCToAllServer()
	return index, term, true
}

//
// need hold rf.mu.lock, while you invoke this method
//
func (rf *Raft) DiscardLogWithLock(lastIncludedIndex int) {
	//DPrintf("[%d] lastIncludedIndex:%d, offset:%d,logs:\n%v", rf.me, lastIncludedIndex, rf.LogManager.Offset, rf.LogManager.Logs)
	//minNextIndex := math.MaxInt32
	//rf.mu.Lock()
	//log.Printf("[%d] discard logs that index<%d,the log is %v\n",
	//	rf.me, lastIncludedIndex, rf.LogManager.get(lastIncludedIndex))
	if rf.LogManager.Logs == nil || len(rf.LogManager.Logs) == 0 {
		return
	}

	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = rf.LogManager.get(lastIncludedIndex).Term
	rf.LogManager.DiscardLogs(lastIncludedIndex)
	//rf.mu.Unlock()
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
	atomic.StoreInt32(&rf.dead, DEATH)
	DPrintf("kill [%d]", rf.me)

	// Your code here, if desired.m3
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == DEATH
}

func getRandTimeout() int64 {
	//return 200 * int64(time.Millisecond)
	return int64(rand.Intn(400)+400) * int64(time.Millisecond)
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
	rf.identifier = rand.Int63() % 1000
	rf.peers = peers
	rf.me = me
	rf.heartBeat = time.Now().UnixNano()
	// Your initialization code here (2A, 2B, 2C).
	rf.timeout = getRandTimeout()
	rf.applyCh = applyCh

	rf.persister = persister
	rf.VoteFor = -1
	rf.LogManager = &LogManager{Logs: []*Entry{}, Offset: 0}

	//rf.log = append(rf.log, &Entry{Term: 0, Command: 0})
	rf.LogManager.appendLog(&Entry{
		Command: nil,
		Term:    0,
	})
	rf.CurrentTerm = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.applyRaftState(persister.ReadRaftState())
	rf.applySnapshot(persister.ReadSnapshot())
	rf.commitIndex = rf.LogManager.Offset
	for range peers {
		rf.matchIndex = append(rf.matchIndex, 0)
		rf.nextIndex = append(rf.nextIndex, 1)
	}
	DPrintf("start raft [%d] commitIndex LogManager.length=%d", rf.me, rf.LogManager.length())
	// initialize from state persisted before a crash
	go rf.sendHeartBeatPeriodically()
	go rf.checkHeartBeat()
	return rf
}
