package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, currentTerm, isleader)
//   start agreement on a new log entry
// rf.GetState() (currentTerm, isLeader)
//   ask a Raft for its current currentTerm, and whether it thinks it is Leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"context"
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
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	Command interface{}
	Term    int
}

func (entry *Entry) String() string {
	return fmt.Sprintf("{command: %v, Term: %d}", entry.Command, entry.Term)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	identifier  int64
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	applyCh     chan ApplyMsg       // channel to connect with client
	dead        int32               // set by Kill()
	heartBeat   int64               // last heartBeat time
	currentTerm int                 // currentTerm number
	log         []*Entry            // index from 1
	nextIndex   []int
	matchIndex  []int
	commitIndex int
	lastApplied int
	VoteFor     int
	state       int
	timeout     int64
	sending     sync.Map // server-> whether the server is sending

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the Leader.
func (rf *Raft) GetState() (int, bool) {
	// TODO safety?
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.VoteFor)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []*Entry
	var currentTerm int
	var voteFor int
	if d.Decode(&log) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil {
		panic("error")
	} else {
		rf.log = log
		rf.currentTerm = currentTerm
		rf.VoteFor = voteFor
	}
}

// unsafe need lock
func (rf *Raft) appendLog(previousIndex int, entries []*Entry) {
	for i, j := previousIndex+1, 0; j < len(entries); i, j = i+1, j+1 {
		if i >= len(rf.log) {
			rf.log = append(rf.log, entries[j])
		} else if entries[j].Term != rf.log[i].Term {
			rf.log = rf.log[0:i]
			rf.log = append(rf.log, entries[j])
		}
	}
	rf.persist()
}

//
// slice log and replace log with it
//
func (rf *Raft) sliceAndReplaceLog(start int, end int) {
	if len(rf.log) >= end {
		rf.log = rf.log[start:end]
	}
	rf.persist()
}

func (rf *Raft) changeTerm(term int, locked bool) {
	if !locked {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	rf.currentTerm = term
	rf.VoteFor = -1
	rf.persist()
}

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	rf.changeTerm(rf.currentTerm+1, true)
	rf.VoteFor = rf.me
	rf.state = Candidate
	rf.heartBeat = time.Now().UnixNano()
	rf.timeout = getRandTimeout()
	fmt.Printf("[%d] converting to candidate\n", rf.me)
	rf.mu.Unlock()
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	rf.state = Leader
	for i, _ := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
	fmt.Printf("[%d] converting to leader\n", rf.me)
	rf.mu.Unlock()
	rf.appendRPCToAllServer()
}

func (rf *Raft) convertToFollower(term int, locked bool) {
	if !locked {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	rf.changeTerm(term, true)
	rf.state = Follower
	rf.VoteFor = -1
	fmt.Printf("[%d] converting to follower\n", rf.me)
}

func (rf *Raft) AttemptRequestVote() {
	DPrintf("[%d] attempting an election at currentTerm %d.", rf.me, rf.currentTerm)
	rf.convertToCandidate()
	rf.mu.Lock()
	voteNum := 1
	term := rf.currentTerm
	rf.mu.Unlock()
	var finished int32 = 1
	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	cond := sync.NewCond(&sync.Mutex{})
	done := false
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		server := i
		go func() {
			reply := &RequestVoteReply{}
			ok := rf.callRequestVoteTimeout(time.Millisecond*50, server, args, reply)
			cond.L.Lock()
			defer cond.Broadcast()
			defer cond.L.Unlock()

			if !ok || done {
				finished++
				return
			}

			if term < reply.Term {
				DPrintf("[%d] the currentTerm is less than server's currentTerm, exit election of this time.",
					rf.me)
				rf.convertToFollower(reply.Term, false)
				finished++
				done = true
				return
			}

			if reply.VoteGranted {
				DPrintf("[%d] obtain vote from %d.", rf.me, server)
				voteNum++
			}
			finished++
		}()
	}
	serverNum := len(rf.peers)
	cond.L.Lock()
	for !done && voteNum <= serverNum/2 && int(finished) < serverNum {
		cond.Wait()
	}
	done = true
	if voteNum > serverNum/2 {

		// If another server attempts an election before this server finishes election,
		// the former server can also obtain mostly vote to be a Leader. in this case,
		// there will exist two leaders.
		// entire process:
		// 		server0 attempting election at currentTerm 1
		// 		server0 send vote request to server1
		// 		server0 send vote request to server2
		// 		server0 receive a vote from server1 (server1 update currentTerm to 1)
		// 		server1 attempting election at currentTerm 2
		//		......

		if term != rf.currentTerm {
			DPrintf("[%d] current currentTerm already changed.", rf.me)
			return
		}

		DPrintf("[%d] the vote number is enough to become Leader, vote=%d currentTerm=%d.",
			rf.me, voteNum, term)
		rf.convertToLeader()
	} else {
		DPrintf("[%d] the vote number isn't enough to become Leader, vote=%d currentTerm=%d.",
			rf.me, voteNum, term)
	}
	cond.L.Unlock()
	DPrintf("[%d] finish the election", rf.me)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
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
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("[%d] receive vote request", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term, true)
	}
	term := rf.log[len(rf.log)-1].Term
	// whether candidate's log newer than the server's
	isNewer := args.LastLogTerm > term || (args.LastLogTerm == term && args.LastLogIndex >= len(rf.log)-1)
	if args.Term < rf.currentTerm {
		DPrintf("[%d] refuse vote request", rf.me)
		return
	} else if (rf.VoteFor == -1 || rf.VoteFor == args.CandidateId) && isNewer {
		rf.convertToFollower(args.Term, true)
		rf.VoteFor = args.CandidateId
		reply.VoteGranted = true
		return
	}
	if !isNewer {
		DPrintf("candidate=%d's log is older than the server %d\n", args.CandidateId, rf.me)
	} else {
		DPrintf("candidateId=%d the server has already vote for %d\n", args.CandidateId, rf.VoteFor)
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
func (rf *Raft) callRequestVoteTimeout(timeout time.Duration, server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("[%d] sending vote request to %d", rf.me, server)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	res := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		res <- ok
	}()
	ok := true
	select {
	case ok = <-res:
	case <-ctx.Done():
		ok = false
	}
	cancel()
	return ok
}

// AppendEntries RPC arguments structure,
// the field name must start with capital letter!
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
}

// AppendEntries RPC
type AppendEntriesReply struct {
	LastTermIndex int // if args.prevLogIndex>=len(rf.log), this variable assigned len(rf.log)-1
	Term          int
	Success       bool
}

//
// append log entries, also is a heartBeat message if args.entries == nil
// it will update the heartBeat time to avoid attempt election
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	atomic.StoreInt64(&rf.heartBeat, time.Now().UnixNano())

	// verify that the previous log is consistent with the leader.
	if args.PrevLogIndex >= len(rf.log) {
		DPrintf("[%d] prevLogIndex:%d,rf.log.length:%d\n", rf.me, args.PrevLogIndex, len(rf.log))
		reply.Success = false
		reply.LastTermIndex = len(rf.log) - 1
		return
	}

	if args.PrevLogIndex != 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d] leader's prevLogTerm term:%d, server's:%d",
			rf.me, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term)
		//rf.sliceAndReplaceLog(0, args.PrevLogIndex)
		reply.Success = false
		i := args.PrevLogIndex
		for ; i > 1; i-- {
			if rf.log[i].Term != rf.log[args.PrevLogIndex].Term {
				break
			}
		}
		reply.LastTermIndex = i
		return
	}

	if args.Term < rf.currentTerm {
		DPrintf("[%d] leader=%d'Term less than currentTerm\n", rf.me, args.LeaderId)
		reply.Success = false
		return
	}

	// if leader's term is more than the server‘s, the server must
	// convert state to follower.
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term, true)
	}

	if args.Entries != nil || len(args.Entries) != 0 {
		rf.appendLog(args.PrevLogIndex, args.Entries)
		DPrintf("[%d] log:%v", rf.me, rf.log)
	}

	// if leader's commitIndex more than the server's,the server update commitIndex to leader's
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := min(args.LeaderCommit, len(rf.log)-1)
		for commitIndex := rf.commitIndex + 1; commitIndex <= newCommitIndex; commitIndex++ {
			msg := ApplyMsg{Command: rf.log[commitIndex].Command, CommandIndex: commitIndex, CommandValid: true}
			DPrintf("[%d] update commitIndex to %d, %v", rf.me, commitIndex, msg)
			rf.sendCommandToClient(rf.log[commitIndex].Command, true, commitIndex)
		}
		rf.commitIndex = newCommitIndex
		//fmt.Printf("server %d:%v\n", rf.me, rf.log)
	}
	reply.Success = true
}

//
// call server's AppendEntries RPC, it will be used for HeartBeat Messages if appendLog is false
//
// return whether the call was successful, and the number of entries sent
//
func (rf *Raft) callAppendEntries(server int, appendLog bool) (*AppendEntriesReply, bool) {
	var args *AppendEntriesArgs
	rf.mu.Lock()
	boolObj, ok := rf.sending.Load(server)
	var isSending bool
	if !ok {
		rf.sending.Store(server, true)
		isSending = false
	} else {
		isSending = boolObj.(bool)
	}
	if isSending {
		rf.mu.Unlock()
		//fmt.Printf("正在发送\n")
		return nil, false
	}
	rf.sending.Store(server, true)

	var entries []*Entry
	nextIndex := rf.nextIndex[server]
	if rf.matchIndex[server] >= rf.nextIndex[server] {
		entries = rf.log[nextIndex:]
	} else {
		entries = rf.log[nextIndex:]
	}

	args = &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  rf.log[nextIndex-1].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	// At some point, this goroutine prepares the args after another goroutine
	// has already received the reply and converted state to follower. In this
	// case, a disconnected leader reconnect to the net, it will send an AppendLog
	// RPC, while it has already changed state and updated term.
	if rf.state != Leader {
		rf.mu.Unlock()
		return nil, false
	}

	rf.mu.Unlock()
	reply := &AppendEntriesReply{}

	ok = rf.sendAppendEntriesTimeout(time.Millisecond*50, server, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.sending.Store(server, false)
	if !ok || rf.state != Leader || rf.currentTerm != args.Term {
		//DPrintf("[%d] send to %d failed", rf.me, server)
		return nil, false
	}

	if reply.Term > rf.currentTerm {
		DPrintf("[%d] current currentTerm less than reply.currentTerm", rf.me)
		rf.convertToFollower(reply.Term, true)
		return nil, false
	}

	if reply.Success {
		//fmt.Printf("server %d\n", server)
		rf.nextIndex[server] += len(entries)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		N := -1
		for i := rf.commitIndex + 1; i < len(rf.log); i++ {
			cnt := 1
			for _, j := range rf.matchIndex {
				if j >= i {
					//fmt.Printf("server commitIndex:%d, %d matchIndex:%d", i, server, j)
					cnt++
				}
			}
			//fmt.Printf("cnt:%v commitIndex:%d matchIndex:%v\n", cnt, rf.commitIndex, rf.matchIndex)
			if cnt > len(rf.peers)/2 && rf.log[i].Term == rf.currentTerm {
				N = i
			}
		}
		if N != -1 {
			for i := rf.commitIndex + 1; i <= N; i++ {
				rf.commitIndex = i
				rf.sendCommandToClient(rf.log[i].Command, true, i)
			}
		}
	} else {
		//fmt.Printf("[%d] %d 匹配失败,nextIndex:%d,command:%v\n args:%v reply:%v\n", rf.me, server,
		//	rf.nextIndex[server], rf.log[rf.nextIndex[server]-1].Command, args, reply)
		rf.nextIndex[server] = reply.LastTermIndex + 1
	}
	return reply, ok
}

//
// call to AppendEntries RPC with timeout.
//
func (rf *Raft) sendAppendEntriesTimeout(timeout time.Duration, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ch := time.After(timeout)
	res := make(chan bool)
	go func() {
		res <- rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}()
	ok := true
	select {
	case ok = <-res:
	case <-ch:
		ok = false
	}
	return ok
}
func (rf *Raft) sendCommandToClient(command interface{}, commandValid bool, commandIndex int) {
	msg := ApplyMsg{
		CommandValid: commandValid,
		Command:      command,
		CommandIndex: commandIndex,
	}
	DPrintf("[%d] commit successfully:%v\n", rf.me, msg)
	rf.applyCh <- msg
}

func (rf *Raft) appendLogToAllServer() {
	successNum := 1
	serverNum := len(rf.peers)
	var isSuccess [10]bool
	done := false
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}

	for i, _ := range rf.peers {
		if i == rf.me || isSuccess[i] {
			continue
		}
		wg.Add(1)
		server := i
		go func() {
			defer wg.Done()
			//DPrintf("[%d] send AppendEntries RPC to %d, nextIndex:%d.\n", rf.me, server, rf.nextIndex[server])
			reply, ok := rf.callAppendEntries(server, true)
			mu.Lock()
			defer mu.Unlock()

			if !ok || done {
				return
			}
			if reply.Success {
				successNum++
				isSuccess[server] = true
				if successNum > serverNum/2 {
					done = true
				}
			}
		}()
	}
	wg.Wait()
}

//
// check whether the heartbeat message sent by the Leader has timeout
//`
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
		rf.appendRPCToAllServer()

		time.Sleep(time.Millisecond * 100)
	}
}

//
// call the AppendEntries RPC of all servers.
//
// return the number of AppendEntriesReply.success==true
//
func (rf *Raft) appendRPCToAllServer() {
	wg := sync.WaitGroup{}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		server := i
		wg.Add(1)
		go func() {
			_, ok := rf.callAppendEntries(server, false)
			if !ok {
				wg.Done()
				return
			}
			wg.Done()
		}()
	}
	wg.Wait()
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
// currentTerm. the third return value is true if this server believes it is
// the Leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}
	//fmt.Printf("command:%v\n", command)
	entry := &Entry{Command: command, Term: rf.currentTerm}
	index := len(rf.log)
	rf.appendLog(len(rf.log)-1, []*Entry{entry})
	term := rf.currentTerm
	rf.mu.Unlock()
	go rf.appendRPCToAllServer()
	return index, term, true
}

func (rf *Raft) getState1() string {
	if rf.state == Leader {
		return "Leader"
	} else if rf.state == Candidate {
		return "Candidate"
	} else {
		return "Follower"
	}
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
	DPrintf("[%d] id:%v killed", rf.me, rf.identifier)
	// Your code here, if desired.m3
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == DEATH
}

func getRandTimeout() int64 {
	//return 200 * int64(time.Millisecond)
	return int64(rand.Intn(400)+200) * int64(time.Millisecond)
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
	rf.persister = persister
	rf.me = me
	rf.heartBeat = time.Now().UnixNano()
	rf.VoteFor = -1
	// Your initialization code here (2A, 2B, 2C).
	rf.timeout = getRandTimeout()
	rf.applyCh = applyCh
	rf.log = append(rf.log, &Entry{Term: 0, Command: 0})

	for range peers {
		rf.matchIndex = append(rf.matchIndex, 0)
		rf.nextIndex = append(rf.nextIndex, 1)
	}
	DPrintf("[%d] timeout:%dms", rf.me, rf.timeout/1_000_000)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.sendHeartBeatPeriodically()
	go rf.checkHeartBeat()
	return rf
}
