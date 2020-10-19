package raft

import (
	"sync/atomic"
	"time"
)

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
	//fmt.Printf("[%d] receive append entries from leader=%d\n", rf.me, args.LeaderId)
	reply.Term = rf.CurrentTerm
	reply.LastTermIndex = rf.LogManager.Offset
	atomic.StoreInt64(&rf.heartBeat, time.Now().UnixNano())

	// verify that the previous log is consistent with the leader.
	if args.PrevLogIndex >= rf.LogManager.length() {
		DPrintf("[%d] prevLogIndex:%d,rf.log.length:%d\n", rf.me, args.PrevLogIndex, rf.LogManager.length())
		reply.Success = false
		reply.LastTermIndex = rf.LogManager.length() - 1
		return
	}
	if args.PrevLogIndex-rf.LogManager.Offset > 0 && rf.LogManager.get(args.PrevLogIndex).Term != args.PrevLogTerm {
		DPrintf("[%d] leader's prevLogTerm term:%d, server's:%d",
			rf.me, args.PrevLogTerm, rf.LogManager.get(args.PrevLogIndex).Term)
		//rf.sliceAndReplaceLog(0, args.PrevLogIndex)
		reply.Success = false
		i := args.PrevLogIndex
		for ; i > 1 && i >= rf.LogManager.Offset; i-- {
			if rf.LogManager.get(i).Term != rf.LogManager.get(args.PrevLogIndex).Term {
				break
			}
		}
		reply.LastTermIndex = i
		return
	}

	if args.Term < rf.CurrentTerm {
		DPrintf("[%d] leader=%d'Term less than CurrentTerm\n", rf.me, args.LeaderId)
		reply.Success = false
		return
	}

	// if leader's term is more than the server‘s, the server must
	// convert state to follower.
	if args.Term > rf.CurrentTerm {
		rf.convertToFollower(args.Term, true)
	}

	if args.PrevLogIndex >= rf.LogManager.Offset && (args.Entries != nil || len(args.Entries) != 0) {
		rf.appendLog(args.PrevLogIndex, args.Entries)
		//DPrintf("[%d] log:%v", rf.me, rf.LogManager.Logs)
	}

	// if leader's commitIndex more than the server's,the server update commitIndex to leader's
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := min(args.LeaderCommit, rf.LogManager.length()-1)
		for commitIndex := rf.commitIndex + 1; commitIndex <= newCommitIndex; commitIndex++ {
			msg := ApplyMsg{Command: rf.LogManager.get(commitIndex).Command, CommandIndex: commitIndex, CommandValid: true}
			DPrintf("[%d] update commitIndex to %d, %v", rf.me, commitIndex, msg)
			needLock := false
			if commitIndex == newCommitIndex {
				needLock = true
			}
			rf.commitIndex = commitIndex
			rf.sendCommandToKV(rf.LogManager.get(commitIndex).Command, true, commitIndex, needLock)
		}

		rf.persist()
		//fmt.Printf("server %d:%v\n", rf.me, rf.log)
	}
	reply.Success = true
}

//
// call server's AppendEntries RPC, it will be used for HeartBeat Messages if appendLog is false
//
// return whether the call was successful, and the number of entries sent
//
func (rf *Raft) callAppendEntries(server int) (*AppendEntriesReply, bool) {
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
		return nil, false
	}
	rf.sending.Store(server, true)
	defer rf.sending.Store(server, false)

	// if the leader discard the log that detects consistency, then send the snapshot
	// to the corresponding follower.
	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		ok := rf.sendSnapshotToServer(server)
		if !ok || rf.nextIndex[server] <= rf.lastIncludedIndex {
			rf.mu.Unlock()
			return nil, false
		}
	}

	var entries []*Entry
	nextIndex := rf.nextIndex[server]
	if rf.matchIndex[server] >= rf.nextIndex[server] {
		entries = rf.LogManager.Slice(nextIndex, rf.LogManager.length())
	} else {
		entries = rf.LogManager.Slice(nextIndex, rf.LogManager.length())
		// TODO don't send all entries.
	}
	//fmt.Printf("[%d] nextIndex:%d, LogManager offset:%d, lastIncludedIndex:%d\n",
	//	rf.me, nextIndex, rf.LogManager.Offset, rf.lastIncludedIndex)
	args = &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  rf.LogManager.get(nextIndex - 1).Term,
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
	if !ok || rf.state != Leader || rf.CurrentTerm != args.Term {
		//DPrintf("[%d] send to %d failed", rf.me, server)
		return nil, false
	}

	if reply.Term > rf.CurrentTerm {
		DPrintf("[%d] current CurrentTerm less than reply.CurrentTerm", rf.me)
		rf.convertToFollower(reply.Term, true)
		return nil, false
	}

	if reply.Success {
		//fmt.Printf("server %d\n", server)
		rf.nextIndex[server] += len(entries)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		N := -1
		for i := rf.commitIndex + 1; i < rf.LogManager.length(); i++ {
			cnt := 1
			for _, j := range rf.matchIndex {
				if j >= i {
					//fmt.Printf("server commitIndex:%d, %d matchIndex:%d", i, server, j)
					cnt++
				}
			}
			//fmt.Printf("cnt:%v commitIndex:%d matchIndex:%v\n", cnt, rf.commitIndex, rf.matchIndex)
			if cnt > len(rf.peers)/2 && rf.LogManager.get(i).Term == rf.CurrentTerm {
				N = i
			}
		}
		if N != -1 {
			for i := rf.commitIndex + 1; i <= N; i++ {
				rf.commitIndex = i
				needLock := false
				if i == N {
					needLock = true
				}
				rf.sendCommandToKV(rf.LogManager.get(i).Command, true, i, needLock)
			}
		}
		return reply, true
	} else {
		//fmt.Printf("[%d] %d 匹配失败,nextIndex:%d,command:%v\n args:%v reply:%v\n", rf.me, server,
		//	rf.nextIndex[server], rf.log[rf.nextIndex[server]-1].Command, args, reply)
		rf.nextIndex[server] = reply.LastTermIndex + 1
	}
	return reply, false
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
