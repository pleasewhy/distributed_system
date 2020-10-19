package raft

import (
	"fmt"
	"time"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	Data              []byte
	LastIncludedIndex int
	LastIncludedTerm  int
}
type InstallSnapshotReply struct {
	Term int
}

// install snapshots rpc
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.convertToFollower(args.Term, true)
		return
	}
	rf.applySnapshot(args.Data)

	rf.LogManager.Offset = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.LogManager.Logs = []*Entry{}
	rf.LogManager.Logs = append(rf.LogManager.Logs, &Entry{
		Command: nil,
		Term:    args.LastIncludedTerm,
	})

	fmt.Printf("[%d] lastIncludedIndex:%d，log.length:%d, commitIndex:%d\n", rf.me, rf.lastIncludedIndex, rf.LogManager.length(),
		rf.commitIndex)

	stateData, _ := rf.GetStateData()
	rf.persister.SaveStateAndSnapshot(stateData, args.Data)
}

func (rf *Raft) sendSnapshotToServer(server int) bool {
	args := &InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		Data:              rf.persister.ReadSnapshot(),
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}
	reply := &InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.callInstallSnapshot(time.Millisecond*50, server, args, reply)
	rf.mu.Lock()
	if ok && reply.Term <= rf.CurrentTerm {
		rf.nextIndex[server] = rf.lastIncludedIndex + 1
		rf.matchIndex[server] = rf.lastIncludedIndex
		return true
	} else if ok {
		rf.convertToFollower(reply.Term, true)
	}
	return false
}

func (rf *Raft) callInstallSnapshot(timeout time.Duration, server int, args *InstallSnapshotArgs,
	reply *InstallSnapshotReply) bool {
	ch := time.After(timeout)
	res := make(chan bool)
	go func() {
		res <- rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	}()
	ok := true
	select {
	case ok = <-res:
	case <-ch:
		ok = false
	}
	return ok
}
