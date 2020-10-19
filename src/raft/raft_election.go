package raft

import (
	"context"
	"log"
	"sync"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your Data here (2A, 2B).
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
	// Your Data here (2A).
	Term        int
	VoteGranted bool
}

//
//  RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] receive vote request", rf.me)

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	if args.Term > rf.CurrentTerm {
		rf.convertToFollower(args.Term, true)
	}
	//term := rf.log[len(rf.log)-1].Term
	term := rf.LogManager.get(rf.LogManager.length() - 1).Term
	// whether candidate's log newer than the server's
	isNewer := args.LastLogTerm > term || (args.LastLogTerm == term && args.LastLogIndex >= rf.LogManager.length()-1)
	if args.Term < rf.CurrentTerm {
		DPrintf("[%d] refuse vote request", rf.me)
		return
	} else if (rf.VoteFor == -1 || rf.VoteFor == args.CandidateId) && isNewer {
		rf.convertToFollower(args.Term, true)
		rf.VoteFor = args.CandidateId
		rf.persist()
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
	DPrintf("[%d] sending vote request to %d, log.length=%v, commitIndex=%v",
		rf.me, server, rf.LogManager.length(), rf.commitIndex)
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

func (rf *Raft) AttemptRequestVote() {
	DPrintf("[%d] attempting an election at CurrentTerm %d.", rf.me, rf.CurrentTerm)
	rf.convertToCandidate()
	rf.mu.Lock()
	voteNum := 1
	term := rf.CurrentTerm
	rf.mu.Unlock()
	var finished int32 = 1
	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: rf.LogManager.length() - 1,
		//LastLogTerm:  rf.log[len(rf.log)-1].Term,
		LastLogTerm: rf.LogManager.get(rf.LogManager.length() - 1).Term,
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
				DPrintf("[%d] the CurrentTerm is less than server's CurrentTerm, exit election of this time.",
					rf.me)
				rf.convertToFollower(reply.Term, false)
				finished++
				done = true
				return
			}

			if reply.VoteGranted {
				log.Printf("[%d] obtain vote from %d.\n", rf.me, server)
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
		// 		server0 attempting election at CurrentTerm 1
		// 		server0 send vote request to server1
		// 		server0 send vote request to server2
		// 		server0 receive a vote from server1 (server1 update CurrentTerm to 1)
		// 		server1 attempting election at CurrentTerm 2
		//		......

		if term != rf.CurrentTerm && rf.state != Candidate {
			DPrintf("[%d] current CurrentTerm already changed.", rf.me)
			return
		}

		DPrintf("[%d] the vote number is enough to become Leader, vote=%d CurrentTerm=%d.",
			rf.me, voteNum, term)
		rf.convertToLeader()
	} else {
		DPrintf("[%d] the vote number isn't enough to become Leader, vote=%d CurrentTerm=%d.",
			rf.me, voteNum, term)
	}
	cond.L.Unlock()
	DPrintf("[%d] finish the election", rf.me)
}
