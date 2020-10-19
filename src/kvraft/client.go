package kvraft

import (
	"../labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers       []*labrpc.ClientEnd
	currentLeader int
	mu            sync.Mutex
	lastOpId      int64
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	DPrintf("send contain request key = %s.", key)
	args := &GetArgs{Key: key}
	reply := &GetReply{}
	ok := ck.callGetRpc(ck.currentLeader, time.Millisecond*200, args, reply)
	if ok && reply.Err == OK {
		return reply.Value
	}
	for {
		//ck.servers[ck.currentLeader].Call("KVServer.Get", args, reply)
		for i := 0; i < len(ck.servers); i++ {
			reply := &GetReply{}
			ok := ck.callGetRpc(i, time.Millisecond*200, args, reply)
			if !ok {
				//fmt.Printf("call %d contain failed\n", i)
				continue
			}

			if reply.Err == OK {
				ck.currentLeader = i
				//fmt.Printf("contain finish {key:%v, value:%v}\n", args.Key, reply.Value)
				return reply.Value
			}
		}
		time.Sleep(time.Millisecond * 100)
	}

	// You will have to modify this function.
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op int) {
	DPrintf("send PutAppend request {key:%v, value:%v}.", key, value)
	ck.mu.Lock()
	defer func() {
		ck.mu.Unlock()
	}()
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		Id:       nrand(),
		LastOpId: ck.lastOpId,
	}
	reply := &PutAppendReply{}
	ok := ck.callAppendPutRpc(ck.currentLeader, time.Millisecond*200, args, reply)
	if ok && reply.Err == OK {
		DPrintf("{key:%v, value:%v} finish.\n", key, value)
		return
	}
	//
	// Firstly attempting the leader of local cached, if it isn't a leader, attempting all
	// server and cache the leader.
	//
	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := &PutAppendReply{}
			ok := ck.callAppendPutRpc(i, CommitTimeout, args, reply)
			if !ok {
				//fmt.Printf("call %d append and put failed\n", i)
				continue
			}

			if reply == nil {
				continue
			}

			if reply.Err == OK {
				ck.currentLeader = i
				ck.lastOpId = args.Id
				DPrintf("{key:%v, value:%v} finish.\n", key, value)
				return
			} else {
				if reply.Err != ErrWrongLeader {
					DPrintf("{key:%v, value:%v} %v", key, value, reply.Err)
				}
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (ck *Clerk) callAppendPutRpc(server int, timeout time.Duration, args *PutAppendArgs, reply *PutAppendReply) bool {
	t := time.After(timeout)
	res := make(chan bool)
	go func() {
		res <- ck.servers[server].Call("KVServer.PutAppend", args, reply)
	}()
	ok := false
	select {
	case <-t:
		ok = false
	case ok = <-res:
	}
	return ok
}

func (ck *Clerk) callGetRpc(server int, timeout time.Duration, args *GetArgs, reply *GetReply) bool {
	t := time.After(timeout)
	res := make(chan bool)
	go func() {
		res <- ck.servers[server].Call("KVServer.Get", args, reply)
	}()
	ok := false
	select {
	case <-t:
		ok = false
	case ok = <-res:
	}
	return ok
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
