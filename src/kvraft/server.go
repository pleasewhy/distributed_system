package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1

const (
	APPEND = iota
	PUT
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation int
	Key       string
	Value     string
	Id        int64
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	noticeCh     chan bool
	dead         int32 // set by Kill()
	kvMap        map[string]string
	maxraftstate int // snapshot if log grows this big
	handleOpMap  map[int64]int64
	successOpMap map[int64]int64
	// Your definitions here.
}

// create a snapshot of the kvMap and returns the encoded byte array.
func (kv *KVServer) createSnapshot() ([]byte, error) {
	bf := new(bytes.Buffer)
	e := labgob.NewEncoder(bf)
	err := e.Encode(kv.kvMap)
	if err != nil {
		return nil, err
	}
	return bf.Bytes(), nil
}

func (kv *KVServer) get0(key string) string {
	v, ok := kv.kvMap[key]
	if !ok {
		return ""
	}
	return v
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[%d] ", kv.me)
	//DPrintf("[%d] received Get Rpc request {%v}", kv.me, args.Key)
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	reply.Value = kv.get0(args.Key)
	reply.Err = OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[%d] received PutAppend Rpc request {key:%v,key:%v}", kv.me, args.Key, args.Value)
	_, ok0 := kv.handleOpMap[args.Id]
	if ok0 {
		DPrintf("{key:%v, value:%v} handling", args.Key, args.Value)
		reply.Err = ErrHandling
		return
	}

	_, ok1 := kv.successOpMap[args.Id]
	if ok1 {
		DPrintf("{key:%v, value:%v} already successful", args.Key, args.Value)
		reply.Err = OK
		return
	}

	kv.handleOpMap[args.Id] = time.Now().UnixNano()
	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		Id:        args.Id,
	}
	kv.rf.Start(op)

	t := time.After(CommitTimeout)
	var ok = true
	reply.Err = OK

	select {
	case <-t:
		ok = false
	case <-kv.noticeCh:
	}
	delete(kv.handleOpMap, args.Id)
	if !ok {
		DPrintf("{key:%v, value:%v} internal error", args.Key, args.Value)
		reply.Err = ErrInternal
	} else {

	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
//
//
func (kv *KVServer) applyCommitLogOfInRealTime() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if !msg.CommandValid {
			continue
		}
		command := msg.Command
		op := command.(Op)
		_, isLeader := kv.rf.GetState()
		if op.Operation == APPEND {
			if isLeader {
				DPrintf("[%d] Append operation {key:%v, value:%v}", kv.me, op.Key, op.Value)
			}
			kv.kvMap[op.Key] = kv.kvMap[op.Key] + op.Value
		} else {
			if isLeader {
				DPrintf("[%d] Put operation {key:%v, value:%v}", kv.me, op.Key, op.Value)
			}
			kv.kvMap[op.Key] = op.Value
		}
		if !isLeader {
			continue
		}
		kv.successOpMap[op.Id] = time.Now().UnixNano()
		kv.noticeCh <- true
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.kvMap = map[string]string{}
	kv.applyCh = make(chan raft.ApplyMsg)
	// You may need initialization code here.
	kv.noticeCh = make(chan bool)
	kv.handleOpMap = map[int64]int64{}
	kv.successOpMap = map[int64]int64{}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	go kv.applyCommitLogOfInRealTime()
	return kv
}
