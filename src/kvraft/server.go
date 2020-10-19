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

const Debug = 0

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
	mapMu        sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	noticeCh     chan bool
	dead         int32 // Set by Kill()
	kvMap        map[string]string
	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big
	successSet   *Set
	handlingSet  *Set // save the Op being handled
}

// create a snapshot of the kvMap and returns the encoded byte array.
func (kv *KVServer) createSnapshot() ([]byte, error) {
	bf := new(bytes.Buffer)
	e := labgob.NewEncoder(bf)
	lastIncludedIndex, lastIncludedTerm := kv.rf.GetIndexAndTerm()
	if e.Encode(kv.kvMap) != nil ||
		e.Encode(lastIncludedIndex) != nil ||
		e.Encode(lastIncludedTerm) != nil {
		panic("encode error: create snapshot failed")
	}
	return bf.Bytes(), nil
}

func (kv *KVServer) get0(key string) string {
	kv.mapMu.Lock()
	defer kv.mapMu.Unlock()
	v, ok := kv.kvMap[key]
	if !ok {
		return ""
	}
	return v
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//DPrintf("[%d] received Get Rpc request {%v}", kv.me, args.Key)
	if !kv.rf.IsConnectMajority() {
		reply.Err = ErrWrongLeader
		return
	}
	t, _ := kv.rf.GetState()
	if t != term {
		reply.Err = ErrInternal
		return
	}
	DPrintf("[%d] contain key=%s", kv.me, args.Key)
	reply.Value = kv.get0(args.Key)
	reply.Err = OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[%d] received PutAppend Rpc request {key:%v, value:%v}", kv.me, args.Key, args.Value)

	ok1 := kv.successSet.contain(args.Id)
	if ok1 {
		DPrintf("{key:%v, value:%v} already successful", args.Key, args.Value)
		reply.Err = OK
		return
	}
	kv.handlingSet.put(args.Id)
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
	case <-kv.noticeCh:
	case <-t:
		ok = false
	}
	kv.handlingSet.delete(args.Id)
	if _, isLeader := kv.rf.GetState(); !isLeader || !ok {
		DPrintf("{key:%v, value:%v} internal error", args.Key, args.Value)
		reply.Err = ErrInternal
	} else {
		kv.successSet.delete(args.LastOpId)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to Set rf.dead (without needing a lock),
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

func (kv *KVServer) applyCommitLogOfInRealTime() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.ApplySnapshot {
			kv.kvMap = msg.Kvmap
			continue
		}

		if !msg.CommandValid {
			continue
		}
		command := msg.Command
		op := command.(Op)
		if ok := kv.successSet.contain(op.Id); ok {
			continue
		}
		_, isLeader := kv.rf.GetState()
		kv.mapMu.Lock()
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
		kv.mapMu.Unlock()

		if kv.maxraftstate > 0 &&
			kv.persister.RaftStateSize() > (int)(float32(kv.maxraftstate)*7.2) {
			if msg.NeedLock {
				kv.rf.Lock()
			}

			//commitIndex, _ := kv.rf.GetIndexAndTerm()

			kv.rf.DiscardLogWithLock(msg.CommandIndex)
			snapshot, _ := kv.createSnapshot()
			state, _ := kv.rf.GetStateData()

			kv.persister.SaveStateAndSnapshot(state, snapshot)
			if msg.NeedLock {
				kv.rf.Unlock()
			}

		}

		kv.successSet.put(op.Id)
		ok := kv.handlingSet.contain(op.Id)
		if ok {
			kv.noticeCh <- true
		}
	}
}

//
// servers[] contains the ports of the Set of
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
	labgob.Register(map[string]string{})
	//labgob.Register()
	_, _ = DPrintf("start kv server")
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.kvMap = map[string]string{}
	kv.applyCh = make(chan raft.ApplyMsg)

	kv.noticeCh = make(chan bool)
	kv.successSet = &Set{keys: []int64{}}
	kv.handlingSet = &Set{keys: []int64{}}
	go kv.applyCommitLogOfInRealTime()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	return kv
}
