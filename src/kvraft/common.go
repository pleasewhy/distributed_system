package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrInternal    = "InternalError"
	ErrHandling    = "Handling"
	CommitTimeout  = time.Millisecond*500
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       int // "Put" or "Append"
	Id       int64
	LastOpId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err      Err
	LeaderId int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
