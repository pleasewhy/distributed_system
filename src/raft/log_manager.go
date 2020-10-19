package raft

import (
	"fmt"
	"sync"
)

type Entry struct {
	Command interface{}
	Term    int
}

type LogManager struct {
	mu     sync.Mutex
	Logs   []*Entry
	Offset int
}

func (entry *Entry) String() string {
	return fmt.Sprintf("{command: %v, Term: %d}", entry.Command, entry.Term)
}

//
// get the value of real index.
//
func (lm *LogManager) get(i int) *Entry {
	//lm.mu.Lock()
	//defer lm.mu.Unlock()
	//log.Printf("i:%d, offset:%d", i, lm.Offset)
	return lm.Logs[i-lm.Offset]
}

//
// append a log
//
func (lm *LogManager) appendLog(entry *Entry) {
	//lm.mu.Lock()
	//defer lm.mu.Unlock()
	lm.Logs = append(lm.Logs, entry)
}

//
// append Logs
//
func (lm *LogManager) appendLogs(entries []*Entry) {
	for i := 0; i < len(entries); i++ {
		lm.appendLog(entries[i])
	}
}

//
// discard Logs with inedx less than and equal i
//
func (lm *LogManager) DiscardLogs(i int) {
	//lm.mu.Lock()
	//defer lm.mu.Unlock()
	if lm.Logs == nil || len(lm.Logs) == 0 {
		return
	}
	lm.Logs = lm.Logs[i-lm.Offset:]
	lm.Offset = i
}

func (lm *LogManager) length() int {
	return len(lm.Logs) + lm.Offset
}

func (lm *LogManager) Slice(start int, end int) []*Entry {
	return lm.Logs[start-lm.Offset : end-lm.Offset]
}
