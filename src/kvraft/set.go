package kvraft

import (
	"sync"
)

const NULL = 0

type Set struct {
	// TODO code it some other time
	// need persist, consider a case that a PutAppend
	// operation is success, but the reply is loss and
	// the server is restart, the client will resend this
	// operation, but the server don't know this operation
	// is already apply.
	keys []int64
	mu   sync.Mutex
	data []byte
}

func (s *Set) contain(key int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range s.keys {
		if key == k {
			return true
		}
	}
	return false
}

func (s *Set) put(key int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < len(s.keys); i++ {
		if s.keys[i] == NULL {
			s.keys[i] = key
			return
		}
	}
	s.keys = append(s.keys, key)
}

func (s *Set) delete(key int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < len(s.keys); i++ {
		if s.keys[i] == key {
			s.keys[i] = NULL
			return
		}
	}
}
