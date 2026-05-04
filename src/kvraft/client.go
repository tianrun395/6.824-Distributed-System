package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	mu         sync.Mutex
	clientId   int64
	SeqNumber  int64
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
	ck.lastLeader = 0
	ck.mu = sync.Mutex{}
	ck.clientId = nrand()
	ck.SeqNumber = 0
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	ck.SeqNumber++
	args := GetArgs{Key: key, ClientId: ck.clientId, SeqNumber: ck.SeqNumber}
	lastLeader := ck.lastLeader
	ck.mu.Unlock()
	for {
		for i := lastLeader; i-lastLeader < len(ck.servers); i++ {
			reply := GetReply{}
			ok := ck.servers[i%len(ck.servers)].Call("KVServer.Get", &args, &reply)
			if ok {
				if reply.Err == OK {
					ck.mu.Lock()
					ck.lastLeader = i % len(ck.servers)
					ck.mu.Unlock()
					return reply.Value
				} else if reply.Err == ErrNoKey {
					ck.mu.Lock()
					ck.lastLeader = i % len(ck.servers)
					ck.mu.Unlock()
					return ""
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	ck.SeqNumber++
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, SeqNumber: ck.SeqNumber}
	lastLeader := ck.lastLeader
	ck.mu.Unlock()
	for {
		for i := lastLeader; i-lastLeader < len(ck.servers); i++ {
			reply := PutAppendReply{}
			ok := ck.servers[i%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)
			if ok {
				if reply.Err == OK {
					ck.mu.Lock()
					ck.lastLeader = i % len(ck.servers)
					ck.mu.Unlock()
					return
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
