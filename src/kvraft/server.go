package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

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
	Key       string
	Value     string
	Op        string // "Put" or "Append" or "Get"
	ClientId  int64
	SeqNumber int64
}

type LastRequestInfo struct {
	SeqNumber int64
	Op        Op
	Reply     interface{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database            map[string]string
	waitCh              map[int]chan Op
	lastRequestByClient map[int64]LastRequestInfo
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	operation := Op{Key: args.Key, Op: "Get", ClientId: args.ClientId, SeqNumber: args.SeqNumber}
	kv.mu.Lock()
	lastRequestInfo, exists := kv.lastRequestByClient[args.ClientId]
	kv.mu.Unlock()
	if !exists || lastRequestInfo.SeqNumber < args.SeqNumber {
		index, _, isLeader := kv.rf.Start(operation)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		op, ok := kv.waitForOp(index)
		if !ok || op.Key != args.Key || op.Op != "Get" || op.ClientId != args.ClientId || op.SeqNumber != args.SeqNumber {
			reply.Err = ErrWrongLeader
			return
		}
		// when hit here, means the raftWaitorAndApplier has applied the command to state machine and populated the lastRequestByClient
		// with the reply, so we can directly read the reply from lastRequestByClient and return to client.
		kv.mu.Lock()
		reply.Err = kv.lastRequestByClient[args.ClientId].Reply.(GetReply).Err
		reply.Value = kv.lastRequestByClient[args.ClientId].Reply.(GetReply).Value
		kv.mu.Unlock()
	} else if exists && lastRequestInfo.SeqNumber == args.SeqNumber {
		kv.mu.Lock()
		reply.Err = kv.lastRequestByClient[args.ClientId].Reply.(GetReply).Err
		reply.Value = kv.lastRequestByClient[args.ClientId].Reply.(GetReply).Value
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	operation := Op{Key: args.Key, Value: args.Value, Op: args.Op, ClientId: args.ClientId, SeqNumber: args.SeqNumber}
	kv.mu.Lock()
	lastRequestInfo, exists := kv.lastRequestByClient[args.ClientId]
	kv.mu.Unlock()
	if !exists || lastRequestInfo.SeqNumber < args.SeqNumber {
		index, _, isLeader := kv.rf.Start(operation)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		op, ok := kv.waitForOp(index)
		if !ok || op.Key != args.Key || op.Value != args.Value || op.Op != args.Op || op.ClientId != args.ClientId || op.SeqNumber != args.SeqNumber {
			reply.Err = ErrWrongLeader
			return
		}
		// when hit here, means the raftWaitorAndApplier has applied the command to state machine and populated the lastRequestByClient
		// with the reply, so we can directly read the reply from lastRequestByClient and return to client.
		kv.mu.Lock()
		reply.Err = kv.lastRequestByClient[args.ClientId].Reply.(PutAppendReply).Err
		kv.mu.Unlock()
	} else if exists && lastRequestInfo.SeqNumber == args.SeqNumber {
		kv.mu.Lock()
		reply.Err = kv.lastRequestByClient[args.ClientId].Reply.(PutAppendReply).Err
		kv.mu.Unlock()
	}
}

func (kv *KVServer) waitForOp(index int) (Op, bool) {
	kv.mu.Lock()
	ch, exists := kv.waitCh[index]
	if !exists {
		kv.waitCh[index] = make(chan Op, 1)
		ch = kv.waitCh[index]
	}
	kv.mu.Unlock()
	select {
	case op := <-ch:
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
		return op, true
	case <-time.After(1000 * time.Millisecond):
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
		return Op{}, false
	}
}

// Receive reply from raft channel and add it to the server wait channel with index as map key.
// If the channel does not exist, create a new one and add the reply to it.
func (kv *KVServer) raftWaitorAndApplier() {
	for {
		cmd := <-kv.applyCh
		if cmd.CommandValid == false {
			continue
		}
		commandIndex := cmd.CommandIndex
		op := cmd.Command.(Op)
		// heard from raft that command is committed, apply to state machine and then send to wait channel.
		clientId := op.ClientId
		seqNumber := op.SeqNumber
		kv.mu.Lock()
		lastRequestInfo, exists := kv.lastRequestByClient[clientId]
		if !exists || lastRequestInfo.SeqNumber < seqNumber {
			switch op.Op {
			case "Get":
				value, exists := kv.database[op.Key]
				err := Err("")
				if !exists {
					err = ErrNoKey
				} else {
					err = OK
				}
				kv.lastRequestByClient[op.ClientId] =
					LastRequestInfo{SeqNumber: op.SeqNumber, Op: op, Reply: GetReply{Err: err, Value: value}}
			case "Put":
				kv.database[op.Key] = op.Value
				kv.lastRequestByClient[op.ClientId] = LastRequestInfo{SeqNumber: op.SeqNumber, Op: op, Reply: PutAppendReply{Err: OK}}
			case "Append":
				kv.database[op.Key] += op.Value
				kv.lastRequestByClient[op.ClientId] = LastRequestInfo{SeqNumber: op.SeqNumber, Op: op, Reply: PutAppendReply{Err: OK}}
			}
		}
		if ch, exists := kv.waitCh[commandIndex]; exists {
			ch <- op
		} else {
			kv.waitCh[commandIndex] = make(chan Op, 1)
			kv.waitCh[commandIndex] <- op
		}
		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.mu = sync.Mutex{}
	kv.database = make(map[string]string)
	kv.dead = 0
	kv.waitCh = make(map[int]chan Op)
	kv.lastRequestByClient = make(map[int64]LastRequestInfo)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.raftWaitorAndApplier()

	return kv
}
